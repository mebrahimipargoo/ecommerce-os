-- V16.4.11 — Automated claim report pipeline: track generated PDFs and submission lifecycle.

DO $$ BEGIN
  CREATE TYPE public.claim_submission_status AS ENUM (
    'draft',
    'ready_to_send',
    'submitted',
    'accepted',
    'rejected'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS public.claim_submissions (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id   UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'::uuid,
  return_id         UUID NOT NULL REFERENCES public.returns(id) ON DELETE CASCADE,
  store_id          UUID REFERENCES public.stores(id) ON DELETE SET NULL,
  report_url        TEXT,
  status            public.claim_submission_status NOT NULL DEFAULT 'draft',
  submission_id     TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.claim_submissions IS
  'Lifecycle of auto-generated claim PDFs: storage path, marketplace case ID, workflow status.';

COMMENT ON COLUMN public.claim_submissions.report_url IS
  'Object path within the claim-reports bucket (not necessarily a public URL).';

COMMENT ON COLUMN public.claim_submissions.submission_id IS
  'Case or claim ID returned by Amazon, Walmart, etc., after manual or agent filing.';

CREATE UNIQUE INDEX IF NOT EXISTS claim_submissions_return_id_key
  ON public.claim_submissions (return_id);

CREATE INDEX IF NOT EXISTS idx_claim_submissions_org_status
  ON public.claim_submissions (organization_id, status);

CREATE INDEX IF NOT EXISTS idx_claim_submissions_org_created
  ON public.claim_submissions (organization_id, created_at DESC);

DROP TRIGGER IF EXISTS trg_claim_submissions_updated_at ON public.claim_submissions;
CREATE TRIGGER trg_claim_submissions_updated_at
  BEFORE UPDATE ON public.claim_submissions
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

ALTER TABLE public.claim_submissions ENABLE ROW LEVEL SECURITY;

CREATE POLICY "claim_submissions_org_isolation"
  ON public.claim_submissions FOR ALL
  USING (organization_id = '00000000-0000-0000-0000-000000000001'::uuid);

-- Private bucket for PDFs; apps use signed URLs (service role) for upload/download.
INSERT INTO storage.buckets (id, name, public)
VALUES ('claim-reports', 'claim-reports', false)
ON CONFLICT (id) DO NOTHING;
