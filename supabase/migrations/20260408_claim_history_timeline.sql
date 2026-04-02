-- Timeline fields for claim_history_logs (claim_id, action, details JSONB, company_id text, actor_label).
-- Keeps legacy columns (submission_id, organization_id, message_content, actor enum) for existing rows and server inserts.

ALTER TABLE public.claim_history_logs
  ADD COLUMN IF NOT EXISTS claim_id UUID REFERENCES public.claim_submissions(id) ON DELETE CASCADE,
  ADD COLUMN IF NOT EXISTS action TEXT,
  ADD COLUMN IF NOT EXISTS details JSONB DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS company_id TEXT,
  ADD COLUMN IF NOT EXISTS actor_label TEXT;

UPDATE public.claim_history_logs
SET
  claim_id = COALESCE(claim_id, submission_id),
  action = COALESCE(action, message_content),
  details = COALESCE(
    details,
    CASE
      WHEN attachments IS NOT NULL AND attachments::text != 'null' THEN attachments
      ELSE '{}'::jsonb
    END
  ),
  company_id = COALESCE(company_id, organization_id::text),
  actor_label = COALESCE(actor_label, actor::text)
WHERE submission_id IS NOT NULL
  AND claim_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_claim_history_logs_claim_company
  ON public.claim_history_logs (claim_id, company_id);

CREATE INDEX IF NOT EXISTS idx_claim_history_logs_company_created
  ON public.claim_history_logs (company_id, created_at DESC);
