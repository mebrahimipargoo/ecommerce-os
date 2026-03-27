-- V16.4.12 — Agent memory (claim_history_logs), CRM fields on claim_submissions.

-- ─── Extend submission lifecycle with evidence-requested state ───────────────
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t ON t.oid = e.enumtypid
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'claim_submission_status'
      AND e.enumlabel = 'evidence_requested'
  ) THEN
    ALTER TYPE public.claim_submission_status ADD VALUE 'evidence_requested';
  END IF;
END $$;

DO $$ BEGIN
  CREATE TYPE public.claim_history_actor AS ENUM (
    'agent',
    'marketplace_bot',
    'human_admin'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

ALTER TABLE public.claim_submissions
  ADD COLUMN IF NOT EXISTS claim_amount       NUMERIC(14, 2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_checked_at    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS success_probability NUMERIC(5, 2);

COMMENT ON COLUMN public.claim_submissions.claim_amount IS
  'Monetary value tracked for this submission (synced from claim or manual).';
COMMENT ON COLUMN public.claim_submissions.last_checked_at IS
  'Last time the external agent polled or updated marketplace status.';
COMMENT ON COLUMN public.claim_submissions.success_probability IS
  'Heuristic 0–100 score from the last marketplace/agent message.';

CREATE TABLE IF NOT EXISTS public.claim_history_logs (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'::uuid,
  submission_id   UUID NOT NULL REFERENCES public.claim_submissions(id) ON DELETE CASCADE,
  actor           public.claim_history_actor NOT NULL,
  message_content TEXT NOT NULL,
  attachments     JSONB NOT NULL DEFAULT '{}',
  status_at_time  TEXT NOT NULL,
  message_kind    TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT claim_history_logs_message_kind_check CHECK (
    message_kind IS NULL OR message_kind IN (
      'marketplace_response',
      'agent_reply',
      'note',
      'system'
    )
  )
);

COMMENT ON TABLE public.claim_history_logs IS
  'Append-only conversation log between agent, marketplace automation, and admins.';
COMMENT ON COLUMN public.claim_history_logs.status_at_time IS
  'claim_submissions.status value after this interaction (stored as text).';
COMMENT ON COLUMN public.claim_history_logs.message_kind IS
  'Optional UI hint: marketplace_response vs agent_reply vs note.';

CREATE INDEX IF NOT EXISTS idx_claim_history_logs_submission
  ON public.claim_history_logs (submission_id, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_claim_history_logs_org_time
  ON public.claim_history_logs (organization_id, created_at DESC);

ALTER TABLE public.claim_history_logs ENABLE ROW LEVEL SECURITY;

CREATE POLICY "claim_history_logs_org_isolation"
  ON public.claim_history_logs FOR ALL
  USING (organization_id = '00000000-0000-0000-0000-000000000001'::uuid);
