-- V16.4.18 — Workspace lifecycle: investigating (Claim Engine investigation flow)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t ON t.oid = e.enumtypid
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'claim_submission_status'
      AND e.enumlabel = 'investigating'
  ) THEN
    ALTER TYPE public.claim_submission_status ADD VALUE 'investigating';
  END IF;
END $$;

COMMENT ON TYPE public.claim_submission_status IS
  'Includes investigating for items opened from Claim Engine investigation UI.';
