-- =============================================================================
-- amazon_settlements — audit / scope columns for settlement recovery & tracing
-- =============================================================================
-- source_file_name: denormalized from raw_report_uploads.file_name at sync.
-- store_id: optional link to imports target store (metadata.import_store_id).
-- updated_at: last upsert touch (app sets on sync; trigger keeps DB-side fresh).
-- =============================================================================

BEGIN;

ALTER TABLE public.amazon_settlements
  ADD COLUMN IF NOT EXISTS source_file_name text,
  ADD COLUMN IF NOT EXISTS store_id uuid REFERENCES public.stores (id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

COMMENT ON COLUMN public.amazon_settlements.source_file_name IS
  'Original upload file name (denormalized at sync from raw_report_uploads.file_name).';
COMMENT ON COLUMN public.amazon_settlements.store_id IS
  'Imports target store when present (metadata.import_store_id on the upload).';
COMMENT ON COLUMN public.amazon_settlements.updated_at IS
  'Row last touched by sync upsert; application sets explicitly on each write.';

-- Idempotent: may already exist from earlier migrations; required if this file runs alone.
CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_amazon_settlements_set_updated_at ON public.amazon_settlements;
CREATE TRIGGER trg_amazon_settlements_set_updated_at
  BEFORE UPDATE ON public.amazon_settlements
  FOR EACH ROW
  EXECUTE FUNCTION public.set_updated_at();

COMMIT;

NOTIFY pgrst, 'reload schema';
