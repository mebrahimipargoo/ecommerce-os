-- Rename source_upload_id -> upload_id on amazon_ledger_staging.
-- The application code (stage route, sync route, deleteRawReportUpload) all
-- now use upload_id.  This migration is idempotent: if upload_id already
-- exists and source_upload_id does not, it's a no-op.

DO $$
BEGIN
  -- Add upload_id if it is missing
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'amazon_ledger_staging'
      AND column_name  = 'upload_id'
  ) THEN
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name   = 'amazon_ledger_staging'
        AND column_name  = 'source_upload_id'
    ) THEN
      ALTER TABLE public.amazon_ledger_staging
        RENAME COLUMN source_upload_id TO upload_id;
    ELSE
      ALTER TABLE public.amazon_ledger_staging
        ADD COLUMN upload_id uuid
        REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL;
    END IF;
  END IF;

  -- Drop old source_upload_id if it still exists alongside upload_id
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'amazon_ledger_staging'
      AND column_name  = 'source_upload_id'
  ) THEN
    ALTER TABLE public.amazon_ledger_staging DROP COLUMN source_upload_id;
  END IF;
END $$;

-- Recreate index under the new name (old index is dropped automatically on rename)
CREATE INDEX IF NOT EXISTS idx_amazon_ledger_staging_upload_id
  ON public.amazon_ledger_staging (upload_id)
  WHERE upload_id IS NOT NULL;

COMMENT ON COLUMN public.amazon_ledger_staging.upload_id IS
  'FK to raw_report_uploads.id — used for targeted cleanup and progress tracking.';

NOTIFY pgrst, 'reload schema';
