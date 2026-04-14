-- Unified Amazon import: every staged row carries report identity + line fingerprint.
-- Prevents silent collapse in staging and supports idempotent upserts on (upload_id, source_line_hash).

ALTER TABLE public.amazon_staging
  ADD COLUMN IF NOT EXISTS report_type text,
  ADD COLUMN IF NOT EXISTS row_number integer,
  ADD COLUMN IF NOT EXISTS source_line_hash text;

COMMENT ON COLUMN public.amazon_staging.report_type IS
  'Mirrors raw_report_uploads.report_type for this row (canonical import kind).';
COMMENT ON COLUMN public.amazon_staging.row_number IS
  '1-based data row index within this upload after header skip (stable within file).';
COMMENT ON COLUMN public.amazon_staging.source_line_hash IS
  'FNV fingerprint of full mapped row + org; aligns with landing-table source_line_hash.';

-- Backfill legacy rows so NOT NULL is safe
UPDATE public.amazon_staging
SET source_line_hash = id::text
WHERE source_line_hash IS NULL;

UPDATE public.amazon_staging
SET row_number = 0
WHERE row_number IS NULL;

ALTER TABLE public.amazon_staging
  ALTER COLUMN source_line_hash SET NOT NULL,
  ALTER COLUMN row_number SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_staging_upload_line_hash
  ON public.amazon_staging (upload_id, source_line_hash);

COMMENT ON INDEX public.uq_amazon_staging_upload_line_hash IS
  'One staging row per distinct logical line per upload; identical lines share a hash.';

-- Operator-facing lifecycle + metrics on the upload row (mirrored in FPS for Realtime)
ALTER TABLE public.raw_report_uploads
  ADD COLUMN IF NOT EXISTS import_pipeline_started_at timestamptz,
  ADD COLUMN IF NOT EXISTS import_pipeline_completed_at timestamptz,
  ADD COLUMN IF NOT EXISTS import_pipeline_failed_at timestamptz;

COMMENT ON COLUMN public.raw_report_uploads.import_pipeline_started_at IS
  'First time Phase 2 (staging) or listing process began for this upload.';
COMMENT ON COLUMN public.raw_report_uploads.import_pipeline_completed_at IS
  'When Phase 3 sync (or listing-only process) completed successfully.';
COMMENT ON COLUMN public.raw_report_uploads.import_pipeline_failed_at IS
  'Last pipeline failure timestamp.';

ALTER TABLE public.file_processing_status
  ADD COLUMN IF NOT EXISTS import_metrics jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN public.file_processing_status.import_metrics IS
  'Structured counters: file rows, staged, sync new/updated/unchanged, duplicates, invalid, etc.';

NOTIFY pgrst, 'reload schema';
