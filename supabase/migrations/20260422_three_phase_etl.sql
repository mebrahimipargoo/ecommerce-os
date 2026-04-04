-- 3-Phase ETL pipeline status additions + product_name column rename.
--
-- Phase 1: uploading  -> mapped (AI mapping complete) | needs_mapping (user must fix)
-- Phase 2: mapped     -> staged  (CSV chunked into amazon_ledger_staging)
-- Phase 3: staged     -> synced  (staging rows moved to domain tables + staging cleaned)
--

-- ── raw_report_uploads: extend status CHECK ──────────────────────────────────
ALTER TABLE public.raw_report_uploads DROP CONSTRAINT IF EXISTS raw_report_uploads_status_check;

ALTER TABLE public.raw_report_uploads
  ADD CONSTRAINT raw_report_uploads_status_check CHECK (
    status = ANY (ARRAY[
      'pending'::text,
      'uploading'::text,
      'processing'::text,
      'synced'::text,
      'complete'::text,
      'failed'::text,
      'cancelled'::text,
      'needs_mapping'::text,
      'ready'::text,
      'uploaded'::text,
      'mapped'::text,
      'staged'::text
    ])
  );

COMMENT ON COLUMN public.raw_report_uploads.status IS
  'Upload lifecycle: uploading | mapped | needs_mapping | staged | synced | failed | cancelled.
   mapped        = upload complete, AI column_mapping verified, awaiting Phase 2 (Process).
   staged        = CSV rows inserted into amazon_ledger_staging, awaiting Phase 3 (Sync).
   needs_mapping = AI could not fully resolve headers; user must use Map Columns modal.
   synced        = domain tables updated, staging cleaned.';

-- ── amazon_ledger_staging: source_upload_id index (already added in 20260404 for some
--    environments; IF NOT EXISTS guard makes this idempotent) ─────────────────
ALTER TABLE public.amazon_ledger_staging
  ADD COLUMN IF NOT EXISTS source_upload_id uuid
  REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_amazon_ledger_staging_source_upload
  ON public.amazon_ledger_staging (source_upload_id)
  WHERE source_upload_id IS NOT NULL;

-- ── products: rename name -> product_name ─────────────────────────────────────
-- Safe rename: adds the new column, copies data, drops old column.
-- Uses DO block so it is idempotent (skips if product_name already exists).
DO $$
BEGIN
  -- Add product_name if absent
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'products'
      AND column_name  = 'product_name'
  ) THEN
    ALTER TABLE public.products ADD COLUMN product_name text;
    -- Copy existing data
    UPDATE public.products SET product_name = name WHERE product_name IS NULL;
    ALTER TABLE public.products ALTER COLUMN product_name SET NOT NULL;
  END IF;

  -- Drop old 'name' column if still present
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'products'
      AND column_name  = 'name'
  ) THEN
    ALTER TABLE public.products DROP COLUMN name;
  END IF;
END $$;

COMMENT ON COLUMN public.products.product_name IS 'Product display name (imported from Amazon Inventory Ledger or SP-API).';

NOTIFY pgrst, 'reload schema';
