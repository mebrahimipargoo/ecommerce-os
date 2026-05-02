-- 20260641_product_identity_staging_rows.sql
--
-- Purpose: dedicated staging table for the Product Identity CSV pipeline.
--
-- Before this migration, the process route parsed the CSV *and* immediately
-- wrote final tables (products, catalog_products, product_identifier_map).
-- That combined step had two problems:
--   1. Progress was stuck at ~5% for the entire run; the UI showed nothing
--      advancing until the very end.
--   2. The "Sync" button had nothing to do because all writes already ran in
--      "Process", making the two-button UI misleading.
--
-- This table creates an intermediate staging layer identical in purpose to
-- `amazon_staging` (which other report types use), but tailored to the
-- Product Identity column schema. After this migration:
--   * Phase 2 (Process) — parse CSV in chunks → write here, track progress.
--   * Phase 3 (Sync)    — read from here → upsert final tables, track progress.
--   * Phase 4 (Generic) — reserved / not_applicable.
--
-- The upload_id + source_physical_row_number unique index makes Phase 2
-- idempotent: if the worker crashes and is retried it simply overwrites
-- already-staged rows via ON CONFLICT DO UPDATE.

BEGIN;

CREATE TABLE IF NOT EXISTS public.product_identity_staging_rows (
  id                          uuid         PRIMARY KEY DEFAULT gen_random_uuid(),
  upload_id                   uuid         NOT NULL REFERENCES public.raw_report_uploads(id) ON DELETE CASCADE,
  organization_id             uuid         NOT NULL,
  store_id                    uuid         NOT NULL,
  source_file_sha256          text,
  source_physical_row_number  integer      NOT NULL,
  source_line_hash            text,
  -- normalised identifier fields (validated by the process step)
  seller_sku                  text,
  asin                        text,
  fnsku                       text,
  upc_code                    text,
  vendor_name                 text,
  mfg_part_number             text,
  product_name                text,
  -- full original CSV row (string values) for traceability
  raw_data                    jsonb        NOT NULL DEFAULT '{}'::jsonb,
  -- parsed/validated normalised fields plus skip flags
  normalized_data             jsonb        NOT NULL DEFAULT '{}'::jsonb,
  -- validation errors written when a field was present but invalid (e.g. bad ASIN format)
  validation_errors           jsonb        NOT NULL DEFAULT '{}'::jsonb,
  created_at                  timestamptz  NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.product_identity_staging_rows IS
  'Intermediate staging table for the Product Identity CSV import pipeline '
  '(Phase 2 → Phase 3). Rows are inserted by /api/settings/imports/process '
  'and consumed by /api/settings/imports/sync.';

-- Idempotency: re-staging the same physical CSV row is a safe ON CONFLICT
-- DO UPDATE because source_physical_row_number is stable across retries.
CREATE UNIQUE INDEX IF NOT EXISTS uq_pi_staging_upload_row
  ON public.product_identity_staging_rows (upload_id, source_physical_row_number);

-- Fast lookup by upload_id (consumed by Phase 3 and the Reset action).
CREATE INDEX IF NOT EXISTS idx_pi_staging_upload_id
  ON public.product_identity_staging_rows (upload_id);

-- Support org-scoped queries and duplicate-sku detection in Phase 3.
CREATE INDEX IF NOT EXISTS idx_pi_staging_org_store_sku
  ON public.product_identity_staging_rows (organization_id, store_id, seller_sku)
  WHERE seller_sku IS NOT NULL;

-- Support identifier-map lookups in Phase 3.
CREATE INDEX IF NOT EXISTS idx_pi_staging_upload_sku
  ON public.product_identity_staging_rows (upload_id, seller_sku)
  WHERE seller_sku IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pi_staging_upload_asin
  ON public.product_identity_staging_rows (upload_id, asin)
  WHERE asin IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pi_staging_upload_fnsku
  ON public.product_identity_staging_rows (upload_id, fnsku)
  WHERE fnsku IS NOT NULL;

ALTER TABLE public.product_identity_staging_rows ENABLE ROW LEVEL SECURITY;

-- Service role (server-side API routes) has full access.
-- Idempotent: second run after a failed COMMIT or manual apply may leave the policy behind.
DROP POLICY IF EXISTS pi_staging_service_role ON public.product_identity_staging_rows;

CREATE POLICY pi_staging_service_role
  ON public.product_identity_staging_rows
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- Add Phase 2 timestamp column to track when staging completed.
-- Safe: ADD COLUMN IF NOT EXISTS is idempotent.
ALTER TABLE public.raw_report_uploads
  ADD COLUMN IF NOT EXISTS import_pipeline_staged_at timestamptz;

COMMENT ON COLUMN public.raw_report_uploads.import_pipeline_staged_at IS
  'Set when Phase 2 (CSV → staging table) completes successfully. NULL until that point.';

NOTIFY pgrst, 'reload schema';

COMMIT;
