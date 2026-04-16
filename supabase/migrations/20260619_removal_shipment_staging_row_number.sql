-- Idempotent REMOVAL_SHIPMENT archive per physical CSV line within an upload:
-- Phase 2 recreates `amazon_staging` rows with new UUIDs but stable `row_number`.
-- Upsert on (organization_id, upload_id, staging_row_number) prevents duplicate
-- archive rows when the same file is re-processed + synced.

BEGIN;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS staging_row_number integer;

COMMENT ON COLUMN public.amazon_removal_shipments.staging_row_number IS
  'amazon_staging.row_number for this upload — stable when staging is recreated; used for idempotent Phase 3 upsert.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removal_shipments_org_upload_rownum
  ON public.amazon_removal_shipments (organization_id, upload_id, staging_row_number)
  WHERE staging_row_number IS NOT NULL;

NOTIFY pgrst, 'reload schema';

COMMIT;
