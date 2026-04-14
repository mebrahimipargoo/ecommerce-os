-- REMOVAL_ORDER Phase 3: upsert must arbitrate on the real business line (`uq_amazon_removals_business_line`),
-- not on (organization_id, upload_id, source_staging_id). Staging ids are lineage only — unique per upload
-- but not a cross-file identity. Drop the staging unique; keep a non-unique index for enrichment joins.

BEGIN;

DROP INDEX IF EXISTS public.uq_amazon_removals_org_upload_source_staging;

CREATE INDEX IF NOT EXISTS idx_amazon_removals_org_upload_source_staging
  ON public.amazon_removals (organization_id, upload_id, source_staging_id);

COMMENT ON INDEX public.idx_amazon_removals_org_upload_source_staging IS
  'Lineage lookup (non-unique). Domain uniqueness is uq_amazon_removals_business_line.';

NOTIFY pgrst, 'reload schema';

COMMIT;
