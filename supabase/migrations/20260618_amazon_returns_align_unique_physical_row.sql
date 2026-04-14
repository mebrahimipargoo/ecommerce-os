-- FBA_RETURNS: sync + PostgREST upsert use (organization_id, source_file_sha256, source_physical_row_number)
-- per 20260616_physical_row_import_identity. Legacy (organization_id, lpn) uniqueness must not remain active
-- or inserts/upserts collide on LPN while ON CONFLICT targets the file-row key.
--
-- Drops: table constraint `amazon_returns_org_lpn_unique` (if present) and index `uq_amazon_returns_org_lpn`.
-- Ensures: `uq_amazon_returns_org_file_row` exists (idempotent).

BEGIN;

ALTER TABLE public.amazon_returns
  DROP CONSTRAINT IF EXISTS amazon_returns_org_lpn_unique;

DROP INDEX IF EXISTS public.uq_amazon_returns_org_lpn;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_returns_org_file_row
  ON public.amazon_returns (organization_id, source_file_sha256, source_physical_row_number);

COMMENT ON INDEX public.uq_amazon_returns_org_file_row IS
  'Import landing identity: one row per physical CSV line (file SHA + row index). Replaces legacy org+lpn uniqueness.';

NOTIFY pgrst, 'reload schema';

COMMIT;
