-- One amazon_removals / expected_packages row per amazon_staging line (exact CSV line).
-- Replaces composite "logical line" indexes so only a true re-upload of the same staging
-- row collides — not two different lines that shared sku/qty/date.

BEGIN;

ALTER TABLE public.amazon_removals
  ADD COLUMN IF NOT EXISTS source_staging_id uuid;

ALTER TABLE public.expected_packages
  ADD COLUMN IF NOT EXISTS source_staging_id uuid;

DROP INDEX IF EXISTS public.uq_amazon_removals_logical_line;
DROP INDEX IF EXISTS public.uq_expected_packages_logical_line;

-- Non-partial unique indexes so PostgREST upsert ON CONFLICT (org, upload, source_staging_id)
-- can infer the arbiter. Multiple rows with source_staging_id IS NULL remain allowed (NULLs distinct).
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removals_org_upload_source_staging
  ON public.amazon_removals (organization_id, upload_id, source_staging_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_expected_packages_org_upload_source_staging
  ON public.expected_packages (organization_id, upload_id, source_staging_id);

COMMENT ON COLUMN public.amazon_removals.source_staging_id IS
  'FK to amazon_staging.id — one removal row per staged CSV line (Universal Importer).';

COMMENT ON COLUMN public.expected_packages.source_staging_id IS
  'Copied from amazon_removals for idempotent worklist upserts per staged line.';

NOTIFY pgrst, 'reload schema';

COMMIT;
