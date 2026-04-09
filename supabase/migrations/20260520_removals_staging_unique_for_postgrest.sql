-- PostgREST/Supabase upsert sends ON CONFLICT (organization_id, upload_id, source_staging_id)
-- without a WHERE clause. PostgreSQL cannot use a *partial* unique index as the conflict
-- arbiter for that form — it must match a non-partial unique index/constraint.
--
-- A full UNIQUE on (organization_id, upload_id, source_staging_id) still allows multiple
-- rows with source_staging_id IS NULL (NULLs are distinct in unique checks), so legacy
-- rows are not forced to dedupe.

BEGIN;

DROP INDEX IF EXISTS public.uq_amazon_removals_source_staging_line;
DROP INDEX IF EXISTS public.uq_expected_packages_source_staging_line;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removals_org_upload_source_staging
  ON public.amazon_removals (organization_id, upload_id, source_staging_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_expected_packages_org_upload_source_staging
  ON public.expected_packages (organization_id, upload_id, source_staging_id);

NOTIFY pgrst, 'reload schema';

COMMIT;
