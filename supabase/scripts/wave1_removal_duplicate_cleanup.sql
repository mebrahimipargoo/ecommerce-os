-- Wave 1 — destructive duplicate cleanup for TEST / staging DBs only.
-- Review wave1_removal_pre_migration_diagnostics.sql (before migrate) or post_migration script after migrate. Run inside a transaction; backup production before use.
-- Keeps one row per duplicate group (lowest id). Adjust ORDER BY if you need "latest wins".

BEGIN;

-- amazon_removals: staging key
DELETE FROM public.amazon_removals ar
USING (
  SELECT id,
         ROW_NUMBER() OVER (
           PARTITION BY organization_id, upload_id, source_staging_id
           ORDER BY created_at ASC NULLS LAST, id ASC
         ) AS rn
  FROM public.amazon_removals
  WHERE source_staging_id IS NOT NULL
) d
WHERE ar.id = d.id AND d.rn > 1;

-- amazon_removals: business key
DELETE FROM public.amazon_removals ar
USING (
  SELECT id,
         ROW_NUMBER() OVER (
           PARTITION BY organization_id, store_id, order_id, sku, fnsku, disposition,
             requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, order_type
           ORDER BY created_at ASC NULLS LAST, id ASC
         ) AS rn
  FROM public.amazon_removals
) d
WHERE ar.id = d.id AND d.rn > 1;

-- expected_packages: staging key
DELETE FROM public.expected_packages ep
USING (
  SELECT id,
         ROW_NUMBER() OVER (
           PARTITION BY organization_id, upload_id, source_staging_id
           ORDER BY created_at ASC NULLS LAST, id ASC
         ) AS rn
  FROM public.expected_packages
  WHERE source_staging_id IS NOT NULL
) d
WHERE ep.id = d.id AND d.rn > 1;

-- expected_packages: business key
DELETE FROM public.expected_packages ep
USING (
  SELECT id,
         ROW_NUMBER() OVER (
           PARTITION BY organization_id, store_id, order_id, sku, fnsku, disposition,
             requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date
           ORDER BY created_at ASC NULLS LAST, id ASC
         ) AS rn
  FROM public.expected_packages
) d
WHERE ep.id = d.id AND d.rn > 1;

-- amazon_removal_shipments: staging key
DELETE FROM public.amazon_removal_shipments sh
USING (
  SELECT id,
         ROW_NUMBER() OVER (
           PARTITION BY organization_id, upload_id, amazon_staging_id
           ORDER BY created_at ASC NULLS LAST, id ASC
         ) AS rn
  FROM public.amazon_removal_shipments
) d
WHERE sh.id = d.id AND d.rn > 1;

-- amazon_removal_shipments: business key
DELETE FROM public.amazon_removal_shipments sh
USING (
  SELECT id,
         ROW_NUMBER() OVER (
           PARTITION BY organization_id, store_id, order_id, tracking_number, sku, fnsku, disposition,
             requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, order_type
           ORDER BY created_at ASC NULLS LAST, id ASC
         ) AS rn
  FROM public.amazon_removal_shipments
) d
WHERE sh.id = d.id AND d.rn > 1;

COMMIT;
