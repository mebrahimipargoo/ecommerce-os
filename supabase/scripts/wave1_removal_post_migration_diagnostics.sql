-- Wave 1 — POST-migration diagnostics (read-only).
-- Run after 20260521_wave1_removal_store_dual_dedupe.sql (typed columns + store_id on amazon_removal_shipments).
-- Full business keys including store_id and shipment typed columns.
-- If CREATE UNIQUE INDEX failed during migration, fix data (e.g. test-only cleanup script) and re-run migration.

-- Staging-line keys (upload idempotency)
SELECT 'amazon_removals_staging' AS scope, organization_id, upload_id, source_staging_id, COUNT(*) AS row_count
FROM public.amazon_removals
WHERE source_staging_id IS NOT NULL
GROUP BY organization_id, upload_id, source_staging_id
HAVING COUNT(*) > 1;

SELECT 'expected_packages_staging' AS scope, organization_id, upload_id, source_staging_id, COUNT(*) AS row_count
FROM public.expected_packages
WHERE source_staging_id IS NOT NULL
GROUP BY organization_id, upload_id, source_staging_id
HAVING COUNT(*) > 1;

SELECT 'amazon_removal_shipments_staging' AS scope, organization_id, upload_id, amazon_staging_id, COUNT(*) AS row_count
FROM public.amazon_removal_shipments
GROUP BY organization_id, upload_id, amazon_staging_id
HAVING COUNT(*) > 1;

-- Business keys (store-scoped; aligns with Wave 1 unique indexes)
SELECT 'amazon_removals_business' AS scope, organization_id, store_id, order_id, sku, fnsku, disposition,
       requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, order_type, COUNT(*) AS row_count
FROM public.amazon_removals
GROUP BY organization_id, store_id, order_id, sku, fnsku, disposition,
         requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, order_type
HAVING COUNT(*) > 1;

SELECT 'expected_packages_business' AS scope, organization_id, store_id, order_id, sku, fnsku, disposition,
       requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, COUNT(*) AS row_count
FROM public.expected_packages
GROUP BY organization_id, store_id, order_id, sku, fnsku, disposition,
         requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date
HAVING COUNT(*) > 1;

SELECT 'amazon_removal_shipments_business' AS scope, organization_id, store_id, order_id, tracking_number, sku, fnsku, disposition,
       requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, order_type, COUNT(*) AS row_count
FROM public.amazon_removal_shipments
GROUP BY organization_id, store_id, order_id, tracking_number, sku, fnsku, disposition,
         requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, order_type
HAVING COUNT(*) > 1;

-- Rows with NULL store_id (nullable by design; informational after migration)
SELECT 'amazon_removals_null_store' AS scope, COUNT(*) AS n FROM public.amazon_removals WHERE store_id IS NULL;
SELECT 'expected_packages_null_store' AS scope, COUNT(*) AS n FROM public.expected_packages WHERE store_id IS NULL;
SELECT 'amazon_removal_shipments_null_store' AS scope, COUNT(*) AS n FROM public.amazon_removal_shipments WHERE store_id IS NULL;
