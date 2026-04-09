-- Wave 1 — PRE-migration diagnostics (read-only).
-- Safe to run before 20260521_wave1_removal_store_dual_dedupe.sql.
-- Uses only columns that exist prior to that migration:
--   amazon_removal_shipments: id, organization_id, upload_id, amazon_staging_id, raw_row, created_at (per 20260513).
-- Does not reference store_id or typed shipment columns on amazon_removal_shipments.

-- Staging-line keys (upload idempotency) — all three tables
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

-- Logical-line duplicate risk without store_id (matches pre–Wave 1 business shape; no store_id column required)
SELECT 'amazon_removals_logical_line_no_store' AS scope, organization_id, order_id, sku, fnsku, disposition,
       requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, order_type, COUNT(*) AS row_count
FROM public.amazon_removals
GROUP BY organization_id, order_id, sku, fnsku, disposition,
         requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, order_type
HAVING COUNT(*) > 1;

SELECT 'expected_packages_logical_line_no_store' AS scope, organization_id, order_id, sku, fnsku, disposition,
       requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, COUNT(*) AS row_count
FROM public.expected_packages
GROUP BY organization_id, order_id, sku, fnsku, disposition,
         requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date
HAVING COUNT(*) > 1;
