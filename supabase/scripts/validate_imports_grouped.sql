-- validate_imports_grouped.sql
--
-- Validation queries for the three identity tables. SELECT-only — safe to run
-- before and after `manual_cleanup_product_identity_org_drift.sql`, and after
-- re-importing the Product Identity CSV under the correct tenant organization.
--
-- Use these to confirm:
--   * No rows under the wrong parent/platform org for the target store.
--   * All Listings rows are still intact in catalog_products.
--   * Re-imported Product Identity rows landed under the correct tenant org.
--
-- Optional scope: paste a WHERE clause if you want to focus on a single org or
-- store — every query below already exposes those columns in the GROUP BY.

-- ──────────────────────────────────────────────────────────────────────────
-- 1. product_identifier_map by (organization_id, store_id, source_report_type, match_source)
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  pim.organization_id,
  pim.store_id,
  pim.source_report_type,
  pim.match_source,
  COUNT(*)                                                AS row_count,
  COUNT(*) FILTER (WHERE pim.fnsku IS NOT NULL)           AS rows_with_fnsku,
  COUNT(*) FILTER (WHERE pim.upc_code IS NOT NULL)        AS rows_with_upc,
  COUNT(*) FILTER (WHERE pim.asin IS NOT NULL)            AS rows_with_asin,
  COUNT(*) FILTER (WHERE pim.seller_sku IS NOT NULL)      AS rows_with_seller_sku,
  AVG(pim.confidence_score)::numeric(10, 4)               AS avg_confidence,
  MIN(pim.first_seen_at)                                  AS first_seen,
  MAX(pim.last_seen_at)                                   AS last_seen
FROM public.product_identifier_map pim
GROUP BY
  pim.organization_id,
  pim.store_id,
  pim.source_report_type,
  pim.match_source
ORDER BY
  pim.organization_id,
  pim.store_id,
  pim.source_report_type NULLS LAST,
  pim.match_source NULLS LAST;

-- ──────────────────────────────────────────────────────────────────────────
-- 2. catalog_products by (organization_id, store_id, source_report_type)
--    Confirms All Listings rows survive the cleanup and Product Identity
--    rows land under the correct tenant org after re-import.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  cp.organization_id,
  cp.store_id,
  cp.source_report_type,
  COUNT(*)                                                AS row_count,
  COUNT(*) FILTER (WHERE cp.fnsku IS NOT NULL)            AS rows_with_fnsku,
  COUNT(*) FILTER (WHERE cp.asin IS NOT NULL)             AS rows_with_asin,
  COUNT(*) FILTER (WHERE cp.item_name IS NOT NULL)        AS rows_with_item_name,
  COUNT(*) FILTER (WHERE cp.listing_status IS NOT NULL)   AS rows_with_listing_status,
  COUNT(*) FILTER (WHERE cp.price IS NOT NULL)            AS rows_with_price,
  COUNT(*) FILTER (WHERE cp.quantity IS NOT NULL)         AS rows_with_quantity,
  COUNT(*) FILTER (WHERE cp.item_condition IS NOT NULL)   AS rows_with_condition,
  MIN(cp.first_seen_at)                                   AS first_seen,
  MAX(cp.last_seen_at)                                    AS last_seen
FROM public.catalog_products cp
GROUP BY
  cp.organization_id,
  cp.store_id,
  cp.source_report_type
ORDER BY
  cp.organization_id,
  cp.store_id,
  cp.source_report_type NULLS LAST;

-- ──────────────────────────────────────────────────────────────────────────
-- 3. products by (organization_id, store_id) — show coverage of the core
--    identity fields owned by the Product Identity CSV.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  p.organization_id,
  p.store_id,
  COUNT(*)                                                AS row_count,
  COUNT(*) FILTER (WHERE p.product_name IS NOT NULL
                   AND p.product_name <> p.sku)           AS rows_with_real_product_name,
  COUNT(*) FILTER (WHERE p.vendor_name IS NOT NULL)       AS rows_with_vendor_name,
  COUNT(*) FILTER (WHERE p.mfg_part_number IS NOT NULL)   AS rows_with_mfg_part_number,
  COUNT(*) FILTER (WHERE p.upc_code IS NOT NULL)          AS rows_with_upc_code,
  COUNT(*) FILTER (WHERE p.asin IS NOT NULL)              AS rows_with_asin,
  COUNT(*) FILTER (WHERE p.fnsku IS NOT NULL)             AS rows_with_fnsku,
  COUNT(*) FILTER (WHERE p.metadata ? 'product_identity_import')
                                                          AS rows_from_product_identity,
  MIN(p.created_at)                                       AS first_created,
  MAX(p.updated_at)                                       AS last_updated
FROM public.products p
GROUP BY
  p.organization_id,
  p.store_id
ORDER BY
  p.organization_id,
  p.store_id;

-- ──────────────────────────────────────────────────────────────────────────
-- 4. Drift sentinel: anything whose store owner organization disagrees with
--    the row's organization_id. After cleanup + re-import this MUST return 0.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  'products' AS table_name,
  p.organization_id            AS row_organization_id,
  s.organization_id            AS store_owner_organization_id,
  p.store_id,
  COUNT(*)                     AS drifted_rows
FROM public.products p
JOIN public.stores s ON s.id = p.store_id
WHERE p.organization_id <> s.organization_id
GROUP BY p.organization_id, s.organization_id, p.store_id

UNION ALL

SELECT
  'catalog_products',
  cp.organization_id,
  s.organization_id,
  cp.store_id,
  COUNT(*)
FROM public.catalog_products cp
JOIN public.stores s ON s.id = cp.store_id
WHERE cp.organization_id <> s.organization_id
GROUP BY cp.organization_id, s.organization_id, cp.store_id

UNION ALL

SELECT
  'product_identifier_map',
  pim.organization_id,
  s.organization_id,
  pim.store_id,
  COUNT(*)
FROM public.product_identifier_map pim
JOIN public.stores s ON s.id = pim.store_id
WHERE pim.organization_id <> s.organization_id
GROUP BY pim.organization_id, s.organization_id, pim.store_id

ORDER BY table_name, drifted_rows DESC;
