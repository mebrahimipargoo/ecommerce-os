-- audit_product_identity_org_drift.sql
--
-- Audit query: surface rows where a Product Identity import wrote
-- product_identifier_map / catalog_products / products under a different
-- organization than the target store actually belongs to.
--
-- Background:
--   The Imports UI lets a super_admin pick an "Organization scope" and a
--   target store. Before the 2026-04-28 fix, super_admin uploads silently
--   defaulted to the actor's profile org (the parent/platform org
--   39f5e74f-0690-4ad0-9edd-3a7f6dd7385b) instead of the tenant org that
--   owns the selected store. This script lists the affected rows so an
--   operator can confirm the drift before running the manual cleanup.
--
-- Safe to run anytime: SELECT-only.
-- Adjust the date filter or org filter at the bottom if needed.

-- ──────────────────────────────────────────────────────────────────────────
-- 1. raw_report_uploads written under an org that does NOT own the store
-- ──────────────────────────────────────────────────────────────────────────
WITH product_identity_uploads AS (
  SELECT
    ru.id                                                          AS upload_id,
    ru.organization_id                                             AS upload_org,
    ru.report_type,
    ru.status,
    ru.created_at,
    ru.file_name,
    NULLIF(ru.metadata ->> 'import_store_id', '')::uuid            AS import_store_id,
    NULLIF(ru.metadata ->> 'ledger_store_id', '')::uuid            AS ledger_store_id
  FROM raw_report_uploads ru
  WHERE ru.report_type = 'PRODUCT_IDENTITY'
)
SELECT
  pu.upload_id,
  pu.report_type,
  pu.status,
  pu.created_at,
  pu.file_name,
  pu.upload_org                                       AS upload_organization_id,
  COALESCE(pu.import_store_id, pu.ledger_store_id)    AS store_id,
  s.organization_id                                   AS store_owner_organization_id
FROM product_identity_uploads pu
LEFT JOIN stores s
  ON s.id = COALESCE(pu.import_store_id, pu.ledger_store_id)
WHERE
  -- Drift: store exists, store has an owner org, and it differs from the upload org
  s.organization_id IS NOT NULL
  AND s.organization_id <> pu.upload_org
ORDER BY pu.created_at DESC;

-- ──────────────────────────────────────────────────────────────────────────
-- 2. product_identifier_map rows whose store_id belongs to one org
--    but whose organization_id is a different org
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  pim.id                       AS identifier_map_id,
  pim.organization_id          AS map_organization_id,
  pim.store_id,
  s.organization_id            AS store_owner_organization_id,
  pim.seller_sku,
  pim.asin,
  pim.fnsku,
  pim.upc_code,
  pim.source_upload_id,
  pim.source_report_type,
  pim.created_at
FROM product_identifier_map pim
JOIN stores s ON s.id = pim.store_id
WHERE
  pim.source_report_type IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY')
  AND pim.organization_id <> s.organization_id
ORDER BY pim.created_at DESC;

-- ──────────────────────────────────────────────────────────────────────────
-- 3. catalog_products rows from PRODUCT_IDENTITY imports with the same drift
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  cp.id                        AS catalog_product_id,
  cp.organization_id           AS catalog_organization_id,
  cp.store_id,
  s.organization_id            AS store_owner_organization_id,
  cp.seller_sku,
  cp.asin,
  cp.fnsku,
  cp.source_upload_id,
  cp.source_report_type,
  cp.created_at
FROM catalog_products cp
JOIN stores s ON s.id = cp.store_id
WHERE
  cp.source_report_type IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY')
  AND cp.organization_id <> s.organization_id
ORDER BY cp.created_at DESC;

-- ──────────────────────────────────────────────────────────────────────────
-- 4. products rows whose metadata says they came from a Product Identity
--    import but whose organization_id no longer matches the store owner
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  p.id                         AS product_id,
  p.organization_id            AS product_organization_id,
  p.store_id,
  s.organization_id            AS store_owner_organization_id,
  p.sku,
  p.product_name,
  p.metadata -> 'product_identity_import' ->> 'source_upload_id'
                                AS source_upload_id,
  p.created_at
FROM products p
JOIN stores s ON s.id = p.store_id
WHERE
  p.metadata ? 'product_identity_import'
  AND p.organization_id <> s.organization_id
ORDER BY p.created_at DESC;

-- ──────────────────────────────────────────────────────────────────────────
-- 5. Summary row counts, grouped by (wrong_org → correct_org), so an
--    operator can quickly size the cleanup before running it.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  'product_identifier_map'                      AS table_name,
  pim.organization_id                           AS wrong_organization_id,
  s.organization_id                             AS correct_organization_id,
  COUNT(*)                                      AS rows_to_reassign
FROM product_identifier_map pim
JOIN stores s ON s.id = pim.store_id
WHERE
  pim.source_report_type IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY')
  AND pim.organization_id <> s.organization_id
GROUP BY pim.organization_id, s.organization_id

UNION ALL

SELECT
  'catalog_products',
  cp.organization_id,
  s.organization_id,
  COUNT(*)
FROM catalog_products cp
JOIN stores s ON s.id = cp.store_id
WHERE
  cp.source_report_type IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY')
  AND cp.organization_id <> s.organization_id
GROUP BY cp.organization_id, s.organization_id

UNION ALL

SELECT
  'products',
  p.organization_id,
  s.organization_id,
  COUNT(*)
FROM products p
JOIN stores s ON s.id = p.store_id
WHERE
  p.metadata ? 'product_identity_import'
  AND p.organization_id <> s.organization_id
GROUP BY p.organization_id, s.organization_id

UNION ALL

SELECT
  'raw_report_uploads',
  ru.organization_id,
  s.organization_id,
  COUNT(*)
FROM raw_report_uploads ru
JOIN stores s
  ON s.id = COALESCE(
    NULLIF(ru.metadata ->> 'import_store_id', '')::uuid,
    NULLIF(ru.metadata ->> 'ledger_store_id', '')::uuid
  )
WHERE
  ru.report_type = 'PRODUCT_IDENTITY'
  AND ru.organization_id <> s.organization_id
GROUP BY ru.organization_id, s.organization_id

ORDER BY table_name, rows_to_reassign DESC;
