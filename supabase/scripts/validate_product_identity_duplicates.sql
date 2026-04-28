-- validate_product_identity_duplicates.sql
--
-- Detects duplicate Product Identity imports by file hash + organization +
-- store + report_type. SELECT-only — safe to run anytime, especially after
-- the targeted cleanup (manual_cleanup_product_identity_org_drift.sql) and
-- after re-importing the CSV through the UI.
--
-- The new app-side guard refuses to create a second active session for the
-- same (organization_id, store_id, content_sha256) when report_type is
-- PRODUCT_IDENTITY, and the partial unique index
-- `uq_product_identifier_map_product_identity` enforces idempotency at the
-- database. These queries help confirm both invariants hold and surface any
-- drift left over from before the fix.

-- ──────────────────────────────────────────────────────────────────────────
-- 1. Active duplicate Product Identity uploads.
--    Groups by (organization_id, report_type, store, content_sha256). Any
--    row with active_count > 1 means the same file is currently active for
--    the same store more than once. After the fix this MUST be 0.
-- ──────────────────────────────────────────────────────────────────────────
WITH product_identity_uploads AS (
  SELECT
    ru.id,
    ru.organization_id,
    ru.report_type,
    ru.status,
    ru.file_name,
    ru.created_at,
    ru.updated_at,
    NULLIF(ru.metadata ->> 'content_sha256', '')                  AS content_sha256,
    NULLIF(ru.metadata ->> 'md5_hash', '')                        AS md5_hash,
    COALESCE(
      NULLIF(ru.metadata ->> 'import_store_id', '')::uuid,
      NULLIF(ru.metadata ->> 'ledger_store_id', '')::uuid
    )                                                              AS store_id,
    -- Lifecycle bucket. Mirrors PRODUCT_IDENTITY_ACTIVE_STATUSES on the server.
    CASE
      WHEN ru.status IN (
        'uploading', 'pending', 'ready', 'uploaded', 'mapped',
        'needs_mapping', 'processing', 'staged', 'synced', 'complete'
      ) THEN 'active'
      WHEN ru.status = 'superseded' THEN 'superseded'
      WHEN ru.status = 'failed'     THEN 'failed'
      WHEN ru.status = 'cancelled'  THEN 'cancelled'
      ELSE 'other'
    END                                                            AS lifecycle_bucket
  FROM public.raw_report_uploads ru
  WHERE ru.report_type = 'PRODUCT_IDENTITY'
)
SELECT
  pu.organization_id,
  pu.report_type,
  pu.store_id,
  pu.content_sha256,
  COUNT(*)                                                AS total_count,
  COUNT(*) FILTER (WHERE pu.lifecycle_bucket = 'active')  AS active_count,
  COUNT(*) FILTER (WHERE pu.lifecycle_bucket = 'superseded') AS superseded_count,
  COUNT(*) FILTER (WHERE pu.lifecycle_bucket = 'failed')  AS failed_count,
  COUNT(*) FILTER (WHERE pu.lifecycle_bucket = 'cancelled') AS cancelled_count,
  MIN(pu.created_at)                                      AS first_imported_at,
  MAX(pu.created_at)                                      AS last_imported_at,
  array_agg(pu.id ORDER BY pu.created_at DESC)            AS upload_ids,
  array_agg(pu.status ORDER BY pu.created_at DESC)        AS statuses,
  array_agg(pu.file_name ORDER BY pu.created_at DESC)     AS file_names
FROM product_identity_uploads pu
WHERE pu.content_sha256 IS NOT NULL
  AND pu.store_id IS NOT NULL
GROUP BY
  pu.organization_id,
  pu.report_type,
  pu.store_id,
  pu.content_sha256
HAVING
  -- Surface any group where >1 active OR (≥1 active AND any superseded older row).
  COUNT(*) FILTER (WHERE pu.lifecycle_bucket = 'active') > 1
ORDER BY
  COUNT(*) FILTER (WHERE pu.lifecycle_bucket = 'active') DESC,
  pu.organization_id,
  pu.store_id;

-- ──────────────────────────────────────────────────────────────────────────
-- 2. Same file in two different stores under the same org. Not necessarily
--    a bug, but useful to surface in case operators are accidentally cross-
--    importing inventory between stores.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  ru.organization_id,
  ru.metadata ->> 'content_sha256'   AS content_sha256,
  COUNT(DISTINCT COALESCE(
    NULLIF(ru.metadata ->> 'import_store_id', '')::uuid,
    NULLIF(ru.metadata ->> 'ledger_store_id', '')::uuid
  )) AS distinct_stores_with_active_import,
  array_agg(DISTINCT COALESCE(
    NULLIF(ru.metadata ->> 'import_store_id', '')::uuid,
    NULLIF(ru.metadata ->> 'ledger_store_id', '')::uuid
  )) AS store_ids,
  array_agg(ru.id ORDER BY ru.created_at DESC) AS upload_ids
FROM public.raw_report_uploads ru
WHERE ru.report_type = 'PRODUCT_IDENTITY'
  AND ru.status IN (
    'uploading', 'pending', 'ready', 'uploaded', 'mapped',
    'needs_mapping', 'processing', 'staged', 'synced', 'complete'
  )
  AND ru.metadata ? 'content_sha256'
GROUP BY ru.organization_id, ru.metadata ->> 'content_sha256'
HAVING COUNT(DISTINCT COALESCE(
         NULLIF(ru.metadata ->> 'import_store_id', '')::uuid,
         NULLIF(ru.metadata ->> 'ledger_store_id', '')::uuid
       )) > 1
ORDER BY ru.organization_id;

-- ──────────────────────────────────────────────────────────────────────────
-- 3. product_identifier_map duplicate detection by deterministic identity
--    tuple. After migration 20260636 the partial unique index makes this
--    impossible going forward; this query is here to confirm zero rows post-
--    migration and to surface legacy duplicates from before the fix.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  pim.organization_id,
  pim.store_id,
  pim.external_listing_id,
  COUNT(*)                              AS row_count,
  array_agg(pim.id  ORDER BY pim.created_at DESC) AS row_ids,
  array_agg(pim.source_upload_id ORDER BY pim.created_at DESC) AS source_upload_ids
FROM public.product_identifier_map pim
WHERE pim.external_listing_id IS NOT NULL
  AND pim.external_listing_id LIKE 'product_identity:%'
GROUP BY pim.organization_id, pim.store_id, pim.external_listing_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC, pim.organization_id, pim.store_id;

-- ──────────────────────────────────────────────────────────────────────────
-- 4. Cross-source duplicates: same (org, store, seller_sku, asin) with rows
--    from BOTH product_identity AND a listing source. This is expected after
--    enrichment merges; surfaces here so operators can confirm match_source
--    and provenance look right.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  pim.organization_id,
  pim.store_id,
  pim.seller_sku,
  pim.asin,
  COUNT(*) FILTER (WHERE pim.source_report_type IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY')) AS product_identity_rows,
  COUNT(*) FILTER (WHERE pim.source_report_type IN ('all_listings', 'active_listings', 'category_listings')) AS listing_rows,
  array_agg(DISTINCT pim.match_source) AS match_sources,
  array_agg(DISTINCT pim.source_report_type) AS source_report_types
FROM public.product_identifier_map pim
WHERE pim.seller_sku IS NOT NULL
  AND pim.asin IS NOT NULL
GROUP BY pim.organization_id, pim.store_id, pim.seller_sku, pim.asin
HAVING
  COUNT(*) FILTER (WHERE pim.source_report_type IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY')) > 0
  AND COUNT(*) FILTER (WHERE pim.source_report_type IN ('all_listings', 'active_listings', 'category_listings')) > 0
ORDER BY pim.organization_id, pim.store_id, pim.seller_sku;
