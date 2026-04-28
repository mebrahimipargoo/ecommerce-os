-- manual_cleanup_product_identity_org_drift.sql
--
-- ⚠ MANUAL — not run by any migration, not auto-run by the app.
-- ⚠ READ THIS WHOLE FILE BEFORE RUNNING ANYTHING.
-- ⚠ EVERY DELETE IS COMMENTED OUT. UNCOMMENT ONLY AFTER VERIFYING ROW COUNTS.
--
-- Purpose
-- ───────
-- Remove ONLY the bad Product Identity import rows that were written under the
-- wrong parent/platform organization for a specific target store, while leaving
-- every All Listings / Active Listings / Category Listings / FBA Inventory row
-- untouched. The catalog_products table already holds valid All Listings data
-- and MUST be preserved.
--
-- Scope (hard-coded constants below — change them in ONE place):
--   wrong_org   = 39f5e74f-0690-4ad0-9edd-3a7f6dd7385b   (parent / platform)
--   correct_org = 00000000-0000-0000-0000-000000000001   (tenant / customer)
--   target_store = 509ee1f6-622c-46a5-8110-7b889ba46c2c
--
-- Tables touched (rows matching ALL of: wrong_org, target_store, and a
-- product-identity source tag — never `all_listings`, `active_listings`,
-- `category_listings`, FBA inventory, or any other report type):
--   1. product_identifier_map
--   2. catalog_products
--   3. products
--   4. catalog_identity_unresolved_backlog
--   5. file_processing_status (rows tied to soon-to-be-deleted uploads)
--   6. raw_report_uploads
--
-- After running you should re-import the Product Identity CSV through the UI
-- under the correct tenant org (the Imports page now forwards the picked
-- organization scope and validates the store belongs to it server-side).

-- ──────────────────────────────────────────────────────────────────────────
-- STEP 0 — Always run this first. It must return 0 rows for any table that
-- you do NOT expect to wipe. Anything unexpected here is a sign that the
-- WHERE clauses below would over-delete and you should stop.
-- ──────────────────────────────────────────────────────────────────────────

-- 0.a Show the bad Product Identity uploads in scope.
SELECT
  ru.id              AS upload_id,
  ru.organization_id AS upload_organization_id,
  ru.report_type,
  ru.status,
  ru.file_name,
  ru.created_at,
  COALESCE(
    NULLIF(ru.metadata ->> 'import_store_id', '')::uuid,
    NULLIF(ru.metadata ->> 'ledger_store_id', '')::uuid
  ) AS metadata_store_id
FROM raw_report_uploads ru
WHERE ru.organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
  AND ru.report_type     = 'PRODUCT_IDENTITY'
  AND COALESCE(
        NULLIF(ru.metadata ->> 'import_store_id', '')::uuid,
        NULLIF(ru.metadata ->> 'ledger_store_id', '')::uuid
      ) = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid
ORDER BY ru.created_at DESC;

-- 0.b Sanity check: NO All Listings / Active Listings / Category Listings /
--     Manage FBA Inventory / etc. row should ever live under
--     (wrong_org, target_store). If this query returns rows, STOP and
--     investigate before doing any DELETE.
SELECT
  'catalog_products' AS table_name,
  cp.source_report_type,
  COUNT(*) AS rows_present
FROM catalog_products cp
WHERE cp.organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
  AND cp.store_id        = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid
  AND cp.source_report_type NOT IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY')
GROUP BY cp.source_report_type

UNION ALL

SELECT
  'product_identifier_map',
  pim.source_report_type,
  COUNT(*)
FROM product_identifier_map pim
WHERE pim.organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
  AND pim.store_id        = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid
  AND pim.source_report_type NOT IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY')
GROUP BY pim.source_report_type;

-- 0.c What WILL be deleted, by table — confirm these counts before COMMIT.
SELECT 'product_identifier_map' AS table_name, COUNT(*) AS rows_to_delete
FROM product_identifier_map
WHERE organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
  AND store_id        = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid
  AND source_report_type IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY')
UNION ALL
SELECT 'catalog_products', COUNT(*)
FROM catalog_products
WHERE organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
  AND store_id        = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid
  AND source_report_type IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY')
UNION ALL
SELECT 'products', COUNT(*)
FROM products
WHERE organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
  AND store_id        = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid
  AND metadata ? 'product_identity_import'
UNION ALL
SELECT 'catalog_identity_unresolved_backlog', COUNT(*)
FROM catalog_identity_unresolved_backlog
WHERE organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
  AND store_id        = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid
  AND source_report_type IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY')
UNION ALL
SELECT 'raw_report_uploads', COUNT(*)
FROM raw_report_uploads ru
WHERE ru.organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
  AND ru.report_type     = 'PRODUCT_IDENTITY'
  AND COALESCE(
        NULLIF(ru.metadata ->> 'import_store_id', '')::uuid,
        NULLIF(ru.metadata ->> 'ledger_store_id', '')::uuid
      ) = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid;


-- ──────────────────────────────────────────────────────────────────────────
-- STEP 1 — Targeted DELETEs.
-- All DELETEs are inside ONE transaction so you can ROLLBACK before COMMIT.
-- Every WHERE clause filters on:
--    organization_id   = wrong parent/platform org
--    store_id          = target store
--    source_report_type IN ('PRODUCT_IDENTITY_IMPORT','PRODUCT_IDENTITY')
-- which guarantees:
--    • All Listings (source_report_type='all_listings'), Active Listings,
--      Category Listings, Manage FBA Inventory, FBA Returns, Removals, etc.
--      are NEVER touched.
--    • Rows that legitimately live under the correct tenant org are NEVER
--      touched (different organization_id).
-- ──────────────────────────────────────────────────────────────────────────
-- BEGIN;
--
-- -- 1.1 product_identifier_map: only Product Identity rows, only the bad scope.
-- DELETE FROM public.product_identifier_map
-- WHERE organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
--   AND store_id        = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid
--   AND source_report_type IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY');
--
-- -- 1.2 catalog_products: only rows tagged as Product Identity origin.
-- --     All Listings rows have source_report_type='all_listings' and are
-- --     filtered out by this WHERE clause.
-- DELETE FROM public.catalog_products
-- WHERE organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
--   AND store_id        = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid
--   AND source_report_type IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY');
--
-- -- 1.3 products: only rows whose metadata says they came from a Product
-- --     Identity import. The product_identity_import key is set exclusively
-- --     by lib/product-identity-import.ts.
-- DELETE FROM public.products
-- WHERE organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
--   AND store_id        = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid
--   AND metadata ? 'product_identity_import';
--
-- -- 1.4 catalog_identity_unresolved_backlog: same Product Identity scope.
-- DELETE FROM public.catalog_identity_unresolved_backlog
-- WHERE organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
--   AND store_id        = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid
--   AND source_report_type IN ('PRODUCT_IDENTITY_IMPORT', 'PRODUCT_IDENTITY');
--
-- -- 1.5 file_processing_status rows tied to the soon-to-be-deleted uploads.
-- --     We delete by upload_id so the FK / progress rows do not become orphans.
-- DELETE FROM public.file_processing_status fps
-- WHERE fps.upload_id IN (
--   SELECT ru.id
--   FROM public.raw_report_uploads ru
--   WHERE ru.organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
--     AND ru.report_type     = 'PRODUCT_IDENTITY'
--     AND COALESCE(
--           NULLIF(ru.metadata ->> 'import_store_id', '')::uuid,
--           NULLIF(ru.metadata ->> 'ledger_store_id', '')::uuid
--         ) = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid
-- );
--
-- -- 1.6 raw_report_uploads: the Product Identity sessions themselves.
-- --     This is the last DELETE so foreign keys above don't fail.
-- DELETE FROM public.raw_report_uploads
-- WHERE organization_id = '39f5e74f-0690-4ad0-9edd-3a7f6dd7385b'::uuid
--   AND report_type     = 'PRODUCT_IDENTITY'
--   AND COALESCE(
--         NULLIF(metadata ->> 'import_store_id', '')::uuid,
--         NULLIF(metadata ->> 'ledger_store_id', '')::uuid
--       ) = '509ee1f6-622c-46a5-8110-7b889ba46c2c'::uuid;
--
-- -- ⚠ STOP HERE. Re-run the STEP 0 counts and confirm everything went to 0.
-- -- ⚠ Then either ROLLBACK to abort, or COMMIT to make it permanent.
-- -- ROLLBACK;
-- -- COMMIT;

-- ──────────────────────────────────────────────────────────────────────────
-- STEP 2 — After COMMIT, reload PostgREST so the schema cache is fresh, and
-- re-import the Product Identity CSV through the Imports UI under
-- the correct tenant organization (00000000-0000-0000-0000-000000000001).
-- The new server-side validation will refuse any upload whose target store
-- does not belong to the chosen organization, so this can no longer drift.
-- ──────────────────────────────────────────────────────────────────────────
-- NOTIFY pgrst, 'reload schema';

-- ──────────────────────────────────────────────────────────────────────────
-- WHAT THIS FILE DELIBERATELY DOES NOT DO
-- ──────────────────────────────────────────────────────────────────────────
--   • It never deletes any catalog_products row whose source_report_type is
--     'all_listings', 'active_listings', or 'category_listings' — those are
--     the valid All Listings rows you want to keep.
--   • It never deletes any product_identifier_map row that came from a
--     listing or inventory ledger (match_source 'listing_catalog' /
--     'inventory_ledger', source_report_type 'all_listings' / etc.).
--   • It never touches rows under the correct tenant org
--     00000000-0000-0000-0000-000000000001 — they are out of scope.
--   • It never DROPs or ALTERs any table; nothing structural changes.
