-- validate_product_identity_import.sql
--
-- Validation queries for the Product Identity CSV import flow.
--
-- Run after uploading
--   UPC, Vendor, Seller SKU, Mfg #, FNSKU, ASIN, Product Name
-- to confirm:
--   1. The classifier wrote raw_report_uploads.report_type = 'PRODUCT_IDENTITY'.
--   2. The Process step ran processProductIdentityUpload and reached status='synced'.
--   3. The Sync step (or re-import) is idempotent.
--   4. The validation block in metadata records detected_headers,
--      detected_report_type, rows_parsed, rows_synced, products_upserted,
--      catalog_products_upserted, identifiers_upserted, and invalid_identifier_counts.
--
-- Replace the :upload_id binding when running for a single upload, or remove
-- the WHERE clause to scan the whole tenant's recent imports.
--
-- All queries are SELECT-only.

-- ──────────────────────────────────────────────────────────────────────────
-- 1. Latest Product Identity uploads per organization, including the
--    validation block written by the process / sync routes.
--    Confirms requirement: report_type=PRODUCT_IDENTITY in raw_report_uploads
--    and the metadata validation keys are populated.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  ru.id                                                          AS upload_id,
  ru.organization_id,
  ru.file_name,
  ru.report_type,
  ru.status,
  ru.created_at,
  ru.updated_at,
  ru.metadata ->> 'content_sha256'                               AS content_sha256,
  COALESCE(
    NULLIF(ru.metadata ->> 'import_store_id', '')::uuid,
    NULLIF(ru.metadata ->> 'ledger_store_id', '')::uuid
  )                                                              AS store_id,
  -- detected_headers / detected_report_type captured at classify time and
  -- mirrored into the validation block at process start.
  ru.metadata -> 'csv_headers'                                   AS detected_headers,
  ru.metadata #>> '{product_identity_validation,detected_report_type}' AS detected_report_type,
  -- counters written by runProductIdentityImportFromUpload at completion.
  (ru.metadata #>> '{product_identity_validation,rows_parsed}')::bigint            AS rows_parsed,
  (ru.metadata #>> '{product_identity_validation,rows_synced}')::bigint            AS rows_synced,
  (ru.metadata #>> '{product_identity_validation,products_upserted}')::bigint      AS products_upserted,
  (ru.metadata #>> '{product_identity_validation,catalog_products_upserted}')::bigint AS catalog_products_upserted,
  (ru.metadata #>> '{product_identity_validation,identifiers_upserted}')::bigint   AS identifiers_upserted,
  ru.metadata -> 'product_identity_validation' -> 'invalid_identifier_counts'      AS invalid_identifier_counts
FROM public.raw_report_uploads ru
WHERE ru.report_type = 'PRODUCT_IDENTITY'
ORDER BY ru.created_at DESC
LIMIT 25;

-- ──────────────────────────────────────────────────────────────────────────
-- 2. file_processing_status mirror — confirms the pipeline reached
--    phase3_status='complete' for the upload (no reliance on the legacy
--    `raw_report_uploads.row_count` column; uses metadata + FPS only).
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  fps.upload_id,
  fps.status,
  fps.current_phase,
  fps.current_phase_label,
  fps.phase1_upload_pct,
  fps.phase2_stage_pct,
  fps.phase3_raw_sync_pct,
  fps.process_pct,
  fps.sync_pct,
  fps.total_rows,
  fps.processed_rows,
  fps.staged_rows_written,
  fps.raw_rows_written,
  fps.import_metrics #>> '{detected_report_type}'                    AS detected_report_type,
  fps.import_metrics #>> '{rows_parsed}'                              AS rows_parsed,
  fps.import_metrics #>> '{rows_synced}'                              AS rows_synced,
  fps.import_metrics #>> '{products_upserted}'                        AS products_upserted,
  fps.import_metrics #>> '{catalog_products_upserted}'                AS catalog_products_upserted,
  fps.import_metrics #>> '{identifiers_upserted}'                     AS identifiers_upserted,
  fps.import_metrics -> 'invalid_identifier_counts'                   AS invalid_identifier_counts
FROM public.file_processing_status fps
JOIN public.raw_report_uploads ru ON ru.id = fps.upload_id
WHERE ru.report_type = 'PRODUCT_IDENTITY'
ORDER BY fps.updated_at DESC NULLS LAST
LIMIT 25;

-- ──────────────────────────────────────────────────────────────────────────
-- 3. Per-upload write counts in the destination tables
--    (products / catalog_products / product_identifier_map).
--    Confirms the Sync step actually upserted the three target tables
--    referenced by requirement 5.
--
--    Row counts here should match (or exceed) the rows_parsed metric.
-- ──────────────────────────────────────────────────────────────────────────
WITH last_pi AS (
  SELECT id, organization_id,
         COALESCE(NULLIF(metadata ->> 'import_store_id','')::uuid,
                  NULLIF(metadata ->> 'ledger_store_id','')::uuid) AS store_id
  FROM public.raw_report_uploads
  WHERE report_type = 'PRODUCT_IDENTITY'
  ORDER BY created_at DESC
  LIMIT 25
)
SELECT
  pi.id                                                          AS upload_id,
  pi.organization_id,
  pi.store_id,
  COUNT(DISTINCT cp.id)                                          AS catalog_products_for_upload,
  COUNT(DISTINCT pim.id)                                         AS product_identifier_map_for_upload,
  COUNT(DISTINCT p.id) FILTER (
    WHERE p.metadata -> 'product_identity_import' ->> 'source_upload_id' = pi.id::text
  )                                                              AS products_touched_by_upload
FROM last_pi pi
LEFT JOIN public.catalog_products cp
  ON cp.organization_id = pi.organization_id
 AND cp.source_upload_id = pi.id
 AND cp.source_report_type IN ('PRODUCT_IDENTITY_IMPORT','PRODUCT_IDENTITY')
LEFT JOIN public.product_identifier_map pim
  ON pim.organization_id = pi.organization_id
 AND pim.source_upload_id = pi.id
 AND pim.source_report_type IN ('PRODUCT_IDENTITY_IMPORT','PRODUCT_IDENTITY')
LEFT JOIN public.products p
  ON p.organization_id = pi.organization_id
 AND p.store_id        = pi.store_id
GROUP BY pi.id, pi.organization_id, pi.store_id
ORDER BY pi.id DESC;

-- ──────────────────────────────────────────────────────────────────────────
-- 4. Idempotency guard — a re-upload of the same file
--    (organization_id, store_id, content_sha256) MUST yield zero new rows
--    in product_identifier_map past the partial unique index. Returns 0
--    rows when invariant holds, one row per offender otherwise.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  pim.organization_id,
  pim.store_id,
  pim.external_listing_id,
  COUNT(*)                                                       AS row_count,
  array_agg(pim.source_upload_id ORDER BY pim.created_at DESC)   AS source_upload_ids
FROM public.product_identifier_map pim
WHERE pim.external_listing_id LIKE 'product_identity:%'
GROUP BY pim.organization_id, pim.store_id, pim.external_listing_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- ──────────────────────────────────────────────────────────────────────────
-- 5. CHECK constraint sanity — confirm 'PRODUCT_IDENTITY' is part of
--    raw_report_uploads_report_type_check (required for the classifier
--    UPDATE to succeed).
-- ──────────────────────────────────────────────────────────────────────────
SELECT conname,
       pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid = 'public.raw_report_uploads'::regclass
  AND conname = 'raw_report_uploads_report_type_check';

-- ──────────────────────────────────────────────────────────────────────────
-- 6. Stuck-UNKNOWN sanity — surface ANY remaining row whose csv_headers
--    exactly match Product Identity but whose report_type still says UNKNOWN.
--    After running migration 20260638 this MUST return 0 rows.
-- ──────────────────────────────────────────────────────────────────────────
WITH header_signals AS (
  SELECT
    ru.id,
    ru.organization_id,
    ru.file_name,
    ru.status,
    ru.created_at,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'upc') AS has_upc,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'vendor') AS has_vendor,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'seller sku') AS has_seller_sku,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') IN ('mfg #', 'mfg#')) AS has_mfg,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'fnsku') AS has_fnsku,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'asin') AS has_asin,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'product name') AS has_product_name
  FROM public.raw_report_uploads ru
  CROSS JOIN LATERAL jsonb_array_elements_text(ru.metadata -> 'csv_headers') AS h(value)
  WHERE ru.report_type = 'UNKNOWN'
    AND jsonb_typeof(ru.metadata -> 'csv_headers') = 'array'
  GROUP BY ru.id, ru.organization_id, ru.file_name, ru.status, ru.created_at
)
SELECT *
FROM header_signals
WHERE has_upc
  AND has_vendor
  AND has_seller_sku
  AND has_mfg
  AND has_fnsku
  AND has_asin
  AND has_product_name
ORDER BY created_at DESC;

-- ──────────────────────────────────────────────────────────────────────────
-- 7. Recovery audit trail — show rows that migration 20260638 promoted
--    from UNKNOWN to PRODUCT_IDENTITY, with the recovery reason.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  ru.id                                                          AS upload_id,
  ru.organization_id,
  ru.file_name,
  ru.report_type,
  ru.status,
  ru.metadata #>> '{product_identity_recovery,reason}'           AS recovery_reason,
  ru.metadata #>> '{product_identity_recovery,recovered_at}'     AS recovered_at,
  ru.metadata #>> '{product_identity_recovery,previous_status}'  AS previous_status
FROM public.raw_report_uploads ru
WHERE ru.metadata ? 'product_identity_recovery'
ORDER BY (ru.metadata #>> '{product_identity_recovery,recovered_at}') DESC NULLS LAST
LIMIT 50;

-- ──────────────────────────────────────────────────────────────────────────
-- 8. Active org/store mismatch drift — any row returned here means an import
--    row's organization_id disagrees with the selected store's owner org.
--    This MUST return 0 rows.
-- ──────────────────────────────────────────────────────────────────────────
WITH upload_store AS (
  SELECT
    ru.id AS upload_id,
    ru.organization_id AS upload_org_id,
    ru.file_name,
    ru.report_type,
    ru.status,
    COALESCE(NULLIF(ru.metadata ->> 'import_store_id', '')::uuid,
             NULLIF(ru.metadata ->> 'ledger_store_id', '')::uuid) AS store_id
  FROM public.raw_report_uploads ru
  WHERE ru.metadata ? 'import_store_id'
     OR ru.metadata ? 'ledger_store_id'
)
SELECT
  us.upload_id,
  us.file_name,
  us.report_type,
  us.status,
  us.upload_org_id,
  us.store_id,
  s.organization_id AS store_owner_org_id
FROM upload_store us
LEFT JOIN public.stores s ON s.id = us.store_id
WHERE us.store_id IS NOT NULL
  AND (s.id IS NULL OR s.organization_id IS DISTINCT FROM us.upload_org_id)
ORDER BY us.file_name;

-- ──────────────────────────────────────────────────────────────────────────
-- 9. Suspicious PRODUCT_IDENTITY rows — returns Product Identity uploads that
--    either look like FBA Inventory OR lack the exact required signature.
--    This MUST return 0 rows for valid Product Identity data.
-- ──────────────────────────────────────────────────────────────────────────
WITH pi_headers AS (
  SELECT
    ru.id AS upload_id,
    ru.organization_id,
    ru.file_name,
    ru.status,
    ru.metadata -> 'csv_headers' AS headers,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'upc') AS has_upc,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'vendor') AS has_vendor,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'seller sku') AS has_seller_sku,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') IN ('mfg #', 'mfg#')) AS has_mfg,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'fnsku') AS has_fnsku,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'asin') AS has_asin,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'product name') AS has_product_name,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = ANY (
      ARRAY['available','inbound quantity','inbound working','inbound received','reserved',
            'total reserved quantity','inventory supply at fba','days of supply',
            'recommended replenishment','sales','snapshot date']
    )) AS has_fba_inventory_signal
  FROM public.raw_report_uploads ru
  LEFT JOIN LATERAL jsonb_array_elements_text(ru.metadata -> 'csv_headers') AS h(value)
    ON jsonb_typeof(ru.metadata -> 'csv_headers') = 'array'
  WHERE ru.report_type = 'PRODUCT_IDENTITY'
  GROUP BY ru.id, ru.organization_id, ru.file_name, ru.status, ru.metadata
)
SELECT *
FROM pi_headers
WHERE lower(file_name) LIKE '%fba inventory%'
   OR lower(file_name) LIKE '%inventory%'
   OR has_fba_inventory_signal
   OR NOT (has_upc AND has_vendor AND has_seller_sku AND has_mfg AND has_fnsku AND has_asin AND has_product_name)
ORDER BY file_name;

-- ──────────────────────────────────────────────────────────────────────────
-- 10. Duplicate active Product Identity uploads by same file + org + store.
--     This MUST return 0 rows. Superseded/failed/cancelled rows are excluded.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  ru.organization_id,
  COALESCE(NULLIF(ru.metadata ->> 'import_store_id','')::uuid,
           NULLIF(ru.metadata ->> 'ledger_store_id','')::uuid) AS store_id,
  ru.metadata ->> 'content_sha256' AS content_sha256,
  COUNT(*) AS active_count,
  array_agg(ru.id ORDER BY ru.created_at DESC) AS upload_ids,
  array_agg(ru.status ORDER BY ru.created_at DESC) AS statuses
FROM public.raw_report_uploads ru
WHERE ru.report_type = 'PRODUCT_IDENTITY'
  AND ru.status IN ('uploading','pending','ready','uploaded','mapped','needs_mapping','processing','staged','synced','complete')
  AND ru.metadata ? 'content_sha256'
GROUP BY ru.organization_id,
         COALESCE(NULLIF(ru.metadata ->> 'import_store_id','')::uuid,
                  NULLIF(ru.metadata ->> 'ledger_store_id','')::uuid),
         ru.metadata ->> 'content_sha256'
HAVING COUNT(*) > 1
ORDER BY active_count DESC;

-- ──────────────────────────────────────────────────────────────────────────
-- 10b. Per-row CSV diagnostics for the latest Product Identity upload.
--      Explains why normalized_rows_count can be lower than rows_parsed:
--        normalized_rows_count = rows_parsed
--                              - rows_missing_seller_sku
--                              - rows_invalid_seller_sku
--      and lists up to 10 invalid Seller SKU values seen in the source file.
-- ──────────────────────────────────────────────────────────────────────────
WITH latest_pi AS (
  SELECT id
  FROM public.raw_report_uploads
  WHERE report_type = 'PRODUCT_IDENTITY'
  ORDER BY created_at DESC
  LIMIT 1
)
SELECT
  ru.id                                                                                AS upload_id,
  (ru.metadata #>> '{product_identity_validation,rows_parsed}')::bigint                AS rows_parsed,
  (ru.metadata #>> '{product_identity_validation,normalized_rows_count}')::bigint      AS normalized_rows_count,
  (ru.metadata #>> '{product_identity_validation,rows_missing_seller_sku}')::bigint    AS rows_missing_seller_sku,
  (ru.metadata #>> '{product_identity_validation,rows_invalid_seller_sku}')::bigint    AS rows_invalid_seller_sku,
  (ru.metadata #>> '{product_identity_validation,rows_skipped}')::bigint               AS rows_skipped,
  ru.metadata -> 'product_identity_validation' -> 'skipped_reason_counts'              AS skipped_reason_counts,
  ru.metadata -> 'product_identity_validation' -> 'invalid_sku_examples'               AS invalid_sku_examples
FROM public.raw_report_uploads ru
WHERE ru.id IN (SELECT id FROM latest_pi);

-- ──────────────────────────────────────────────────────────────────────────
-- 11. Duplicate Product Identity Seller SKUs in the latest upload's CSV.
--     The dedupe step in lib/product-identity-import.ts collapses these into
--     one product/catalog row, but the original CSV row count is preserved
--     in metadata.product_identity_validation.normalized_rows_count and the
--     dedupe counters expose how many were collapsed:
--       normalized_rows_count        = parsed CSV rows
--       unique_product_sku_count     = distinct (org, store, sku)
--       duplicate_sku_count          = collapsed duplicates
--       duplicate_sku_conflict_count = duplicates with ASIN/FNSKU disagreement
-- ──────────────────────────────────────────────────────────────────────────
WITH latest_pi AS (
  SELECT id, organization_id,
         COALESCE(NULLIF(metadata ->> 'import_store_id','')::uuid,
                  NULLIF(metadata ->> 'ledger_store_id','')::uuid) AS store_id,
         metadata,
         created_at
  FROM public.raw_report_uploads
  WHERE report_type = 'PRODUCT_IDENTITY'
  ORDER BY created_at DESC
  LIMIT 1
)
SELECT
  pi.id                                                          AS upload_id,
  pi.organization_id,
  pi.store_id,
  (pi.metadata #>> '{product_identity_validation,normalized_rows_count}')::bigint        AS normalized_rows_count,
  (pi.metadata #>> '{product_identity_validation,unique_product_sku_count}')::bigint     AS unique_product_sku_count,
  (pi.metadata #>> '{product_identity_validation,duplicate_sku_count}')::bigint          AS duplicate_sku_count,
  (pi.metadata #>> '{product_identity_validation,duplicate_sku_conflict_count}')::bigint AS duplicate_sku_conflict_count,
  (pi.metadata #>> '{product_identity_validation,catalog_unique_count}')::bigint         AS catalog_unique_count,
  (pi.metadata #>> '{product_identity_validation,identifier_unique_count}')::bigint      AS identifier_unique_count
FROM latest_pi pi;

-- ──────────────────────────────────────────────────────────────────────────
-- 12. Products created/updated by the latest Product Identity upload.
--     Compares unique SKUs the importer planned to write against the
--     `products` rows tagged with that upload id.
-- ──────────────────────────────────────────────────────────────────────────
WITH latest_pi AS (
  SELECT id, organization_id,
         COALESCE(NULLIF(metadata ->> 'import_store_id','')::uuid,
                  NULLIF(metadata ->> 'ledger_store_id','')::uuid) AS store_id
  FROM public.raw_report_uploads
  WHERE report_type = 'PRODUCT_IDENTITY'
  ORDER BY created_at DESC
  LIMIT 1
)
SELECT
  pi.id                                                          AS upload_id,
  pi.organization_id,
  pi.store_id,
  COUNT(DISTINCT p.id) FILTER (
    WHERE p.metadata -> 'product_identity_import' ->> 'source_upload_id' = pi.id::text
  )                                                              AS products_touched_by_upload,
  COUNT(DISTINCT cp.id)                                          AS catalog_products_for_upload,
  COUNT(DISTINCT pim.id)                                         AS product_identifier_map_for_upload
FROM latest_pi pi
LEFT JOIN public.products p
  ON p.organization_id = pi.organization_id
 AND p.store_id        = pi.store_id
LEFT JOIN public.catalog_products cp
  ON cp.organization_id = pi.organization_id
 AND cp.source_upload_id = pi.id
 AND cp.source_report_type IN ('PRODUCT_IDENTITY_IMPORT','PRODUCT_IDENTITY')
LEFT JOIN public.product_identifier_map pim
  ON pim.organization_id = pi.organization_id
 AND pim.source_upload_id = pi.id
 AND pim.source_report_type IN ('PRODUCT_IDENTITY_IMPORT','PRODUCT_IDENTITY')
GROUP BY pi.id, pi.organization_id, pi.store_id;

-- ──────────────────────────────────────────────────────────────────────────
-- 13. Persisted pipeline state for the latest upload.
--     The Imports page reads this exact view to rebuild the pipeline card on
--     reload / focus return. Should be one row per upload_id.
-- ──────────────────────────────────────────────────────────────────────────
WITH latest_pi AS (
  SELECT id
  FROM public.raw_report_uploads
  WHERE report_type = 'PRODUCT_IDENTITY'
  ORDER BY created_at DESC
  LIMIT 1
)
SELECT
  ru.id                              AS upload_id,
  ru.organization_id,
  ru.report_type,
  ru.status                          AS upload_status,
  fps.status                         AS fps_status,
  fps.current_phase,
  fps.current_phase_label,
  fps.upload_pct,
  fps.process_pct,
  fps.sync_pct,
  fps.phase1_upload_pct,
  fps.phase2_stage_pct,
  fps.phase3_raw_sync_pct,
  fps.phase4_generic_pct,
  fps.phase2_status,
  fps.phase3_status,
  fps.phase4_status,
  fps.total_rows,
  fps.processed_rows,
  fps.staged_rows_written,
  fps.raw_rows_written,
  fps.error_message
FROM public.raw_report_uploads ru
LEFT JOIN public.file_processing_status fps ON fps.upload_id = ru.id
WHERE ru.id IN (SELECT id FROM latest_pi);

-- ──────────────────────────────────────────────────────────────────────────
-- 14. No duplicate active process for the same upload.
--     A row is considered "in-flight" when raw_report_uploads.status is in
--     {processing, syncing, uploading} or file_processing_status.status is
--     {processing, syncing}. The optimistic lock in /api/settings/imports/process
--     and /api/settings/imports/sync only allows a single transition into
--     "processing", so this MUST return 0 rows.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  ru.id            AS upload_id,
  ru.organization_id,
  ru.status        AS upload_status,
  fps.status       AS fps_status,
  ru.updated_at,
  fps.updated_at   AS fps_updated_at
FROM public.raw_report_uploads ru
JOIN public.file_processing_status fps ON fps.upload_id = ru.id
WHERE ru.status IN ('processing', 'syncing', 'uploading')
  AND fps.status IN ('processing', 'syncing')
  AND fps.upload_id IN (
    SELECT upload_id
    FROM public.file_processing_status
    WHERE status IN ('processing','syncing')
    GROUP BY upload_id
    HAVING COUNT(*) > 1
  );

-- ──────────────────────────────────────────────────────────────────────────
-- Manual cleanup template (DO NOT run automatically).
--
-- If query #9 shows a misclassified Product Identity upload that already wrote
-- destination rows, review the upload id(s), then clean only Product Identity
-- rows tied to that source_upload_id. This intentionally does NOT touch
-- all_listings, FBA inventory, ledger, removals, or any other report family.
--
-- BEGIN;
--   DELETE FROM public.product_identifier_map
--   WHERE source_upload_id = '<upload_id>'::uuid
--     AND source_report_type IN ('PRODUCT_IDENTITY_IMPORT','PRODUCT_IDENTITY');
--
--   DELETE FROM public.catalog_products
--   WHERE source_upload_id = '<upload_id>'::uuid
--     AND source_report_type IN ('PRODUCT_IDENTITY_IMPORT','PRODUCT_IDENTITY');
--
--   UPDATE public.raw_report_uploads
--   SET status = 'failed',
--       metadata = jsonb_set(COALESCE(metadata,'{}'::jsonb), '{manual_cleanup_note}',
--                            to_jsonb('Manual Product Identity cleanup performed after operator review'::text), true),
--       updated_at = now()
--   WHERE id = '<upload_id>'::uuid;
-- COMMIT;
