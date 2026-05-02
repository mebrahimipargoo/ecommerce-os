-- validate_product_identity_pipeline.sql
--
-- Six validation queries for the new Phase 2 / Phase 3 Product Identity pipeline.
-- All SELECT-only — safe to run anytime.
-- After running these you should see:
--   query 1: latest upload with status='staged' (after Process) or 'synced' (after Sync)
--   query 2: staging rows for the latest upload (non-zero after Process, 0 after Sync)
--   query 3: products/catalog/map rows written by the latest upload
--   query 4: staging rows that exist but have not yet been synced to final tables
--   query 5: no uploads stuck in processing longer than 10 minutes
--   query 6: final tables not written during Process (products should have 0 rows
--             with source_upload_id matching an upload whose raw_report_uploads.status='processing')
--
-- Replace '00000000-0000-0000-0000-000000000001' with actual org or leave to scan all.

-- ──────────────────────────────────────────────────────────────────────────
-- 1. Latest Product Identity upload status + FPS progress
-- ──────────────────────────────────────────────────────────────────────────
WITH latest_pi AS (
  SELECT id
  FROM public.raw_report_uploads
  WHERE report_type = 'PRODUCT_IDENTITY'
  ORDER BY created_at DESC
  LIMIT 1
)
SELECT
  ru.id                             AS upload_id,
  ru.organization_id,
  ru.report_type,
  ru.status                         AS upload_status,
  ru.created_at,
  ru.updated_at,
  fps.status                        AS fps_status,
  fps.current_phase,
  fps.current_phase_label,
  fps.upload_pct,
  fps.phase2_stage_pct,
  fps.process_pct,
  fps.phase3_raw_sync_pct,
  fps.sync_pct,
  fps.phase2_status,
  fps.phase3_status,
  fps.total_rows,
  fps.processed_rows,
  fps.staged_rows_written,
  fps.raw_rows_written,
  fps.next_action_key,
  fps.error_message,
  fps.import_metrics #>> '{rows_staged}'            AS rows_staged,
  fps.import_metrics #>> '{unique_product_sku_count}' AS unique_product_sku_count,
  fps.import_metrics #>> '{products_upserted}'      AS products_upserted,
  fps.import_metrics #>> '{identifiers_upserted}'   AS identifiers_upserted
FROM public.raw_report_uploads ru
LEFT JOIN public.file_processing_status fps ON fps.upload_id = ru.id
WHERE ru.id IN (SELECT id FROM latest_pi);

-- ──────────────────────────────────────────────────────────────────────────
-- 2. Staging row count by upload_id (non-zero after Process, zero after Sync
--    if staging rows are NOT purged; they stay for audit)
-- ──────────────────────────────────────────────────────────────────────────
WITH latest_pi AS (
  SELECT id, organization_id
  FROM public.raw_report_uploads
  WHERE report_type = 'PRODUCT_IDENTITY'
  ORDER BY created_at DESC
  LIMIT 5
)
SELECT
  pi.id                                AS upload_id,
  pi.organization_id,
  COUNT(psr.id)                        AS staging_row_count,
  COUNT(psr.id) FILTER (WHERE psr.seller_sku IS NULL) AS staging_rows_no_sku,
  COUNT(psr.id) FILTER (WHERE psr.seller_sku IS NOT NULL) AS staging_rows_with_sku
FROM latest_pi pi
LEFT JOIN public.product_identity_staging_rows psr
  ON psr.upload_id = pi.id
  AND psr.organization_id = pi.organization_id
GROUP BY pi.id, pi.organization_id
ORDER BY pi.id DESC;

-- ──────────────────────────────────────────────────────────────────────────
-- 3. Final table counts by upload_id (written during Phase 3 — Sync)
-- ──────────────────────────────────────────────────────────────────────────
WITH latest_pi AS (
  SELECT id, organization_id,
         COALESCE(NULLIF(metadata ->> 'import_store_id','')::uuid,
                  NULLIF(metadata ->> 'ledger_store_id','')::uuid) AS store_id
  FROM public.raw_report_uploads
  WHERE report_type = 'PRODUCT_IDENTITY'
  ORDER BY created_at DESC
  LIMIT 5
)
SELECT
  pi.id                                                            AS upload_id,
  pi.organization_id,
  pi.store_id,
  COUNT(DISTINCT cp.id)                                            AS catalog_products,
  COUNT(DISTINCT pim.id)                                           AS identifier_map_rows,
  COUNT(DISTINCT p.id) FILTER (
    WHERE p.metadata -> 'product_identity_import' ->> 'source_upload_id' = pi.id::text
  )                                                                AS products_touched
FROM latest_pi pi
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
 AND p.store_id = pi.store_id
GROUP BY pi.id, pi.organization_id, pi.store_id
ORDER BY pi.id DESC;

-- ──────────────────────────────────────────────────────────────────────────
-- 4. Rows staged but not yet synced
--    An upload in status='staged' should have staging rows but 0 final rows.
--    An upload in status='synced' should have both staging rows AND final rows
--    (staging is kept for audit).
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  ru.id                           AS upload_id,
  ru.status                       AS upload_status,
  COUNT(psr.id)                   AS staging_rows,
  COUNT(cp.id)                    AS catalog_products_written
FROM public.raw_report_uploads ru
JOIN public.product_identity_staging_rows psr ON psr.upload_id = ru.id
LEFT JOIN public.catalog_products cp
  ON cp.organization_id = ru.organization_id
 AND cp.source_upload_id = ru.id
 AND cp.source_report_type IN ('PRODUCT_IDENTITY_IMPORT','PRODUCT_IDENTITY')
WHERE ru.report_type = 'PRODUCT_IDENTITY'
GROUP BY ru.id, ru.status
HAVING COUNT(psr.id) > 0 AND COUNT(cp.id) = 0
ORDER BY ru.updated_at DESC;

-- ──────────────────────────────────────────────────────────────────────────
-- 5. Stuck processing uploads older than 10 minutes
--    Must return 0 rows when the pipeline is healthy.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  ru.id                               AS upload_id,
  ru.organization_id,
  ru.report_type,
  ru.status,
  ru.import_pipeline_started_at,
  EXTRACT(EPOCH FROM (now() - ru.import_pipeline_started_at)) / 60.0 AS minutes_stuck,
  fps.current_phase,
  fps.current_phase_label,
  fps.phase2_stage_pct,
  fps.phase3_raw_sync_pct,
  fps.error_message
FROM public.raw_report_uploads ru
LEFT JOIN public.file_processing_status fps ON fps.upload_id = ru.id
WHERE ru.status = 'processing'
  AND ru.report_type = 'PRODUCT_IDENTITY'
  AND ru.import_pipeline_started_at < now() - interval '10 minutes'
ORDER BY ru.import_pipeline_started_at;

-- ──────────────────────────────────────────────────────────────────────────
-- 6. Confirm Process does NOT write final tables before Sync
--    Checks that no catalog_products / product_identifier_map rows with
--    source_upload_id matching an upload that is still 'processing' or
--    'staged' (has not yet reached 'synced') exist.
--    Must return 0 rows.
-- ──────────────────────────────────────────────────────────────────────────
SELECT
  ru.id           AS upload_id,
  ru.status       AS upload_status,
  'catalog_products' AS final_table,
  COUNT(cp.id)    AS rows_written_before_sync
FROM public.raw_report_uploads ru
JOIN public.catalog_products cp
  ON cp.organization_id = ru.organization_id
 AND cp.source_upload_id = ru.id
 AND cp.source_report_type IN ('PRODUCT_IDENTITY_IMPORT','PRODUCT_IDENTITY')
WHERE ru.report_type = 'PRODUCT_IDENTITY'
  AND ru.status IN ('processing','staged')
GROUP BY ru.id, ru.status
HAVING COUNT(cp.id) > 0

UNION ALL

SELECT
  ru.id           AS upload_id,
  ru.status       AS upload_status,
  'product_identifier_map' AS final_table,
  COUNT(pim.id)   AS rows_written_before_sync
FROM public.raw_report_uploads ru
JOIN public.product_identifier_map pim
  ON pim.organization_id = ru.organization_id
 AND pim.source_upload_id = ru.id
 AND pim.source_report_type IN ('PRODUCT_IDENTITY_IMPORT','PRODUCT_IDENTITY')
WHERE ru.report_type = 'PRODUCT_IDENTITY'
  AND ru.status IN ('processing','staged')
GROUP BY ru.id, ru.status
HAVING COUNT(pim.id) > 0

ORDER BY upload_id, final_table;
