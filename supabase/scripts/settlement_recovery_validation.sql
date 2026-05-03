-- =============================================================================
-- Post-sync settlement validation (edit UUID literals)
-- =============================================================================
-- Run after successful SETTLEMENT sync for the canonical upload.
-- =============================================================================

-- Replace literals:
--   '<ORG_UUID>'
--   '<UPLOAD_UUID>'
--   content_sha256 from raw_report_uploads.metadata if you need file-scoped checks

-- 1) Staging count (should match pre-sync expectation if sync not yet cleared staging)
-- SELECT COUNT(*) AS staging_rows
-- FROM public.amazon_staging st
-- WHERE st.organization_id = '<ORG_UUID>'::uuid
--   AND st.upload_id = '<UPLOAD_UUID>'::uuid;

-- 2) Domain rows for this upload
-- SELECT COUNT(*) AS settlement_rows_for_upload
-- FROM public.amazon_settlements s
-- WHERE s.organization_id = '<ORG_UUID>'::uuid
--   AND s.upload_id = '<UPLOAD_UUID>'::uuid;

-- 3) Duplicate probe — must return 0 rows (matches sync ON CONFLICT key)
-- SELECT organization_id, source_file_sha256, source_physical_row_number, COUNT(*) AS n
-- FROM public.amazon_settlements
-- WHERE organization_id = '<ORG_UUID>'::uuid
--   AND upload_id = '<UPLOAD_UUID>'::uuid
-- GROUP BY 1, 2, 3
-- HAVING COUNT(*) > 1;

-- 4) Sample compare — 20 rows: staging row_number + raw excerpt vs domain
-- WITH st AS (
--   SELECT st.row_number, st.raw_row
--   FROM public.amazon_staging st
--   WHERE st.organization_id = '<ORG_UUID>'::uuid
--     AND st.upload_id = '<UPLOAD_UUID>'::uuid
--   ORDER BY st.row_number
--   LIMIT 20
-- )
-- SELECT st.row_number,
--        st.raw_row,
--        s.settlement_id,
--        s.order_id,
--        s.transaction_type,
--        s.amount_total,
--        s.posted_date,
--        s.source_physical_row_number,
--        s.raw_data
-- FROM st
-- LEFT JOIN public.amazon_settlements s
--   ON s.organization_id = '<ORG_UUID>'::uuid
--  AND s.upload_id = '<UPLOAD_UUID>'::uuid
--  AND s.source_physical_row_number = st.row_number;

SELECT 'settlement_recovery_validation_sql_loaded' AS hint,
       'Uncomment and parameterize queries above' AS next_step;
