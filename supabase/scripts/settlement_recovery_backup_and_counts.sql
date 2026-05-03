-- =============================================================================
-- Settlement recovery — GUARDED backup + counts (run manually in SQL editor)
-- =============================================================================
-- Replace the three UUID literals below, then run section-by-section.
-- Order: counts → CREATE backup → INSERT backup → verify → DELETE (last).
-- =============================================================================

-- ── 0) Parameters — edit literals only ───────────────────────────────────────
-- organization_id for scoped queries
-- upload_id for staging count + optional scoped delete

DO $$
DECLARE
  v_org uuid := NULL;  -- e.g. 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid;
  v_upload uuid := NULL;  -- e.g. '11111111-2222-3333-4444-555555555555'::uuid;
BEGIN
  IF v_org IS NULL THEN
    RAISE NOTICE 'Edit v_org / v_upload in this DO block, or run the standalone SELECTs below with literals.';
  END IF;
END $$;

-- ── 1) Row counts (always run first) ─────────────────────────────────────────
SELECT 'amazon_settlements_total' AS label, COUNT(*)::bigint AS n
FROM public.amazon_settlements;

-- Staging rows for your upload (mirror app: organization_id + upload_id):
-- SELECT 'amazon_staging_for_upload' AS label, COUNT(*)::bigint AS n
-- FROM public.amazon_staging st
-- WHERE st.organization_id = '<ORG_UUID>'::uuid
--   AND st.upload_id = '<UPLOAD_UUID>'::uuid;

-- Provenance sample: uploads contributing settlement rows for an org
-- SELECT s.upload_id, rr.file_name, rr.report_type,
--        COUNT(*)::bigint AS settlement_rows
-- FROM public.amazon_settlements s
-- JOIN public.raw_report_uploads rr ON rr.id = s.upload_id
-- WHERE s.organization_id = '<ORG_UUID>'::uuid
-- GROUP BY 1, 2, 3
-- ORDER BY settlement_rows DESC;

-- ── 2) Backup table (required before any DELETE on amazon_settlements) ───────
-- Fixed audit name from recovery plan. First run creates structure only:
CREATE TABLE IF NOT EXISTS public.amazon_settlements_backup_before_new_mapping AS
SELECT *
FROM public.amazon_settlements
WHERE false;

-- Full snapshot (uncomment when ready; re-run TRUNCATE if you need a fresh copy):
-- TRUNCATE public.amazon_settlements_backup_before_new_mapping;
-- INSERT INTO public.amazon_settlements_backup_before_new_mapping
-- SELECT * FROM public.amazon_settlements;

SELECT 'backup_row_count' AS label, COUNT(*)::bigint AS n
FROM public.amazon_settlements_backup_before_new_mapping;

SELECT 'live_row_count' AS label, COUNT(*)::bigint AS n
FROM public.amazon_settlements;

-- ── 3) Scoped DELETE templates (ONLY after backup row_count = live_row_count) ─
-- DELETE FROM public.amazon_settlements s
-- WHERE s.organization_id = '<ORG_UUID>'::uuid
--   AND s.upload_id = '<BAD_UPLOAD_UUID>'::uuid;
