-- Run after migration 20260703120000_drop_ledger_final_unique_idx.sql is applied.
-- Replace org / upload UUIDs where noted. Expect: no ledger_final_unique_idx;
-- uq_amazon_inventory_ledger_org_file_row present; dup_groups = 0.

-- 1) Index sanity (ledger_final_unique_idx should be absent)
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'amazon_inventory_ledger'
ORDER BY indexname;

-- 2) Staging rows for one upload (set upload_id)
-- SELECT count(*) AS staging_rows
-- FROM public.amazon_staging
-- WHERE organization_id = 'YOUR_ORG_UUID'::uuid
--   AND upload_id = 'YOUR_UPLOAD_UUID'::uuid;

-- 3) Landed ledger rows for same upload
-- SELECT count(*) AS ledger_rows
-- FROM public.amazon_inventory_ledger
-- WHERE organization_id = 'YOUR_ORG_UUID'::uuid
--   AND upload_id = 'YOUR_UPLOAD_UUID'::uuid;

-- 4) Duplicate groups on physical-line key (expect 0)
SELECT count(*) AS dup_groups
FROM (
  SELECT 1
  FROM public.amazon_inventory_ledger
  WHERE organization_id = 'YOUR_ORG_UUID'::uuid
  GROUP BY organization_id, source_file_sha256, source_physical_row_number
  HAVING count(*) > 1
) d;
