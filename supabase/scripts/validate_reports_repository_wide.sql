-- Validation after migration 20260704130000_amazon_reports_repository_wide_columns.sql
-- Replace UUID literals before running.

-- 1) Physical-line uniqueness (expect dup_groups = 0)
SELECT count(*) AS dup_groups
FROM (
  SELECT 1
  FROM public.amazon_reports_repository
  WHERE organization_id = '00000000-0000-0000-0000-000000000000'::uuid
  GROUP BY organization_id, source_file_sha256, source_physical_row_number
  HAVING count(*) > 1
) d;

-- 2) Indexes on amazon_reports_repository
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'amazon_reports_repository'
ORDER BY indexname;

-- 3) Per-upload smoke (wide columns populated)
-- SELECT
--   count(*) FILTER (WHERE product_sales IS NOT NULL) AS with_product_sales,
--   count(*) FILTER (WHERE transaction_status IS NOT NULL) AS with_tx_status,
--   count(*)
-- FROM public.amazon_reports_repository
-- WHERE organization_id = '...'::uuid AND upload_id = '...'::uuid;
