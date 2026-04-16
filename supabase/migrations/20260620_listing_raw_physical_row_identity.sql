-- LISTING raw archive: idempotent identity is (organization_id, source_file_sha256, source_physical_row_number)
-- so the same physical file bytes re-imported under a new upload_id do not stack duplicate raw rows.

BEGIN;

ALTER TABLE public.amazon_listing_report_rows_raw
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;

UPDATE public.amazon_listing_report_rows_raw AS t
SET
  source_file_sha256 = COALESCE(
    (
      SELECT NULLIF(lower(trim(rr.metadata->>'content_sha256')), '')
      FROM public.raw_report_uploads rr
      WHERE rr.id = t.source_upload_id
    ),
    'legacy-upload-' || t.source_upload_id::text
  ),
  source_physical_row_number = t.row_number
WHERE t.source_file_sha256 IS NULL
   OR t.source_physical_row_number IS NULL;

DELETE FROM public.amazon_listing_report_rows_raw AS a
USING public.amazon_listing_report_rows_raw AS b
WHERE a.id > b.id
  AND a.organization_id = b.organization_id
  AND a.source_file_sha256 = b.source_file_sha256
  AND a.source_physical_row_number = b.source_physical_row_number;

DROP INDEX IF EXISTS public.uq_amazon_listing_report_rows_raw_upload_row;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_listing_report_rows_raw_org_file_physical_row
  ON public.amazon_listing_report_rows_raw (organization_id, source_file_sha256, source_physical_row_number);

ALTER TABLE public.amazon_listing_report_rows_raw
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;

COMMENT ON INDEX public.uq_amazon_listing_report_rows_raw_org_file_physical_row IS
  'One raw listing line per org + file SHA-256 + physical CSV line index (header = line 1; first data row = 2).';

NOTIFY pgrst, 'reload schema';

COMMIT;
