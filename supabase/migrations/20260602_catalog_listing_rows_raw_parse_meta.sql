-- Raw listing archive: explicit parse metadata + stable line hash (no behavior change to canonical layer).

BEGIN;

ALTER TABLE public.catalog_listing_rows_raw
  ADD COLUMN IF NOT EXISTS source_line_hash text,
  ADD COLUMN IF NOT EXISTS parse_status text NOT NULL DEFAULT 'parsed',
  ADD COLUMN IF NOT EXISTS parse_error text;

COMMENT ON COLUMN public.catalog_listing_rows_raw.source_line_hash IS
  'SHA-256 hex of the physical line bytes (UTF-8) for integrity / dedup diagnostics; not a business dedupe key.';

COMMENT ON COLUMN public.catalog_listing_rows_raw.parse_status IS
  'parsed | skipped_empty (not stored) | skipped_malformed — only stored rows use skipped_malformed.';

ALTER TABLE public.catalog_listing_rows_raw
  DROP CONSTRAINT IF EXISTS catalog_listing_rows_raw_parse_status_check;

ALTER TABLE public.catalog_listing_rows_raw
  ADD CONSTRAINT catalog_listing_rows_raw_parse_status_check
  CHECK (parse_status = ANY (ARRAY['parsed'::text, 'skipped_empty'::text, 'skipped_malformed'::text]));

NOTIFY pgrst, 'reload schema';

COMMIT;
