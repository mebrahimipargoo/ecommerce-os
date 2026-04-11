-- Layer: raw archive for listing CSV rows (one row per source line, no business dedupe).
-- Canonical merge remains on public.catalog_products (unique org + store + seller_sku + asin, NULLS NOT DISTINCT).

BEGIN;

CREATE TABLE IF NOT EXISTS public.catalog_listing_rows_raw (
  id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id      uuid NOT NULL,
  store_id             uuid REFERENCES public.stores (id) ON DELETE SET NULL,
  source_upload_id     uuid NOT NULL REFERENCES public.raw_report_uploads (id) ON DELETE CASCADE,
  source_report_type   text NOT NULL,
  row_number           integer NOT NULL,
  seller_sku           text,
  asin                 text,
  listing_id           text,
  raw_payload          jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at           timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.catalog_listing_rows_raw IS
  'Raw listing export lines: one row per parsed CSV line for audit and API-first replay. No dedupe; canonical identity lives on catalog_products.';

COMMENT ON COLUMN public.catalog_listing_rows_raw.raw_payload IS
  'Normalized column mapping applied (same shape as listing mappers use).';

CREATE INDEX IF NOT EXISTS idx_catalog_listing_rows_raw_org_upload
  ON public.catalog_listing_rows_raw (organization_id, source_upload_id);

CREATE INDEX IF NOT EXISTS idx_catalog_listing_rows_raw_org_seller_sku
  ON public.catalog_listing_rows_raw (organization_id, seller_sku)
  WHERE seller_sku IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_catalog_listing_rows_raw_org_asin
  ON public.catalog_listing_rows_raw (organization_id, asin)
  WHERE asin IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_catalog_listing_rows_raw_upload_row
  ON public.catalog_listing_rows_raw (source_upload_id, row_number);

ALTER TABLE public.catalog_listing_rows_raw ENABLE ROW LEVEL SECURITY;

CREATE POLICY IF NOT EXISTS "catalog_listing_rows_raw: org members can select"
  ON public.catalog_listing_rows_raw
  FOR SELECT
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "catalog_listing_rows_raw: org members can insert"
  ON public.catalog_listing_rows_raw
  FOR INSERT
  WITH CHECK (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "catalog_listing_rows_raw: org members can update"
  ON public.catalog_listing_rows_raw
  FOR UPDATE
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "catalog_listing_rows_raw: org members can delete"
  ON public.catalog_listing_rows_raw
  FOR DELETE
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "catalog_listing_rows_raw: service role bypass"
  ON public.catalog_listing_rows_raw
  AS PERMISSIVE
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

NOTIFY pgrst, 'reload schema';

COMMIT;
