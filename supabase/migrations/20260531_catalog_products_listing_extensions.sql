-- Layer 5: additional listing columns + documentation of canonical dedupe key.
-- Duplicate prevention: one row per (organization_id, store_id, seller_sku, asin) with NULLS NOT DISTINCT.

BEGIN;

ALTER TABLE public.catalog_products
  ADD COLUMN IF NOT EXISTS source_upload_id uuid
    REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS listing_id text,
  ADD COLUMN IF NOT EXISTS product_id text,
  ADD COLUMN IF NOT EXISTS product_id_type text,
  ADD COLUMN IF NOT EXISTS item_condition text,
  ADD COLUMN IF NOT EXISTS merchant_shipping_group text;

COMMENT ON COLUMN public.catalog_products.source_upload_id IS
  'raw_report_uploads.id for the listing file that last wrote this row (nullable for legacy rows).';

COMMENT ON COLUMN public.catalog_products.listing_id IS
  'Amazon listing id when present in the export (e.g. listing-id).';

COMMENT ON COLUMN public.catalog_products.product_id IS
  'Amazon product-id column when distinct from ASIN (e.g. product-id in flat files).';

COMMENT ON COLUMN public.catalog_products.product_id_type IS
  'product-id-type from the listing export when present.';

COMMENT ON COLUMN public.catalog_products.item_condition IS
  'Item condition from the listing export when present.';

COMMENT ON COLUMN public.catalog_products.merchant_shipping_group IS
  'merchant-shipping-group (or equivalent) from the listing export when present.';

COMMENT ON INDEX public.uq_catalog_products_canonical_identity IS
  'Prevents duplicate catalog rows: uniqueness on (organization_id, store_id, seller_sku, asin) with NULLS NOT DISTINCT. '
  'Multiple source lines mapping to the same key collapse to one row via ON CONFLICT upsert.';

DROP FUNCTION IF EXISTS public.merge_catalog_product_from_listing_json(uuid, uuid, text, jsonb);

CREATE OR REPLACE FUNCTION public.merge_catalog_product_from_listing_json(
  p_organization_id uuid,
  p_store_id uuid,
  p_source_report_type text,
  p_row jsonb,
  p_source_upload_id uuid DEFAULT NULL
)
RETURNS uuid
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $fn$
DECLARE
  v_sku text := nullif(
    trim(both from coalesce(p_row->>'seller-sku', p_row->>'seller_sku', p_row->>'sku', '')),
    ''
  );
  v_asin text := nullif(
    trim(both from coalesce(p_row->>'asin1', p_row->>'asin', '')),
    ''
  );
  v_id uuid;
BEGIN
  IF v_sku IS NULL OR v_asin IS NULL THEN
    RETURN NULL;
  END IF;

  INSERT INTO public.catalog_products (
    organization_id,
    store_id,
    source_report_type,
    source_upload_id,
    seller_sku,
    asin,
    fnsku,
    item_name,
    item_description,
    fulfillment_channel,
    listing_status,
    price,
    quantity,
    open_date,
    listing_id,
    product_id,
    product_id_type,
    item_condition,
    merchant_shipping_group,
    raw_payload
  )
  VALUES (
    p_organization_id,
    p_store_id,
    p_source_report_type,
    p_source_upload_id,
    v_sku,
    v_asin,
    nullif(trim(both from coalesce(p_row->>'fnsku', '')), ''),
    nullif(trim(both from coalesce(p_row->>'item-name', p_row->>'item_name', '')), ''),
    nullif(trim(both from coalesce(p_row->>'item-description', p_row->>'item_description', '')), ''),
    nullif(trim(both from coalesce(p_row->>'fulfillment-channel', p_row->>'fulfillment_channel', '')), ''),
    nullif(trim(both from coalesce(p_row->>'status', '')), ''),
    nullif(trim(both from coalesce(p_row->>'price', '')), '')::numeric,
    nullif(trim(both from coalesce(p_row->>'quantity', '')), '')::integer,
    nullif(trim(both from coalesce(p_row->>'open-date', p_row->>'open_date', '')), '')::timestamptz,
    nullif(trim(both from coalesce(p_row->>'listing-id', p_row->>'listing_id', '')), ''),
    nullif(trim(both from coalesce(p_row->>'product-id', p_row->>'product_id', '')), ''),
    nullif(trim(both from coalesce(p_row->>'product-id-type', p_row->>'product_id_type', '')), ''),
    nullif(trim(both from coalesce(p_row->>'item-condition', p_row->>'item_condition', '')), ''),
    nullif(trim(both from coalesce(p_row->>'merchant-shipping-group', p_row->>'merchant_shipping_group', '')), ''),
    coalesce(p_row, '{}'::jsonb)
  )
  ON CONFLICT (organization_id, store_id, seller_sku, asin)
  DO UPDATE SET
    source_report_type = coalesce(excluded.source_report_type, public.catalog_products.source_report_type),
    source_upload_id   = coalesce(excluded.source_upload_id, public.catalog_products.source_upload_id),
    fnsku              = coalesce(excluded.fnsku, public.catalog_products.fnsku),
    item_name          = coalesce(excluded.item_name, public.catalog_products.item_name),
    item_description   = coalesce(excluded.item_description, public.catalog_products.item_description),
    fulfillment_channel = coalesce(excluded.fulfillment_channel, public.catalog_products.fulfillment_channel),
    listing_status     = coalesce(excluded.listing_status, public.catalog_products.listing_status),
    price              = coalesce(excluded.price, public.catalog_products.price),
    quantity           = coalesce(excluded.quantity, public.catalog_products.quantity),
    open_date          = coalesce(excluded.open_date, public.catalog_products.open_date),
    listing_id         = coalesce(excluded.listing_id, public.catalog_products.listing_id),
    product_id         = coalesce(excluded.product_id, public.catalog_products.product_id),
    product_id_type    = coalesce(excluded.product_id_type, public.catalog_products.product_id_type),
    item_condition     = coalesce(excluded.item_condition, public.catalog_products.item_condition),
    merchant_shipping_group = coalesce(excluded.merchant_shipping_group, public.catalog_products.merchant_shipping_group),
    raw_payload        = public.catalog_products.raw_payload || excluded.raw_payload
  RETURNING id INTO v_id;

  RETURN v_id;
END;
$fn$;

COMMENT ON FUNCTION public.merge_catalog_product_from_listing_json(uuid, uuid, text, jsonb, uuid) IS
  'Idempotent upsert from one normalized listing row (JSON keys like seller-sku, asin1). Optional p_source_upload_id tags provenance. Returns NULL if seller_sku or asin missing.';

GRANT EXECUTE ON FUNCTION public.merge_catalog_product_from_listing_json(uuid, uuid, text, jsonb, uuid) TO service_role;

NOTIFY pgrst, 'reload schema';

COMMIT;
