-- Layer 5 prep: product catalog from listing exports (Category / All / Active Listings).
-- Not derived from removal or shipment data.

BEGIN;

CREATE TABLE IF NOT EXISTS public.catalog_products (
  id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id      uuid NOT NULL,
  store_id             uuid REFERENCES public.stores (id) ON DELETE SET NULL,
  source_report_type   text,
  seller_sku           text,
  asin                 text,
  fnsku                text,
  item_name            text,
  item_description     text,
  fulfillment_channel  text,
  listing_status       text,
  price                numeric,
  quantity             integer,
  open_date            timestamptz,
  raw_payload          jsonb NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at        timestamptz NOT NULL DEFAULT now(),
  last_seen_at         timestamptz NOT NULL DEFAULT now(),
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.catalog_products IS
  'Amazon listing/catalog exports (Category, All, or Active Listings). Identity is org + store + seller_sku + asin; not joined to removals in this layer.';

COMMENT ON COLUMN public.catalog_products.source_report_type IS
  'Origin shape: category_listings | all_listings | active_listings (from raw_report_uploads.report_type).';

COMMENT ON COLUMN public.catalog_products.raw_payload IS
  'Full normalized CSV row (string values) for audit and future column adds.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_catalog_products_canonical_identity
  ON public.catalog_products (
    organization_id,
    store_id,
    seller_sku,
    asin
  )
  NULLS NOT DISTINCT;

CREATE INDEX IF NOT EXISTS idx_catalog_products_org_seller_sku
  ON public.catalog_products (organization_id, seller_sku)
  WHERE seller_sku IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_catalog_products_org_asin
  ON public.catalog_products (organization_id, asin)
  WHERE asin IS NOT NULL;

CREATE OR REPLACE FUNCTION public.trg_catalog_products_seen_and_updated()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $fn$
BEGIN
  IF tg_op = 'INSERT' THEN
    new.first_seen_at := coalesce(new.first_seen_at, now());
    new.last_seen_at := coalesce(new.last_seen_at, now());
  ELSIF tg_op = 'UPDATE' THEN
    new.first_seen_at := old.first_seen_at;
    new.created_at := old.created_at;
    new.last_seen_at := now();
  END IF;
  new.updated_at := now();
  RETURN new;
END;
$fn$;

DROP TRIGGER IF EXISTS trg_catalog_products_seen ON public.catalog_products;
CREATE TRIGGER trg_catalog_products_seen
  BEFORE INSERT OR UPDATE ON public.catalog_products
  FOR EACH ROW
  EXECUTE FUNCTION public.trg_catalog_products_seen_and_updated();

ALTER TABLE public.catalog_products ENABLE ROW LEVEL SECURITY;

CREATE POLICY IF NOT EXISTS "catalog_products: org members can select"
  ON public.catalog_products
  FOR SELECT
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "catalog_products: org members can insert"
  ON public.catalog_products
  FOR INSERT
  WITH CHECK (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "catalog_products: org members can update"
  ON public.catalog_products
  FOR UPDATE
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "catalog_products: org members can delete"
  ON public.catalog_products
  FOR DELETE
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "catalog_products: service role bypass"
  ON public.catalog_products
  AS PERMISSIVE
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- Optional SQL-side merge for scripts / backfills (same identity as app upsert).
CREATE OR REPLACE FUNCTION public.merge_catalog_product_from_listing_json(
  p_organization_id uuid,
  p_store_id uuid,
  p_source_report_type text,
  p_row jsonb
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
    raw_payload
  )
  VALUES (
    p_organization_id,
    p_store_id,
    p_source_report_type,
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
    coalesce(p_row, '{}'::jsonb)
  )
  ON CONFLICT (organization_id, store_id, seller_sku, asin)
  DO UPDATE SET
    source_report_type = coalesce(excluded.source_report_type, public.catalog_products.source_report_type),
    fnsku              = coalesce(excluded.fnsku, public.catalog_products.fnsku),
    item_name          = coalesce(excluded.item_name, public.catalog_products.item_name),
    item_description   = coalesce(excluded.item_description, public.catalog_products.item_description),
    fulfillment_channel = coalesce(excluded.fulfillment_channel, public.catalog_products.fulfillment_channel),
    listing_status     = coalesce(excluded.listing_status, public.catalog_products.listing_status),
    price              = coalesce(excluded.price, public.catalog_products.price),
    quantity           = coalesce(excluded.quantity, public.catalog_products.quantity),
    open_date          = coalesce(excluded.open_date, public.catalog_products.open_date),
    raw_payload        = public.catalog_products.raw_payload || excluded.raw_payload
  RETURNING id INTO v_id;

  RETURN v_id;
END;
$fn$;

COMMENT ON FUNCTION public.merge_catalog_product_from_listing_json(uuid, uuid, text, jsonb) IS
  'Idempotent upsert from one normalized listing row (JSON keys like seller-sku, asin1). Returns NULL if seller_sku or asin missing.';

GRANT EXECUTE ON FUNCTION public.merge_catalog_product_from_listing_json(uuid, uuid, text, jsonb) TO service_role;

-- Extend raw_report_uploads.report_type CHECK for listing exports
ALTER TABLE public.raw_report_uploads
  DROP CONSTRAINT IF EXISTS raw_report_uploads_report_type_check;

ALTER TABLE public.raw_report_uploads
  ADD CONSTRAINT raw_report_uploads_report_type_check CHECK (
    report_type = ANY (ARRAY[
      'FBA_RETURNS'::text,
      'REMOVAL_ORDER'::text,
      'INVENTORY_LEDGER'::text,
      'REIMBURSEMENTS'::text,
      'SETTLEMENT'::text,
      'SAFET_CLAIMS'::text,
      'TRANSACTIONS'::text,
      'REPORTS_REPOSITORY'::text,
      'UNKNOWN'::text,
      'fba_customer_returns'::text,
      'reimbursements'::text,
      'inventory_ledger'::text,
      'safe_t_claims'::text,
      'transaction_view'::text,
      'settlement_repository'::text,
      'REMOVAL_SHIPMENT'::text,
      'CATEGORY_LISTINGS'::text,
      'ALL_LISTINGS'::text,
      'ACTIVE_LISTINGS'::text
    ])
  );

NOTIFY pgrst, 'reload schema';

COMMIT;
