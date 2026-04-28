-- 20260635_product_identity_enrichment_priority.sql
--
-- Per-field enrichment priority + provenance for Product Identity / All Listings
-- / Manage FBA Inventory. Adds two columns and three idempotent merge
-- functions. All writes use the same pattern:
--   * never overwrite a trusted non-null value with NULL
--   * never overwrite a value whose recorded source has higher priority
--   * always record (source, priority, written_at, confidence) in field_provenance
--
-- Source priority by field family
-- ─────────────────────────────────
--                                        priority (higher = more trusted)
--   products.product_name                product_identity   = 100
--                                        all_listings       =  60
--                                        active_listings    =  60
--                                        category_listings  =  60
--                                        inventory_ledger   =  40
--                                        backfill / unknown =  10
--
--   products.vendor_name                 product_identity   = 100   (only authoritative source today)
--   products.mfg_part_number             product_identity   = 100
--   products.upc_code                    product_identity   = 100
--                                        all_listings       =  50   (rarely populated, fill-only)
--
--   products.asin                        all_listings       = 100   (Amazon-assigned)
--                                        product_identity   =  90
--   products.fnsku                       fba_inventory      = 100
--                                        all_listings       =  80
--                                        product_identity   =  60
--
--   catalog_products.item_name           all_listings       = 100
--                                        active_listings    =  90
--                                        category_listings  =  85
--                                        product_identity   =  60
--
--   catalog_products.fnsku               fba_inventory      = 100
--                                        all_listings       =  80
--                                        active_listings    =  70
--                                        product_identity   =  60
--
--   catalog_products.item_condition      fba_inventory      = 100
--                                        all_listings       =  60
--
--   catalog_products.price / quantity /
--   listing_status / fulfillment_channel all_listings       = 100
--                                        active_listings    =  90
--                                        category_listings  =  60
--
-- The numeric scale is intentional (not 1..3) so a future report family can
-- slot in between two existing sources without a schema change.

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────
-- 1. Provenance columns (additive, NULL-safe).
-- ──────────────────────────────────────────────────────────────────────────
ALTER TABLE public.products
  ADD COLUMN IF NOT EXISTS field_provenance jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN public.products.field_provenance IS
  'Per-field provenance map written by the enrichment merge functions: '
  '{"vendor_name":{"source":"product_identity","priority":100,"written_at":"…","confidence":1.0}, …}. '
  'Used by merge functions to refuse downgrade overwrites.';

ALTER TABLE public.catalog_products
  ADD COLUMN IF NOT EXISTS field_provenance jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN public.catalog_products.field_provenance IS
  'Per-field provenance map; same shape as products.field_provenance. '
  'All Listings merges keep listing fields; FBA Inventory merges keep FNSKU/condition.';

-- ──────────────────────────────────────────────────────────────────────────
-- 2. Helper: should this incoming (source, priority) be allowed to overwrite
--    the existing field?
--    Rules:
--      a) NULL incoming  → never overwrite (preserve trusted non-null).
--      b) NULL existing  → always allow (fill the gap).
--      c) Otherwise allow only when incoming priority STRICTLY > existing
--         priority. Equal priority keeps the older value (stable / idempotent).
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.identity_merge_should_overwrite(
  p_existing_value     anyelement,
  p_existing_provenance jsonb,
  p_field_key          text,
  p_incoming_value     anyelement,
  p_incoming_priority  integer
)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog, public
AS $fn$
DECLARE
  v_existing_priority integer;
BEGIN
  IF p_incoming_value IS NULL THEN
    RETURN FALSE;
  END IF;
  IF p_existing_value IS NULL THEN
    RETURN TRUE;
  END IF;
  v_existing_priority := COALESCE(
    NULLIF(p_existing_provenance #>> ARRAY[p_field_key, 'priority'], '')::int,
    0
  );
  RETURN COALESCE(p_incoming_priority, 0) > v_existing_priority;
END;
$fn$;

COMMENT ON FUNCTION public.identity_merge_should_overwrite(anyelement, jsonb, text, anyelement, integer) IS
  'Returns TRUE only when the incoming value is non-null and its source priority '
  'strictly outranks the priority recorded in field_provenance for the same key.';

-- ──────────────────────────────────────────────────────────────────────────
-- 3. Helper: build a new provenance jsonb entry for one field.
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.identity_merge_build_provenance(
  p_field_key   text,
  p_source      text,
  p_priority    integer,
  p_confidence  numeric,
  p_upload_id   uuid
)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $fn$
  SELECT jsonb_build_object(
    p_field_key,
    jsonb_build_object(
      'source',     p_source,
      'priority',   p_priority,
      'confidence', COALESCE(p_confidence, 1.0),
      'upload_id',  p_upload_id,
      'written_at', to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
    )
  );
$fn$;

-- ──────────────────────────────────────────────────────────────────────────
-- 4. merge_product_identity_into_products — Product Identity CSV is the
--    authoritative source for vendor / mfg / upc / product_name. ASIN and
--    FNSKU are accepted at lower priority (filled only when missing or when
--    no listing/inventory source has supplied them yet).
--
--    Returns the affected products.id; NULL when sku is missing.
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.merge_product_identity_into_products(
  p_organization_id   uuid,
  p_store_id          uuid,
  p_sku               text,
  p_product_name      text,
  p_vendor_name       text,
  p_mfg_part_number   text,
  p_upc_code          text,
  p_asin              text,
  p_fnsku             text,
  p_source_upload_id  uuid,
  p_confidence        numeric DEFAULT 1.0
)
RETURNS uuid
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $fn$
DECLARE
  v_existing public.products;
  v_id uuid;
  v_now timestamptz := now();
  v_prov jsonb;
BEGIN
  IF p_sku IS NULL OR btrim(p_sku) = '' THEN
    RETURN NULL;
  END IF;

  SELECT * INTO v_existing
  FROM public.products
  WHERE organization_id = p_organization_id
    AND store_id        = p_store_id
    AND sku             = p_sku;

  IF v_existing.id IS NULL THEN
    -- New row: product_identity supplies everything it has, at full priority.
    v_prov := '{}'::jsonb
      || public.identity_merge_build_provenance('product_name',     'product_identity', 100, p_confidence, p_source_upload_id)
      || public.identity_merge_build_provenance('vendor_name',      'product_identity', 100, p_confidence, p_source_upload_id)
      || public.identity_merge_build_provenance('mfg_part_number',  'product_identity', 100, p_confidence, p_source_upload_id)
      || public.identity_merge_build_provenance('upc_code',         'product_identity', 100, p_confidence, p_source_upload_id)
      || public.identity_merge_build_provenance('asin',             'product_identity',  90, p_confidence, p_source_upload_id)
      || public.identity_merge_build_provenance('fnsku',            'product_identity',  60, p_confidence, p_source_upload_id);

    INSERT INTO public.products (
      organization_id, store_id, sku,
      product_name, vendor_name, mfg_part_number, upc_code, asin, fnsku,
      field_provenance,
      last_seen_at, last_catalog_sync_at
    )
    VALUES (
      p_organization_id, p_store_id, p_sku,
      COALESCE(p_product_name, p_sku),
      p_vendor_name, p_mfg_part_number, p_upc_code, p_asin, p_fnsku,
      v_prov,
      v_now, v_now
    )
    RETURNING id INTO v_id;
    RETURN v_id;
  END IF;

  -- Existing row: per-field merge with priority guard.
  v_prov := COALESCE(v_existing.field_provenance, '{}'::jsonb);

  IF public.identity_merge_should_overwrite(v_existing.product_name, v_prov, 'product_name', p_product_name, 100) THEN
    UPDATE public.products
       SET product_name = p_product_name,
           field_provenance = v_prov || public.identity_merge_build_provenance('product_name', 'product_identity', 100, p_confidence, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
    SELECT field_provenance INTO v_prov FROM public.products WHERE id = v_existing.id;
  END IF;

  IF public.identity_merge_should_overwrite(v_existing.vendor_name, v_prov, 'vendor_name', p_vendor_name, 100) THEN
    UPDATE public.products
       SET vendor_name = p_vendor_name,
           field_provenance = v_prov || public.identity_merge_build_provenance('vendor_name', 'product_identity', 100, p_confidence, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
    SELECT field_provenance INTO v_prov FROM public.products WHERE id = v_existing.id;
  END IF;

  IF public.identity_merge_should_overwrite(v_existing.mfg_part_number, v_prov, 'mfg_part_number', p_mfg_part_number, 100) THEN
    UPDATE public.products
       SET mfg_part_number = p_mfg_part_number,
           field_provenance = v_prov || public.identity_merge_build_provenance('mfg_part_number', 'product_identity', 100, p_confidence, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
    SELECT field_provenance INTO v_prov FROM public.products WHERE id = v_existing.id;
  END IF;

  IF public.identity_merge_should_overwrite(v_existing.upc_code, v_prov, 'upc_code', p_upc_code, 100) THEN
    UPDATE public.products
       SET upc_code = p_upc_code,
           field_provenance = v_prov || public.identity_merge_build_provenance('upc_code', 'product_identity', 100, p_confidence, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
    SELECT field_provenance INTO v_prov FROM public.products WHERE id = v_existing.id;
  END IF;

  -- ASIN/FNSKU: lower priority for product identity; only fill when missing
  -- or when current source is weaker.
  IF public.identity_merge_should_overwrite(v_existing.asin, v_prov, 'asin', p_asin, 90) THEN
    UPDATE public.products
       SET asin = p_asin,
           field_provenance = v_prov || public.identity_merge_build_provenance('asin', 'product_identity', 90, p_confidence, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
    SELECT field_provenance INTO v_prov FROM public.products WHERE id = v_existing.id;
  END IF;

  IF public.identity_merge_should_overwrite(v_existing.fnsku, v_prov, 'fnsku', p_fnsku, 60) THEN
    UPDATE public.products
       SET fnsku = p_fnsku,
           field_provenance = v_prov || public.identity_merge_build_provenance('fnsku', 'product_identity', 60, p_confidence, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
  END IF;

  -- Always bump last_catalog_sync_at so external dashboards know the row was touched.
  UPDATE public.products
     SET last_catalog_sync_at = v_now
   WHERE id = v_existing.id;

  RETURN v_existing.id;
END;
$fn$;

COMMENT ON FUNCTION public.merge_product_identity_into_products(uuid, uuid, text, text, text, text, text, text, text, uuid, numeric) IS
  'Per-field merge of one Product Identity CSV row into public.products. Never overwrites a trusted non-null with NULL, never downgrades a higher-priority field. Records provenance.';

GRANT EXECUTE ON FUNCTION public.merge_product_identity_into_products(uuid, uuid, text, text, text, text, text, text, text, uuid, numeric) TO service_role;

-- ──────────────────────────────────────────────────────────────────────────
-- 5. merge_listing_into_catalog_products_v2 — All Listings / Active Listings /
--    Category Listings sync. Owns item_name, listing_status, price, quantity,
--    fulfillment_channel, listing_id; lower priority for FNSKU and condition.
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.merge_listing_into_catalog_products_v2(
  p_organization_id   uuid,
  p_store_id          uuid,
  p_source_report_type text,        -- 'all_listings' | 'active_listings' | 'category_listings'
  p_seller_sku        text,
  p_asin              text,
  p_fnsku             text,
  p_item_name         text,
  p_item_description  text,
  p_fulfillment_channel text,
  p_listing_status    text,
  p_price             numeric,
  p_quantity          integer,
  p_open_date         timestamptz,
  p_listing_id        text,
  p_product_id        text,
  p_product_id_type   text,
  p_item_condition    text,
  p_merchant_shipping_group text,
  p_raw_payload       jsonb,
  p_source_upload_id  uuid
)
RETURNS uuid
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $fn$
DECLARE
  v_existing public.catalog_products;
  v_id uuid;
  v_now timestamptz := now();
  v_prov jsonb;
  v_listing_priority integer := CASE p_source_report_type
    WHEN 'all_listings'      THEN 100
    WHEN 'active_listings'   THEN  90
    WHEN 'category_listings' THEN  85
    ELSE                          50
  END;
BEGIN
  IF p_seller_sku IS NULL OR p_asin IS NULL THEN
    RETURN NULL;
  END IF;

  SELECT * INTO v_existing
  FROM public.catalog_products
  WHERE organization_id = p_organization_id
    AND store_id        = p_store_id
    AND seller_sku      = p_seller_sku
    AND asin            = p_asin;

  IF v_existing.id IS NULL THEN
    v_prov := '{}'::jsonb
      || public.identity_merge_build_provenance('item_name',           p_source_report_type, v_listing_priority, 1.0, p_source_upload_id)
      || public.identity_merge_build_provenance('item_description',    p_source_report_type, v_listing_priority, 1.0, p_source_upload_id)
      || public.identity_merge_build_provenance('fulfillment_channel', p_source_report_type, v_listing_priority, 1.0, p_source_upload_id)
      || public.identity_merge_build_provenance('listing_status',      p_source_report_type, v_listing_priority, 1.0, p_source_upload_id)
      || public.identity_merge_build_provenance('price',               p_source_report_type, v_listing_priority, 1.0, p_source_upload_id)
      || public.identity_merge_build_provenance('quantity',            p_source_report_type, v_listing_priority, 1.0, p_source_upload_id)
      || public.identity_merge_build_provenance('open_date',           p_source_report_type, v_listing_priority, 1.0, p_source_upload_id)
      || public.identity_merge_build_provenance('listing_id',          p_source_report_type, v_listing_priority, 1.0, p_source_upload_id)
      || public.identity_merge_build_provenance('product_id',          p_source_report_type, v_listing_priority, 1.0, p_source_upload_id)
      || public.identity_merge_build_provenance('product_id_type',     p_source_report_type, v_listing_priority, 1.0, p_source_upload_id)
      || public.identity_merge_build_provenance('merchant_shipping_group', p_source_report_type, v_listing_priority, 1.0, p_source_upload_id)
      || public.identity_merge_build_provenance('fnsku',               p_source_report_type, GREATEST(v_listing_priority - 20, 50), 1.0, p_source_upload_id)
      || public.identity_merge_build_provenance('item_condition',      p_source_report_type, GREATEST(v_listing_priority - 40, 30), 1.0, p_source_upload_id);

    INSERT INTO public.catalog_products (
      organization_id, store_id, source_report_type, source_upload_id,
      seller_sku, asin, fnsku,
      item_name, item_description, fulfillment_channel, listing_status,
      price, quantity, open_date,
      listing_id, product_id, product_id_type,
      item_condition, merchant_shipping_group,
      raw_payload, field_provenance
    )
    VALUES (
      p_organization_id, p_store_id, p_source_report_type, p_source_upload_id,
      p_seller_sku, p_asin, p_fnsku,
      p_item_name, p_item_description, p_fulfillment_channel, p_listing_status,
      p_price, p_quantity, p_open_date,
      p_listing_id, p_product_id, p_product_id_type,
      p_item_condition, p_merchant_shipping_group,
      COALESCE(p_raw_payload, '{}'::jsonb), v_prov
    )
    RETURNING id INTO v_id;
    RETURN v_id;
  END IF;

  v_prov := COALESCE(v_existing.field_provenance, '{}'::jsonb);

  -- One UPDATE per field that wins the priority check; raw_payload is always merged.
  IF public.identity_merge_should_overwrite(v_existing.item_name, v_prov, 'item_name', p_item_name, v_listing_priority) THEN
    UPDATE public.catalog_products
       SET item_name = p_item_name,
           field_provenance = v_prov || public.identity_merge_build_provenance('item_name', p_source_report_type, v_listing_priority, 1.0, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
    SELECT field_provenance INTO v_prov FROM public.catalog_products WHERE id = v_existing.id;
  END IF;

  IF public.identity_merge_should_overwrite(v_existing.item_description, v_prov, 'item_description', p_item_description, v_listing_priority) THEN
    UPDATE public.catalog_products
       SET item_description = p_item_description,
           field_provenance = v_prov || public.identity_merge_build_provenance('item_description', p_source_report_type, v_listing_priority, 1.0, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
    SELECT field_provenance INTO v_prov FROM public.catalog_products WHERE id = v_existing.id;
  END IF;

  IF public.identity_merge_should_overwrite(v_existing.fulfillment_channel, v_prov, 'fulfillment_channel', p_fulfillment_channel, v_listing_priority) THEN
    UPDATE public.catalog_products
       SET fulfillment_channel = p_fulfillment_channel,
           field_provenance = v_prov || public.identity_merge_build_provenance('fulfillment_channel', p_source_report_type, v_listing_priority, 1.0, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
    SELECT field_provenance INTO v_prov FROM public.catalog_products WHERE id = v_existing.id;
  END IF;

  IF public.identity_merge_should_overwrite(v_existing.listing_status, v_prov, 'listing_status', p_listing_status, v_listing_priority) THEN
    UPDATE public.catalog_products
       SET listing_status = p_listing_status,
           field_provenance = v_prov || public.identity_merge_build_provenance('listing_status', p_source_report_type, v_listing_priority, 1.0, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
    SELECT field_provenance INTO v_prov FROM public.catalog_products WHERE id = v_existing.id;
  END IF;

  IF public.identity_merge_should_overwrite(v_existing.price, v_prov, 'price', p_price, v_listing_priority) THEN
    UPDATE public.catalog_products
       SET price = p_price,
           field_provenance = v_prov || public.identity_merge_build_provenance('price', p_source_report_type, v_listing_priority, 1.0, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
    SELECT field_provenance INTO v_prov FROM public.catalog_products WHERE id = v_existing.id;
  END IF;

  IF public.identity_merge_should_overwrite(v_existing.quantity, v_prov, 'quantity', p_quantity, v_listing_priority) THEN
    UPDATE public.catalog_products
       SET quantity = p_quantity,
           field_provenance = v_prov || public.identity_merge_build_provenance('quantity', p_source_report_type, v_listing_priority, 1.0, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
    SELECT field_provenance INTO v_prov FROM public.catalog_products WHERE id = v_existing.id;
  END IF;

  IF public.identity_merge_should_overwrite(v_existing.fnsku, v_prov, 'fnsku', p_fnsku, GREATEST(v_listing_priority - 20, 50)) THEN
    UPDATE public.catalog_products
       SET fnsku = p_fnsku,
           field_provenance = v_prov || public.identity_merge_build_provenance('fnsku', p_source_report_type, GREATEST(v_listing_priority - 20, 50), 1.0, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
  END IF;

  -- raw_payload is jsonb-merged (last writer wins per key) but never wiped.
  UPDATE public.catalog_products
     SET raw_payload = COALESCE(public.catalog_products.raw_payload, '{}'::jsonb) || COALESCE(p_raw_payload, '{}'::jsonb),
         source_report_type = COALESCE(public.catalog_products.source_report_type, p_source_report_type),
         source_upload_id   = COALESCE(p_source_upload_id, public.catalog_products.source_upload_id),
         last_seen_at = v_now
   WHERE id = v_existing.id;

  RETURN v_existing.id;
END;
$fn$;

COMMENT ON FUNCTION public.merge_listing_into_catalog_products_v2 IS
  'All Listings / Active Listings / Category Listings merge with per-field priority guard and provenance tracking. Listings own item_name/listing_status/price/quantity; FNSKU and item_condition are filled at lower priority so FBA Inventory can override.';

GRANT EXECUTE ON FUNCTION public.merge_listing_into_catalog_products_v2(
  uuid, uuid, text, text, text, text, text, text, text, text, numeric, integer,
  timestamptz, text, text, text, text, text, jsonb, uuid
) TO service_role;

-- ──────────────────────────────────────────────────────────────────────────
-- 6. merge_fba_inventory_into_catalog_products — owns FNSKU, item_condition,
--    fba availability fields. Never overwrites listing-owned fields.
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.merge_fba_inventory_into_catalog_products(
  p_organization_id   uuid,
  p_store_id          uuid,
  p_seller_sku        text,
  p_asin              text,
  p_fnsku             text,
  p_item_condition    text,
  p_source_upload_id  uuid
)
RETURNS uuid
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $fn$
DECLARE
  v_existing public.catalog_products;
  v_now timestamptz := now();
  v_prov jsonb;
BEGIN
  IF p_seller_sku IS NULL OR p_asin IS NULL THEN
    RETURN NULL;
  END IF;

  SELECT * INTO v_existing
  FROM public.catalog_products
  WHERE organization_id = p_organization_id
    AND store_id        = p_store_id
    AND seller_sku      = p_seller_sku
    AND asin            = p_asin;

  IF v_existing.id IS NULL THEN
    -- FBA inventory is not allowed to invent a fresh catalog row by itself —
    -- callers should sync All Listings first. Return NULL so the caller can
    -- enqueue a backlog entry.
    RETURN NULL;
  END IF;

  v_prov := COALESCE(v_existing.field_provenance, '{}'::jsonb);

  IF public.identity_merge_should_overwrite(v_existing.fnsku, v_prov, 'fnsku', p_fnsku, 100) THEN
    UPDATE public.catalog_products
       SET fnsku = p_fnsku,
           field_provenance = v_prov || public.identity_merge_build_provenance('fnsku', 'fba_inventory', 100, 1.0, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
    SELECT field_provenance INTO v_prov FROM public.catalog_products WHERE id = v_existing.id;
  END IF;

  IF public.identity_merge_should_overwrite(v_existing.item_condition, v_prov, 'item_condition', p_item_condition, 100) THEN
    UPDATE public.catalog_products
       SET item_condition = p_item_condition,
           field_provenance = v_prov || public.identity_merge_build_provenance('item_condition', 'fba_inventory', 100, 1.0, p_source_upload_id),
           last_seen_at = v_now
     WHERE id = v_existing.id;
  END IF;

  RETURN v_existing.id;
END;
$fn$;

COMMENT ON FUNCTION public.merge_fba_inventory_into_catalog_products IS
  'Manage FBA Inventory enrichment: highest-priority writer for fnsku and item_condition. Refuses to create new catalog rows; All Listings sync must seed them first.';

GRANT EXECUTE ON FUNCTION public.merge_fba_inventory_into_catalog_products(uuid, uuid, text, text, text, text, uuid) TO service_role;

NOTIFY pgrst, 'reload schema';

COMMIT;
