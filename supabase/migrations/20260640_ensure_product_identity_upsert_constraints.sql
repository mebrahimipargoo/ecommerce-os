-- 20260640_ensure_product_identity_upsert_constraints.sql
--
-- Ensures the Product Identity processor has the exact arbiters it uses for
-- Supabase/PostgREST upserts:
--   * products:              organization_id + store_id + sku
--   * catalog_products:      organization_id + store_id + seller_sku + asin
--   * product_identifier_map: organization_id + store_id + external_listing_id
--                            for product_identity:* rows only
--
-- This is additive/idempotent except for dropping the older org-wide
-- products_sku_organization_id_key, which blocks the same SKU from existing
-- in multiple stores under one org.

BEGIN;

-- products must be keyed per tenant store, not per org.
ALTER TABLE public.products
  ADD COLUMN IF NOT EXISTS store_id uuid,
  ADD COLUMN IF NOT EXISTS vendor_name text,
  ADD COLUMN IF NOT EXISTS mfg_part_number text,
  ADD COLUMN IF NOT EXISTS upc_code text,
  ADD COLUMN IF NOT EXISTS asin text,
  ADD COLUMN IF NOT EXISTS fnsku text,
  ADD COLUMN IF NOT EXISTS metadata jsonb DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS last_seen_at timestamptz,
  ADD COLUMN IF NOT EXISTS last_catalog_sync_at timestamptz;

ALTER TABLE public.products
  DROP CONSTRAINT IF EXISTS products_sku_organization_id_key;

DROP INDEX IF EXISTS public.products_sku_organization_id_key;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.products'::regclass
      AND conname = 'products_organization_store_sku_key'
  ) THEN
    ALTER TABLE public.products
      ADD CONSTRAINT products_organization_store_sku_key
      UNIQUE NULLS NOT DISTINCT (organization_id, store_id, sku);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_products_org_store_sku
  ON public.products (organization_id, store_id, sku);

-- catalog_products identity used by Product Identity and listing imports.
CREATE UNIQUE INDEX IF NOT EXISTS uq_catalog_products_canonical_identity
  ON public.catalog_products (organization_id, store_id, seller_sku, asin)
  NULLS NOT DISTINCT;

-- product_identifier_map idempotency for Product Identity rows only.
CREATE UNIQUE INDEX IF NOT EXISTS uq_product_identifier_map_product_identity
  ON public.product_identifier_map (organization_id, store_id, external_listing_id)
  WHERE external_listing_id IS NOT NULL
    AND external_listing_id LIKE 'product_identity:%';

NOTIFY pgrst, 'reload schema';

COMMIT;
