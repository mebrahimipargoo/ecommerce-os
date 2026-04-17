-- Identifier bridge: listing + inventory ledger enrichment columns, ledger resolution cache.
-- Additive only; safe if `product_identifier_map` already exists with a subset of columns.

BEGIN;

CREATE TABLE IF NOT EXISTS public.product_identifier_map (
  id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id    uuid NOT NULL,
  product_id           uuid,
  catalog_product_id   uuid,
  store_id             uuid,
  seller_sku           text,
  asin                 text,
  fnsku                text,
  msku                 text,
  title                text,
  disposition          text,
  external_listing_id  text,
  source_upload_id     uuid,
  source_report_type   text,
  match_source         text,
  inventory_source     text,
  confidence_score     numeric(10, 4),
  first_seen_at        timestamptz,
  last_seen_at         timestamptz,
  is_primary           boolean DEFAULT true,
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.product_identifier_map IS
  'Bridge identifiers between internal products, marketplace listing snapshots (catalog_products), and operational reports.';

-- Ledger-only bridge rows may exist before a listing snapshot is present.
ALTER TABLE public.product_identifier_map
  ALTER COLUMN catalog_product_id DROP NOT NULL;

ALTER TABLE public.product_identifier_map
  ADD COLUMN IF NOT EXISTS msku text,
  ADD COLUMN IF NOT EXISTS title text,
  ADD COLUMN IF NOT EXISTS disposition text,
  ADD COLUMN IF NOT EXISTS match_source text,
  ADD COLUMN IF NOT EXISTS inventory_source text,
  ADD COLUMN IF NOT EXISTS confidence_score numeric(10, 4),
  ADD COLUMN IF NOT EXISTS first_seen_at timestamptz;

CREATE INDEX IF NOT EXISTS idx_product_identifier_map_org_fnsku_store
  ON public.product_identifier_map (organization_id, store_id, fnsku)
  WHERE fnsku IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_product_identifier_map_org_sku_store_asin
  ON public.product_identifier_map (organization_id, store_id, seller_sku, asin)
  WHERE seller_sku IS NOT NULL AND asin IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_product_identifier_map_org_asin
  ON public.product_identifier_map (organization_id, asin)
  WHERE asin IS NOT NULL;

ALTER TABLE public.amazon_inventory_ledger
  ADD COLUMN IF NOT EXISTS resolved_product_id uuid,
  ADD COLUMN IF NOT EXISTS resolved_catalog_product_id uuid,
  ADD COLUMN IF NOT EXISTS identifier_resolution_status text,
  ADD COLUMN IF NOT EXISTS identifier_resolution_confidence numeric(10, 4);

COMMENT ON COLUMN public.amazon_inventory_ledger.resolved_product_id IS
  'Optional: internal product_id resolved via product_identifier_map after ledger import.';
COMMENT ON COLUMN public.amazon_inventory_ledger.resolved_catalog_product_id IS
  'Optional: catalog_products.id resolved via product_identifier_map.';
COMMENT ON COLUMN public.amazon_inventory_ledger.identifier_resolution_status IS
  'resolved | ambiguous | unresolved from identifier bridge matching.';

COMMIT;
