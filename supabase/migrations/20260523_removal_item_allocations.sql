-- Bridge: removal demand (amazon_removals) ↔ shipment box line supply (shipment_box_items).
-- One removal line may split across multiple shipment_box_items; allocation/scanned quantities live here.

BEGIN;

CREATE TABLE IF NOT EXISTS public.removal_item_allocations (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id       uuid NOT NULL,
  store_id              uuid REFERENCES public.stores (id) ON DELETE SET NULL,
  removal_id            uuid NOT NULL REFERENCES public.amazon_removals (id) ON DELETE CASCADE,
  shipment_box_item_id  uuid NOT NULL REFERENCES public.shipment_box_items (id) ON DELETE CASCADE,
  order_id              text,
  sku                   text,
  fnsku                 text,
  disposition           text,
  tracking_number       text,
  allocated_quantity    numeric NOT NULL DEFAULT 0,
  scanned_quantity      numeric NOT NULL DEFAULT 0,
  status                text NOT NULL DEFAULT 'pending',
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT removal_item_allocations_allocated_non_negative
    CHECK (allocated_quantity >= 0),
  CONSTRAINT removal_item_allocations_scanned_non_negative
    CHECK (scanned_quantity >= 0),
  CONSTRAINT removal_item_allocations_scanned_lte_allocated
    CHECK (scanned_quantity <= allocated_quantity)
);

CREATE INDEX IF NOT EXISTS idx_removal_item_allocations_removal_id
  ON public.removal_item_allocations (removal_id);

CREATE INDEX IF NOT EXISTS idx_removal_item_allocations_shipment_box_item_id
  ON public.removal_item_allocations (shipment_box_item_id);

CREATE INDEX IF NOT EXISTS idx_removal_item_allocations_org_store_order_id
  ON public.removal_item_allocations (organization_id, store_id, order_id);

CREATE INDEX IF NOT EXISTS idx_removal_item_allocations_org_store_sku_fnsku
  ON public.removal_item_allocations (organization_id, store_id, sku, fnsku);

CREATE INDEX IF NOT EXISTS idx_removal_item_allocations_org_status
  ON public.removal_item_allocations (organization_id, status);

CREATE UNIQUE INDEX IF NOT EXISTS uq_removal_item_allocations_org_store_removal_box_item
  ON public.removal_item_allocations (
    organization_id,
    store_id,
    removal_id,
    shipment_box_item_id
  )
  NULLS NOT DISTINCT;

ALTER TABLE public.removal_item_allocations ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "removal_item_allocations: service_role bypass"
  ON public.removal_item_allocations;
CREATE POLICY "removal_item_allocations: service_role bypass"
  ON public.removal_item_allocations
  AS PERMISSIVE FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

COMMENT ON TABLE public.removal_item_allocations IS
  'Bridge between removal demand (amazon_removals) and shipment box item supply (shipment_box_items); source of truth for split allocation and scan quantities.';

NOTIFY pgrst, 'reload schema';

COMMIT;
