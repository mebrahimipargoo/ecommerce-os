-- Scan/allocation tree: tracking (container) -> box -> item quantities.
-- No sync/UI/backfill; application code to be added later.

BEGIN;

CREATE TABLE IF NOT EXISTS public.shipment_containers (
  id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id    uuid NOT NULL,
  store_id           uuid REFERENCES public.stores (id) ON DELETE SET NULL,
  tracking_number    text NOT NULL,
  carrier            text,
  shipment_date      timestamptz,
  order_date         timestamptz,
  status             text NOT NULL DEFAULT 'pending',
  metadata           jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.shipment_boxes (
  id                     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id        uuid NOT NULL,
  store_id               uuid REFERENCES public.stores (id) ON DELETE SET NULL,
  shipment_container_id  uuid NOT NULL REFERENCES public.shipment_containers (id) ON DELETE CASCADE,
  box_code               text,
  box_label              text,
  packing_slip_code      text,
  status                 text NOT NULL DEFAULT 'pending',
  metadata               jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at             timestamptz NOT NULL DEFAULT now(),
  updated_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.shipment_box_items (
  id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id    uuid NOT NULL,
  store_id           uuid REFERENCES public.stores (id) ON DELETE SET NULL,
  shipment_box_id    uuid NOT NULL REFERENCES public.shipment_boxes (id) ON DELETE CASCADE,
  order_id           text,
  sku                text,
  fnsku              text,
  disposition        text,
  expected_quantity  numeric NOT NULL DEFAULT 0,
  scanned_quantity   numeric NOT NULL DEFAULT 0,
  status             text NOT NULL DEFAULT 'pending',
  metadata           jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now()
);

-- shipment_containers: filter by org/store and resolve a container by tracking (scanner + dashboards).
CREATE INDEX IF NOT EXISTS idx_shipment_containers_org_store_tracking
  ON public.shipment_containers (organization_id, store_id, tracking_number);

CREATE INDEX IF NOT EXISTS idx_shipment_containers_org_created
  ON public.shipment_containers (organization_id, created_at DESC);

-- shipment_boxes: list boxes under a container (FK traversal); filter by org/store.
CREATE INDEX IF NOT EXISTS idx_shipment_boxes_container
  ON public.shipment_boxes (shipment_container_id);

CREATE INDEX IF NOT EXISTS idx_shipment_boxes_org_store_container
  ON public.shipment_boxes (organization_id, store_id, shipment_container_id);

-- shipment_box_items: line items under a box; match to removal/order lines within org/store.
CREATE INDEX IF NOT EXISTS idx_shipment_box_items_box
  ON public.shipment_box_items (shipment_box_id);

CREATE INDEX IF NOT EXISTS idx_shipment_box_items_org_store_order
  ON public.shipment_box_items (organization_id, store_id, order_id);

CREATE INDEX IF NOT EXISTS idx_shipment_box_items_org_store_sku_fnsku
  ON public.shipment_box_items (organization_id, store_id, sku, fnsku);

CREATE INDEX IF NOT EXISTS idx_shipment_box_items_org_status
  ON public.shipment_box_items (organization_id, status);

ALTER TABLE public.shipment_containers ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.shipment_boxes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.shipment_box_items ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "shipment_containers: service_role bypass" ON public.shipment_containers;
CREATE POLICY "shipment_containers: service_role bypass"
  ON public.shipment_containers AS PERMISSIVE FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "shipment_boxes: service_role bypass" ON public.shipment_boxes;
CREATE POLICY "shipment_boxes: service_role bypass"
  ON public.shipment_boxes AS PERMISSIVE FOR ALL TO service_role USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "shipment_box_items: service_role bypass" ON public.shipment_box_items;
CREATE POLICY "shipment_box_items: service_role bypass"
  ON public.shipment_box_items AS PERMISSIVE FOR ALL TO service_role USING (true) WITH CHECK (true);

COMMENT ON TABLE public.shipment_containers IS
  'Tracking/pallet-level node for scan allocation; parent of shipment_boxes.';
COMMENT ON TABLE public.shipment_boxes IS
  'Box/carton node under a shipment_container; parent of shipment_box_items.';
COMMENT ON TABLE public.shipment_box_items IS
  'Expected vs scanned quantities per SKU/FNSKU line under a box; supports splits across boxes/trackings.';

NOTIFY pgrst, 'reload schema';

COMMIT;
