-- expected_packages: domain table for Removal Order Detail imports.
-- Removal Order Detail CSVs contain one row per (order_id, sku) combination.
-- upload_id tags every row with the source import for targeted cleanup.

CREATE TABLE IF NOT EXISTS public.expected_packages (
  id                 uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  organization_id    uuid NOT NULL,
  upload_id          uuid REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  order_id           text NOT NULL,
  sku                text NOT NULL DEFAULT '',
  tracking_number    text,
  requested_quantity integer,
  shipped_quantity   integer,
  disposed_quantity  integer,
  cancelled_quantity integer,
  order_status       text,
  disposition        text,
  order_date         date,
  created_at         timestamp with time zone DEFAULT now() NOT NULL,
  updated_at         timestamp with time zone DEFAULT now() NOT NULL
);

-- Unique per (org, order, sku) — supports idempotent upserts.
CREATE UNIQUE INDEX IF NOT EXISTS expected_packages_org_order_sku
  ON public.expected_packages (organization_id, order_id, sku);

CREATE INDEX IF NOT EXISTS idx_expected_packages_upload_id
  ON public.expected_packages (upload_id)
  WHERE upload_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_expected_packages_org
  ON public.expected_packages (organization_id);

COMMENT ON TABLE public.expected_packages IS
  'Removal Order Detail lines imported from Amazon Seller Central.
   Each row is one (order_id, sku) pair within a removal shipment.
   upload_id enables targeted sync and cleanup per import session.';

NOTIFY pgrst, 'reload schema';
