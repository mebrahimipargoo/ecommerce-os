-- Optional inbound shipment / carrier tracking for pallets (plain text — not a UUID).
ALTER TABLE public.pallets
  ADD COLUMN IF NOT EXISTS tracking_number text NULL;

COMMENT ON COLUMN public.pallets.tracking_number IS
  'Optional carrier / pro / inbound tracking number for the pallet shipment.';
