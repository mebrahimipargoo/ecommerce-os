-- ETL: optional batch_id on amazon_staging + expected pallet hierarchy for removal pipeline.

ALTER TABLE public.amazon_staging
  ADD COLUMN IF NOT EXISTS batch_id uuid;

CREATE INDEX IF NOT EXISTS idx_amazon_staging_batch_id
  ON public.amazon_staging (batch_id)
  WHERE batch_id IS NOT NULL;

COMMENT ON COLUMN public.amazon_staging.batch_id IS
  'Optional UUID grouping all rows from one ETL upload (POST /etl/upload-removal).';

CREATE TABLE IF NOT EXISTS public.expected_pallets (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id     uuid NOT NULL,
  status              text NOT NULL DEFAULT 'Pending',
  tracking_number     text,
  order_id            text,
  batch_id            uuid,
  created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_expected_pallets_org_created
  ON public.expected_pallets (organization_id, created_at DESC);

COMMENT ON TABLE public.expected_pallets IS
  'Parent pallet expectation per tracking_number (or order_id) from amazon_staging ETL.';

CREATE TABLE IF NOT EXISTS public.expected_items (
  id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  expected_pallet_id   uuid NOT NULL REFERENCES public.expected_pallets (id) ON DELETE CASCADE,
  sku                  text NOT NULL,
  quantity             numeric NOT NULL DEFAULT 0,
  created_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_expected_items_pallet
  ON public.expected_items (expected_pallet_id);

COMMENT ON TABLE public.expected_items IS
  'SKU lines linked to an expected_pallets row (aggregated quantities per SKU per group).';

NOTIFY pgrst, 'reload schema';
