-- Wave 1: Imports Target Store (store_id) + dual-layer uniqueness for removals pipeline.
-- Layer A (upload/staging): uq_*_org_upload_source_staging on amazon_removals / expected_packages;
--   uq_amazon_removal_shipments_org_upload_staging on amazon_removal_shipments.
-- Layer B (business): uq_amazon_removals_business_line, uq_expected_packages_business_line,
--   uq_amazon_removal_shipments_business_line (typed columns — enrichment must not use raw_row alone).
--
-- store_id remains NULLABLE at the database: new sync paths should set it from Imports Target Store;
-- legacy rows may stay NULL until backfilled. Backfill below uses only explicit sources (no arbitrary store pick).
--
-- Before applying: run supabase/scripts/wave1_removal_diagnostics.sql
-- If index creation fails: run supabase/scripts/wave1_removal_duplicate_cleanup.sql (test DB only), then retry.

BEGIN;

ALTER TABLE public.amazon_removals
  ADD COLUMN IF NOT EXISTS store_id uuid REFERENCES public.stores (id) ON DELETE SET NULL;

ALTER TABLE public.expected_packages
  ADD COLUMN IF NOT EXISTS store_id uuid REFERENCES public.stores (id) ON DELETE SET NULL;

ALTER TABLE public.expected_packages
  ADD COLUMN IF NOT EXISTS order_type text;

COMMENT ON COLUMN public.amazon_removals.store_id IS
  'Imports Target Store (stores.id). Nullable for legacy rows; new syncs should set from upload metadata.';

COMMENT ON COLUMN public.expected_packages.store_id IS
  'Imports Target Store — aligns expected lines with amazon_removals business dedupe. Nullable for legacy.';

COMMENT ON COLUMN public.expected_packages.order_type IS
  'Mirrors removal order_type for Return-filtered worklist rows (Wave 1).';

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS store_id uuid REFERENCES public.stores (id) ON DELETE SET NULL;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS order_id text;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS sku text;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS fnsku text;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS disposition text;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS tracking_number text;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS carrier text;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS shipment_date date;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS order_date date;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS order_type text;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS requested_quantity integer;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS shipped_quantity integer;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS disposed_quantity integer;

ALTER TABLE public.amazon_removal_shipments
  ADD COLUMN IF NOT EXISTS cancelled_quantity integer;

COMMENT ON COLUMN public.amazon_removal_shipments.store_id IS
  'Imports Target Store — scopes shipment archive and business dedupe. Nullable for legacy.';

-- Backfill store_id only from explicit sources (no arbitrary “first store” in org):
--   raw_report_uploads.metadata.import_store_id, ledger_store_id, organization_settings.default_store_id
UPDATE public.amazon_removals ar
SET store_id = sub.sid
FROM (
  SELECT
    ar2.id,
    COALESCE(
      NULLIF(trim(rr.metadata->>'import_store_id'), '')::uuid,
      NULLIF(trim(rr.metadata->>'ledger_store_id'), '')::uuid,
      os.default_store_id
    ) AS sid
  FROM public.amazon_removals ar2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = ar2.upload_id
  LEFT JOIN public.organization_settings os ON os.organization_id = ar2.organization_id
  WHERE ar2.store_id IS NULL
) AS sub
WHERE ar.id = sub.id AND sub.sid IS NOT NULL;

UPDATE public.expected_packages ep
SET store_id = sub.sid
FROM (
  SELECT
    ep2.id,
    COALESCE(
      NULLIF(trim(rr.metadata->>'import_store_id'), '')::uuid,
      NULLIF(trim(rr.metadata->>'ledger_store_id'), '')::uuid,
      os.default_store_id
    ) AS sid
  FROM public.expected_packages ep2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = ep2.upload_id
  LEFT JOIN public.organization_settings os ON os.organization_id = ep2.organization_id
  WHERE ep2.store_id IS NULL
) AS sub
WHERE ep.id = sub.id AND sub.sid IS NOT NULL;

UPDATE public.amazon_removal_shipments sh
SET store_id = sub.sid
FROM (
  SELECT
    sh2.id,
    COALESCE(
      NULLIF(trim(rr.metadata->>'import_store_id'), '')::uuid,
      NULLIF(trim(rr.metadata->>'ledger_store_id'), '')::uuid,
      os.default_store_id
    ) AS sid
  FROM public.amazon_removal_shipments sh2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = sh2.upload_id
  LEFT JOIN public.organization_settings os ON os.organization_id = sh2.organization_id
  WHERE sh2.store_id IS NULL
) AS sub
WHERE sh.id = sub.id AND sub.sid IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removals_business_line
  ON public.amazon_removals (
    organization_id,
    store_id,
    order_id,
    sku,
    fnsku,
    disposition,
    requested_quantity,
    shipped_quantity,
    disposed_quantity,
    cancelled_quantity,
    order_date,
    order_type
  )
  NULLS NOT DISTINCT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_expected_packages_business_line
  ON public.expected_packages (
    organization_id,
    store_id,
    order_id,
    sku,
    fnsku,
    disposition,
    requested_quantity,
    shipped_quantity,
    disposed_quantity,
    cancelled_quantity,
    order_date
  )
  NULLS NOT DISTINCT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removal_shipments_org_upload_staging
  ON public.amazon_removal_shipments (organization_id, upload_id, amazon_staging_id)
  NULLS NOT DISTINCT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removal_shipments_business_line
  ON public.amazon_removal_shipments (
    organization_id,
    store_id,
    order_id,
    tracking_number,
    sku,
    fnsku,
    disposition,
    requested_quantity,
    shipped_quantity,
    disposed_quantity,
    cancelled_quantity,
    order_date,
    order_type
  )
  NULLS NOT DISTINCT;

CREATE INDEX IF NOT EXISTS idx_amazon_removals_org_store_order
  ON public.amazon_removals (organization_id, store_id, order_id);

CREATE INDEX IF NOT EXISTS idx_amazon_removal_shipments_org_store_order
  ON public.amazon_removal_shipments (organization_id, store_id, order_id);

NOTIFY pgrst, 'reload schema';

COMMIT;
