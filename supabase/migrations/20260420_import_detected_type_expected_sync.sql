-- Smart import: data sync status, expected_returns / expected_packages,
-- and tenant-scoped products upserts for inventory ledger.
-- Classification is stored on `raw_report_uploads.report_type` (see migration 20260421).

-- ── raw_report_uploads: sync lifecycle ──────────────────────────────────────
ALTER TABLE public.raw_report_uploads
  ADD COLUMN IF NOT EXISTS data_sync_status text NOT NULL DEFAULT 'pending';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'raw_report_uploads_data_sync_status_check'
  ) THEN
    ALTER TABLE public.raw_report_uploads
      ADD CONSTRAINT raw_report_uploads_data_sync_status_check CHECK (
        data_sync_status = ANY (ARRAY['pending'::text, 'synced'::text, 'failed'::text])
      );
  END IF;
END $$;

COMMENT ON COLUMN public.raw_report_uploads.data_sync_status IS
  'Whether Process/Sync routed rows into domain tables: pending | synced | failed.';

-- ── products: tenant column + composite uniqueness (legacy global barcode preserved) ──
ALTER TABLE public.products
  ADD COLUMN IF NOT EXISTS organization_id uuid
  REFERENCES public.organization_settings (organization_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_products_organization_id
  ON public.products (organization_id)
  WHERE organization_id IS NOT NULL;

ALTER TABLE public.products DROP CONSTRAINT IF EXISTS products_barcode_key;

CREATE UNIQUE INDEX IF NOT EXISTS products_barcode_legacy_unique
  ON public.products (barcode)
  WHERE organization_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS products_org_barcode_unique
  ON public.products (organization_id, barcode)
  WHERE organization_id IS NOT NULL;

COMMENT ON COLUMN public.products.organization_id IS
  'Tenant scope for catalog cache; NULL = pre-migration global rows.';

-- ── expected_returns: FBA customer returns expectations ───────────────────────
CREATE TABLE IF NOT EXISTS public.expected_returns (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id   uuid NOT NULL REFERENCES public.organization_settings (organization_id) ON DELETE CASCADE,
  lpn               text NOT NULL,
  asin              text,
  order_id          text,
  source_upload_id  uuid REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  raw_row           jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT expected_returns_org_lpn_unique UNIQUE (organization_id, lpn)
);

CREATE INDEX IF NOT EXISTS idx_expected_returns_org_created
  ON public.expected_returns (organization_id, created_at DESC);

COMMENT ON TABLE public.expected_returns IS
  'Imported FBA return expectations (license plate + reason style CSVs); tenant-scoped.';

-- ── expected_packages: removal / shipment expectations ───────────────────────
CREATE TABLE IF NOT EXISTS public.expected_packages (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id   uuid NOT NULL REFERENCES public.organization_settings (organization_id) ON DELETE CASCADE,
  tracking_number   text NOT NULL,
  shipment_id       text NOT NULL,
  source_upload_id  uuid REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  raw_row           jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT expected_packages_org_track_ship_unique UNIQUE (organization_id, tracking_number, shipment_id)
);

CREATE INDEX IF NOT EXISTS idx_expected_packages_org_created
  ON public.expected_packages (organization_id, created_at DESC);

COMMENT ON TABLE public.expected_packages IS
  'Imported removal-order style rows (tracking + shipment id); tenant-scoped.';

-- Touch triggers
CREATE OR REPLACE FUNCTION public.touch_expected_returns_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_touch_expected_returns ON public.expected_returns;
CREATE TRIGGER trg_touch_expected_returns
  BEFORE UPDATE ON public.expected_returns
  FOR EACH ROW EXECUTE FUNCTION public.touch_expected_returns_updated_at();

CREATE OR REPLACE FUNCTION public.touch_expected_packages_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_touch_expected_packages ON public.expected_packages;
CREATE TRIGGER trg_touch_expected_packages
  BEFORE UPDATE ON public.expected_packages
  FOR EACH ROW EXECUTE FUNCTION public.touch_expected_packages_updated_at();

-- ── RLS ─────────────────────────────────────────────────────────────────────
ALTER TABLE public.expected_returns ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.expected_packages ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "expected_returns_all_own_org" ON public.expected_returns;
CREATE POLICY "expected_returns_all_own_org"
  ON public.expected_returns FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

DROP POLICY IF EXISTS "expected_packages_all_own_org" ON public.expected_packages;
CREATE POLICY "expected_packages_all_own_org"
  ON public.expected_packages FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

NOTIFY pgrst, 'reload schema';
