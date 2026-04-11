-- =============================================================================
-- Create tables for Amazon report types that were previously unhandled:
--   ALL_ORDERS, REPLACEMENTS, FBA_GRADE_AND_RESELL, MANAGE_FBA_INVENTORY,
--   FBA_INVENTORY, RESERVED_INVENTORY, FEE_PREVIEW, MONTHLY_STORAGE_FEES
--
-- Design principles:
--   • Additive only — no existing tables are touched.
--   • source_line_hash (FNV fingerprint of full row content) is the dedup key.
--   • Conflict key: (organization_id, source_line_hash) — idempotent on re-import.
--   • A few business columns extracted per table; everything else → raw_data JSONB.
--   • Updated_at trigger is added once via a shared helper.
-- =============================================================================

-- ── amazon_all_orders ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.amazon_all_orders (
  id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id     uuid        NOT NULL,
  store_id            uuid        REFERENCES public.stores (id) ON DELETE SET NULL,
  source_upload_id    uuid        REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  source_line_hash    text        NOT NULL,
  order_id            text,
  purchase_date       timestamptz,
  order_status        text,
  fulfillment_channel text,
  sales_channel       text,
  raw_data            jsonb,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE  public.amazon_all_orders IS 'Raw rows from Amazon "All Orders" report (one row per order line).';
COMMENT ON COLUMN public.amazon_all_orders.source_line_hash IS 'FNV fingerprint of full row content; dedup key for idempotent re-import.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_all_orders_org_hash
  ON public.amazon_all_orders (organization_id, source_line_hash);
CREATE INDEX IF NOT EXISTS idx_amazon_all_orders_org_upload
  ON public.amazon_all_orders (organization_id, source_upload_id);
CREATE INDEX IF NOT EXISTS idx_amazon_all_orders_order
  ON public.amazon_all_orders (organization_id, order_id);

-- ── amazon_replacements ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.amazon_replacements (
  id                   uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id      uuid        NOT NULL,
  store_id             uuid        REFERENCES public.stores (id) ON DELETE SET NULL,
  source_upload_id     uuid        REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  source_line_hash     text        NOT NULL,
  order_id             text,
  replacement_order_id text,
  asin                 text,
  sku                  text,
  order_date           timestamptz,
  raw_data             jsonb,
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE  public.amazon_replacements IS 'Raw rows from Amazon Replacements report.';
COMMENT ON COLUMN public.amazon_replacements.source_line_hash IS 'FNV fingerprint of full row content; dedup key for idempotent re-import.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_replacements_org_hash
  ON public.amazon_replacements (organization_id, source_line_hash);
CREATE INDEX IF NOT EXISTS idx_amazon_replacements_org_upload
  ON public.amazon_replacements (organization_id, source_upload_id);

-- ── amazon_fba_grade_and_resell ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.amazon_fba_grade_and_resell (
  id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id  uuid        NOT NULL,
  store_id         uuid        REFERENCES public.stores (id) ON DELETE SET NULL,
  source_upload_id uuid        REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  source_line_hash text        NOT NULL,
  asin             text,
  fnsku            text,
  sku              text,
  grade            text,
  units            integer,
  raw_data         jsonb,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE  public.amazon_fba_grade_and_resell IS 'Raw rows from Amazon FBA Grade and Resell report.';
COMMENT ON COLUMN public.amazon_fba_grade_and_resell.source_line_hash IS 'FNV fingerprint of full row content; dedup key for idempotent re-import.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_fba_grade_and_resell_org_hash
  ON public.amazon_fba_grade_and_resell (organization_id, source_line_hash);
CREATE INDEX IF NOT EXISTS idx_amazon_fba_grade_and_resell_org_upload
  ON public.amazon_fba_grade_and_resell (organization_id, source_upload_id);

-- ── amazon_manage_fba_inventory ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.amazon_manage_fba_inventory (
  id                       uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id          uuid        NOT NULL,
  store_id                 uuid        REFERENCES public.stores (id) ON DELETE SET NULL,
  source_upload_id         uuid        REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  source_line_hash         text        NOT NULL,
  asin                     text,
  fnsku                    text,
  sku                      text,
  afn_fulfillable_quantity integer,
  raw_data                 jsonb,
  created_at               timestamptz NOT NULL DEFAULT now(),
  updated_at               timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE  public.amazon_manage_fba_inventory IS 'Raw rows from Amazon Manage FBA Inventory (AFN) report.';
COMMENT ON COLUMN public.amazon_manage_fba_inventory.source_line_hash IS 'FNV fingerprint of full row content; dedup key for idempotent re-import.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_manage_fba_inventory_org_hash
  ON public.amazon_manage_fba_inventory (organization_id, source_line_hash);
CREATE INDEX IF NOT EXISTS idx_amazon_manage_fba_inventory_org_upload
  ON public.amazon_manage_fba_inventory (organization_id, source_upload_id);

-- ── amazon_fba_inventory ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.amazon_fba_inventory (
  id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id  uuid        NOT NULL,
  store_id         uuid        REFERENCES public.stores (id) ON DELETE SET NULL,
  source_upload_id uuid        REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  source_line_hash text        NOT NULL,
  asin             text,
  fnsku            text,
  sku              text,
  quantity         integer,
  raw_data         jsonb,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE  public.amazon_fba_inventory IS 'Raw rows from Amazon FBA Inventory Health report.';
COMMENT ON COLUMN public.amazon_fba_inventory.source_line_hash IS 'FNV fingerprint of full row content; dedup key for idempotent re-import.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_fba_inventory_org_hash
  ON public.amazon_fba_inventory (organization_id, source_line_hash);
CREATE INDEX IF NOT EXISTS idx_amazon_fba_inventory_org_upload
  ON public.amazon_fba_inventory (organization_id, source_upload_id);

-- ── amazon_reserved_inventory ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.amazon_reserved_inventory (
  id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id   uuid        NOT NULL,
  store_id          uuid        REFERENCES public.stores (id) ON DELETE SET NULL,
  source_upload_id  uuid        REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  source_line_hash  text        NOT NULL,
  asin              text,
  fnsku             text,
  sku               text,
  reserved_quantity integer,
  raw_data          jsonb,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE  public.amazon_reserved_inventory IS 'Raw rows from Amazon Reserved Inventory report.';
COMMENT ON COLUMN public.amazon_reserved_inventory.source_line_hash IS 'FNV fingerprint of full row content; dedup key for idempotent re-import.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_reserved_inventory_org_hash
  ON public.amazon_reserved_inventory (organization_id, source_line_hash);
CREATE INDEX IF NOT EXISTS idx_amazon_reserved_inventory_org_upload
  ON public.amazon_reserved_inventory (organization_id, source_upload_id);

-- ── amazon_fee_preview ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.amazon_fee_preview (
  id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id  uuid        NOT NULL,
  store_id         uuid        REFERENCES public.stores (id) ON DELETE SET NULL,
  source_upload_id uuid        REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  source_line_hash text        NOT NULL,
  asin             text,
  fnsku            text,
  sku              text,
  price            numeric,
  estimated_fee    numeric,
  currency         text,
  raw_data         jsonb,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE  public.amazon_fee_preview IS 'Raw rows from Amazon Fee Preview report.';
COMMENT ON COLUMN public.amazon_fee_preview.source_line_hash IS 'FNV fingerprint of full row content; dedup key for idempotent re-import.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_fee_preview_org_hash
  ON public.amazon_fee_preview (organization_id, source_line_hash);
CREATE INDEX IF NOT EXISTS idx_amazon_fee_preview_org_upload
  ON public.amazon_fee_preview (organization_id, source_upload_id);

-- ── amazon_monthly_storage_fees ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.amazon_monthly_storage_fees (
  id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id  uuid        NOT NULL,
  store_id         uuid        REFERENCES public.stores (id) ON DELETE SET NULL,
  source_upload_id uuid        REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  source_line_hash text        NOT NULL,
  asin             text,
  fnsku            text,
  sku              text,
  storage_month    text,
  storage_rate     numeric,
  currency         text,
  raw_data         jsonb,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE  public.amazon_monthly_storage_fees IS 'Raw rows from Amazon Monthly Storage Fees report.';
COMMENT ON COLUMN public.amazon_monthly_storage_fees.source_line_hash IS 'FNV fingerprint of full row content; dedup key for idempotent re-import.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_monthly_storage_fees_org_hash
  ON public.amazon_monthly_storage_fees (organization_id, source_line_hash);
CREATE INDEX IF NOT EXISTS idx_amazon_monthly_storage_fees_org_upload
  ON public.amazon_monthly_storage_fees (organization_id, source_upload_id);

-- ── RLS: enable + service_role bypass for all 8 tables ───────────────────────
ALTER TABLE public.amazon_all_orders           ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.amazon_replacements         ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.amazon_fba_grade_and_resell ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.amazon_manage_fba_inventory ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.amazon_fba_inventory        ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.amazon_reserved_inventory   ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.amazon_fee_preview          ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.amazon_monthly_storage_fees ENABLE ROW LEVEL SECURITY;

-- Service role bypass (import routes run as service_role)
DROP POLICY IF EXISTS amazon_all_orders_service_bypass          ON public.amazon_all_orders;
DROP POLICY IF EXISTS amazon_replacements_service_bypass        ON public.amazon_replacements;
DROP POLICY IF EXISTS amazon_fba_grade_resell_service_bypass    ON public.amazon_fba_grade_and_resell;
DROP POLICY IF EXISTS amazon_manage_fba_inv_service_bypass      ON public.amazon_manage_fba_inventory;
DROP POLICY IF EXISTS amazon_fba_inventory_service_bypass       ON public.amazon_fba_inventory;
DROP POLICY IF EXISTS amazon_reserved_inv_service_bypass        ON public.amazon_reserved_inventory;
DROP POLICY IF EXISTS amazon_fee_preview_service_bypass         ON public.amazon_fee_preview;
DROP POLICY IF EXISTS amazon_monthly_storage_service_bypass     ON public.amazon_monthly_storage_fees;

CREATE POLICY amazon_all_orders_service_bypass
  ON public.amazon_all_orders FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY amazon_replacements_service_bypass
  ON public.amazon_replacements FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY amazon_fba_grade_resell_service_bypass
  ON public.amazon_fba_grade_and_resell FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY amazon_manage_fba_inv_service_bypass
  ON public.amazon_manage_fba_inventory FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY amazon_fba_inventory_service_bypass
  ON public.amazon_fba_inventory FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY amazon_reserved_inv_service_bypass
  ON public.amazon_reserved_inventory FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY amazon_fee_preview_service_bypass
  ON public.amazon_fee_preview FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY amazon_monthly_storage_service_bypass
  ON public.amazon_monthly_storage_fees FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Intentionally no authenticated-role policies here — same pattern as
-- `amazon_removal_shipments` (20260513): API imports use `service_role` only.
-- If you later run `20260416_rls_enterprise_security.sql` (defines
-- `public.get_my_organization_id()`), add org-scoped SELECT policies in a small
-- follow-up migration.

-- ── Extend raw_report_uploads.report_type CHECK constraint ────────────────────
-- Drop the current constraint (re-add with all new types below).
ALTER TABLE public.raw_report_uploads
  DROP CONSTRAINT IF EXISTS raw_report_uploads_report_type_check;

ALTER TABLE public.raw_report_uploads
  ADD CONSTRAINT raw_report_uploads_report_type_check CHECK (
    report_type = ANY (ARRAY[
      'FBA_RETURNS'::text,
      'REMOVAL_ORDER'::text,
      'REMOVAL_SHIPMENT'::text,
      'INVENTORY_LEDGER'::text,
      'REIMBURSEMENTS'::text,
      'SETTLEMENT'::text,
      'SAFET_CLAIMS'::text,
      'TRANSACTIONS'::text,
      'REPORTS_REPOSITORY'::text,
      'ALL_ORDERS'::text,
      'REPLACEMENTS'::text,
      'FBA_GRADE_AND_RESELL'::text,
      'MANAGE_FBA_INVENTORY'::text,
      'FBA_INVENTORY'::text,
      'RESERVED_INVENTORY'::text,
      'FEE_PREVIEW'::text,
      'MONTHLY_STORAGE_FEES'::text,
      'UNKNOWN'::text,
      'CATEGORY_LISTINGS'::text,
      'ALL_LISTINGS'::text,
      'ACTIVE_LISTINGS'::text,
      -- legacy slugs kept for backward compat
      'fba_customer_returns'::text,
      'reimbursements'::text,
      'inventory_ledger'::text,
      'safe_t_claims'::text,
      'transaction_view'::text,
      'settlement_repository'::text
    ])
  );

NOTIFY pgrst, 'reload schema';
