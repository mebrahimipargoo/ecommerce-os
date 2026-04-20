-- =============================================================================
-- Wave 4 — Stabilise the four FBA-inventory report families:
--   MANAGE_FBA_INVENTORY · FBA_INVENTORY · INBOUND_PERFORMANCE · AMAZON_FULFILLED_INVENTORY
--
-- Goals:
--   1. Extend amazon_manage_fba_inventory and amazon_fba_inventory with the
--      full native column set used by the typed mapper. All ADDs are IF NOT
--      EXISTS so this migration is safe to re-run and does not touch the data
--      already loaded by previous waves.
--   2. Create amazon_inbound_performance and amazon_amazon_fulfilled_inventory
--      with the same physical-row identity contract used everywhere else
--      (organization_id + source_file_sha256 + source_physical_row_number).
--   3. Extend the raw_report_uploads.report_type CHECK constraint so the two
--      new report types can be persisted from the upload metadata flow.
--
-- Non-goals:
--   * Does NOT modify behavior of any previously working importer (RETURNS,
--     REMOVAL_*, INVENTORY_LEDGER, REIMBURSEMENTS, SETTLEMENT, SAFET_CLAIMS,
--     TRANSACTIONS, REPORTS_REPOSITORY, ALL_LISTINGS, ACTIVE_LISTINGS,
--     CATEGORY_LISTINGS).
--   * Does NOT delete or rename existing tables / views / indexes.
--   * Any column not modeled here continues to land in raw_data via
--     packPayloadForSupabase().
-- =============================================================================

BEGIN;

-- ── 1) amazon_manage_fba_inventory — add full native column set ──────────────
ALTER TABLE public.amazon_manage_fba_inventory
  ADD COLUMN IF NOT EXISTS product_name                   text,
  ADD COLUMN IF NOT EXISTS condition                      text,
  ADD COLUMN IF NOT EXISTS your_price                     numeric,
  ADD COLUMN IF NOT EXISTS mfn_listing_exists             boolean,
  ADD COLUMN IF NOT EXISTS mfn_fulfillable_quantity       integer,
  ADD COLUMN IF NOT EXISTS afn_listing_exists             boolean,
  ADD COLUMN IF NOT EXISTS afn_warehouse_quantity         integer,
  ADD COLUMN IF NOT EXISTS afn_unsellable_quantity        integer,
  ADD COLUMN IF NOT EXISTS afn_reserved_quantity          integer,
  ADD COLUMN IF NOT EXISTS afn_total_quantity             integer,
  ADD COLUMN IF NOT EXISTS per_unit_volume                numeric,
  ADD COLUMN IF NOT EXISTS afn_inbound_working_quantity   integer,
  ADD COLUMN IF NOT EXISTS afn_inbound_shipped_quantity   integer,
  ADD COLUMN IF NOT EXISTS afn_inbound_receiving_quantity integer,
  ADD COLUMN IF NOT EXISTS afn_researching_quantity       integer,
  ADD COLUMN IF NOT EXISTS afn_reserved_future_supply     integer,
  ADD COLUMN IF NOT EXISTS afn_future_supply_buyable      integer,
  ADD COLUMN IF NOT EXISTS store                          text;

-- ── 2) amazon_fba_inventory (Inventory Health) — add full native column set ──
ALTER TABLE public.amazon_fba_inventory
  ADD COLUMN IF NOT EXISTS snapshot_date                                          text,
  ADD COLUMN IF NOT EXISTS product_name                                           text,
  ADD COLUMN IF NOT EXISTS condition                                              text,
  ADD COLUMN IF NOT EXISTS available                                              integer,
  ADD COLUMN IF NOT EXISTS pending_removal_quantity                               integer,
  ADD COLUMN IF NOT EXISTS inv_age_0_to_90_days                                   integer,
  ADD COLUMN IF NOT EXISTS inv_age_91_to_180_days                                 integer,
  ADD COLUMN IF NOT EXISTS inv_age_181_to_270_days                                integer,
  ADD COLUMN IF NOT EXISTS inv_age_271_to_365_days                                integer,
  ADD COLUMN IF NOT EXISTS inv_age_366_to_455_days                                integer,
  ADD COLUMN IF NOT EXISTS inv_age_456_plus_days                                  integer,
  ADD COLUMN IF NOT EXISTS currency                                               text,
  ADD COLUMN IF NOT EXISTS units_shipped_t7                                       integer,
  ADD COLUMN IF NOT EXISTS units_shipped_t30                                      integer,
  ADD COLUMN IF NOT EXISTS units_shipped_t60                                      integer,
  ADD COLUMN IF NOT EXISTS units_shipped_t90                                      integer,
  ADD COLUMN IF NOT EXISTS alert                                                  text,
  ADD COLUMN IF NOT EXISTS your_price                                             numeric,
  ADD COLUMN IF NOT EXISTS sales_price                                            numeric,
  ADD COLUMN IF NOT EXISTS recommended_action                                     text,
  ADD COLUMN IF NOT EXISTS sell_through                                           numeric,
  ADD COLUMN IF NOT EXISTS item_volume                                            numeric,
  ADD COLUMN IF NOT EXISTS volume_unit_measurement                                text,
  ADD COLUMN IF NOT EXISTS storage_type                                           text,
  ADD COLUMN IF NOT EXISTS storage_volume                                         numeric,
  ADD COLUMN IF NOT EXISTS marketplace                                            text,
  ADD COLUMN IF NOT EXISTS product_group                                          text,
  ADD COLUMN IF NOT EXISTS sales_rank                                             integer,
  ADD COLUMN IF NOT EXISTS days_of_supply                                         integer,
  ADD COLUMN IF NOT EXISTS estimated_excess_quantity                              integer,
  ADD COLUMN IF NOT EXISTS weeks_of_cover_t30                                     numeric,
  ADD COLUMN IF NOT EXISTS weeks_of_cover_t90                                     numeric,
  ADD COLUMN IF NOT EXISTS estimated_storage_cost_next_month                      numeric,
  ADD COLUMN IF NOT EXISTS inbound_quantity                                       integer,
  ADD COLUMN IF NOT EXISTS inbound_working                                        integer,
  ADD COLUMN IF NOT EXISTS inbound_shipped                                        integer,
  ADD COLUMN IF NOT EXISTS inbound_received                                       integer,
  ADD COLUMN IF NOT EXISTS no_sale_last_6_months                                  integer,
  ADD COLUMN IF NOT EXISTS total_reserved_quantity                                integer,
  ADD COLUMN IF NOT EXISTS unfulfillable_quantity                                 integer,
  ADD COLUMN IF NOT EXISTS historical_days_of_supply                              integer,
  ADD COLUMN IF NOT EXISTS fba_minimum_inventory_level                            integer,
  ADD COLUMN IF NOT EXISTS fba_inventory_level_health_status                      text,
  ADD COLUMN IF NOT EXISTS recommended_ship_in_quantity                           integer,
  ADD COLUMN IF NOT EXISTS recommended_ship_in_date                               text,
  ADD COLUMN IF NOT EXISTS inventory_age_snapshot_date                            text,
  ADD COLUMN IF NOT EXISTS inventory_supply_at_fba                                integer,
  ADD COLUMN IF NOT EXISTS reserved_fc_transfer                                   integer,
  ADD COLUMN IF NOT EXISTS reserved_fc_processing                                 integer,
  ADD COLUMN IF NOT EXISTS reserved_customer_order                                integer,
  ADD COLUMN IF NOT EXISTS total_days_of_supply_including_open_shipments          integer,
  ADD COLUMN IF NOT EXISTS supplier                                               text,
  ADD COLUMN IF NOT EXISTS is_seasonal_in_next_3_months                           boolean,
  ADD COLUMN IF NOT EXISTS season_name                                            text,
  ADD COLUMN IF NOT EXISTS season_start_date                                      text,
  ADD COLUMN IF NOT EXISTS season_end_date                                        text;

-- ── 3) amazon_inbound_performance — NEW table ────────────────────────────────
CREATE TABLE IF NOT EXISTS public.amazon_inbound_performance (
  id                            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id               uuid        NOT NULL,
  store_id                      uuid        REFERENCES public.stores (id) ON DELETE SET NULL,
  source_upload_id              uuid        REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  source_line_hash              text        NOT NULL,
  source_file_sha256            text        NOT NULL,
  source_physical_row_number    integer     NOT NULL,
  issue_reported_date           text,
  shipment_creation_date        text,
  fba_shipment_id               text,
  fba_carton_id                 text,
  fulfillment_center_id         text,
  sku                           text,
  fnsku                         text,
  asin                          text,
  product_name                  text,
  problem_type                  text,
  problem_quantity              integer,
  expected_quantity             integer,
  received_quantity             integer,
  performance_measurement_unit  text,
  coaching_level                text,
  fee_type                      text,
  currency                      text,
  fee_total                     numeric,
  problem_level                 text,
  alert_status                  text,
  raw_data                      jsonb,
  created_at                    timestamptz NOT NULL DEFAULT now(),
  updated_at                    timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.amazon_inbound_performance IS
  'Raw rows from Amazon Inbound Performance report — one row per shipment problem entry.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_inbound_performance_org_file_row
  ON public.amazon_inbound_performance (organization_id, source_file_sha256, source_physical_row_number);

CREATE INDEX IF NOT EXISTS idx_amazon_inbound_performance_org_upload
  ON public.amazon_inbound_performance (organization_id, source_upload_id);

CREATE INDEX IF NOT EXISTS idx_amazon_inbound_performance_shipment
  ON public.amazon_inbound_performance (organization_id, fba_shipment_id);

ALTER TABLE public.amazon_inbound_performance ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS amazon_inbound_performance_service_bypass ON public.amazon_inbound_performance;
CREATE POLICY amazon_inbound_performance_service_bypass
  ON public.amazon_inbound_performance FOR ALL TO service_role
  USING (true) WITH CHECK (true);

-- ── 4) amazon_amazon_fulfilled_inventory — NEW table ─────────────────────────
-- Naming convention: amazon_<report-slug>; the report slug itself is
-- AMAZON_FULFILLED_INVENTORY so the destination table is
-- amazon_amazon_fulfilled_inventory (matches sync_target_table in the registry).
CREATE TABLE IF NOT EXISTS public.amazon_amazon_fulfilled_inventory (
  id                            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id               uuid        NOT NULL,
  store_id                      uuid        REFERENCES public.stores (id) ON DELETE SET NULL,
  source_upload_id              uuid        REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  source_line_hash              text        NOT NULL,
  source_file_sha256            text        NOT NULL,
  source_physical_row_number    integer     NOT NULL,
  seller_sku                    text,
  fulfillment_channel_sku       text,
  asin                          text,
  condition_type                text,
  warehouse_condition_code      text,
  quantity_available            integer,
  raw_data                      jsonb,
  created_at                    timestamptz NOT NULL DEFAULT now(),
  updated_at                    timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.amazon_amazon_fulfilled_inventory IS
  'Raw rows from Amazon Fulfilled Inventory (AFI) snapshot — never to be confused with ALL_LISTINGS / ACTIVE_LISTINGS.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_amazon_fulfilled_inventory_org_file_row
  ON public.amazon_amazon_fulfilled_inventory (organization_id, source_file_sha256, source_physical_row_number);

CREATE INDEX IF NOT EXISTS idx_amazon_amazon_fulfilled_inventory_org_upload
  ON public.amazon_amazon_fulfilled_inventory (organization_id, source_upload_id);

CREATE INDEX IF NOT EXISTS idx_amazon_amazon_fulfilled_inventory_sku_asin
  ON public.amazon_amazon_fulfilled_inventory (organization_id, seller_sku, asin);

ALTER TABLE public.amazon_amazon_fulfilled_inventory ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS amazon_amazon_fulfilled_inventory_service_bypass
  ON public.amazon_amazon_fulfilled_inventory;
CREATE POLICY amazon_amazon_fulfilled_inventory_service_bypass
  ON public.amazon_amazon_fulfilled_inventory FOR ALL TO service_role
  USING (true) WITH CHECK (true);

-- ── 5) Extend raw_report_uploads.report_type CHECK ───────────────────────────
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
      'INBOUND_PERFORMANCE'::text,
      'AMAZON_FULFILLED_INVENTORY'::text,
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

COMMIT;

NOTIFY pgrst, 'reload schema';
