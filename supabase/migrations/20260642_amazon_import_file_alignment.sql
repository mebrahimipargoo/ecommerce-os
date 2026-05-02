-- =============================================================================
-- 20260642 — Amazon import file alignment (additive only).
--
-- Adds physical columns to existing tables for correct typed import of:
--   1) Amazon Fulfilled Shipments report     → amazon_all_orders
--   2) Amazon Transaction / Payment Detail   → amazon_settlements
--   3) Simple Transactions Summary           → amazon_transactions
--   4) Headerless Inventory Ledger           → amazon_inventory_ledger
--   5) Restock Inventory                     → amazon_manage_fba_inventory
--   6) Amazon Fulfilled Inventory            → amazon_amazon_fulfilled_inventory
--
-- Strict rules:
--   • No new tables.
--   • Every ALTER is `ADD COLUMN IF NOT EXISTS` — safe to re-run.
--   • Idempotent indexes use `CREATE INDEX IF NOT EXISTS`.
--   • Adds resolved_product_id / resolved_catalog_product_id / resolution status
--     columns so the post-import resolver can patch rows in place.
-- =============================================================================

BEGIN;

-- ── 1) amazon_all_orders ─────────────────────────────────────────────────────
-- Fulfilled Shipments report typed columns.
ALTER TABLE public.amazon_all_orders
  ADD COLUMN IF NOT EXISTS amazon_order_id     text,
  ADD COLUMN IF NOT EXISTS merchant_order_id   text,
  ADD COLUMN IF NOT EXISTS sku                 text,
  ADD COLUMN IF NOT EXISTS product_name        text,
  ADD COLUMN IF NOT EXISTS quantity            integer,
  ADD COLUMN IF NOT EXISTS currency            text,
  ADD COLUMN IF NOT EXISTS item_price          numeric,
  ADD COLUMN IF NOT EXISTS item_tax            numeric,
  ADD COLUMN IF NOT EXISTS shipping_price      numeric,
  ADD COLUMN IF NOT EXISTS ship_country        text,
  ADD COLUMN IF NOT EXISTS resolved_product_id           uuid,
  ADD COLUMN IF NOT EXISTS resolved_catalog_product_id   uuid,
  ADD COLUMN IF NOT EXISTS identifier_resolution_status  text,
  ADD COLUMN IF NOT EXISTS identifier_resolution_confidence numeric(10, 4);

COMMENT ON COLUMN public.amazon_all_orders.amazon_order_id IS
  'Amazon Order Id from Fulfilled Shipments report; mirrored to order_id for legacy joins.';
COMMENT ON COLUMN public.amazon_all_orders.merchant_order_id IS
  'Merchant Order Id from Fulfilled Shipments report.';
COMMENT ON COLUMN public.amazon_all_orders.sku IS
  'Merchant SKU from Fulfilled Shipments line.';

CREATE INDEX IF NOT EXISTS idx_amazon_all_orders_org_amazon_order_id
  ON public.amazon_all_orders (organization_id, amazon_order_id)
  WHERE amazon_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_amazon_all_orders_org_sku
  ON public.amazon_all_orders (organization_id, sku)
  WHERE sku IS NOT NULL;

-- ── 2) amazon_settlements ────────────────────────────────────────────────────
-- Transaction / Payment Detail report typed columns.
ALTER TABLE public.amazon_settlements
  ADD COLUMN IF NOT EXISTS quantity                 integer,
  ADD COLUMN IF NOT EXISTS marketplace              text,
  ADD COLUMN IF NOT EXISTS account_type             text,
  ADD COLUMN IF NOT EXISTS fulfillment_channel      text,
  ADD COLUMN IF NOT EXISTS product_sales_tax        numeric,
  ADD COLUMN IF NOT EXISTS shipping_credits         numeric,
  ADD COLUMN IF NOT EXISTS shipping_credits_tax     numeric,
  ADD COLUMN IF NOT EXISTS gift_wrap_credits        numeric,
  ADD COLUMN IF NOT EXISTS giftwrap_credits_tax     numeric,
  ADD COLUMN IF NOT EXISTS regulatory_fee           numeric,
  ADD COLUMN IF NOT EXISTS tax_on_regulatory_fee    numeric,
  ADD COLUMN IF NOT EXISTS promotional_rebates      numeric,
  ADD COLUMN IF NOT EXISTS promotional_rebates_tax  numeric,
  ADD COLUMN IF NOT EXISTS marketplace_withheld_tax numeric,
  ADD COLUMN IF NOT EXISTS other_transaction_fees   numeric,
  ADD COLUMN IF NOT EXISTS other_amount             numeric,
  ADD COLUMN IF NOT EXISTS transaction_status       text,
  ADD COLUMN IF NOT EXISTS transaction_release_date timestamptz,
  ADD COLUMN IF NOT EXISTS resolved_product_id            uuid,
  ADD COLUMN IF NOT EXISTS resolved_catalog_product_id    uuid,
  ADD COLUMN IF NOT EXISTS identifier_resolution_status   text,
  ADD COLUMN IF NOT EXISTS identifier_resolution_confidence numeric(10, 4);

CREATE INDEX IF NOT EXISTS idx_amazon_settlements_org_order
  ON public.amazon_settlements (organization_id, order_id)
  WHERE order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_amazon_settlements_org_sku
  ON public.amazon_settlements (organization_id, sku)
  WHERE sku IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_amazon_settlements_org_resolved_product
  ON public.amazon_settlements (organization_id, resolved_product_id)
  WHERE resolved_product_id IS NOT NULL;

-- ── 3) amazon_transactions ───────────────────────────────────────────────────
-- Simple Transactions Summary keeps order_id/transaction_type/amount as natives;
-- only resolver columns are needed.
ALTER TABLE public.amazon_transactions
  ADD COLUMN IF NOT EXISTS resolved_product_id            uuid,
  ADD COLUMN IF NOT EXISTS resolved_catalog_product_id    uuid,
  ADD COLUMN IF NOT EXISTS identifier_resolution_status   text,
  ADD COLUMN IF NOT EXISTS identifier_resolution_confidence numeric(10, 4);

CREATE INDEX IF NOT EXISTS idx_amazon_transactions_org_order
  ON public.amazon_transactions (organization_id, order_id)
  WHERE order_id IS NOT NULL;

-- ── 4) amazon_inventory_ledger ───────────────────────────────────────────────
-- Headerless ledger positional columns (col1=event_date, col15=event_timestamp).
ALTER TABLE public.amazon_inventory_ledger
  ADD COLUMN IF NOT EXISTS event_date          date,
  ADD COLUMN IF NOT EXISTS event_timestamp     timestamptz,
  ADD COLUMN IF NOT EXISTS product_name        text,
  ADD COLUMN IF NOT EXISTS country             text,
  -- typed identifier columns (originally only fnsku was stored at the root)
  ADD COLUMN IF NOT EXISTS sku                 text,
  ADD COLUMN IF NOT EXISTS asin                text;

COMMENT ON COLUMN public.amazon_inventory_ledger.event_date IS
  'Event date from Amazon ledger (positional col1 in headerless export). Distinct from created_at (import timestamp).';
COMMENT ON COLUMN public.amazon_inventory_ledger.event_timestamp IS
  'Event timestamp from Amazon ledger (positional col15 in headerless export).';

CREATE INDEX IF NOT EXISTS idx_amazon_inventory_ledger_org_event_date
  ON public.amazon_inventory_ledger (organization_id, event_date)
  WHERE event_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_amazon_inventory_ledger_org_sku
  ON public.amazon_inventory_ledger (organization_id, sku)
  WHERE sku IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_amazon_inventory_ledger_org_asin
  ON public.amazon_inventory_ledger (organization_id, asin)
  WHERE asin IS NOT NULL;

-- ── 5) amazon_manage_fba_inventory ───────────────────────────────────────────
-- Restock Inventory typed columns.
ALTER TABLE public.amazon_manage_fba_inventory
  ADD COLUMN IF NOT EXISTS inbound_quantity            integer,
  ADD COLUMN IF NOT EXISTS fc_transfer_quantity        integer,
  ADD COLUMN IF NOT EXISTS fc_processing_quantity      integer,
  ADD COLUMN IF NOT EXISTS customer_order_quantity     integer,
  ADD COLUMN IF NOT EXISTS recommended_replenishment_qty integer,
  ADD COLUMN IF NOT EXISTS recommended_ship_date       date,
  ADD COLUMN IF NOT EXISTS recommended_action          text,
  ADD COLUMN IF NOT EXISTS unit_storage_size           text,
  ADD COLUMN IF NOT EXISTS resolved_product_id            uuid,
  ADD COLUMN IF NOT EXISTS resolved_catalog_product_id    uuid,
  ADD COLUMN IF NOT EXISTS identifier_resolution_status   text,
  ADD COLUMN IF NOT EXISTS identifier_resolution_confidence numeric(10, 4);

-- ── 6) amazon_amazon_fulfilled_inventory ─────────────────────────────────────
ALTER TABLE public.amazon_amazon_fulfilled_inventory
  ADD COLUMN IF NOT EXISTS resolved_product_id            uuid,
  ADD COLUMN IF NOT EXISTS resolved_catalog_product_id    uuid,
  ADD COLUMN IF NOT EXISTS identifier_resolution_status   text,
  ADD COLUMN IF NOT EXISTS identifier_resolution_confidence numeric(10, 4);

CREATE INDEX IF NOT EXISTS idx_amazon_afi_org_resolved_product
  ON public.amazon_amazon_fulfilled_inventory (organization_id, resolved_product_id)
  WHERE resolved_product_id IS NOT NULL;

COMMIT;

NOTIFY pgrst, 'reload schema';
