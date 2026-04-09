-- =============================================================================
-- Zero-Data-Loss Fix: amazon_removals + expected_packages schema update
--
-- Problem 1 (Data Loss in amazon_removals):
--   The old 3-column constraint (organization_id, order_id, sku) collapsed
--   4,908 staging rows → ~962 rows. Amazon splits removal data by disposition
--   (Sellable vs CustomerDamaged) AND by tracking number (one SKU, many boxes).
--
--   Fix: 5-column constraint with NULLS NOT DISTINCT (Postgres 15+).
--   New key: (organization_id, order_id, sku, disposition, tracking_number)
--
-- Problem 2 (Missing columns):
--   Removal Shipment Detail files contain carrier, shipment_date, order_date
--   that were not stored. Added with ADD COLUMN IF NOT EXISTS.
--
-- Problem 3 (expected_packages unique key):
--   The old 3-column key prevented multiple tracking numbers for the same
--   order+SKU (multiple boxes). New 4-column key includes tracking_number.
--   Also adds carrier + shipment_date columns for warehouse scanner display.
-- =============================================================================

BEGIN;

-- ── amazon_removals: add missing columns ─────────────────────────────────────
ALTER TABLE public.amazon_removals
  ADD COLUMN IF NOT EXISTS carrier       text,
  ADD COLUMN IF NOT EXISTS shipment_date date,
  ADD COLUMN IF NOT EXISTS order_date    date;

-- ── amazon_removals: replace 3-column key with 5-column key ─────────────────
DROP INDEX IF EXISTS public.uq_amazon_removals_org_order_sku;
ALTER TABLE public.amazon_removals DROP CONSTRAINT IF EXISTS unique_removal_order_sku;
ALTER TABLE public.amazon_removals DROP CONSTRAINT IF EXISTS uq_amazon_removals_org_order_sku;

-- Each distinct (org, order, sku, disposition, tracking) combo is one unique row.
-- NULLS NOT DISTINCT: (org, order, sku, NULL, NULL) counts as a single slot.
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removals_composite
  ON public.amazon_removals (organization_id, order_id, sku, disposition, tracking_number)
  NULLS NOT DISTINCT;

-- Lookup index for the "find all rows for order+SKU" query in the ETL merge step
CREATE INDEX IF NOT EXISTS idx_amazon_removals_org_order_sku
  ON public.amazon_removals (organization_id, order_id, sku);

-- ── expected_packages: add carrier + shipment_date columns ───────────────────
ALTER TABLE public.expected_packages
  ADD COLUMN IF NOT EXISTS carrier       text,
  ADD COLUMN IF NOT EXISTS shipment_date date;

-- ── expected_packages: replace 3-column key with 4-column key ───────────────
-- Old key: (organization_id, order_id, sku) — blocked multiple boxes per order+SKU
-- New key: (organization_id, order_id, sku, tracking_number) NULLS NOT DISTINCT
--   • tracking_number = NULL → one pre-scan row before the shipment arrives
--   • tracking_number = '1Z…' → one scannable entry per unique box
DROP INDEX IF EXISTS public.expected_packages_org_order_sku;

CREATE UNIQUE INDEX IF NOT EXISTS uq_expected_packages_org_order_sku_trk
  ON public.expected_packages (organization_id, order_id, sku, tracking_number)
  NULLS NOT DISTINCT;

-- Notify PostgREST to reload schema cache
NOTIFY pgrst, 'reload schema';

COMMIT;
