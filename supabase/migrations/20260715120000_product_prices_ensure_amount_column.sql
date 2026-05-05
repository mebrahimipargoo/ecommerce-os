-- Align public.product_prices with PIM schema (20260705120000_pim_model_stabilization.sql).
-- Old databases often have an older product_prices row shape; CREATE TABLE IF NOT EXISTS never
-- added new columns. The catalog RPC reads: amount, currency, observed_at (and id, org, store, product).
--
-- This migration only ADDS missing columns and backfills; it does not drop legacy columns.

BEGIN;

-- ── Columns expected by pim_catalog_products_page and dashboard APIs ───────────
ALTER TABLE public.product_prices ADD COLUMN IF NOT EXISTS amount numeric(18, 6);
ALTER TABLE public.product_prices ADD COLUMN IF NOT EXISTS currency text;
ALTER TABLE public.product_prices ADD COLUMN IF NOT EXISTS observed_at timestamptz;
ALTER TABLE public.product_prices ADD COLUMN IF NOT EXISTS source text;
ALTER TABLE public.product_prices ADD COLUMN IF NOT EXISTS source_upload_id uuid;
ALTER TABLE public.product_prices ADD COLUMN IF NOT EXISTS metadata jsonb;
ALTER TABLE public.product_prices ADD COLUMN IF NOT EXISTS created_at timestamptz;

-- ── Backfill amount from legacy names ─────────────────────────────────────────
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'product_prices' AND column_name = 'price'
  ) THEN
    EXECUTE $u$
      UPDATE public.product_prices
      SET amount = price::numeric
      WHERE amount IS NULL AND price IS NOT NULL
    $u$;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'product_prices' AND column_name = 'unit_price'
  ) THEN
    EXECUTE $u$
      UPDATE public.product_prices
      SET amount = unit_price::numeric
      WHERE amount IS NULL AND unit_price IS NOT NULL
    $u$;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'product_prices' AND column_name = 'cost'
  ) THEN
    EXECUTE $u$
      UPDATE public.product_prices
      SET amount = cost::numeric
      WHERE amount IS NULL AND cost IS NOT NULL
    $u$;
  END IF;
END $$;

UPDATE public.product_prices SET amount = 0 WHERE amount IS NULL;

-- ── observed_at / created_at (RPC orders by observed_at) ───────────────────────
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'product_prices' AND column_name = 'recorded_at'
  ) THEN
    EXECUTE $u$
      UPDATE public.product_prices
      SET observed_at = recorded_at
      WHERE observed_at IS NULL AND recorded_at IS NOT NULL
    $u$;
  END IF;
END $$;

UPDATE public.product_prices SET observed_at = now() WHERE observed_at IS NULL;

UPDATE public.product_prices
SET created_at = COALESCE(created_at, observed_at, now())
WHERE created_at IS NULL;

UPDATE public.product_prices SET currency = 'USD' WHERE currency IS NULL OR btrim(currency) = '';

UPDATE public.product_prices SET metadata = '{}'::jsonb WHERE metadata IS NULL;

-- ── Defaults and NOT NULL (match stabilization) ───────────────────────────────
ALTER TABLE public.product_prices ALTER COLUMN amount SET DEFAULT 0;
ALTER TABLE public.product_prices ALTER COLUMN amount SET NOT NULL;

ALTER TABLE public.product_prices ALTER COLUMN currency SET DEFAULT 'USD';
ALTER TABLE public.product_prices ALTER COLUMN currency SET NOT NULL;

ALTER TABLE public.product_prices ALTER COLUMN observed_at SET DEFAULT now();
ALTER TABLE public.product_prices ALTER COLUMN observed_at SET NOT NULL;

ALTER TABLE public.product_prices ALTER COLUMN metadata SET DEFAULT '{}'::jsonb;
ALTER TABLE public.product_prices ALTER COLUMN metadata SET NOT NULL;

ALTER TABLE public.product_prices ALTER COLUMN created_at SET DEFAULT now();
ALTER TABLE public.product_prices ALTER COLUMN created_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_product_prices_org_store_product_observed
  ON public.product_prices (organization_id, store_id, product_id, observed_at DESC);

COMMENT ON TABLE public.product_prices IS
  'Append-only price observations per org + store + product; latest row answers “current price”.';

COMMENT ON COLUMN public.product_prices.amount IS
  'Price/cost observation amount; used by PIM catalog RPC for latest price.';

NOTIFY pgrst, 'reload schema';

COMMIT;
