-- =============================================================================
-- 1. stores: add is_default boolean column
--    Allows one store per organisation to be auto-selected in the importer.
-- 2. amazon_transactions: add amount column
--    Hard-coded fallback mapping for CSV "amount" column from Reports Repository
--    Transactions files (prevents the value from being buried in raw_data).
-- =============================================================================

-- ── 1. stores.is_default ─────────────────────────────────────────────────────
ALTER TABLE public.stores
  ADD COLUMN IF NOT EXISTS is_default boolean DEFAULT false;

-- Enforce at most one default store per organisation via a partial unique index.
CREATE UNIQUE INDEX IF NOT EXISTS uq_stores_default_per_org
  ON public.stores (organization_id)
  WHERE is_default = true;

COMMENT ON COLUMN public.stores.is_default IS
  'When true this store is auto-selected in the Universal Importer target-store dropdown.
   At most one store per organisation can be the default (enforced by partial unique index).';

-- ── 2. amazon_transactions.amount ────────────────────────────────────────────
ALTER TABLE public.amazon_transactions
  ADD COLUMN IF NOT EXISTS amount numeric;

COMMENT ON COLUMN public.amazon_transactions.amount IS
  'Transaction amount from Amazon CSV "amount" column (Reports Repository Transactions).
   Hard-coded fallback mapping — prevents burial in raw_data JSONB.';

-- Reload PostgREST schema cache so new columns are immediately accessible.
NOTIFY pgrst, 'reload schema';
