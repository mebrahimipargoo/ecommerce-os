-- =============================================================================
-- Fix all amazon_* partial indexes → full indexes
--
-- Problem: migration 20260502 created all unique indexes as partial indexes
-- with WHERE clauses (e.g. WHERE lpn IS NOT NULL). Supabase upsert with
-- onConflict cannot resolve a partial index without embedding the WHERE
-- predicate in the ON CONFLICT clause — which the client does not support.
-- Error: "there is no unique or exclusion constraint matching the ON CONFLICT"
--
-- Fix: drop all partial indexes, recreate as full indexes with NULLS NOT DISTINCT
-- (Postgres 15+) so NULL values are treated as equal for uniqueness checks.
--
-- Also fixes amazon_transactions: old index had 3 columns but conflict key
-- specifies 4 (organization_id, order_id, transaction_type, amount).
-- =============================================================================

-- amazon_returns
DROP INDEX IF EXISTS public.uq_amazon_returns_org_lpn;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_returns_org_lpn
  ON public.amazon_returns (organization_id, lpn)
  NULLS NOT DISTINCT;

-- amazon_removals
DROP INDEX IF EXISTS public.uq_amazon_removals_org_order_sku;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removals_org_order_sku
  ON public.amazon_removals (organization_id, order_id, sku)
  NULLS NOT DISTINCT;

-- amazon_inventory_ledger (ledger_final_unique_idx full index also exists — both are fine)
DROP INDEX IF EXISTS public.uq_amazon_inventory_ledger_org_fnsku;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_inventory_ledger_org_fnsku
  ON public.amazon_inventory_ledger (organization_id, fnsku, disposition, location, event_type)
  NULLS NOT DISTINCT;

-- amazon_settlements
DROP INDEX IF EXISTS public.uq_amazon_settlements_org_settlement_id;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_settlements_org_settlement_id
  ON public.amazon_settlements (organization_id, settlement_id)
  NULLS NOT DISTINCT;

-- amazon_safet_claims
DROP INDEX IF EXISTS public.uq_amazon_safet_claims_org_claim_id;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_safet_claims_org_claim_id
  ON public.amazon_safet_claims (organization_id, safet_claim_id)
  NULLS NOT DISTINCT;

-- amazon_transactions (was 3-col index but conflict key needs 4: add amount)
DROP INDEX IF EXISTS public.uq_amazon_transactions_org_order_type;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_transactions_org_order_type
  ON public.amazon_transactions (organization_id, order_id, transaction_type, amount)
  NULLS NOT DISTINCT;

NOTIFY pgrst, 'reload schema';
