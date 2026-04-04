-- =============================================================================
-- Amazon domain table schema alignment
--
-- Fixes required after comparing live DB schema against TypeScript mappers:
--   1. Add raw_data JSONB to amazon_settlements (was missing from original DDL)
--   2. Add unique indexes to all 7 amazon_ domain tables so upsert ON CONFLICT works
-- =============================================================================

-- ── 1. amazon_settlements: add missing raw_data column ────────────────────────
ALTER TABLE public.amazon_settlements
  ADD COLUMN IF NOT EXISTS raw_data jsonb;

COMMENT ON COLUMN public.amazon_settlements.raw_data IS
  'JSONB bucket for CSV columns that do not map to a named DB column — prevents schema cache errors.';

-- ── 2. Unique indexes for upsert ON CONFLICT support ──────────────────────────
-- amazon_returns  (multi-tenant: org + lpn)
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_returns_org_lpn
  ON public.amazon_returns (organization_id, lpn)
  WHERE lpn IS NOT NULL;

-- amazon_removals  (replace non-tenant constraint/index with org-scoped one)
ALTER TABLE public.amazon_removals DROP CONSTRAINT IF EXISTS unique_removal_order_sku;
DROP INDEX IF EXISTS unique_removal_order_sku;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removals_org_order_sku
  ON public.amazon_removals (organization_id, order_id, sku)
  WHERE order_id IS NOT NULL;

-- amazon_reimbursements  (org + reimbursement_id)
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_reimbursements_org_reimb_id
  ON public.amazon_reimbursements (organization_id, reimbursement_id)
  WHERE reimbursement_id IS NOT NULL;

-- amazon_settlements  (org + settlement_id)
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_settlements_org_settlement_id
  ON public.amazon_settlements (organization_id, settlement_id)
  WHERE settlement_id IS NOT NULL;

-- amazon_safet_claims  (org + safet_claim_id)
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_safet_claims_org_claim_id
  ON public.amazon_safet_claims (organization_id, safet_claim_id)
  WHERE safet_claim_id IS NOT NULL;

-- amazon_transactions  (org + order_id + transaction_type)
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_transactions_org_order_type
  ON public.amazon_transactions (organization_id, order_id, transaction_type)
  WHERE order_id IS NOT NULL;

-- amazon_inventory_ledger  (org + fnsku)
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_inventory_ledger_org_fnsku
  ON public.amazon_inventory_ledger (organization_id, fnsku)
  WHERE fnsku IS NOT NULL;

-- ── Reload PostgREST schema cache ─────────────────────────────────────────────
NOTIFY pgrst, 'reload schema';
