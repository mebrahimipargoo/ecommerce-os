-- =============================================================================
-- Global Refactor: Amazon_ prefix standardization
-- 1. Rename amazon_ledger_staging  →  amazon_staging
-- 2. Add raw_data JSONB to all amazon_ domain tables (idempotent)
-- 3. Re-create indexes under the new staging table name
-- =============================================================================

-- ── 1. Rename staging table ───────────────────────────────────────────────────
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'amazon_ledger_staging'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'amazon_staging'
  ) THEN
    ALTER TABLE public.amazon_ledger_staging RENAME TO amazon_staging;
  END IF;
END $$;

-- ── 2. Re-create indexes on renamed staging table ────────────────────────────
CREATE INDEX IF NOT EXISTS idx_amazon_staging_upload_id
  ON public.amazon_staging (upload_id)
  WHERE upload_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_amazon_staging_org_id
  ON public.amazon_staging (organization_id);

-- ── 3. raw_data JSONB column on all amazon_ domain tables ────────────────────
-- amazon_returns (FBA Returns)
ALTER TABLE public.amazon_returns
  ADD COLUMN IF NOT EXISTS raw_data jsonb;

-- amazon_removals (Removal Orders)
ALTER TABLE public.amazon_removals
  ADD COLUMN IF NOT EXISTS raw_data jsonb;

-- amazon_inventory_ledger (Inventory Ledger)
ALTER TABLE public.amazon_inventory_ledger
  ADD COLUMN IF NOT EXISTS raw_data jsonb;

-- amazon_reimbursements
ALTER TABLE public.amazon_reimbursements
  ADD COLUMN IF NOT EXISTS raw_data jsonb;

-- amazon_settlements
ALTER TABLE public.amazon_settlements
  ADD COLUMN IF NOT EXISTS raw_data jsonb;

-- amazon_safet_claims  — also ensure the correct column names exist
ALTER TABLE public.amazon_safet_claims
  ADD COLUMN IF NOT EXISTS safet_claim_id text;

ALTER TABLE public.amazon_safet_claims
  ADD COLUMN IF NOT EXISTS total_reimbursement_amount numeric;

ALTER TABLE public.amazon_safet_claims
  ADD COLUMN IF NOT EXISTS raw_data jsonb;

-- amazon_transactions
ALTER TABLE public.amazon_transactions
  ADD COLUMN IF NOT EXISTS raw_data jsonb;

-- ── 4. Comments ───────────────────────────────────────────────────────────────
COMMENT ON COLUMN public.amazon_returns.raw_data IS
  'JSONB bucket for CSV columns that do not map to a named DB column — prevents schema cache errors.';
COMMENT ON COLUMN public.amazon_removals.raw_data IS
  'JSONB bucket for CSV columns that do not map to a named DB column.';
COMMENT ON COLUMN public.amazon_inventory_ledger.raw_data IS
  'JSONB bucket for CSV columns that do not map to a named DB column.';
COMMENT ON COLUMN public.amazon_reimbursements.raw_data IS
  'JSONB bucket for CSV columns that do not map to a named DB column.';
COMMENT ON COLUMN public.amazon_settlements.raw_data IS
  'JSONB bucket for CSV columns that do not map to a named DB column.';
COMMENT ON COLUMN public.amazon_safet_claims.raw_data IS
  'JSONB bucket for CSV columns that do not map to a named DB column.';
COMMENT ON COLUMN public.amazon_transactions.raw_data IS
  'JSONB bucket for CSV columns that do not map to a named DB column.';

COMMENT ON COLUMN public.amazon_safet_claims.safet_claim_id IS
  'Primary SAFE-T claim identifier from Amazon CSV (maps from CSV "SAFE-T Claim ID").';
COMMENT ON COLUMN public.amazon_safet_claims.total_reimbursement_amount IS
  'Reimbursement total from Amazon CSV (maps from CSV "Reimbursement Amount").';

NOTIFY pgrst, 'reload schema';
