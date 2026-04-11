-- =============================================================================
-- Fix amazon_transactions unique constraint.
--
-- Problem: (organization_id, order_id, transaction_type) [and its 4-column
--   variant with amount] incorrectly collapses DISTINCT financial transaction
--   lines that share the same order_id + transaction_type but differ in SKU,
--   settlement_id, posted_date, or fee sub-type.  This silently drops data on
--   every import of a report with repeated order lines.
--
-- Fix:
--   1. Add source_line_hash TEXT — a deterministic FNV fingerprint of the
--      FULL normalized row content, computed in the JS mapper before insert.
--   2. Backfill existing rows with their UUID (already unique per row) so the
--      NOT NULL constraint can be applied without migrating old data.
--   3. Drop the old narrow index(es).
--   4. Add new correct index on (organization_id, source_line_hash).
--
-- Re-import behaviour after this fix:
--   • Same logical row from same or a different file → same hash → idempotent
--     upsert (row updated in place, no duplicate).
--   • Different rows (different amounts, SKUs, fee types, dates) → different
--     hashes → separate rows inserted.
--   • No cross-org collision possible (organization_id is always the first
--     segment of the hash input).
-- =============================================================================

-- 1. Add the column (idempotent — IF NOT EXISTS)
ALTER TABLE public.amazon_transactions
  ADD COLUMN IF NOT EXISTS source_line_hash text;

-- 2. Backfill: old rows get hash = id::text (guaranteed unique; content hash
--    is only meaningful for NEW imports after this migration).
UPDATE public.amazon_transactions
  SET source_line_hash = id::text
  WHERE source_line_hash IS NULL;

-- 3. Enforce NOT NULL now that every row has a value
ALTER TABLE public.amazon_transactions
  ALTER COLUMN source_line_hash SET NOT NULL;

-- 4. Drop old narrow constraint(s) — both possible names from prior migrations
DROP INDEX IF EXISTS uq_amazon_transactions_org_order_type;
DROP INDEX IF EXISTS uq_amazon_transactions_org_order_type_amount;
ALTER TABLE public.amazon_transactions
  DROP CONSTRAINT IF EXISTS uq_amazon_transactions_org_order_type;
ALTER TABLE public.amazon_transactions
  DROP CONSTRAINT IF EXISTS uq_amazon_transactions_org_order_type_amount;

-- 5. Add correct wide key (no WHERE partial — source_line_hash is always set)
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_transactions_org_hash
  ON public.amazon_transactions (organization_id, source_line_hash);

COMMENT ON COLUMN public.amazon_transactions.source_line_hash IS
  'FNV fingerprint of full row content (org_id + all CSV field values), computed '
  'in the JS mapper. Used as the dedup key: same logical row from any file → same '
  'hash → idempotent upsert. Old rows backfilled with id::text.';

NOTIFY pgrst, 'reload schema';
