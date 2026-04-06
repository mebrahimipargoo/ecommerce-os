-- =============================================================================
-- Amazon Settlement flat-file (.txt TSV) → amazon_settlements
--
-- Adds header-level physical columns from the settlement report + per-line
-- dedupe key. Replaces the old (organization_id, settlement_id) unique index
-- which only allowed one row per settlement.
-- =============================================================================

ALTER TABLE public.amazon_settlements
  ADD COLUMN IF NOT EXISTS settlement_start_date timestamptz,
  ADD COLUMN IF NOT EXISTS settlement_end_date timestamptz,
  ADD COLUMN IF NOT EXISTS deposit_date timestamptz,
  ADD COLUMN IF NOT EXISTS total_amount numeric,
  ADD COLUMN IF NOT EXISTS currency text,
  ADD COLUMN IF NOT EXISTS amazon_line_key text;

COMMENT ON COLUMN public.amazon_settlements.settlement_start_date IS
  'Amazon settlement-start-date from flat settlement .txt (header columns).';
COMMENT ON COLUMN public.amazon_settlements.settlement_end_date IS
  'Amazon settlement-end-date from flat settlement .txt.';
COMMENT ON COLUMN public.amazon_settlements.deposit_date IS
  'Amazon deposit-date from flat settlement .txt.';
COMMENT ON COLUMN public.amazon_settlements.total_amount IS
  'Amazon total-amount on the line (summary row) or null on detail lines.';
COMMENT ON COLUMN public.amazon_settlements.currency IS
  'ISO currency from settlement report (e.g. USD).';
COMMENT ON COLUMN public.amazon_settlements.amazon_line_key IS
  'Stable dedupe key per import line (hash). Upsert target with organization_id + upload_id.';

-- Backfill existing rows before NOT NULL + unique index
UPDATE public.amazon_settlements
SET amazon_line_key = id::text
WHERE amazon_line_key IS NULL OR trim(amazon_line_key) = '';

ALTER TABLE public.amazon_settlements
  ALTER COLUMN amazon_line_key SET NOT NULL;

DROP INDEX IF EXISTS public.uq_amazon_settlements_org_settlement_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_settlements_org_upload_line
  ON public.amazon_settlements (organization_id, upload_id, amazon_line_key)
  NULLS NOT DISTINCT;

COMMENT ON INDEX public.uq_amazon_settlements_org_upload_line IS
  'One row per (org, upload, line) for settlement flat files; legacy rows use amazon_line_key = id.';

NOTIFY pgrst, 'reload schema';
