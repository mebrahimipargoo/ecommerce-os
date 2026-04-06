-- Physical columns for Amazon settlement flat-file lines (TSV) synced as TRANSACTIONS.
ALTER TABLE public.amazon_transactions
  ADD COLUMN IF NOT EXISTS settlement_id text,
  ADD COLUMN IF NOT EXISTS sku text,
  ADD COLUMN IF NOT EXISTS posted_date timestamptz;

COMMENT ON COLUMN public.amazon_transactions.settlement_id IS
  'Amazon settlement-id from flat-file settlement / transaction detail reports.';
COMMENT ON COLUMN public.amazon_transactions.sku IS
  'Merchant SKU from the report line (when present).';
COMMENT ON COLUMN public.amazon_transactions.posted_date IS
  'Amazon posted-date / event timestamp from the report line.';

NOTIFY pgrst, 'reload schema';
