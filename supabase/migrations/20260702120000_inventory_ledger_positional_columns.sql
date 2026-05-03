-- Additive columns for strict positional (col1–col15) Inventory Ledger imports.
-- No drops, no truncates, no index changes.

BEGIN;

ALTER TABLE public.amazon_inventory_ledger
  ADD COLUMN IF NOT EXISTS reference_id text,
  ADD COLUMN IF NOT EXISTS reason_code text,
  ADD COLUMN IF NOT EXISTS reconciled_quantity integer,
  ADD COLUMN IF NOT EXISTS unreconciled_quantity integer,
  ADD COLUMN IF NOT EXISTS source_file_name text,
  ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

COMMENT ON COLUMN public.amazon_inventory_ledger.reference_id IS
  'Amazon Inventory Ledger positional column 7 (reference id).';
COMMENT ON COLUMN public.amazon_inventory_ledger.reason_code IS
  'Amazon Inventory Ledger positional column 11 (reason code).';
COMMENT ON COLUMN public.amazon_inventory_ledger.reconciled_quantity IS
  'Amazon Inventory Ledger positional column 13.';
COMMENT ON COLUMN public.amazon_inventory_ledger.unreconciled_quantity IS
  'Amazon Inventory Ledger positional column 14.';
COMMENT ON COLUMN public.amazon_inventory_ledger.source_file_name IS
  'Original upload file name for audit (optional; set by sync).';

COMMIT;

NOTIFY pgrst, 'reload schema';
