-- Parsed packing-slip / manifest line items (JSON array of { sku, expected_qty, description? }).
-- Kept distinct from expected_items for enterprise reconciliation + future schema evolution.

ALTER TABLE public.packages
  ADD COLUMN IF NOT EXISTS manifest_data JSONB DEFAULT NULL;

COMMENT ON COLUMN public.packages.manifest_data IS
  'Structured manifest lines extracted from packing slip OCR — drives Expected vs Scanned reconciliation.';
