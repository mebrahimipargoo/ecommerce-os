-- Extra Amazon Removal Order Detail columns (stored on amazon_removals only).
-- expected_packages does not get these; worklist generation filters order_type = Return.

BEGIN;

ALTER TABLE public.amazon_removals
  ADD COLUMN IF NOT EXISTS order_source text,
  ADD COLUMN IF NOT EXISTS order_type text,
  ADD COLUMN IF NOT EXISTS last_updated_date date,
  ADD COLUMN IF NOT EXISTS in_process_quantity integer,
  ADD COLUMN IF NOT EXISTS removal_fee numeric(18, 6),
  ADD COLUMN IF NOT EXISTS currency text;

COMMENT ON COLUMN public.amazon_removals.order_source IS 'e.g. order-source from Removal Order Detail.';
COMMENT ON COLUMN public.amazon_removals.order_type IS 'Disposal | Liquidations | Return — worklist uses Return only.';
COMMENT ON COLUMN public.amazon_removals.last_updated_date IS 'last-updated-date from report.';
COMMENT ON COLUMN public.amazon_removals.in_process_quantity IS 'in-process-quantity from report.';
COMMENT ON COLUMN public.amazon_removals.removal_fee IS 'removal-fee from report.';
COMMENT ON COLUMN public.amazon_removals.currency IS 'currency from report.';

NOTIFY pgrst, 'reload schema';

COMMIT;
