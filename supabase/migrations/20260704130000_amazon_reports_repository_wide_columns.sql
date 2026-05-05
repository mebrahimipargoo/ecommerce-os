-- =============================================================================
-- amazon_reports_repository — wide Reports Repository export (additive columns).
-- Preserves uq_amazon_reports_repo_org_file_row (org + file sha + physical row).
-- No DROP/TRUNCATE; settlement and ledger pipelines unchanged.
-- =============================================================================

ALTER TABLE public.amazon_reports_repository
  ADD COLUMN IF NOT EXISTS quantity numeric,
  ADD COLUMN IF NOT EXISTS marketplace text,
  ADD COLUMN IF NOT EXISTS account_type text,
  ADD COLUMN IF NOT EXISTS fulfillment text,
  ADD COLUMN IF NOT EXISTS order_city text,
  ADD COLUMN IF NOT EXISTS order_state text,
  ADD COLUMN IF NOT EXISTS order_postal text,
  ADD COLUMN IF NOT EXISTS tax_collection_model text,
  ADD COLUMN IF NOT EXISTS product_sales numeric,
  ADD COLUMN IF NOT EXISTS product_sales_tax numeric,
  ADD COLUMN IF NOT EXISTS shipping_credits numeric,
  ADD COLUMN IF NOT EXISTS shipping_credits_tax numeric,
  ADD COLUMN IF NOT EXISTS gift_wrap_credits numeric,
  ADD COLUMN IF NOT EXISTS giftwrap_credits_tax numeric,
  ADD COLUMN IF NOT EXISTS regulatory_fee numeric,
  ADD COLUMN IF NOT EXISTS tax_on_regulatory_fee numeric,
  ADD COLUMN IF NOT EXISTS promotional_rebates numeric,
  ADD COLUMN IF NOT EXISTS promotional_rebates_tax numeric,
  ADD COLUMN IF NOT EXISTS marketplace_withheld_tax numeric,
  ADD COLUMN IF NOT EXISTS selling_fees numeric,
  ADD COLUMN IF NOT EXISTS fba_fees numeric,
  ADD COLUMN IF NOT EXISTS other_transaction_fees numeric,
  ADD COLUMN IF NOT EXISTS other_amount numeric,
  ADD COLUMN IF NOT EXISTS transaction_status text,
  ADD COLUMN IF NOT EXISTS transaction_release_date timestamptz;

-- Optional product linking (nullable; non-blocking sync).
ALTER TABLE public.amazon_reports_repository
  ADD COLUMN IF NOT EXISTS store_id uuid,
  ADD COLUMN IF NOT EXISTS product_id uuid,
  ADD COLUMN IF NOT EXISTS catalog_product_id uuid,
  ADD COLUMN IF NOT EXISTS product_match_method text,
  ADD COLUMN IF NOT EXISTS product_match_confidence numeric,
  ADD COLUMN IF NOT EXISTS product_matched_at timestamptz;

COMMENT ON COLUMN public.amazon_reports_repository.other_amount IS
  'Maps CSV column "other" (Amazon wording); avoids reserved word "other" as column name.';

NOTIFY pgrst, 'reload schema';
