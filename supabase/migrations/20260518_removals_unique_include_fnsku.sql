-- Amazon often outputs literal merchant sku "UNKNOW" for multiple distinct FNSKUs on the
-- same removal order — uniqueness must include fnsku or hundreds of lines collapse.

BEGIN;

ALTER TABLE public.expected_packages
  ADD COLUMN IF NOT EXISTS fnsku text;

DROP INDEX IF EXISTS public.uq_amazon_removals_logical_line;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removals_logical_line
  ON public.amazon_removals (
    organization_id,
    order_id,
    sku,
    fnsku,
    disposition,
    requested_quantity,
    shipped_quantity,
    disposed_quantity,
    cancelled_quantity,
    order_date,
    order_type
  )
  NULLS NOT DISTINCT;

DROP INDEX IF EXISTS public.uq_expected_packages_logical_line;

CREATE UNIQUE INDEX IF NOT EXISTS uq_expected_packages_logical_line
  ON public.expected_packages (
    organization_id,
    order_id,
    sku,
    fnsku,
    disposition,
    requested_quantity,
    shipped_quantity,
    disposed_quantity,
    cancelled_quantity,
    order_date
  )
  NULLS NOT DISTINCT;

COMMENT ON COLUMN public.expected_packages.fnsku IS
  'FNSKU from Removal Order Detail — required to distinguish UNKNOW merchant sku lines.';

NOTIFY pgrst, 'reload schema';

COMMIT;
