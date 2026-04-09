-- Split lines that share the same quantities but differ by request/order date
-- (common in Removal Order Detail). Prevents hundreds of CSV rows collapsing
-- into one row when all quantity columns are NULL or identical.

BEGIN;

DROP INDEX IF EXISTS public.uq_amazon_removals_logical_line;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removals_logical_line
  ON public.amazon_removals (
    organization_id,
    order_id,
    sku,
    disposition,
    requested_quantity,
    shipped_quantity,
    disposed_quantity,
    cancelled_quantity,
    order_date
  )
  NULLS NOT DISTINCT;

DROP INDEX IF EXISTS public.uq_expected_packages_logical_line;

CREATE UNIQUE INDEX IF NOT EXISTS uq_expected_packages_logical_line
  ON public.expected_packages (
    organization_id,
    order_id,
    sku,
    disposition,
    requested_quantity,
    shipped_quantity,
    disposed_quantity,
    cancelled_quantity,
    order_date
  )
  NULLS NOT DISTINCT;

NOTIFY pgrst, 'reload schema';

COMMIT;
