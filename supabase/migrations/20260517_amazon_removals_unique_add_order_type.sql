-- amazon_removals: include order_type in the logical-line unique index so the same
-- order/sku/qty/date cannot collide across Disposal vs Liquidations vs Return.
--
-- expected_packages: unchanged — no order_type column there; Phase 4 only upserts
-- Return rows via application filter, and the 8-column + order_date key is enough.

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
    order_date,
    order_type
  )
  NULLS NOT DISTINCT;

NOTIFY pgrst, 'reload schema';

COMMIT;
