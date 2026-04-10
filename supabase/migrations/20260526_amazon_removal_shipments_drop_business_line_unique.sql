-- =============================================================================
-- Raw archive: `amazon_removal_shipments` must store one row per staging line
-- (unique on organization_id, upload_id, amazon_staging_id). The business-line
-- unique index prevented multiple archive rows when two staging lines mapped to
-- identical typed columns — blocking true 1:1 source preservation. Dedupe and
-- aggregation belong in derived layers (tree rebuild, allocations, reporting).
-- =============================================================================

BEGIN;

DROP INDEX IF EXISTS public.uq_amazon_removal_shipments_business_line;

-- Non-unique lookup on the former business-line columns (query paths only).
CREATE INDEX IF NOT EXISTS idx_amazon_removal_shipments_business_line_lookup
  ON public.amazon_removal_shipments (
    organization_id,
    store_id,
    order_id,
    tracking_number,
    sku,
    fnsku,
    disposition,
    requested_quantity,
    shipped_quantity,
    disposed_quantity,
    cancelled_quantity,
    order_date,
    order_type
  );

NOTIFY pgrst, 'reload schema';

COMMIT;
