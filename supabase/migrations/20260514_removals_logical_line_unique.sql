-- Logical removal line: uniqueness excludes tracking_number (filled later from Shipment Detail).
-- Includes four quantity columns so distinct Amazon CSV lines are not collapsed.
-- Aligns expected_packages with amazon_removals for Phase 4 worklist upserts.

BEGIN;

DROP INDEX IF EXISTS public.uq_amazon_removals_composite;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removals_logical_line
  ON public.amazon_removals (
    organization_id,
    order_id,
    sku,
    disposition,
    requested_quantity,
    shipped_quantity,
    disposed_quantity,
    cancelled_quantity
  )
  NULLS NOT DISTINCT;

DROP INDEX IF EXISTS public.uq_expected_packages_composite;

CREATE UNIQUE INDEX IF NOT EXISTS uq_expected_packages_logical_line
  ON public.expected_packages (
    organization_id,
    order_id,
    sku,
    disposition,
    requested_quantity,
    shipped_quantity,
    disposed_quantity,
    cancelled_quantity
  )
  NULLS NOT DISTINCT;

NOTIFY pgrst, 'reload schema';

COMMIT;
