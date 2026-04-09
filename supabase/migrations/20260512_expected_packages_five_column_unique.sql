-- Align expected_packages uniqueness with amazon_removals so each distinct
-- (org, order, sku, disposition, tracking) survives Phase 4 — no collapsing
-- Sellable vs Damaged (or split quantities) into a single worklist row.
--
-- Replaces the 4-column index from 20260510.

BEGIN;

DROP INDEX IF EXISTS public.uq_expected_packages_org_order_sku_trk;

CREATE UNIQUE INDEX IF NOT EXISTS uq_expected_packages_composite
  ON public.expected_packages (
    organization_id,
    order_id,
    sku,
    disposition,
    tracking_number
  )
  NULLS NOT DISTINCT;

NOTIFY pgrst, 'reload schema';

COMMIT;
