-- =============================================================================
-- Drop the legacy amazon_removals_order_sku_unique constraint
--
-- The previous migration (20260510) replaced the 3-column unique key with a
-- 5-column key, but a legacy constraint named "amazon_removals_order_sku_unique"
-- was not listed in our drop list. This causes upserts to fail even though the
-- correct 5-column conflict key is being supplied.
--
-- This migration drops any remaining legacy constraints/indexes so only the
-- new uq_amazon_removals_composite index governs uniqueness.
-- =============================================================================

BEGIN;

-- Drop ALL known variants of the old narrow unique constraint
ALTER TABLE public.amazon_removals DROP CONSTRAINT IF EXISTS amazon_removals_order_sku_unique;
ALTER TABLE public.amazon_removals DROP CONSTRAINT IF EXISTS unique_removal_order_sku;
ALTER TABLE public.amazon_removals DROP CONSTRAINT IF EXISTS uq_amazon_removals_org_order_sku;

DROP INDEX IF EXISTS public.amazon_removals_order_sku_unique;
DROP INDEX IF EXISTS public.unique_removal_order_sku;
DROP INDEX IF EXISTS public.uq_amazon_removals_org_order_sku;

-- Ensure the 5-column composite index exists (idempotent guard)
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_removals_composite
  ON public.amazon_removals (organization_id, order_id, sku, disposition, tracking_number)
  NULLS NOT DISTINCT;

NOTIFY pgrst, 'reload schema';

COMMIT;
