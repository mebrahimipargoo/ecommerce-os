-- =============================================================================
-- Fix amazon_inventory_ledger unique constraint
--
-- Problem: migration 20260502 created a 2-column unique index
--   (organization_id, fnsku)
-- but the sync route uses a 5-column onConflict key:
--   (organization_id, fnsku, disposition, location, event_type)
--
-- Postgres requires the ON CONFLICT clause to exactly match an existing unique
-- index. When no 5-column index exists, Postgres falls back to a plain INSERT,
-- which then immediately violates the 2-column index — producing the error:
--   "duplicate key value violates unique constraint uq_amazon_inventory_ledger_org_fnsku"
--
-- Fix: drop the 2-column index and replace it with the correct 5-column one.
-- =============================================================================

-- Drop the incorrect 2-column index from migration 20260502
DROP INDEX IF EXISTS public.uq_amazon_inventory_ledger_org_fnsku;

-- Create the correct 5-column index that matches the sync route's onConflict key.
-- Partial index (fnsku IS NOT NULL) keeps it lean — rows without an FNSKU are
-- not addressable via the ledger upsert path anyway.
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_inventory_ledger_org_fnsku
  ON public.amazon_inventory_ledger (
    organization_id,
    fnsku,
    disposition,
    location,
    event_type
  )
  WHERE fnsku IS NOT NULL;

COMMENT ON INDEX public.uq_amazon_inventory_ledger_org_fnsku IS
  'Supports upsert ON CONFLICT (organization_id, fnsku, disposition, location, event_type). '
  'Replaces the incorrect 2-column index from migration 20260502.';

-- Reload PostgREST schema cache so the new index is visible immediately.
NOTIFY pgrst, 'reload schema';
