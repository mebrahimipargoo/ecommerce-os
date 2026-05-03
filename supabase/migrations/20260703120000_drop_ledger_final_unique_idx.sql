-- =============================================================================
-- Drop legacy unsafe unique index on amazon_inventory_ledger (5 business columns).
--
-- Event-level Inventory Ledger exports can have many rows sharing
-- (organization_id, fnsku, disposition, location, event_type). Correct
-- import identity is the physical line:
--   (organization_id, source_file_sha256, source_physical_row_number)
-- enforced by uq_amazon_inventory_ledger_org_file_row — NOT dropped here.
--
-- Settlement and other tables are untouched.
-- =============================================================================

DROP INDEX IF EXISTS public.ledger_final_unique_idx;

NOTIFY pgrst, 'reload schema';
