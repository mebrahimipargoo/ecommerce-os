-- =============================================================================
-- Remove legacy UNIQUE (organization_id, settlement_id) on amazon_settlements
--
-- Flat settlement .txt files repeat the same settlement_id on every line.
-- Constraint names vary by environment:
--   - amazon_settlement_id_unique (table CONSTRAINT from older DDL)
--   - uq_amazon_settlements_org_settlement_id (migration-created index)
--
-- Line-level identity is (organization_id, upload_id, amazon_line_key) only.
-- =============================================================================

ALTER TABLE public.amazon_settlements
  DROP CONSTRAINT IF EXISTS amazon_settlement_id_unique;

DROP INDEX IF EXISTS public.amazon_settlement_id_unique;

DROP INDEX IF EXISTS public.uq_amazon_settlements_org_settlement_id;

-- Re-assert the correct unique index (idempotent if 20260507 already ran)
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_settlements_org_upload_line
  ON public.amazon_settlements (organization_id, upload_id, amazon_line_key)
  NULLS NOT DISTINCT;

NOTIFY pgrst, 'reload schema';
