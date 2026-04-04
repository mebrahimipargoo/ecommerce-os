-- =============================================================================
-- Fix amazon_reimbursements: add sku column + replace 2-column index
--
-- Problem: the sync route maps a `sku` field and uses conflict key
--   (organization_id, reimbursement_id, sku)
-- but the table had no `sku` column and only a 2-column unique index.
-- This caused: "Could not find the 'sku' column in the schema cache"
--
-- Fix:
--   1. Add sku text column
--   2. Drop old 2-column index (uq_amazon_reimbursements_org_reimb_id)
--   3. Create correct 3-column index with NULLS NOT DISTINCT
-- =============================================================================

ALTER TABLE public.amazon_reimbursements
  ADD COLUMN IF NOT EXISTS sku text;

COMMENT ON COLUMN public.amazon_reimbursements.sku IS
  'Seller SKU for the reimbursed item. One reimbursement_id can cover multiple SKUs.';

DROP INDEX IF EXISTS public.uq_amazon_reimbursements_org_reimb_id;

-- Full (non-partial) index required: Supabase onConflict cannot resolve a
-- partial index (WHERE clause) without embedding the predicate in ON CONFLICT.
-- NULLS NOT DISTINCT: treats NULL sku values as equal for uniqueness.
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_reimbursements_org_reimb_sku
  ON public.amazon_reimbursements (organization_id, reimbursement_id, sku)
  NULLS NOT DISTINCT;

COMMENT ON INDEX public.uq_amazon_reimbursements_org_reimb_sku IS
  'Full unique index (no WHERE) required for Supabase ON CONFLICT (organization_id, reimbursement_id, sku). '
  'NULLS NOT DISTINCT prevents (org, reimb_id, NULL) bypass.';

NOTIFY pgrst, 'reload schema';
