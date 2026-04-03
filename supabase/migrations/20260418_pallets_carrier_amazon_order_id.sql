-- ============================================================
-- Migration: Add carrier_name and amazon_order_id to pallets
-- Date: 2026-04-18
--
-- Context:
--   The WMS "Flexible Auto-fill" system inherits data top-down:
--     Pallet → Package → Item
--   To enable pallet-level inheritance, pallets need:
--     • carrier_name      — shipping carrier (e.g. "UPS", "FedEx")
--     • amazon_order_id   — Amazon order reference (propagates to packages)
--
-- packages.carrier_name  → already exists (20250319_returns_v4_constraints)
-- packages.rma_number    → already exists (20260325_packages_rma_number)
-- packages.order_id      → already exists (20260327_packages_amazon_order_id)
-- returns.rma_number     → already exists (20260402120000_returns_rma_number)
--
-- PROTECTED (DO NOT TOUCH):
--   claim_submissions, system_settings, claim_submission_status enum
-- ============================================================

-- 1. carrier_name on pallets (nullable — optional at pallet creation)
ALTER TABLE public.pallets
  ADD COLUMN IF NOT EXISTS carrier_name text DEFAULT NULL;

COMMENT ON COLUMN public.pallets.carrier_name IS
  'Primary shipping carrier for this pallet (e.g. "UPS", "FedEx", "USPS"). '
  'Inherited by child packages when empty — the Package form auto-fills this '
  'value when the operator selects this pallet.';

-- 2. amazon_order_id on pallets (nullable — not all pallets have a single order)
ALTER TABLE public.pallets
  ADD COLUMN IF NOT EXISTS amazon_order_id text DEFAULT NULL;

COMMENT ON COLUMN public.pallets.amazon_order_id IS
  'Amazon (or other marketplace) order ID for this pallet. '
  'Inherited by child packages, then by child items, when set. '
  'Format: 111-1234567-8901234. Linked return items use this for claim submissions.';

-- 3. Refresh PostgREST schema cache
NOTIFY pgrst, 'reload schema';
