-- Adds AI-extracted packing-slip manifest to the packages table.
-- Run in Supabase SQL Editor → safe to re-run (idempotent via ADD COLUMN IF NOT EXISTS).
--
-- Format stored in expected_items:
--   [{ "sku": "B08N5WRWNW", "expected_qty": 2, "description": "Echo Dot" }, ...]

alter table public.packages
  add column if not exists expected_items jsonb default null;

comment on column public.packages.expected_items is
  'AI-extracted list of expected SKU line-items from a scanned packing slip.
   Array of {sku, expected_qty, description} objects.
   Populated by the Manifest OCR flow in the Package drawer.
   Used by the Smart Diff reconciliation table to colour-code missing vs matched items.';
