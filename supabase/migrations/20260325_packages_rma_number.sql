-- Shift RMA tracking to the packages table.
-- Items now link to their RMA relationally through package_id.
-- Run in Supabase SQL Editor — idempotent (ADD COLUMN IF NOT EXISTS).

alter table public.packages
  add column if not exists rma_number text default null;

comment on column public.packages.rma_number is
  'Return Merchandise Authorization number for this package.
The RMA is now captured at the package level (not the item level).
Items link back to the RMA relationally through their package_id.';
