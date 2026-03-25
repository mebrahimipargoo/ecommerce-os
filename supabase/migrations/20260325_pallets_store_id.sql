-- ─────────────────────────────────────────────────────────────────────────────
-- Migration: Add store_id to pallets table
-- Completes the Pallet → Package → Item store inheritance chain.
-- Run AFTER 20260325_saas_stores_usage.sql (which creates the stores table).
-- ─────────────────────────────────────────────────────────────────────────────

alter table public.pallets
  add column if not exists store_id uuid
    references public.stores(id) on delete set null
    default null;

comment on column public.pallets.store_id is
  'The connected store this pallet was received / processed for.
   Inherited downstream via the hierarchy: Pallet → Package → Item.
   When a package is created inside this pallet the store_id auto-fills.';

create index if not exists idx_pallets_store_id
  on public.pallets (store_id)
  where store_id is not null;
