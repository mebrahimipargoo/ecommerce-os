-- Run this in Supabase SQL Editor: Dashboard > SQL Editor > New query
-- Add organization_id to marketplaces for multi-tenant / hierarchical access
alter table public.marketplaces
add column if not exists organization_id uuid not null default '00000000-0000-0000-0000-000000000001';

create index if not exists idx_marketplaces_organization_id
  on public.marketplaces (organization_id);

-- RBAC: minimum role required to manage this marketplace connection
alter table public.marketplaces
add column if not exists role_required text not null default 'admin';
