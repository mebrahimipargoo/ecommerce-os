-- Returns processing table
-- Run in Supabase SQL Editor: Dashboard > SQL Editor > New query

create table if not exists public.returns (
  id              uuid        primary key default gen_random_uuid(),
  organization_id uuid        not null,
  rma_number      text        not null,
  marketplace     text        not null,   -- 'amazon' | 'walmart' | 'ebay'
  item_name       text        not null,
  condition       text        not null default 'unknown',
  status          text        not null default 'received',
  notes           text,
  created_at      timestamptz not null default now()
);

-- Index for org-scoped queries
create index if not exists idx_returns_org
  on public.returns (organization_id, created_at desc);

-- Optional RLS placeholder (enable when auth is wired up)
-- alter table public.returns enable row level security;
