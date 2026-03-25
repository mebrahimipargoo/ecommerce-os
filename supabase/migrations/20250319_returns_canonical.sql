-- ============================================================
-- CANONICAL returns table — paste into Supabase SQL Editor
-- Safe to run against a fresh database or an existing table.
-- ============================================================

-- 1. Create the table if it does not exist yet
create table if not exists public.returns (
  id              uuid        primary key default gen_random_uuid(),
  organization_id uuid        not null,
  rma_number      text        not null,
  marketplace     text        not null,   -- 'amazon' | 'walmart' | 'ebay'
  item_name       text        not null default '',
  condition       text        not null default 'unknown',
                                          -- 'sellable' | 'wrong_item' | 'empty_box' | 'customer_damaged'
  status          text        not null default 'pending_evidence',
                                          -- 'received' | 'pending_evidence' | 'ready_for_claim'
                                          -- | 'processing' | 'completed' | 'flagged'
  notes           text,
  photo_evidence  jsonb       default null,
                                          -- { "shipping_label": 2, "outer_box": 1, "damage_closeup": 3 }
  created_at      timestamptz not null default now()
);

-- 2. Add any columns that may be missing on an existing table (idempotent)
alter table public.returns
  add column if not exists item_name      text  not null default '';

alter table public.returns
  add column if not exists condition      text  not null default 'unknown';

alter table public.returns
  add column if not exists photo_evidence jsonb default null;

-- 3. Ensure the status default is up to date
alter table public.returns
  alter column status set default 'pending_evidence';

-- 4. Performance index for org-scoped queries (safe if already exists)
create index if not exists idx_returns_org
  on public.returns (organization_id, created_at desc);

-- 5. Optional: enable Row-Level Security when auth is wired up
-- alter table public.returns enable row level security;

-- ── Column reference ──────────────────────────────────────────
-- id              UUID (PK, auto)
-- organization_id UUID (required — multi-tenant scope)
-- rma_number      TEXT (the RMA / LPN scanned or typed)
-- marketplace     TEXT ('amazon' | 'walmart' | 'ebay')
-- item_name       TEXT (product name or SKU)
-- condition       TEXT (issue_type from the wizard)
-- status          TEXT (auto-derived by the server action)
-- notes           TEXT (operator notes + evidence summary)
-- photo_evidence  JSONB (map of category_id → photo count)
-- created_at      TIMESTAMPTZ (auto)
