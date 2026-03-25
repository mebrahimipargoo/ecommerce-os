-- ─────────────────────────────────────────────────────────────────────────────
-- Migration: SaaS Architecture — Stores, Store IDs on packages/returns,
--            and workspace-level usage tracking
-- Date: 2026-03-25
-- ─────────────────────────────────────────────────────────────────────────────

-- ══════════════════════════════════════════════════════════════════════════════
-- 1. STORES TABLE
--    Represents physical or virtual store fronts connected to the workspace.
--    This is a higher-level concept than `marketplaces` (credentials):
--    a Store is a named unit (e.g. "Main Amazon Store", "eBay Clearance Store").
-- ══════════════════════════════════════════════════════════════════════════════

create table if not exists stores (
  id          uuid        primary key default gen_random_uuid(),
  name        text        not null,
  platform    text        not null default 'unknown',  -- 'amazon', 'walmart', 'ebay', 'target', 'custom'
  is_active   boolean     not null default true,
  marketplace_id uuid     references marketplaces(id) on delete set null,
  organization_id uuid    not null default '00000000-0000-0000-0000-000000000001'::uuid,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);

comment on table stores is
  'Named store entities for multi-channel SaaS tracking. Each store can link to a marketplace credential row.';

comment on column stores.platform is
  'Marketplace platform key: amazon | walmart | ebay | target | shopify | custom';

comment on column stores.is_active is
  'Soft-delete flag. Inactive stores are hidden from dropdowns but data is preserved.';

-- Auto-update updated_at on row change
create or replace function update_stores_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_stores_updated_at on stores;
create trigger trg_stores_updated_at
  before update on stores
  for each row execute function update_stores_updated_at();

-- Index for org scoping + fast active-store lookups
create index if not exists idx_stores_org_active
  on stores (organization_id, is_active)
  where is_active = true;

-- ══════════════════════════════════════════════════════════════════════════════
-- 2. ADD store_id TO packages TABLE
--    Packages (inbound shipments / pallets of returns) are now tied to a store.
-- ══════════════════════════════════════════════════════════════════════════════

alter table packages
  add column if not exists store_id uuid references stores(id) on delete set null;

comment on column packages.store_id is
  'The connected store this package was received from / processed for.';

create index if not exists idx_packages_store_id on packages (store_id);

-- ══════════════════════════════════════════════════════════════════════════════
-- 3. ADD store_id TO returns TABLE (individual scanned items)
--    Each returned item can be attributed to a specific store.
-- ══════════════════════════════════════════════════════════════════════════════

alter table returns
  add column if not exists store_id uuid references stores(id) on delete set null;

comment on column returns.store_id is
  'The connected store this return item originated from. Supersedes the plain-text marketplace field for multi-store workspaces.';

create index if not exists idx_returns_store_id on returns (store_id);

-- ══════════════════════════════════════════════════════════════════════════════
-- 4. WORKSPACE USAGE TABLE
--    Tracks metered usage for SaaS billing enforcement.
--    One row per workspace per billing period (reset monthly via cron job).
-- ══════════════════════════════════════════════════════════════════════════════

create table if not exists workspace_usage (
  id                   uuid        primary key default gen_random_uuid(),
  organization_id      uuid        not null default '00000000-0000-0000-0000-000000000001'::uuid,
  billing_period_start date        not null default date_trunc('month', current_date)::date,
  ai_api_calls_count   integer     not null default 0 check (ai_api_calls_count >= 0),
  scanned_items_count  integer     not null default 0 check (scanned_items_count >= 0),
  current_plan         text        not null default 'Free',
  created_at           timestamptz not null default now(),
  updated_at           timestamptz not null default now(),

  unique (organization_id, billing_period_start)
);

comment on table workspace_usage is
  'Per-workspace, per-billing-period usage counters for AI API calls and scanned item volume.';

comment on column workspace_usage.ai_api_calls_count is
  'Running count of AI/OCR API calls made this billing period (packing slip scans, label OCR, etc.).';

comment on column workspace_usage.scanned_items_count is
  'Running count of individual return items scanned and logged this billing period.';

comment on column workspace_usage.current_plan is
  'Snapshot of the workspace plan at time of last update: Free | Pro | Enterprise';

-- Auto-update updated_at
create or replace function update_workspace_usage_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_workspace_usage_updated_at on workspace_usage;
create trigger trg_workspace_usage_updated_at
  before update on workspace_usage
  for each row execute function update_workspace_usage_updated_at();

-- Seed the default workspace's current billing period row
insert into workspace_usage (organization_id, billing_period_start, current_plan)
values (
  '00000000-0000-0000-0000-000000000001'::uuid,
  date_trunc('month', current_date)::date,
  'Free'
)
on conflict (organization_id, billing_period_start) do nothing;

-- ══════════════════════════════════════════════════════════════════════════════
-- 5. HELPER FUNCTION: increment_ai_usage
--    Call this from the server each time an AI/OCR API call is made.
--    Upserts the current billing period row and increments the counter.
-- ══════════════════════════════════════════════════════════════════════════════

create or replace function increment_ai_usage(
  p_org_id  uuid    default '00000000-0000-0000-0000-000000000001'::uuid,
  p_amount  integer default 1
)
returns void language plpgsql as $$
begin
  insert into workspace_usage (organization_id, billing_period_start, ai_api_calls_count)
  values (p_org_id, date_trunc('month', current_date)::date, p_amount)
  on conflict (organization_id, billing_period_start)
  do update set
    ai_api_calls_count = workspace_usage.ai_api_calls_count + excluded.ai_api_calls_count,
    updated_at = now();
end;
$$;

-- ══════════════════════════════════════════════════════════════════════════════
-- 6. HELPER FUNCTION: increment_items_usage
--    Call after each item scan/log to track scanned_items_count quota.
-- ══════════════════════════════════════════════════════════════════════════════

create or replace function increment_items_usage(
  p_org_id  uuid    default '00000000-0000-0000-0000-000000000001'::uuid,
  p_amount  integer default 1
)
returns void language plpgsql as $$
begin
  insert into workspace_usage (organization_id, billing_period_start, scanned_items_count)
  values (p_org_id, date_trunc('month', current_date)::date, p_amount)
  on conflict (organization_id, billing_period_start)
  do update set
    scanned_items_count = workspace_usage.scanned_items_count + excluded.scanned_items_count,
    updated_at = now();
end;
$$;

-- ══════════════════════════════════════════════════════════════════════════════
-- 7. ROW LEVEL SECURITY
-- ══════════════════════════════════════════════════════════════════════════════

-- Stores: org-scoped RLS
alter table stores enable row level security;

create policy "stores_org_isolation"
  on stores for all
  using (organization_id = '00000000-0000-0000-0000-000000000001'::uuid);

-- Workspace usage: org-scoped RLS
alter table workspace_usage enable row level security;

create policy "workspace_usage_org_isolation"
  on workspace_usage for all
  using (organization_id = '00000000-0000-0000-0000-000000000001'::uuid);
