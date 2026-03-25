-- ============================================================
-- V2 — Audit trail, created_by/updated_by, return_audit_log
-- Run in Supabase SQL Editor → safe to re-run (idempotent)
-- ============================================================

-- ── 1. Audit columns on returns ───────────────────────────────────────────────

alter table public.returns
  add column if not exists created_by  text        not null default 'operator',
  add column if not exists updated_at  timestamptz not null default now(),
  add column if not exists updated_by  text        default null;

-- ── 2. Audit columns on pallets ───────────────────────────────────────────────

alter table public.pallets
  add column if not exists created_by  text        not null default 'operator',
  add column if not exists updated_at  timestamptz not null default now(),
  add column if not exists updated_by  text        default null;

-- ── 3. Auto-update updated_at on row changes ──────────────────────────────────

create or replace function public.set_updated_at()
returns trigger language plpgsql as $$
begin
  NEW.updated_at = now();
  return NEW;
end;
$$;

drop trigger if exists trg_returns_updated_at on public.returns;
create trigger trg_returns_updated_at
  before update on public.returns
  for each row execute function public.set_updated_at();

drop trigger if exists trg_pallets_updated_at on public.pallets;
create trigger trg_pallets_updated_at
  before update on public.pallets
  for each row execute function public.set_updated_at();

-- ── 4. Return audit log table ─────────────────────────────────────────────────
--
--  Tracks every meaningful state change:
--    action  = 'created' | 'updated' | 'status_changed' | 'deleted'
--    field   = column that changed (for 'updated')
--    old_value / new_value = before / after stringified values

create table if not exists public.return_audit_log (
  id              uuid        primary key default gen_random_uuid(),
  organization_id uuid        not null,
  return_id       uuid        references public.returns(id)  on delete set null,
  pallet_id       uuid        references public.pallets(id)  on delete set null,
  action          text        not null,
                              -- 'created' | 'updated' | 'status_changed' | 'deleted'
  field           text        default null,
  old_value       text        default null,
  new_value       text        default null,
  actor           text        not null default 'operator',
  created_at      timestamptz not null default now()
);

create index if not exists idx_audit_return
  on public.return_audit_log (return_id, created_at desc);

create index if not exists idx_audit_org
  on public.return_audit_log (organization_id, created_at desc);

create index if not exists idx_audit_actor
  on public.return_audit_log (actor, created_at desc);

comment on table public.return_audit_log is
  'Immutable record of every operator action on returns and pallets.
   Use this table to investigate disputes, verify SLA compliance, and
   generate operator performance reports.';

comment on column public.return_audit_log.actor is
  'Username or user.id of the operator who performed the action.
   Comes from Supabase Auth session in production; mock name in V1.';

-- ── 5. Pallet audit log table ─────────────────────────────────────────────────

create table if not exists public.pallet_audit_log (
  id              uuid        primary key default gen_random_uuid(),
  organization_id uuid        not null,
  pallet_id       uuid        references public.pallets(id) on delete set null,
  action          text        not null,
  field           text        default null,
  old_value       text        default null,
  new_value       text        default null,
  actor           text        not null default 'operator',
  created_at      timestamptz not null default now()
);

create index if not exists idx_pallet_audit_pallet
  on public.pallet_audit_log (pallet_id, created_at desc);

create index if not exists idx_pallet_audit_org
  on public.pallet_audit_log (organization_id, created_at desc);

-- ── 6. Helper view: recent activity feed ─────────────────────────────────────
--  Merges both logs into a unified timeline (useful for admin dashboard).

create or replace view public.v_activity_feed as
  select
    id, organization_id,
    'return'  as entity_type,
    return_id as entity_id,
    action, field, old_value, new_value, actor, created_at
  from public.return_audit_log
  union all
  select
    id, organization_id,
    'pallet'  as entity_type,
    pallet_id as entity_id,
    action, field, old_value, new_value, actor, created_at
  from public.pallet_audit_log
;

comment on view public.v_activity_feed is
  'Unified audit timeline across returns and pallets.
   Query with ORDER BY created_at DESC LIMIT 50 for the activity dashboard.';
