-- ─────────────────────────────────────────────────────────────────────────────
-- V3 Migration: Packages table (middle tier in pallets → packages → returns)
-- Idempotent — safe to run multiple times.
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Packages table ─────────────────────────────────────────────────────────
create table if not exists public.packages (
  id                  uuid        primary key default gen_random_uuid(),
  organization_id     uuid        not null,
  package_number      text        not null,
  tracking_number     text        default null,
  expected_item_count int         not null default 0,
  actual_item_count   int         not null default 0,
  pallet_id           uuid        references public.pallets(id) on delete set null default null,
  status              text        not null default 'open',
  discrepancy_note    text        default null,
  created_by          text        not null default 'operator',
  updated_by          text        default null,
  created_at          timestamptz not null default now(),
  updated_at          timestamptz not null default now()
);

create index if not exists idx_packages_org_status
  on public.packages (organization_id, status, created_at desc);

create index if not exists idx_packages_pallet
  on public.packages (pallet_id)
  where pallet_id is not null;

comment on column public.packages.status is
  'open | closed | suspicious | submitted';

comment on column public.packages.discrepancy_note is
  'Required operator note when actual_item_count != expected_item_count on close.';

-- 2. Add package_id FK to returns ────────────────────────────────────────────
alter table public.returns
  add column if not exists package_id uuid
    references public.packages(id) on delete set null default null;

create index if not exists idx_returns_package
  on public.returns (package_id)
  where package_id is not null;

-- 3. Trigger: keep packages.actual_item_count in sync ────────────────────────
create or replace function public.sync_package_item_count()
returns trigger language plpgsql as $$
begin
  if TG_OP = 'INSERT' and NEW.package_id is not null then
    update public.packages
       set actual_item_count = actual_item_count + 1
     where id = NEW.package_id;

  elsif TG_OP = 'DELETE' and OLD.package_id is not null then
    update public.packages
       set actual_item_count = greatest(actual_item_count - 1, 0)
     where id = OLD.package_id;

  elsif TG_OP = 'UPDATE'
    and (OLD.package_id is distinct from NEW.package_id)
  then
    -- Decrement old package
    if OLD.package_id is not null then
      update public.packages
         set actual_item_count = greatest(actual_item_count - 1, 0)
       where id = OLD.package_id;
    end if;
    -- Increment new package
    if NEW.package_id is not null then
      update public.packages
         set actual_item_count = actual_item_count + 1
       where id = NEW.package_id;
    end if;
  end if;

  return coalesce(NEW, OLD);
end;
$$;

drop trigger if exists trg_sync_package_item_count on public.returns;
create trigger trg_sync_package_item_count
  after insert or update or delete on public.returns
  for each row execute function public.sync_package_item_count();

-- 4. Auto-update updated_at on packages ─────────────────────────────────────
-- (re-uses the set_updated_at() function created in the v2 migration)
drop trigger if exists trg_packages_updated_at on public.packages;
create trigger trg_packages_updated_at
  before update on public.packages
  for each row execute function public.set_updated_at();

-- 5. Package audit log ────────────────────────────────────────────────────────
create table if not exists public.package_audit_log (
  id              uuid        primary key default gen_random_uuid(),
  organization_id uuid        not null,
  package_id      uuid        references public.packages(id) on delete set null,
  action          text        not null,
  field           text        default null,
  old_value       text        default null,
  new_value       text        default null,
  actor           text        not null default 'operator',
  created_at      timestamptz not null default now()
);

create index if not exists idx_pkg_audit_org
  on public.package_audit_log (organization_id, created_at desc);

create index if not exists idx_pkg_audit_pkg
  on public.package_audit_log (package_id, created_at desc);
