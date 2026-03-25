-- ─────────────────────────────────────────────────────────────────────────────
-- V4 Migration: Unique constraints, LPN column, carrier_name
-- Idempotent — safe to run multiple times.
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Add LPN (License Plate Number) to returns ────────────────────────────────
--    Amazon prints a unique LPN on every return label at the FC.
--    Scanning a duplicate means the same label was processed twice → block it.
alter table public.returns
  add column if not exists lpn text default null;

-- Partial unique index per org (null values are excluded — they never conflict)
create unique index if not exists idx_returns_lpn_unique
  on public.returns (organization_id, lpn)
  where lpn is not null;

comment on column public.returns.lpn is
  'Amazon License Plate Number — unique per org. Prevents double-scanning the same return label.';

-- 2. Add carrier_name to packages ─────────────────────────────────────────────
alter table public.packages
  add column if not exists carrier_name text default null;

comment on column public.packages.carrier_name is
  'Shipping carrier (UPS, FedEx, USPS, DHL, Amazon Logistics, etc.).';

-- 3. Unique tracking_number per org ───────────────────────────────────────────
--    A tracking number can only belong to one inbound package per org.
create unique index if not exists idx_packages_tracking_unique
  on public.packages (organization_id, tracking_number)
  where tracking_number is not null;

comment on column public.packages.tracking_number is
  'Carrier tracking number — unique per org. Prevents registering the same package twice.';

-- 4. Ensure updated_at trigger fires on packages ──────────────────────────────
--    (guard in case the V3 migration was not run first)
do $$
begin
  if not exists (
    select 1 from pg_trigger
    where tgname = 'trg_packages_updated_at'
    and tgrelid = 'public.packages'::regclass
  ) then
    create trigger trg_packages_updated_at
      before update on public.packages
      for each row execute function public.set_updated_at();
  end if;
end;
$$;

-- 5. Ensure created_by / updated_by / updated_at exist on both tables ─────────
alter table public.returns
  add column if not exists created_by text not null default 'operator',
  add column if not exists updated_by text default null,
  add column if not exists updated_at timestamptz not null default now();

alter table public.packages
  add column if not exists created_by text not null default 'operator',
  add column if not exists updated_by text default null,
  add column if not exists updated_at timestamptz not null default now();

-- organisation_id guard (make sure returns has it)
alter table public.returns
  add column if not exists organization_id uuid not null default '00000000-0000-0000-0000-000000000001';
