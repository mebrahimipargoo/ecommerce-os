-- ============================================================
-- V1 MVP: Pallets + RBAC role column
-- Run in Supabase SQL Editor: Dashboard > SQL Editor > New query
-- ============================================================

-- 1. User profiles table with role column
--    If you already have a profiles table, skip the CREATE and run
--    only the ALTER at the end of this section.
create table if not exists public.profiles (
  id              uuid  primary key references auth.users(id) on delete cascade,
  organization_id uuid,
  name            text,
  role            text  not null default 'operator',
                        -- 'admin' | 'operator'
  created_at      timestamptz not null default now()
);

-- Add role to existing profiles table (safe if column already exists)
alter table public.profiles
  add column if not exists role text not null default 'operator';

comment on column public.profiles.role is
  'RBAC role for this user.
   operator – can insert returns and create pallets; cannot edit or delete.
   admin    – full access: insert, edit, delete, close/submit pallets.
   Enforce in Row-Level Security policies when auth is fully wired up.';


-- 2. Pallets table
create table if not exists public.pallets (
  id                  uuid        primary key default gen_random_uuid(),
  organization_id     uuid        not null,
  pallet_number       text        not null,
  manifest_photo_url  text        default null,
  status              text        not null default 'open',
                                  -- 'open' | 'closed' | 'submitted'
  notes               text        default null,
  created_at          timestamptz not null default now()
);

create index if not exists idx_pallets_org_status
  on public.pallets (organization_id, status, created_at desc);

comment on column public.pallets.status is 'open | closed | submitted';
comment on column public.pallets.manifest_photo_url is
  'URL/path for the pallet manifest or packing-slip photo.
   In V1 this stores a filename; V2 will upload to Supabase Storage.';


-- 3. Add pallet_id FK to returns
--    NULL = single-item return (always allowed).
--    Non-null = item belongs to a batch/pallet workflow.
alter table public.pallets
  add column if not exists item_count int not null default 0;

alter table public.returns
  add column if not exists pallet_id uuid
    references public.pallets(id) on delete set null
    default null;

create index if not exists idx_returns_pallet
  on public.returns (pallet_id)
  where pallet_id is not null;

comment on column public.returns.pallet_id is
  'NULL for single-item returns.
   FK to pallets.id for returns processed inside the pallet batch workflow.';


-- 4. Trigger to keep pallets.item_count accurate
create or replace function public.sync_pallet_item_count()
returns trigger language plpgsql as $$
begin
  if TG_OP = 'INSERT' and NEW.pallet_id is not null then
    update public.pallets set item_count = item_count + 1
      where id = NEW.pallet_id;
  elsif TG_OP = 'DELETE' and OLD.pallet_id is not null then
    update public.pallets set item_count = greatest(item_count - 1, 0)
      where id = OLD.pallet_id;
  elsif TG_OP = 'UPDATE' then
    if OLD.pallet_id is distinct from NEW.pallet_id then
      if OLD.pallet_id is not null then
        update public.pallets set item_count = greatest(item_count - 1, 0)
          where id = OLD.pallet_id;
      end if;
      if NEW.pallet_id is not null then
        update public.pallets set item_count = item_count + 1
          where id = NEW.pallet_id;
      end if;
    end if;
  end if;
  return coalesce(NEW, OLD);
end;
$$;

drop trigger if exists trg_sync_pallet_item_count on public.returns;
create trigger trg_sync_pallet_item_count
  after insert or update or delete on public.returns
  for each row execute function public.sync_pallet_item_count();
