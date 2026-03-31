-- JSONB gallery for incident photos (URLs array shape: { "urls": ["https://...", ...] }).
alter table public.pallets
  add column if not exists photo_evidence jsonb default null;

alter table public.packages
  add column if not exists photo_evidence jsonb default null;

comment on column public.pallets.photo_evidence is
  'Optional JSONB evidence gallery; prefer { "urls": string[] } for uploaded incident-photos.';

comment on column public.packages.photo_evidence is
  'Optional JSONB evidence gallery; prefer { "urls": string[] } for uploaded incident-photos.';

create index if not exists idx_pallets_photo_evidence_gin
  on public.pallets using gin (photo_evidence)
  where photo_evidence is not null;

create index if not exists idx_packages_photo_evidence_gin
  on public.packages using gin (photo_evidence)
  where photo_evidence is not null;

insert into storage.buckets (id, name, public)
values ('incident-photos', 'incident-photos', true)
on conflict (id) do nothing;
