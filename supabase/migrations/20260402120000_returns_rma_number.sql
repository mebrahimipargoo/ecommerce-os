-- Optional seller RMA on return lines (distinct from `returns.lpn` and `packages.rma_number`).
alter table public.returns
  add column if not exists rma_number text null;

comment on column public.returns.rma_number is
  'Optional seller RMA / authorization number stored on the return item.';
