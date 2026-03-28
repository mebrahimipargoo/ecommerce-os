-- Amazon / marketplace order id at package level (inherited by return items for claims).
alter table public.packages
  add column if not exists order_id text null;

comment on column public.packages.order_id is
  'External marketplace order id (e.g. Amazon) — inherited by linked returns for claim_submissions.source_payload.';
