-- Product Catalog: local cache for external product lookups (Amazon SP-API, etc.)
-- Each row is keyed by barcode (UPC / ASIN / FNSKU).
-- When a barcode is scanned we check here first; on a miss we hit the adapter
-- and write the result back so subsequent scans are instant.

create table if not exists public.products (
  id         uuid        primary key default gen_random_uuid(),
  barcode    text        unique not null,
  name       text        not null,
  image_url  text,
  price      numeric(10, 2),
  source     text        not null default 'Amazon',
  created_at timestamptz not null default now()
);

-- Fast barcode lookups
create index if not exists idx_products_barcode on public.products (barcode);

-- Row-level security (inherits your existing Supabase auth setup)
alter table public.products enable row level security;

-- Authenticated users can read the catalog
create policy "products_select"
  on public.products for select
  using (true);

-- Authenticated users can insert new cache entries
create policy "products_insert"
  on public.products for insert
  with check (true);
