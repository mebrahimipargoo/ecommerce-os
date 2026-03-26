-- Merchant / warehouse SKU (MSKU) — distinct from FNSKU (FBA barcode) and ASIN (catalog).
alter table public.returns add column if not exists seller_sku text null;

comment on column public.returns.seller_sku is 'Seller SKU (MSKU) — Amazon warehouse / Seller Central SKU; distinct from fnsku (FBA network label) and asin.';

create index if not exists idx_returns_seller_sku
  on public.returns (seller_sku)
  where seller_sku is not null;
