-- Migrate marketplaces to credentials JSONB for provider-agnostic credential storage
-- Run in Supabase SQL Editor: Dashboard > SQL Editor > New query

-- 1. Add credentials JSONB column
alter table public.marketplaces
add column if not exists credentials jsonb;

-- 2. Migrate existing data: Amazon uses seller_id + LWA; Walmart uses client_id + client_secret
update public.marketplaces
set credentials = jsonb_build_object(
  'seller_id', coalesce(seller_id, ''),
  'lwa_client_id', coalesce(lwa_client_id, ''),
  'lwa_client_secret', coalesce(lwa_client_secret, '')
)
where provider = 'amazon_sp_api' and credentials is null;

update public.marketplaces
set credentials = jsonb_build_object(
  'client_id', coalesce(lwa_client_id, ''),
  'client_secret', coalesce(lwa_client_secret, '')
)
where provider = 'walmart_api' and credentials is null;

-- 3. Set default for any remaining rows
update public.marketplaces set credentials = '{}'::jsonb where credentials is null;

-- 4. Make credentials NOT NULL
alter table public.marketplaces alter column credentials set not null;

-- 5. Drop legacy columns
alter table public.marketplaces drop column if exists seller_id;
alter table public.marketplaces drop column if exists lwa_client_id;
alter table public.marketplaces drop column if exists lwa_client_secret;

-- 6. Add GIN index for credentials queries (optional, useful for JSONB filters)
create index if not exists idx_marketplaces_credentials
  on public.marketplaces using gin (credentials);
