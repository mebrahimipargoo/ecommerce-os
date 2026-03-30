-- Marketplace credentials belong on `marketplaces`; remove legacy JSONB on `stores`.

DROP INDEX IF EXISTS public.idx_stores_api_keys_gin;

ALTER TABLE public.stores
  DROP COLUMN IF EXISTS api_keys;
