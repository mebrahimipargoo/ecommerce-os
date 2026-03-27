-- Enterprise claim fields (V16.4.10) — extend marketplace-synced claims with identifiers and links.
-- Requires existing `claims` table (adapter sync). Safe to re-run.

ALTER TABLE public.claims ADD COLUMN IF NOT EXISTS return_id UUID REFERENCES public.returns(id) ON DELETE SET NULL;
ALTER TABLE public.claims ADD COLUMN IF NOT EXISTS item_name TEXT;
ALTER TABLE public.claims ADD COLUMN IF NOT EXISTS asin TEXT;
ALTER TABLE public.claims ADD COLUMN IF NOT EXISTS fnsku TEXT;
ALTER TABLE public.claims ADD COLUMN IF NOT EXISTS sku TEXT;
ALTER TABLE public.claims ADD COLUMN IF NOT EXISTS marketplace_claim_id TEXT;
ALTER TABLE public.claims ADD COLUMN IF NOT EXISTS marketplace_link_status TEXT;
ALTER TABLE public.claims ADD COLUMN IF NOT EXISTS store_id UUID REFERENCES public.stores(id) ON DELETE SET NULL;

COMMENT ON COLUMN public.claims.marketplace_link_status IS 'UI status: pending | verified | broken | unknown';

CREATE INDEX IF NOT EXISTS idx_claims_return_id ON public.claims (return_id) WHERE return_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_claims_store_id ON public.claims (store_id) WHERE store_id IS NOT NULL;
