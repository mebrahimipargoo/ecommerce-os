-- Phase 2: Global platform catalog (Super Admin), return icon FK, Amazon ledger staging.

-- ─── Global marketplace catalog (icons / slugs — not per-org credential rows) ───
CREATE TABLE IF NOT EXISTS public.platform_marketplaces (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name        TEXT NOT NULL,
  slug        TEXT NOT NULL,
  icon_url    TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT platform_marketplaces_slug_unique UNIQUE (slug)
);

COMMENT ON TABLE public.platform_marketplaces IS
  'Global catalog of sales channels (Amazon, Walmart, …) for icons and UI — managed by Super Admins.';

CREATE INDEX IF NOT EXISTS idx_platform_marketplaces_slug_lower
  ON public.platform_marketplaces (lower(slug));

-- ─── Optional link from returns → platform row for icon resolution ───
ALTER TABLE public.returns
  ADD COLUMN IF NOT EXISTS platform_marketplace_id UUID
  REFERENCES public.platform_marketplaces(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_returns_platform_marketplace_id
  ON public.returns (platform_marketplace_id)
  WHERE platform_marketplace_id IS NOT NULL;

COMMENT ON COLUMN public.returns.platform_marketplace_id IS
  'FK to platform_marketplaces for icon; legacy rows still use `marketplace` text + slug match.';

-- ─── Amazon Inventory Ledger staging (never writes to returns) ───
CREATE TABLE IF NOT EXISTS public.amazon_ledger_staging (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id   UUID NOT NULL,
  snapshot_date     DATE,
  raw_row           JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.amazon_ledger_staging IS
  'Chunked Amazon Inventory Ledger CSV staging; tenant-scoped via organization_id.';

CREATE INDEX IF NOT EXISTS idx_amazon_ledger_staging_org_created
  ON public.amazon_ledger_staging (organization_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_amazon_ledger_staging_org_snapshot
  ON public.amazon_ledger_staging (organization_id, snapshot_date);

-- Seed default Amazon platform (optional)
INSERT INTO public.platform_marketplaces (name, slug, icon_url)
VALUES (
  'Amazon',
  'amazon',
  'https://upload.wikimedia.org/wikipedia/commons/a/a9/Amazon_logo.svg'
)
ON CONFLICT ON CONSTRAINT platform_marketplaces_slug_unique DO NOTHING;
