-- Workspace-issued API keys for external agents/bots.
-- `api_key` stores SHA-256 hex digest of the secret (plaintext shown once at creation).

CREATE TABLE IF NOT EXISTS public.organization_api_keys (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id   UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'::uuid,
  name              TEXT NOT NULL DEFAULT '',
  role              TEXT NOT NULL DEFAULT 'integration',
  api_key           TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_organization_api_keys_org
  ON public.organization_api_keys (organization_id, created_at DESC);

COMMENT ON TABLE public.organization_api_keys IS
  'API keys for bots/integrations; `api_key` is SHA-256 hex of the secret, not plaintext.';

COMMENT ON COLUMN public.organization_api_keys.api_key IS
  'SHA-256 hex digest of the full secret; never store plaintext after creation.';
