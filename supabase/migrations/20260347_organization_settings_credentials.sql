-- Server-side secrets for integrations (e.g. org OpenAI key for proxy). Never expose to client.

ALTER TABLE public.organization_settings
  ADD COLUMN IF NOT EXISTS credentials jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN public.organization_settings.credentials IS
  'Server-only secrets map (e.g. openai_api_key). Populated by admin / migrations; never expose to client.';
