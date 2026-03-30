-- Company logo URL (canonical for branding) + public `logos` storage bucket.

ALTER TABLE public.organization_settings
  ADD COLUMN IF NOT EXISTS logo_url TEXT DEFAULT NULL;

COMMENT ON COLUMN public.organization_settings.logo_url IS
  'Public URL of the tenant company logo (object in the logos storage bucket).';

INSERT INTO storage.buckets (id, name, public)
VALUES ('logos', 'logos', true)
ON CONFLICT (id) DO NOTHING;
