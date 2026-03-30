-- App-managed user directory (not auth.users). Profile photos in public `profiles` bucket.

CREATE TABLE IF NOT EXISTS public.user_profiles (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id   UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'::uuid,
  full_name         TEXT NOT NULL DEFAULT '',
  email             TEXT NOT NULL,
  role              TEXT NOT NULL DEFAULT 'operator',
  photo_url         TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT user_profiles_org_email_unique UNIQUE (organization_id, email)
);

CREATE INDEX IF NOT EXISTS idx_user_profiles_org
  ON public.user_profiles (organization_id);

COMMENT ON TABLE public.user_profiles IS
  'Directory of workspace users; photo_url points at objects in the public profiles Storage bucket.';

COMMENT ON COLUMN public.user_profiles.photo_url IS
  'Public URL of profile photo in the profiles storage bucket.';

INSERT INTO storage.buckets (id, name, public)
VALUES ('profiles', 'profiles', true)
ON CONFLICT (id) DO NOTHING;
