-- Align app code with tenant column name `company_id` on `user_profiles` (when still named `organization_id`).

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'user_profiles' AND column_name = 'organization_id'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'user_profiles' AND column_name = 'company_id'
  ) THEN
    ALTER TABLE public.user_profiles RENAME COLUMN organization_id TO company_id;
    ALTER TABLE public.user_profiles DROP CONSTRAINT IF EXISTS user_profiles_org_email_unique;
    ALTER TABLE public.user_profiles
      ADD CONSTRAINT user_profiles_company_email_unique UNIQUE (company_id, email);
  END IF;
END $$;

DROP INDEX IF EXISTS public.idx_user_profiles_org;

CREATE INDEX IF NOT EXISTS idx_user_profiles_company
  ON public.user_profiles (company_id);
