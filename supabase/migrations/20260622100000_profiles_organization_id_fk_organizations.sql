-- ============================================================
-- Migration: profiles.organization_id → organizations(id)
-- Date: 2026-06-22
--
-- Re-targets the existing FK so PostgREST can embed
--   organizations(name)
-- from profiles (same UUID domain as organization_settings.organization_id).
-- ============================================================

ALTER TABLE public.profiles
  DROP CONSTRAINT IF EXISTS profiles_organization_id_fkey;

ALTER TABLE public.profiles
  ADD CONSTRAINT profiles_organization_id_fkey
  FOREIGN KEY (organization_id)
  REFERENCES public.organizations (id)
  ON DELETE SET NULL;

COMMENT ON CONSTRAINT profiles_organization_id_fkey ON public.profiles IS
  'Tenant FK — profiles.organization_id references public.organizations(id). '
  'Matches organization_settings.organization_id in normal deployments.';

NOTIFY pgrst, 'reload schema';
