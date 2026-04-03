-- ============================================================
-- Migration: Register the FK from profiles → organization_settings
--
-- Context (Rule 5):
--   The core tenant table is `organization_settings`, keyed by its
--   `organization_id` UUID column (NOT the `id` PK).
--   `profiles.organization_id` must FK-reference that column so
--   PostgREST can infer the relationship and Supabase schema cache
--   never throws "could not find a relationship between profiles and …".
--
-- Steps:
--   1. Ensure organization_settings.organization_id is UNIQUE
--      (required before any table can FK-reference a non-PK column).
--   2. Add the FK on profiles.organization_id.
-- ============================================================

-- Step 1: UNIQUE constraint on organization_settings.organization_id
-- (idempotent — skipped if already present)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM   pg_constraint
    WHERE  conrelid = 'public.organization_settings'::regclass
    AND    contype  = 'u'
    AND    conname  = 'organization_settings_organization_id_unique'
  ) THEN
    ALTER TABLE public.organization_settings
      ADD CONSTRAINT organization_settings_organization_id_unique
      UNIQUE (organization_id);
  END IF;
END $$;

-- Step 2: FK on profiles.organization_id → organization_settings(organization_id)
-- ON DELETE SET NULL keeps the profile row alive even if an org is removed.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM   pg_constraint
    WHERE  conrelid = 'public.profiles'::regclass
    AND    contype  = 'f'
    AND    conname  = 'profiles_organization_id_fkey'
  ) THEN
    ALTER TABLE public.profiles
      ADD CONSTRAINT profiles_organization_id_fkey
      FOREIGN KEY (organization_id)
      REFERENCES public.organization_settings (organization_id)
      ON DELETE SET NULL;
  END IF;
END $$;

-- Ensure index exists for FK lookups (idempotent)
CREATE INDEX IF NOT EXISTS idx_profiles_organization_id
  ON public.profiles (organization_id);

COMMENT ON CONSTRAINT profiles_organization_id_fkey ON public.profiles IS
  'Tenant FK — links each profile to its organization via organization_settings.organization_id (Rule 5).';
