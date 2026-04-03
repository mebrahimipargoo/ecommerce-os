-- ============================================================
-- Migration: 5-Tier RBAC + GBAC Foundation
-- Date: 2026-04-17
--
-- Changes:
--   1. Add `team_groups` JSONB column to public.profiles
--      (GBAC foundation — stores group/team slug membership)
--   2. Extend `profiles.role` to accept the two new tiers:
--      'employee' and 'system_employee'
--   3. Update the auth-trigger default so new users land on
--      'operator' (warehouse default; promotable by admins)
--   4. Refresh PostgREST schema cache
--
-- PROTECTED (DO NOT TOUCH):
--   claim_submissions, system_settings,
--   claim_submission_status enum, photo-inheritance triggers
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- 1. Add team_groups column (idempotent)
--
-- Stores a JSONB array of group/team slugs, e.g.:
--   ["warehouse-a", "returns-team", "qa-reviewers"]
-- Used by future object-level GBAC checks server-side.
-- NULL / empty array = no group restrictions applied.
-- ─────────────────────────────────────────────────────────────

ALTER TABLE public.profiles
  ADD COLUMN IF NOT EXISTS team_groups jsonb NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN public.profiles.team_groups IS
  'GBAC: JSONB array of group/team slugs the user belongs to. '
  'Used for future object-level access checks. '
  'Example: ["warehouse-a","returns-team"]. Empty array = no group restrictions.';

-- Index for GIN lookups (e.g. team_groups @> ''["warehouse-a"]'')
CREATE INDEX IF NOT EXISTS idx_profiles_team_groups
  ON public.profiles USING gin (team_groups);

-- ─────────────────────────────────────────────────────────────
-- 2. Extend the role CHECK constraint to include the two new
--    tiers: 'employee' and 'system_employee'
--
-- The existing constraint is dropped (idempotent-safe via the
-- DO block) then recreated with all 5 valid values.
-- ─────────────────────────────────────────────────────────────

DO $$
BEGIN
  -- Drop old single-column check if it exists
  IF EXISTS (
    SELECT 1
    FROM   pg_constraint
    WHERE  conrelid = 'public.profiles'::regclass
    AND    contype  = 'c'
    AND    conname  = 'profiles_role_check'
  ) THEN
    ALTER TABLE public.profiles DROP CONSTRAINT profiles_role_check;
  END IF;
END $$;

ALTER TABLE public.profiles
  ADD CONSTRAINT profiles_role_check
  CHECK (role IN ('super_admin', 'system_employee', 'admin', 'employee', 'operator'));

COMMENT ON COLUMN public.profiles.role IS
  '5-Tier RBAC: super_admin | system_employee | admin | employee | operator. '
  'Ordered from highest to lowest privilege. '
  'Default for new signups: operator (promotable by admin).';

-- ─────────────────────────────────────────────────────────────
-- 3. Ensure the auth trigger default role is 'operator'
--    (re-create the function to enforce the updated default)
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.handle_new_auth_user()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  INSERT INTO public.profiles (
    id,
    full_name,
    role,
    organization_id,
    team_groups,
    created_at
  )
  VALUES (
    NEW.id,
    COALESCE(
      NEW.raw_user_meta_data->>'full_name',
      NEW.raw_user_meta_data->>'name',
      split_part(NEW.email, '@', 1)
    ),
    'operator',                                         -- lowest privilege; admin can promote
    '00000000-0000-0000-0000-000000000001'::uuid,       -- default org; admin can re-assign
    '[]'::jsonb,                                        -- empty GBAC groups on signup
    NOW()
  )
  ON CONFLICT (id) DO NOTHING;
  RETURN NEW;
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- 4. Update the RLS helper to remain SECURITY DEFINER after
--    the profiles constraint change (no-op if unchanged)
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.get_my_organization_id()
RETURNS uuid
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT organization_id
  FROM   public.profiles
  WHERE  id = auth.uid()
  LIMIT  1;
$$;

-- ─────────────────────────────────────────────────────────────
-- 5. Refresh PostgREST schema cache
-- ─────────────────────────────────────────────────────────────

NOTIFY pgrst, 'reload schema';
