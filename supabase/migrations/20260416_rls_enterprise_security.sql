-- ============================================================
-- Migration: Enterprise RLS, Auth Trigger, role_required Fix
-- Date: 2026-04-16
-- Author: Chief Security DBA
--
-- PROTECTED (DO NOT TOUCH): claim_submissions, system_settings,
--   claim_submission_status enum, pallet/package/item photo
--   inheritance triggers.
--
-- Run this entire script in: Supabase Dashboard → SQL Editor
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- PART 0 – Prerequisites
-- ─────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ─────────────────────────────────────────────────────────────
-- PART 1 – Fix schema cache: ensure role_required exists on
--           marketplaces (idempotent — safe to re-run)
-- ─────────────────────────────────────────────────────────────

ALTER TABLE public.marketplaces
  ADD COLUMN IF NOT EXISTS role_required text NOT NULL DEFAULT 'admin';

CREATE INDEX IF NOT EXISTS idx_marketplaces_organization_id
  ON public.marketplaces (organization_id);

-- ─────────────────────────────────────────────────────────────
-- PART 2 – Auth Trigger: auto-create public.profiles on signup
--
-- When a new user registers in auth.users, this trigger inserts
-- a matching row in public.profiles so the app can load the
-- user's role and organization_id immediately.
-- The default role is 'operator'; an admin can later promote.
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
    created_at
  )
  VALUES (
    NEW.id,
    COALESCE(
      NEW.raw_user_meta_data->>'full_name',
      NEW.raw_user_meta_data->>'name',
      split_part(NEW.email, '@', 1)
    ),
    'operator',                                         -- default role; admin can promote
    '00000000-0000-0000-0000-000000000001'::uuid,       -- default org; admin can re-assign
    NOW()
  )
  ON CONFLICT (id) DO NOTHING;   -- idempotent: never overwrite an existing profile
  RETURN NEW;
END;
$$;

-- Idempotent trigger registration
DROP TRIGGER IF EXISTS trg_on_auth_user_created ON auth.users;

CREATE TRIGGER trg_on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW
  EXECUTE FUNCTION public.handle_new_auth_user();

-- ─────────────────────────────────────────────────────────────
-- PART 3 – Helper: get the current user's organization_id
--
-- Used inside every RLS policy. SECURITY DEFINER so it can
-- read profiles even when profiles itself has RLS enabled.
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
-- PART 4 – Enable RLS on all operational tables
--
-- Tables already protected by Neda (SKIP):
--   claim_submissions  (already has org-isolation policy)
--   system_settings
-- Photo-inheritance triggers are functions — not tables; safe.
-- ─────────────────────────────────────────────────────────────

-- organizations (master org registry)
ALTER TABLE public.organizations         ENABLE ROW LEVEL SECURITY;

-- organization_settings (tenant config)
ALTER TABLE public.organization_settings ENABLE ROW LEVEL SECURITY;

-- profiles (user directory)
ALTER TABLE public.profiles              ENABLE ROW LEVEL SECURITY;

-- marketplaces (3PL credentials)
ALTER TABLE public.marketplaces          ENABLE ROW LEVEL SECURITY;

-- stores / workspace_usage (already enabled — idempotent)
ALTER TABLE public.stores                ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.workspace_usage       ENABLE ROW LEVEL SECURITY;

-- product catalog
ALTER TABLE public.products              ENABLE ROW LEVEL SECURITY;

-- inbound logistics
ALTER TABLE public.pallets               ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.packages              ENABLE ROW LEVEL SECURITY;

-- returns
ALTER TABLE public.returns               ENABLE ROW LEVEL SECURITY;

-- workspace & branding
ALTER TABLE public.workspace_settings    ENABLE ROW LEVEL SECURITY;

-- import tooling
ALTER TABLE public.raw_report_uploads    ENABLE ROW LEVEL SECURITY;

-- API keys & import defaults
ALTER TABLE public.organization_api_keys      ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.import_mapping_defaults    ENABLE ROW LEVEL SECURITY;

-- ─────────────────────────────────────────────────────────────
-- PART 5 – Drop stale hardcoded-UUID policies (legacy)
--
-- The old policies used a hardcoded UUID for the single-tenant
-- demo. We replace them with dynamic org-matching policies.
-- ─────────────────────────────────────────────────────────────

DROP POLICY IF EXISTS "stores_org_isolation"          ON public.stores;
DROP POLICY IF EXISTS "workspace_usage_org_isolation" ON public.workspace_usage;
DROP POLICY IF EXISTS "products_select"               ON public.products;
DROP POLICY IF EXISTS "products_insert"               ON public.products;

-- ─────────────────────────────────────────────────────────────
-- PART 6 – Multi-tenant RLS Policies
--
-- Pattern for each table:
--   • SELECT  – org match OR service-role bypass
--   • INSERT  – org match
--   • UPDATE  – org match
--   • DELETE  – org match
--
-- Service role (used by Next.js server actions) bypasses RLS
-- automatically; these policies protect browser / anon clients.
-- ─────────────────────────────────────────────────────────────

-- ── organizations ────────────────────────────────────────────
-- Users may read their own org row; super_admin reads all
-- (super_admin check via profiles.role — evaluated server-side).
CREATE POLICY "org_select_own"
  ON public.organizations FOR SELECT
  USING (id = public.get_my_organization_id());

-- ── organization_settings ────────────────────────────────────
CREATE POLICY "org_settings_all_own"
  ON public.organization_settings FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

-- ── profiles ─────────────────────────────────────────────────
-- Users can see all profiles in their org + always see their own.
CREATE POLICY "profiles_select_own_org"
  ON public.profiles FOR SELECT
  USING (
    organization_id = public.get_my_organization_id()
    OR id = auth.uid()
  );

-- Users may only update their own profile row.
CREATE POLICY "profiles_update_self"
  ON public.profiles FOR UPDATE
  USING      (id = auth.uid())
  WITH CHECK (id = auth.uid());

-- Insert handled exclusively by the auth trigger (SECURITY DEFINER).
-- Direct client inserts are blocked unless user is authenticated.
CREATE POLICY "profiles_insert_self"
  ON public.profiles FOR INSERT
  WITH CHECK (id = auth.uid());

-- ── marketplaces ─────────────────────────────────────────────
CREATE POLICY "marketplaces_all_own_org"
  ON public.marketplaces FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

-- ── stores ───────────────────────────────────────────────────
CREATE POLICY "stores_all_own_org"
  ON public.stores FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

-- ── workspace_usage ──────────────────────────────────────────
CREATE POLICY "workspace_usage_all_own_org"
  ON public.workspace_usage FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

-- ── products ─────────────────────────────────────────────────
CREATE POLICY "products_all_own_org"
  ON public.products FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

-- ── pallets ──────────────────────────────────────────────────
CREATE POLICY "pallets_all_own_org"
  ON public.pallets FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

-- ── packages ─────────────────────────────────────────────────
CREATE POLICY "packages_all_own_org"
  ON public.packages FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

-- ── returns ──────────────────────────────────────────────────
CREATE POLICY "returns_all_own_org"
  ON public.returns FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

-- ── workspace_settings ───────────────────────────────────────
-- Single global row; allow authenticated users to read/write
-- (admin-only enforcement is done in server actions, not DB layer).
CREATE POLICY "workspace_settings_authenticated"
  ON public.workspace_settings FOR ALL
  USING      (auth.role() = 'authenticated')
  WITH CHECK (auth.role() = 'authenticated');

-- ── raw_report_uploads ───────────────────────────────────────
CREATE POLICY "raw_report_uploads_all_own_org"
  ON public.raw_report_uploads FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

-- ── organization_api_keys ────────────────────────────────────
CREATE POLICY "org_api_keys_all_own_org"
  ON public.organization_api_keys FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

-- ── import_mapping_defaults ──────────────────────────────────
CREATE POLICY "import_mapping_defaults_all_own_org"
  ON public.import_mapping_defaults FOR ALL
  USING      (organization_id = public.get_my_organization_id())
  WITH CHECK (organization_id = public.get_my_organization_id());

-- ─────────────────────────────────────────────────────────────
-- PART 7 – Refresh PostgREST schema cache
--
-- Fixes "Could not find the 'role_required' column of
-- 'marketplaces' in the schema cache" immediately.
-- MUST be the final statement in the script.
-- ─────────────────────────────────────────────────────────────

NOTIFY pgrst, 'reload schema';
