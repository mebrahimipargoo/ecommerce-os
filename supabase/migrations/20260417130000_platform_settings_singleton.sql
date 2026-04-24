-- ============================================================
-- Migration: Singleton platform branding (separate from tenant organization_settings)
-- Date: 2026-04-17
--
-- - public.platform_settings: one row (id = true) for product name + logo
-- - RLS: authenticated read; updates restricted to internal system roles
-- - Seed: idempotent default app_name
-- ============================================================

-- Shared touch helper (idempotent if already created by workspace_settings migration)
CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS public.platform_settings (
  id          boolean PRIMARY KEY DEFAULT true,
  app_name    text        NOT NULL,
  logo_url    text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT platform_settings_singleton_chk CHECK (id = true)
);

COMMENT ON TABLE public.platform_settings IS
  'Singleton (id = true) platform / product branding. Tenant white-label lives in organization_settings + workspace_settings.';

COMMENT ON COLUMN public.platform_settings.app_name IS
  'Product name shown in app shell chrome (sidebar, document title).';

COMMENT ON COLUMN public.platform_settings.logo_url IS
  'Optional public URL for the platform logo in shell chrome; monogram fallback when null.';

DROP TRIGGER IF EXISTS trg_platform_settings_updated_at ON public.platform_settings;
CREATE TRIGGER trg_platform_settings_updated_at
  BEFORE UPDATE ON public.platform_settings
  FOR EACH ROW
  EXECUTE FUNCTION public.set_updated_at();

-- Seed default row (idempotent)
INSERT INTO public.platform_settings (id, app_name, logo_url)
VALUES (true, 'RECOVRA', NULL)
ON CONFLICT (id) DO NOTHING;

-- ── RLS ─────────────────────────────────────────────────────

ALTER TABLE public.platform_settings ENABLE ROW LEVEL SECURITY;

-- Internal staff: role_id → roles.scope = ''system'', or legacy profiles.role text.
CREATE OR REPLACE FUNCTION public.can_manage_platform_settings()
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT COALESCE(
    (
      SELECT
        CASE
          WHEN r.id IS NOT NULL AND r.scope = 'system' THEN true
          WHEN r.id IS NULL AND lower(btrim(COALESCE(p.role, ''))) IN (
            'super_admin',
            'system_admin',
            'system_employee',
            'programmer',
            'customer_service'
          ) THEN true
          ELSE false
        END
      FROM public.profiles p
      LEFT JOIN public.roles r ON r.id = p.role_id
      WHERE p.id = auth.uid()
    ),
    false
  );
$$;

COMMENT ON FUNCTION public.can_manage_platform_settings() IS
  'True when the current auth user may UPDATE platform_settings (internal system roles only).';

DROP POLICY IF EXISTS "platform_settings_select_authenticated" ON public.platform_settings;
CREATE POLICY "platform_settings_select_authenticated"
  ON public.platform_settings
  FOR SELECT
  TO authenticated
  USING (true);

-- Login and marketing surfaces may read product name/logo URL without a session.
DROP POLICY IF EXISTS "platform_settings_select_anon" ON public.platform_settings;
CREATE POLICY "platform_settings_select_anon"
  ON public.platform_settings
  FOR SELECT
  TO anon
  USING (true);

DROP POLICY IF EXISTS "platform_settings_update_platform_staff" ON public.platform_settings;
CREATE POLICY "platform_settings_update_platform_staff"
  ON public.platform_settings
  FOR UPDATE
  TO authenticated
  USING (public.can_manage_platform_settings())
  WITH CHECK (public.can_manage_platform_settings());

REVOKE ALL ON public.platform_settings FROM PUBLIC;
GRANT SELECT ON public.platform_settings TO anon;
GRANT SELECT ON public.platform_settings TO authenticated;
GRANT UPDATE ON public.platform_settings TO authenticated;
