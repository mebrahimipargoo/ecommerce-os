-- Multi-tenant SaaS: optional display name per organization + admin helper to list orgs.

ALTER TABLE public.organization_settings
  ADD COLUMN IF NOT EXISTS company_display_name text;

COMMENT ON COLUMN public.organization_settings.company_display_name IS
  'Human-readable company name for Super Admin filters and cross-tenant tables.';

-- Distinct tenant IDs that appear in logistics tables or have settings rows, with a display label.
CREATE OR REPLACE FUNCTION public.list_workspace_organizations_for_admin()
RETURNS TABLE (organization_id uuid, display_name text)
LANGUAGE sql
STABLE
AS $$
  SELECT DISTINCT o.organization_id,
    COALESCE(
      NULLIF(TRIM(os.company_display_name), ''),
      o.organization_id::text
    ) AS display_name
  FROM (
    SELECT r.organization_id FROM public.returns r WHERE r.deleted_at IS NULL
    UNION
    SELECT p.organization_id FROM public.packages p WHERE p.deleted_at IS NULL
    UNION
    SELECT pt.organization_id FROM public.pallets pt WHERE pt.deleted_at IS NULL
    UNION
    SELECT s.organization_id FROM public.organization_settings s
  ) AS o
  LEFT JOIN public.organization_settings os ON os.organization_id = o.organization_id;
$$;
