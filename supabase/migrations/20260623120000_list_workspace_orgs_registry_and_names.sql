-- Workspace switcher: include every active org from public.organizations (not only orgs
-- that already have returns/packages/pallets/settings). Fallback display label uses
-- organizations.name when company_display_name is unset (instead of raw UUID text).

CREATE OR REPLACE FUNCTION public.list_workspace_organizations_for_admin()
RETURNS TABLE (organization_id uuid, display_name text)
LANGUAGE sql
STABLE
AS $$
  SELECT DISTINCT o.tenant_id,
    COALESCE(
      NULLIF(TRIM(os.company_display_name), ''),
      NULLIF(TRIM(org.name), ''),
      o.tenant_id::text
    ) AS display_name
  FROM (
    SELECT r.organization_id AS tenant_id FROM public.returns r WHERE r.deleted_at IS NULL
    UNION
    SELECT p.organization_id AS tenant_id FROM public.packages p WHERE p.deleted_at IS NULL
    UNION
    SELECT pt.organization_id AS tenant_id FROM public.pallets pt WHERE pt.deleted_at IS NULL
    UNION
    SELECT s.organization_id AS tenant_id FROM public.organization_settings s
    UNION
    SELECT reg.id AS tenant_id FROM public.organizations reg
    WHERE COALESCE(reg.is_active, true)
  ) AS o
  LEFT JOIN public.organization_settings os ON os.organization_id = o.tenant_id
  LEFT JOIN public.organizations org ON org.id = o.tenant_id;
$$;
