-- Align list_workspace_organizations_for_admin with tenant column `company_id` on returns / packages / pallets.

CREATE OR REPLACE FUNCTION public.list_workspace_organizations_for_admin()
RETURNS TABLE (company_id uuid, display_name text)
LANGUAGE sql
STABLE
AS $$
  SELECT DISTINCT o.tenant_id,
    COALESCE(
      NULLIF(TRIM(os.company_display_name), ''),
      o.tenant_id::text
    ) AS display_name
  FROM (
    SELECT r.company_id AS tenant_id FROM public.returns r WHERE r.deleted_at IS NULL
    UNION
    SELECT p.company_id AS tenant_id FROM public.packages p WHERE p.deleted_at IS NULL
    UNION
    SELECT pt.company_id AS tenant_id FROM public.pallets pt WHERE pt.deleted_at IS NULL
    UNION
    SELECT s.company_id AS tenant_id FROM public.organization_settings s
  ) AS o
  LEFT JOIN public.organization_settings os ON os.company_id = o.tenant_id;
$$;
