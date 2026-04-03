-- RPC used by Super Admin org picker: tenant column is `organization_id` everywhere.
CREATE OR REPLACE FUNCTION public.list_workspace_organizations_for_admin()
RETURNS TABLE (organization_id uuid, display_name text)
LANGUAGE sql
STABLE
AS $$
  SELECT DISTINCT o.tenant_id,
    COALESCE(
      NULLIF(TRIM(os.company_display_name), ''),
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
  ) AS o
  LEFT JOIN public.organization_settings os ON os.organization_id = o.tenant_id;
$$;
