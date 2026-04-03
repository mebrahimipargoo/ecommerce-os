-- Tenant column rename: organization_settings.organization_id → company_id (app / PostgREST alignment).

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'organization_settings'
      AND column_name = 'organization_id'
  )
  AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'organization_settings'
      AND column_name = 'company_id'
  ) THEN
    ALTER TABLE public.organization_settings
      RENAME COLUMN organization_id TO company_id;
  END IF;
END $$;

-- RPC return shape must match app: company_id (not organization_id).
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
    SELECT r.organization_id AS tenant_id FROM public.returns r WHERE r.deleted_at IS NULL
    UNION
    SELECT p.organization_id AS tenant_id FROM public.packages p WHERE p.deleted_at IS NULL
    UNION
    SELECT pt.organization_id AS tenant_id FROM public.pallets pt WHERE pt.deleted_at IS NULL
    UNION
    SELECT s.company_id AS tenant_id FROM public.organization_settings s
  ) AS o
  LEFT JOIN public.organization_settings os ON os.company_id = o.tenant_id;
$$;
