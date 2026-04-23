-- ============================================================
-- Idempotent: guarantee public.permissions exists for PostgREST.
--
-- Error: "Could not find the table 'public.permissions' in the schema cache"
-- usually means either (1) this migration was never applied to the linked DB, or
-- (2) the table was created but PostgREST needs a schema reload.
--
-- Requires public.roles (see 20260621100000_roles_scoped_model.sql).
-- ============================================================

CREATE TABLE IF NOT EXISTS public.permissions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  key text NOT NULL,
  name text NOT NULL,
  module text NOT NULL,
  description text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT permissions_key_unique UNIQUE (key)
);

CREATE INDEX IF NOT EXISTS idx_permissions_module ON public.permissions (module);
CREATE INDEX IF NOT EXISTS idx_permissions_module_name ON public.permissions (module, name);

CREATE TABLE IF NOT EXISTS public.role_permissions (
  role_id uuid NOT NULL REFERENCES public.roles (id) ON DELETE CASCADE,
  permission_id uuid NOT NULL REFERENCES public.permissions (id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT role_permissions_pkey PRIMARY KEY (role_id, permission_id)
);

CREATE INDEX IF NOT EXISTS idx_role_permissions_permission_id ON public.role_permissions (permission_id);

DO $$
BEGIN
  IF to_regclass('public.groups') IS NOT NULL THEN
    CREATE TABLE IF NOT EXISTS public.group_permissions (
      group_id uuid NOT NULL REFERENCES public.groups (id) ON DELETE CASCADE,
      permission_id uuid NOT NULL REFERENCES public.permissions (id) ON DELETE CASCADE,
      created_at timestamptz NOT NULL DEFAULT now(),
      CONSTRAINT group_permissions_pkey PRIMARY KEY (group_id, permission_id)
    );
    CREATE INDEX IF NOT EXISTS idx_group_permissions_permission_id ON public.group_permissions (permission_id);
  END IF;
END $$;

INSERT INTO public.permissions (key, name, module, description)
VALUES
  ('inventory.view', 'View', 'inventory', 'View inventory'),
  ('inventory.edit', 'Edit', 'inventory', 'Edit inventory records'),
  ('inventory.adjust', 'Adjust', 'inventory', 'Adjust stock levels'),
  ('claims.view', 'View', 'claims', 'View claims'),
  ('claims.create', 'Create', 'claims', 'Create claims'),
  ('returns.view', 'View', 'returns', 'View returns'),
  ('returns.edit', 'Edit', 'returns', 'Edit returns'),
  ('settings.view', 'View', 'settings', 'View organization settings'),
  ('settings.manage_users', 'Manage users', 'settings', 'Manage users and assignments'),
  ('platform.access_management', 'Access management', 'platform', 'Use platform access / permissions UI')
ON CONFLICT (key) DO UPDATE SET
  name        = EXCLUDED.name,
  module      = EXCLUDED.module,
  description = EXCLUDED.description;

ALTER TABLE public.permissions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.role_permissions ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "permissions_select_authenticated" ON public.permissions;
CREATE POLICY "permissions_select_authenticated"
  ON public.permissions FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS "role_permissions_select_authenticated" ON public.role_permissions;
CREATE POLICY "role_permissions_select_authenticated"
  ON public.role_permissions FOR SELECT TO authenticated USING (true);

GRANT SELECT ON public.permissions TO authenticated;
GRANT SELECT ON public.role_permissions TO authenticated;
GRANT SELECT ON public.permissions TO service_role;
GRANT SELECT ON public.role_permissions TO service_role;

DO $$
BEGIN
  IF to_regclass('public.group_permissions') IS NOT NULL THEN
    EXECUTE 'ALTER TABLE public.group_permissions ENABLE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS "group_permissions_select_authenticated" ON public.group_permissions';
    EXECUTE $p$
      CREATE POLICY "group_permissions_select_authenticated"
        ON public.group_permissions FOR SELECT TO authenticated USING (true)
    $p$;
    EXECUTE 'GRANT SELECT ON public.group_permissions TO authenticated';
    EXECUTE 'GRANT SELECT ON public.group_permissions TO service_role';
  END IF;
END $$;

NOTIFY pgrst, 'reload schema';
