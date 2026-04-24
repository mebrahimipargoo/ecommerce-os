-- Paste into Supabase Dashboard → SQL Editor (project linked to NEXT_PUBLIC_SUPABASE_URL).
-- Use this if migrations were never applied to the remote database.
-- After run: wait a few seconds, or in Dashboard use Project Settings → API → reload if available.

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

DO $$
BEGIN
  IF to_regclass('public.roles') IS NOT NULL THEN
    CREATE TABLE IF NOT EXISTS public.role_permissions (
      role_id uuid NOT NULL REFERENCES public.roles (id) ON DELETE CASCADE,
      permission_id uuid NOT NULL REFERENCES public.permissions (id) ON DELETE CASCADE,
      created_at timestamptz NOT NULL DEFAULT now(),
      CONSTRAINT role_permissions_pkey PRIMARY KEY (role_id, permission_id)
    );
    ALTER TABLE public.role_permissions ENABLE ROW LEVEL SECURITY;
    DROP POLICY IF EXISTS "role_permissions_select_authenticated" ON public.role_permissions;
    CREATE POLICY "role_permissions_select_authenticated"
      ON public.role_permissions FOR SELECT TO authenticated USING (true);
    GRANT SELECT ON public.role_permissions TO authenticated;
    GRANT SELECT ON public.role_permissions TO service_role;
  END IF;
  IF to_regclass('public.groups') IS NOT NULL THEN
    CREATE TABLE IF NOT EXISTS public.group_permissions (
      group_id uuid NOT NULL REFERENCES public.groups (id) ON DELETE CASCADE,
      permission_id uuid NOT NULL REFERENCES public.permissions (id) ON DELETE CASCADE,
      created_at timestamptz NOT NULL DEFAULT now(),
      CONSTRAINT group_permissions_pkey PRIMARY KEY (group_id, permission_id)
    );
    ALTER TABLE public.group_permissions ENABLE ROW LEVEL SECURITY;
    DROP POLICY IF EXISTS "group_permissions_select_authenticated" ON public.group_permissions;
    CREATE POLICY "group_permissions_select_authenticated"
      ON public.group_permissions FOR SELECT TO authenticated USING (true);
    GRANT SELECT ON public.group_permissions TO authenticated;
    GRANT SELECT ON public.group_permissions TO service_role;
  END IF;
END $$;

ALTER TABLE public.permissions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "permissions_select_authenticated" ON public.permissions;
CREATE POLICY "permissions_select_authenticated"
  ON public.permissions FOR SELECT TO authenticated USING (true);
GRANT SELECT ON public.permissions TO authenticated;
GRANT SELECT ON public.permissions TO service_role;

NOTIFY pgrst, 'reload schema';
