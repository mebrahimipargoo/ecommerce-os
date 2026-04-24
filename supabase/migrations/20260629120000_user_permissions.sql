-- Per-user direct permission grants (union with role + group in app)
CREATE TABLE IF NOT EXISTS public.user_permissions (
  profile_id uuid NOT NULL REFERENCES public.profiles (id) ON DELETE CASCADE,
  permission_id uuid NOT NULL REFERENCES public.permissions (id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT user_permissions_pkey PRIMARY KEY (profile_id, permission_id)
);

CREATE INDEX IF NOT EXISTS idx_user_permissions_permission_id ON public.user_permissions (permission_id);

ALTER TABLE public.user_permissions ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "user_permissions_select_authenticated" ON public.user_permissions;
CREATE POLICY "user_permissions_select_authenticated"
  ON public.user_permissions
  FOR SELECT
  TO authenticated
  USING (true);

GRANT SELECT ON public.user_permissions TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_permissions TO service_role;

NOTIFY pgrst, 'reload schema';
