-- Per-user, per–module_feature overrides (replaces ad-hoc user_permissions for access UI; user_permissions table retained unused here)
CREATE TABLE IF NOT EXISTS public.user_feature_access_overrides (
  profile_id uuid NOT NULL REFERENCES public.profiles (id) ON DELETE CASCADE,
  module_feature_id uuid NOT NULL REFERENCES public.module_features (id) ON DELETE CASCADE,
  access_level text NOT NULL CHECK (access_level IN ('none', 'read', 'write')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT user_feature_access_overrides_pkey PRIMARY KEY (profile_id, module_feature_id)
);

CREATE INDEX IF NOT EXISTS idx_user_feature_access_overrides_module_feature_id
  ON public.user_feature_access_overrides (module_feature_id);

DROP TRIGGER IF EXISTS trg_user_feature_access_overrides_updated_at ON public.user_feature_access_overrides;
CREATE TRIGGER trg_user_feature_access_overrides_updated_at
  BEFORE UPDATE ON public.user_feature_access_overrides
  FOR EACH ROW
  EXECUTE FUNCTION public.set_updated_at();

ALTER TABLE public.user_feature_access_overrides ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "user_feature_access_overrides_select_authenticated" ON public.user_feature_access_overrides;
CREATE POLICY "user_feature_access_overrides_select_authenticated"
  ON public.user_feature_access_overrides FOR SELECT TO authenticated USING (true);
GRANT SELECT ON public.user_feature_access_overrides TO authenticated;
GRANT ALL ON public.user_feature_access_overrides TO service_role;

-- Catalog: `settings` + `platform` + `tech_debug` modules and features for admin/sidebar-style keys
INSERT INTO public.modules (id, key, name, sort_order)
SELECT gen_random_uuid(), v.key, v.name, v.ord
FROM (VALUES
  ('settings', 'Settings', 50),
  ('platform', 'Platform', 60),
  ('tech_debug', 'Tech debug', 70)
) AS v(key, name, ord)
WHERE NOT EXISTS (SELECT 1 FROM public.modules m WHERE m.key = v.key);

INSERT INTO public.module_features (id, module_id, key, name, sort_order)
SELECT gen_random_uuid(), m.id, v.fkey, v.fname, v.ord
FROM public.modules m
JOIN (VALUES
  ('settings', 'stores', 'Stores', 1),
  ('settings', 'users', 'Users', 2),
  ('platform', 'branding', 'Branding', 1),
  ('platform', 'organizations', 'Organizations', 2),
  ('platform', 'users', 'Users', 3),
  ('platform', 'access', 'Access management', 4),
  ('tech_debug', 'access', 'Access', 1)
) AS v(mkey, fkey, fname, ord) ON m.key = v.mkey
WHERE NOT EXISTS (
  SELECT 1 FROM public.module_features f WHERE f.module_id = m.id AND f.key = v.fkey
);

-- Permission keys: module.feature.action (matches inferFeatureKeyFromPermission)
INSERT INTO public.permissions (id, key, name, module, description)
SELECT gen_random_uuid(), v.pkey, v.pname, v.mod, v.descr
FROM (VALUES
  ('settings.stores.read', 'View', 'settings', 'View stores'),
  ('settings.stores.write', 'Edit', 'settings', 'Edit stores'),
  ('settings.stores.manage', 'Manage', 'settings', 'Manage stores'),
  ('settings.users.read', 'View', 'settings', 'View users area'),
  ('settings.users.write', 'Edit', 'settings', 'Edit users area'),
  ('settings.users.manage', 'Manage', 'settings', 'Manage users area'),
  ('platform.branding.read', 'View', 'platform', 'View platform branding'),
  ('platform.branding.write', 'Edit', 'platform', 'Edit platform branding'),
  ('platform.branding.manage', 'Manage', 'platform', 'Manage platform branding'),
  ('platform.organizations.read', 'View', 'platform', 'View organizations list'),
  ('platform.organizations.write', 'Edit', 'platform', 'Edit organizations'),
  ('platform.organizations.manage', 'Manage', 'platform', 'Manage organizations'),
  ('platform.users.read', 'View', 'platform', 'View platform user directory'),
  ('platform.users.write', 'Edit', 'platform', 'Edit platform users'),
  ('platform.users.manage', 'Manage', 'platform', 'Manage platform users'),
  ('platform.access.read', 'View', 'platform', 'View access management'),
  ('platform.access.write', 'Edit', 'platform', 'Edit access (roles, groups)'),
  ('platform.access.manage', 'Manage', 'platform', 'Full access management'),
  ('tech_debug.access', 'Access', 'tech_debug', 'Tech debug tools')
) AS v(pkey, pname, mod, descr)
ON CONFLICT (key) DO UPDATE SET
  name = EXCLUDED.name,
  module = EXCLUDED.module,
  description = EXCLUDED.description;

NOTIFY pgrst, 'reload schema';
