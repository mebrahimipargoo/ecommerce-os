-- Extra flat permission keys for site feature tree / future role-group assignment (idempotent).

INSERT INTO public.permissions (key, name, module, description)
VALUES
  ('dashboard.view', 'Dashboard (view)', 'dashboard', 'View dashboard / home'),
  ('dashboard.edit', 'Dashboard (edit)', 'dashboard', 'Change dashboard-level data'),
  ('settings.stores.view', 'Stores & adapters (view)', 'settings', 'View stores & adapters settings'),
  ('settings.stores.edit', 'Stores & adapters (edit)', 'settings', 'Edit stores & adapters'),
  ('settings.company.view', 'Company settings (view)', 'settings', 'View company branding settings'),
  ('settings.company.edit', 'Company settings (edit)', 'settings', 'Edit company branding'),
  ('settings.imports.access', 'Settings imports (access)', 'settings', 'Use imports under settings'),
  ('settings.imports.manage', 'Settings imports (manage)', 'settings', 'Manage imports configuration'),
  ('tenant.users.view', 'Tenant users (view)', 'tenant_admin', 'View tenant user list'),
  ('tenant.users.manage', 'Tenant users (manage)', 'tenant_admin', 'Manage tenant users'),
  ('tenant.imports.access', 'Tenant imports (view)', 'tenant_admin', 'View admin imports'),
  ('tenant.imports.manage', 'Tenant imports (manage)', 'tenant_admin', 'Run / manage admin imports'),
  ('tenant.admin_settings.view', 'Admin settings (view)', 'tenant_admin', 'View admin settings area'),
  ('tenant.admin_settings.manage', 'Admin settings (manage)', 'tenant_admin', 'Change admin settings'),
  ('wms.scanner.use', 'Scanner', 'wms', 'Use warehouse scanner'),
  ('profile.view', 'Profile (view)', 'account', 'View own profile'),
  ('profile.edit', 'Profile (edit)', 'account', 'Edit own profile'),
  ('platform.branding.view', 'Platform branding (view)', 'platform', 'View platform product branding'),
  ('platform.branding.manage', 'Platform branding (manage)', 'platform', 'Edit platform product branding'),
  ('platform.organizations.view', 'Organizations (view)', 'platform', 'View platform organizations'),
  ('platform.organizations.manage', 'Organizations (manage)', 'platform', 'Create or edit organizations'),
  ('platform.users.view', 'Platform users (view)', 'platform', 'View platform user directory'),
  ('platform.users.manage', 'Platform users (manage)', 'platform', 'Manage platform users')
ON CONFLICT (key) DO UPDATE SET
  name        = EXCLUDED.name,
  module      = EXCLUDED.module,
  description = EXCLUDED.description;

NOTIFY pgrst, 'reload schema';
