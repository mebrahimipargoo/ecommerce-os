/**
 * Legacy and umbrella permission keys (non–sidebar RWM) merged during `syncSidebarCatalogToDatabase`.
 * Sidebar-generated `*.read` / `*.write` / `*.manage` from `lib/sidebar-config.ts` take precedence
 * for duplicate keys. Keys that duplicate sidebar RWM are never written twice to the map.
 *
 * @see {@link ./sidebar-config}
 */

type PermissionDefinition = {
  key: string;
  name: string;
  /** DB grouping column; may match the first segment of `key` (see `tenant.*` using module `tenant_admin`). */
  module: string;
  feature_key: string;
  action: string;
  description: string;
};

function p(def: PermissionDefinition): PermissionDefinition {
  return def;
}

/** `module.feature.read|write|manage` (modern Access Management shape). */
function rwm(
  module: string,
  feature: string,
  nameR: string,
  nameW: string,
  nameM: string,
  descR: string,
  descW: string,
  descM: string,
): PermissionDefinition[] {
  return [
    p({ key: `${module}.${feature}.read`, name: nameR, module, feature_key: feature, action: "read", description: descR }),
    p({ key: `${module}.${feature}.write`, name: nameW, module, feature_key: feature, action: "write", description: descW }),
    p({ key: `${module}.${feature}.manage`, name: nameM, module, feature_key: feature, action: "manage", description: descM }),
  ];
}

const CORE: PermissionDefinition[] = [
  // ----- inventory -----
  p({
    key: "inventory.view",
    name: "View",
    module: "inventory",
    feature_key: "general",
    action: "view",
    description: "View inventory",
  }),
  p({
    key: "inventory.edit",
    name: "Edit",
    module: "inventory",
    feature_key: "general",
    action: "edit",
    description: "Edit inventory records",
  }),
  p({
    key: "inventory.adjust",
    name: "Adjust",
    module: "inventory",
    feature_key: "general",
    action: "adjust",
    description: "Adjust stock levels",
  }),
  // ----- claims -----
  p({
    key: "claims.view",
    name: "View",
    module: "claims",
    feature_key: "general",
    action: "view",
    description: "View claims",
  }),
  p({
    key: "claims.create",
    name: "Create",
    module: "claims",
    feature_key: "general",
    action: "create",
    description: "Create claims",
  }),
  // ----- returns -----
  p({
    key: "returns.view",
    name: "View",
    module: "returns",
    feature_key: "general",
    action: "view",
    description: "View returns",
  }),
  p({
    key: "returns.edit",
    name: "Edit",
    module: "returns",
    feature_key: "general",
    action: "edit",
    description: "Edit returns",
  }),
  // ----- dashboard -----
  p({
    key: "dashboard.view",
    name: "View",
    module: "dashboard",
    feature_key: "general",
    action: "view",
    description: "View dashboard / home",
  }),
  p({
    key: "dashboard.edit",
    name: "Edit",
    module: "dashboard",
    feature_key: "general",
    action: "edit",
    description: "Change dashboard-level data",
  }),
  // ----- account / profile -----
  p({
    key: "profile.view",
    name: "View",
    module: "account",
    feature_key: "profile",
    action: "view",
    description: "View own profile",
  }),
  p({
    key: "profile.edit",
    name: "Edit",
    module: "account",
    feature_key: "profile",
    action: "edit",
    description: "Edit own profile",
  }),
  // ----- wms -----
  p({
    key: "wms.scanner.use",
    name: "Use",
    module: "wms",
    feature_key: "scanner",
    action: "use",
    description: "Use warehouse scanner",
  }),
  // ----- settings (tenant): legacy & coarse-grained -----
  p({
    key: "settings.view",
    name: "View",
    module: "settings",
    feature_key: "organization",
    action: "view",
    description: "View organization settings",
  }),
  p({
    key: "settings.manage_users",
    name: "Manage users",
    module: "settings",
    feature_key: "users",
    action: "manage_users",
    description: "Manage users and assignments",
  }),
  p({
    key: "settings.stores.view",
    name: "View",
    module: "settings",
    feature_key: "stores",
    action: "view",
    description: "View stores & adapters settings (legacy key)",
  }),
  p({
    key: "settings.stores.edit",
    name: "Edit",
    module: "settings",
    feature_key: "stores",
    action: "edit",
    description: "Edit stores & adapters (legacy key)",
  }),
  p({
    key: "settings.company.view",
    name: "View",
    module: "settings",
    feature_key: "company",
    action: "view",
    description: "View company branding settings",
  }),
  p({
    key: "settings.company.edit",
    name: "Edit",
    module: "settings",
    feature_key: "company",
    action: "edit",
    description: "Edit company branding",
  }),
  p({
    key: "settings.imports.access",
    name: "Access",
    module: "settings",
    feature_key: "imports",
    action: "access",
    description: "Use imports under settings",
  }),
  p({
    key: "settings.imports.manage",
    name: "Manage",
    module: "settings",
    feature_key: "imports",
    action: "manage",
    description: "Manage imports configuration",
  }),
  // ----- settings: modern RWM (aligns with module_features) -----
  ...rwm(
    "settings",
    "stores",
    "View",
    "Edit",
    "Manage",
    "View stores",
    "Edit stores",
    "Manage stores",
  ),
  ...rwm(
    "settings",
    "users",
    "View",
    "Edit",
    "Manage",
    "View users area in organization settings",
    "Edit users area",
    "Manage users area",
  ),
  // ----- tenant admin (module column tenant_admin) -----
  p({
    key: "tenant.users.view",
    name: "View",
    module: "tenant_admin",
    feature_key: "users",
    action: "view",
    description: "View tenant user list",
  }),
  p({
    key: "tenant.users.manage",
    name: "Manage",
    module: "tenant_admin",
    feature_key: "users",
    action: "manage",
    description: "Manage tenant users",
  }),
  p({
    key: "tenant.imports.access",
    name: "Access",
    module: "tenant_admin",
    feature_key: "imports",
    action: "access",
    description: "View admin imports",
  }),
  p({
    key: "tenant.imports.manage",
    name: "Manage",
    module: "tenant_admin",
    feature_key: "imports",
    action: "manage",
    description: "Run / manage admin imports",
  }),
  p({
    key: "tenant.admin_settings.view",
    name: "View",
    module: "tenant_admin",
    feature_key: "admin_settings",
    action: "view",
    description: "View admin settings area",
  }),
  p({
    key: "tenant.admin_settings.manage",
    name: "Manage",
    module: "tenant_admin",
    feature_key: "admin_settings",
    action: "manage",
    description: "Change admin settings",
  }),
  // ----- platform: legacy two-tier keys (view/manage) -----
  p({
    key: "platform.branding.view",
    name: "View",
    module: "platform",
    feature_key: "branding",
    action: "view",
    description: "View platform product branding (legacy key)",
  }),
  p({
    key: "platform.organizations.view",
    name: "View",
    module: "platform",
    feature_key: "organizations",
    action: "view",
    description: "View platform organizations (legacy key)",
  }),
  p({
    key: "platform.users.view",
    name: "View",
    module: "platform",
    feature_key: "users",
    action: "view",
    description: "View platform user directory (legacy key)",
  }),
  p({
    key: "platform.access_management",
    name: "Access management",
    module: "platform",
    feature_key: "access",
    action: "access_management",
    description: "Use platform access / permissions UI (umbrella; prefer platform.access.* for RWM)",
  }),
  // ----- platform: RWM (matches module_features / Access Management) -----
  ...rwm(
    "platform",
    "branding",
    "View",
    "Edit",
    "Manage",
    "View platform branding",
    "Edit platform branding",
    "Manage platform branding",
  ),
  ...rwm(
    "platform",
    "organizations",
    "View",
    "Edit",
    "Manage",
    "View organizations list",
    "Edit organizations",
    "Manage organizations",
  ),
  ...rwm(
    "platform",
    "users",
    "View",
    "Edit",
    "Manage",
    "View platform user directory",
    "Edit platform users",
    "Manage platform users",
  ),
  ...rwm(
    "platform",
    "access",
    "View",
    "Edit",
    "Manage",
    "View access management",
    "Edit access (roles, groups, user overrides)",
    "Full access management",
  ),
  // ----- tech debug -----
  p({
    key: "tech_debug.access",
    name: "Access",
    module: "tech_debug",
    feature_key: "access",
    action: "access",
    description: "Tech debug tools (single gate)",
  }),
];

function sortByKey(list: PermissionDefinition[]): PermissionDefinition[] {
  return [...list].sort((a, b) => a.key.localeCompare(b.key));
}

function assertUniqueKeys(list: readonly PermissionDefinition[]): void {
  const seen = new Set<string>();
  for (const row of list) {
    if (seen.has(row.key)) {
      throw new Error(`[sidebar-catalog-extras] Duplicate permission key: ${row.key}`);
    }
    seen.add(row.key);
  }
}

const _sorted = sortByKey(CORE);
assertUniqueKeys(_sorted);

export type CatalogExtraPermission = {
  key: string;
  name: string;
  module: string;
  action: string;
  description: string;
};

/**
 * All legacy / umbrella keys (plus historical RWM triples where they don’t conflict with the sidebar,
 * or are skipped on upsert if the sidebar RWM key already won).
 */
export const EXTRA_CATALOG_PERMISSIONS: readonly CatalogExtraPermission[] = _sorted.map((r) => ({
  key: r.key,
  name: r.name,
  module: r.module,
  action: r.action,
  description: r.description,
}));
