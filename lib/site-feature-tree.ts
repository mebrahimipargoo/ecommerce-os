/**
 * Hierarchical catalog of app areas (pages) for Access Management “User access” inspector.
 * Permission keys are optional hints for DB-backed grants; legacy RBAC still applies via
 * computeFeatureAccessRows in lib/site-feature-access-rows.ts.
 */

export type SiteFeatureNode = {
  id: string;
  label: string;
  /** Primary route when applicable */
  path?: string;
  /** DB permission keys that grant read / view access */
  readKeys?: string[];
  /** DB permission keys that grant write / mutate access */
  writeKeys?: string[];
  children?: SiteFeatureNode[];
};

/** Tree aligned with main App Router pages under app/ (auth/login pages omitted). */
export const SITE_FEATURE_TREE: SiteFeatureNode[] = [
  {
    id: "grp.operations",
    label: "Operations",
    children: [
      {
        id: "ops.dashboard",
        label: "Dashboard",
        path: "/",
        readKeys: ["dashboard.view"],
        writeKeys: ["dashboard.edit"],
      },
      {
        id: "ops.returns",
        label: "Returns",
        path: "/returns",
        readKeys: ["returns.view"],
        writeKeys: ["returns.edit"],
      },
      {
        id: "ops.claim_engine",
        label: "Claim engine",
        path: "/claim-engine",
        readKeys: ["claims.view"],
        writeKeys: ["claims.create"],
      },
      {
        id: "ops.report_history",
        label: "Report history",
        path: "/claim-engine/report-history",
        readKeys: ["claims.view"],
        writeKeys: ["claims.create"],
      },
      {
        id: "ops.investigation",
        label: "Investigation (submission)",
        path: "/claim-engine/investigation/[id]",
        readKeys: ["claims.view"],
        writeKeys: ["claims.create"],
      },
    ],
  },
  {
    id: "grp.warehouse",
    label: "Warehouse (WMS)",
    children: [
      {
        id: "wms.scanner",
        label: "Scanner",
        path: "/scanner",
        readKeys: ["wms.scanner.use"],
        writeKeys: ["wms.scanner.use"],
      },
    ],
  },
  {
    id: "grp.settings",
    label: "Settings",
    children: [
      {
        id: "settings.stores",
        label: "Stores & adapters",
        path: "/settings",
        readKeys: ["settings.stores.read", "settings.view", "settings.stores.view"],
        writeKeys: ["settings.stores.write", "settings.stores.edit", "settings.manage_users"],
      },
      {
        id: "settings.company",
        label: "Company",
        path: "/settings/company",
        readKeys: ["settings.view", "settings.company.view"],
        writeKeys: ["settings.company.edit", "settings.manage_users"],
      },
      {
        id: "settings.imports",
        label: "Imports",
        path: "/settings/imports",
        readKeys: ["settings.view", "settings.imports.access"],
        writeKeys: ["settings.imports.manage"],
      },
    ],
  },
  {
    id: "grp.tenant_admin",
    label: "Administration (tenant)",
    children: [
      {
        id: "admin.users",
        label: "Users",
        path: "/users",
        readKeys: ["settings.users.read", "settings.manage_users", "tenant.users.view"],
        writeKeys: ["settings.users.write", "settings.manage_users", "tenant.users.manage"],
      },
      {
        id: "admin.imports",
        label: "Imports (admin)",
        path: "/imports",
        readKeys: ["tenant.imports.access"],
        writeKeys: ["tenant.imports.manage"],
      },
      {
        id: "admin.settings",
        label: "Admin settings",
        path: "/admin/settings",
        readKeys: ["tenant.admin_settings.view"],
        writeKeys: ["tenant.admin_settings.manage"],
      },
    ],
  },
  {
    id: "grp.platform",
    label: "Platform",
    children: [
      {
        id: "platform.branding",
        label: "Product branding",
        path: "/platform/settings",
        readKeys: ["platform.branding.read", "platform.branding.view"],
        writeKeys: ["platform.branding.write", "platform.branding.manage"],
      },
      {
        id: "platform.organizations",
        label: "Organizations",
        path: "/platform/organizations",
        readKeys: ["platform.organizations.read", "platform.organizations.view"],
        writeKeys: ["platform.organizations.write", "platform.organizations.manage"],
      },
      {
        id: "platform.organization_new",
        label: "New organization",
        path: "/platform/organizations/new",
        readKeys: ["platform.organizations.write", "platform.organizations.manage"],
        writeKeys: ["platform.organizations.write", "platform.organizations.manage"],
      },
      {
        id: "platform.organization_detail",
        label: "Organization detail",
        path: "/platform/organizations/[id]",
        readKeys: ["platform.organizations.read", "platform.organizations.view"],
        writeKeys: ["platform.organizations.write", "platform.organizations.manage"],
      },
      {
        id: "platform.users",
        label: "Platform users",
        path: "/platform/users",
        readKeys: ["platform.users.read", "platform.users.view"],
        writeKeys: ["platform.users.write", "platform.users.manage"],
      },
      {
        id: "platform.access",
        label: "Access management",
        path: "/platform/access",
        readKeys: ["platform.access.read", "platform.access_management"],
        writeKeys: ["platform.access.write", "platform.access_management"],
      },
      {
        id: "platform.access_catalog",
        label: "Role & group catalog",
        path: "/platform/access/catalog",
        readKeys: ["platform.access.read", "platform.access_management"],
        writeKeys: ["platform.access.write", "platform.access_management"],
      },
    ],
  },
  {
    id: "grp.tech_debug",
    label: "Tech debug",
    children: [
      {
        id: "tech_debug.tools",
        label: "Debug tools",
        readKeys: ["tech_debug.access"],
        writeKeys: ["tech_debug.access"],
      },
    ],
  },
  {
    id: "grp.account",
    label: "Account",
    children: [
      {
        id: "account.profile",
        label: "Profile",
        path: "/profile",
        readKeys: ["profile.view"],
        writeKeys: ["profile.edit"],
      },
    ],
  },
];
