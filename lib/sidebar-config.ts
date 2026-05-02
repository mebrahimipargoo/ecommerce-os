/**
 * Single source of truth for app navigation, Access Management feature tree, and DB catalog sync
 * (modules, module_features, permissions) — see `lib/sidebar-sync.ts`.
 * Additional flat (non–nav) permission keys: `lib/sidebar-catalog-extras.ts` (merged on sync; see `EXTRA_CATALOG_PERMISSIONS`).
 */

import type { RbacPermissions } from "../hooks/useRbacPermissions";
import {
  EXTRA_CATALOG_PERMISSIONS,
  type CatalogExtraPermission,
} from "./sidebar-catalog-extras";

export { type CatalogExtraPermission, EXTRA_CATALOG_PERMISSIONS };

export type SidebarRbac = keyof RbacPermissions | "always";

/** Lucide icon name — resolved in `AppShell` / `lib/sidebar-icons.ts`. */
export type SidebarIconName =
  | "Package"
  | "RotateCcw"
  | "ClipboardList"
  | "Banknote"
  | "DollarSign"
  | "ShieldAlert"
  | "FileText"
  | "Settings"
  | "Building2"
  | "Users"
  | "Palette"
  | "Network"
  | "Shield"
  | "Database"
  | "FileUp"
  | "ScanLine";

export type SidebarLeaf = {
  kind: "leaf";
  id: string;
  label: string;
  path: string;
  /** Nav icon; when omitted, parent group icon is not used (caller may default). */
  icon?: SidebarIconName;
  /**
   * Stable id for this screen (used in entitlements and overrides).
   * Convention: `module.subfeature` (e.g. `platform.organizations`).
   */
  featureKey: string;
  /**
   * Permission key prefix: rows synced as `permissionBase + ".read" | ".write" | ".manage"`.
   * Usually equal to `featureKey`.
   */
  permissionBase: string;
  /** Maps to a flag from `useRbacPermissions` (or "always"). */
  rbac: SidebarRbac;
  /** Sort order within the parent group. */
  order: number;
  /**
   * When `false`, the item is not shown in the main nav (still included in access sync and RWM
   * permissions if `permissionBase` is set). Omitted = visible.
   */
  showInSidebar?: boolean;
};

export type SidebarGroup = {
  kind: "group";
  id: string;
  label: string;
  icon: SidebarIconName;
  /** `public.modules.key` and grouping in Access Management. */
  moduleKey: string;
  order: number;
  children: SidebarLeaf[];
};

export type SidebarSection = {
  id: string;
  label: string;
  groups: SidebarGroup[];
  order: number;
};

export const MAIN_SIDEBAR: SidebarSection[] = [
  {
    id: "core",
    label: "",
    order: 0,
    groups: [
      {
        kind: "group",
        id: "warehouse",
        label: "Warehouse Operations",
        icon: "Package",
        moduleKey: "operations",
        order: 10,
        children: [
          {
            kind: "leaf",
            id: "returns",
            label: "Returns Processing",
            path: "/returns",
            featureKey: "operations.returns",
            permissionBase: "operations.returns",
            icon: "RotateCcw",
            rbac: "canSeeReturns",
            order: 1,
          },
          {
            kind: "leaf",
            id: "inventory",
            label: "Inventory Ledger",
            path: "/inventory",
            featureKey: "operations.inventory",
            permissionBase: "operations.inventory",
            icon: "ClipboardList",
            rbac: "always",
            order: 2,
          },
        ],
      },
      {
        kind: "group",
        id: "finance",
        label: "Finance & Claims",
        icon: "DollarSign",
        moduleKey: "finance",
        order: 20,
        children: [
          {
            kind: "leaf",
            id: "settlements",
            label: "Settlements",
            path: "/settlements",
            featureKey: "finance.settlements",
            permissionBase: "finance.settlements",
            icon: "Banknote",
            rbac: "always",
            order: 1,
          },
          {
            kind: "leaf",
            id: "claim_engine",
            label: "Claim Engine",
            path: "/claim-engine",
            featureKey: "claims.engine",
            permissionBase: "claims.engine",
            icon: "ShieldAlert",
            rbac: "canSeeClaimEngine",
            order: 2,
          },
          {
            kind: "leaf",
            id: "report_history",
            label: "Report History",
            path: "/claim-engine/report-history",
            featureKey: "claims.report_history",
            permissionBase: "claims.report_history",
            icon: "FileText",
            rbac: "canSeeReportHistory",
            order: 3,
          },
        ],
      },
      {
        kind: "group",
        id: "etl_imports",
        label: "Data Management",
        icon: "Database",
        moduleKey: "etl",
        order: 25,
        children: [
          {
            kind: "leaf",
            id: "amazon_etl",
            label: "Amazon ETL",
            path: "/dashboard/amazon-etl",
            featureKey: "etl.amazon_etl",
            permissionBase: "etl.amazon_etl",
            icon: "Database",
            rbac: "always",
            order: 1,
          },
        ],
      },
      {
        kind: "group",
        id: "system",
        label: "System Settings",
        icon: "Settings",
        moduleKey: "settings",
        order: 30,
        children: [
          {
            kind: "leaf",
            id: "stores",
            label: "Stores & Adapters",
            path: "/settings",
            featureKey: "settings.stores",
            permissionBase: "settings.stores",
            rbac: "canSeeSettings",
            order: 1,
          },
          {
            kind: "leaf",
            id: "tenant_users",
            label: "Users",
            path: "/users",
            featureKey: "settings.users",
            permissionBase: "settings.users",
            rbac: "canSeeUsers",
            order: 2,
          },
        ],
      },
      {
        kind: "group",
        id: "platform",
        label: "Platform Settings",
        icon: "Settings",
        moduleKey: "platform",
        order: 40,
        children: [
          {
            kind: "leaf",
            id: "platform_branding",
            label: "Product branding",
            path: "/platform/settings",
            featureKey: "platform.branding",
            permissionBase: "platform.branding",
            rbac: "canSeePlatformAdmin",
            order: 1,
          },
          {
            kind: "leaf",
            id: "platform_organizations",
            label: "Organizations",
            path: "/platform/organizations",
            featureKey: "platform.organizations",
            permissionBase: "platform.organizations",
            rbac: "canSeePlatformAdmin",
            order: 2,
          },
          {
            kind: "leaf",
            id: "platform_users",
            label: "Platform users",
            path: "/platform/users",
            featureKey: "platform.users",
            permissionBase: "platform.users",
            rbac: "canSeePlatformUserDirectory",
            order: 3,
          },
          {
            kind: "leaf",
            id: "platform_access",
            label: "Access Management",
            path: "/platform/access",
            featureKey: "platform.access",
            permissionBase: "platform.access",
            rbac: "canSeePlatformAccess",
            order: 4,
          },
        ],
      },
    ],
  },
  {
    id: "admin",
    label: "System Admin",
    order: 50,
    groups: [
      {
        kind: "group",
        id: "admin_imports",
        label: "System Admin",
        icon: "Database",
        moduleKey: "tenant_admin",
        order: 10,
        children: [
          {
            kind: "leaf",
            id: "imports",
            label: "Imports",
            path: "/imports",
            featureKey: "tenant_admin.imports",
            permissionBase: "tenant_admin.imports",
            rbac: "canSeeImports",
            order: 1,
          },
        ],
      },
    ],
  },
];

export const WMS_ONLY_NAV: { section: "wms"; label: string; order: number; leaves: SidebarLeaf[] } = {
  section: "wms",
  label: "WMS",
  order: 0,
  leaves: [
    {
      kind: "leaf",
      id: "wms_scan",
      label: "Scan Item",
      path: "/returns",
      featureKey: "wms.scanner",
      permissionBase: "wms.scanner",
      icon: "ScanLine",
      rbac: "always",
      order: 1,
    },
  ],
};

/** Tech debug: permission-only gate (no primary route in sidebar). */
export const TECH_DEBUG_LEAF: SidebarLeaf = {
  kind: "leaf",
  id: "tech_debug",
  label: "Tech debug",
  path: "/",
  featureKey: "tech_debug.access",
  permissionBase: "tech_debug.access",
  rbac: "canSeeTechDebug",
  order: 99,
};

export function isLeafVisibleByRbac(leaf: SidebarLeaf, p: RbacPermissions): boolean {
  if (leaf.rbac === "always") return true;
  return Boolean(p[leaf.rbac as keyof RbacPermissions]);
}

export function flattenSidebarLeaves(): SidebarLeaf[] {
  const out: SidebarLeaf[] = [];
  for (const sec of MAIN_SIDEBAR) {
    for (const g of sec.groups) {
      out.push(...g.children);
    }
  }
  out.push(...WMS_ONLY_NAV.leaves);
  out.push(TECH_DEBUG_LEAF);
  return out;
}

/** Display + sort for `public.modules` rows; keys match the first segment of each leaf’s `permissionBase`. */
export const MODULE_CATALOG: Record<string, { name: string; sort_order: number }> = {
  operations: { name: "Operations", sort_order: 10 },
  finance: { name: "Finance", sort_order: 20 },
  claims: { name: "Claims", sort_order: 30 },
  settings: { name: "Settings", sort_order: 40 },
  platform: { name: "Platform", sort_order: 50 },
  etl:          { name: "ETL / Imports", sort_order: 35 },
  tenant_admin: { name: "Tenant admin", sort_order: 60 },
  wms: { name: "WMS", sort_order: 5 },
  tech_debug: { name: "Tech debug", sort_order: 90 },
};

export function getModuleRowsForSync(): { moduleKey: string; name: string; sort_order: number }[] {
  const keys = new Set<string>();
  for (const leaf of flattenSidebarLeaves()) {
    keys.add(parsePermissionBase(leaf.permissionBase).module);
  }
  for (const row of EXTRA_CATALOG_PERMISSIONS) {
    const m = String(row.module ?? "").trim() || "general";
    keys.add(m);
  }
  return [...keys]
    .map((k) => {
      const cat = MODULE_CATALOG[k];
      return {
        moduleKey: k,
        name: cat?.name ?? k,
        sort_order: cat?.sort_order ?? 500,
      };
    })
    .sort((a, b) => a.sort_order - b.sort_order || a.moduleKey.localeCompare(b.moduleKey));
}

/**
 * @returns module first segment, feature = rest (e.g. `platform` / `users` from `platform.users`).
 */
export function parsePermissionBase(permissionBase: string): { module: string; featureKey: string } {
  const parts = permissionBase.split(".").filter(Boolean);
  if (parts.length < 2) {
    return { module: parts[0] ?? "general", featureKey: "general" };
  }
  const [mod, ...rest] = parts;
  return { module: mod!, featureKey: rest.join(".") || "general" };
}
