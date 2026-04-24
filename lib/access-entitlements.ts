/**
 * Access Management: generalized module / module_feature tree + org entitlement checks.
 * Permissions remain flat in `public.permissions`; grouping is derived from `modules`,
 * `module_features`, and permission key shape (`module.feature.action` when applicable).
 */

export const GENERAL_FEATURE_KEY = "_general";

export type AccessModuleRow = {
  id: string;
  key: string;
  name: string;
  /** When present in DB, lower sorts first. */
  sort_order?: number | null;
};

export type AccessModuleFeatureRow = {
  id: string;
  module_id: string;
  key: string;
  name: string;
  sort_order?: number | null;
};

export type AccessPermissionRow = {
  id: string;
  key: string;
  name: string;
  module: string;
  description: string | null;
};

/** Last segment of `module.feature.action` (or legacy two-part keys) → UI bucket. */
export type PermissionActionGroup = "read" | "write" | "manage" | "other";

export const PERMISSION_ACTION_GROUP_ORDER: PermissionActionGroup[] = [
  "read",
  "write",
  "manage",
  "other",
];

export const PERMISSION_ACTION_LABEL: Record<PermissionActionGroup, string> = {
  read: "Read",
  write: "Write",
  manage: "Manage",
  other: "Other",
};

export type PermissionActionBuckets = {
  read: AccessPermissionRow[];
  write: AccessPermissionRow[];
  manage: AccessPermissionRow[];
  other: AccessPermissionRow[];
};

export type ModuleFeatureBucket = {
  /** DB feature row when present; omitted bucket uses synthetic id */
  feature: AccessModuleFeatureRow | null;
  featureKey: string;
  /** Permissions under this feature, grouped for R/W/M (and Other when action is unclassified). */
  byAction: PermissionActionBuckets;
  /** Set by `sortAndAnnotateModuleFeatureTreeBySidebar` — sidebar nav group (e.g. Warehouse Operations). */
  sidebarGroupLabel?: string;
  /** Sidebar leaf order (lower first). */
  sidebarOrder?: number;
};

export type ModuleFeatureTreeNode = {
  module: AccessModuleRow;
  features: ModuleFeatureBucket[];
};

export type OrgEntitlementSnapshot = {
  modulesExplicit: boolean;
  moduleEntitled: Map<string, boolean>;
  featuresExplicitByModuleId: Map<string, boolean>;
  featureEntitled: Map<string, boolean>;
};

/** Entitlement display for Access Management: Enabled / Disabled / Not configured. */
export type OrgEntitlementUiStatus = "enabled" | "disabled" | "not_configured";

/** JSON-serializable org entitlement state for client components. */
export type OrgEntitlementsPayload = {
  modulesExplicit: boolean;
  moduleEntitledById: Record<string, boolean>;
  featuresExplicitByModuleId: Record<string, boolean>;
  featureEntitledById: Record<string, boolean>;
  /**
   * When set from `organization_modules` / `organization_module_features`, drives accurate
   * Enabled / Disabled / Not configured in Access Management. Optional for backward compatibility.
   */
  moduleEntitlementModeById?: Record<string, OrgEntitlementUiStatus>;
  moduleFeatureEntitlementModeById?: Record<string, OrgEntitlementUiStatus>;
};

export function snapshotToPayload(s: OrgEntitlementSnapshot): OrgEntitlementsPayload {
  return {
    modulesExplicit: s.modulesExplicit,
    moduleEntitledById: Object.fromEntries(s.moduleEntitled),
    featuresExplicitByModuleId: Object.fromEntries(s.featuresExplicitByModuleId),
    featureEntitledById: Object.fromEntries(s.featureEntitled),
  };
}

export function payloadToSnapshot(p: OrgEntitlementsPayload): OrgEntitlementSnapshot {
  return {
    modulesExplicit: p.modulesExplicit,
    moduleEntitled: new Map(Object.entries(p.moduleEntitledById)),
    featuresExplicitByModuleId: new Map(Object.entries(p.featuresExplicitByModuleId)),
    featureEntitled: new Map(Object.entries(p.featureEntitledById)),
  };
}

/**
 * For internal orgs: every module in the catalog tree is considered entitled for access-management
 * display (and runtime org checks) without reading `organization_modules`.
 */
export function orgEntitlementPayloadAllFeaturesUnderModules(
  tree: ModuleFeatureTreeNode[],
): OrgEntitlementsPayload {
  const moduleEntitledById: Record<string, boolean> = {};
  const moduleEntitlementModeById: Record<string, OrgEntitlementUiStatus> = {};
  for (const node of tree) {
    moduleEntitledById[node.module.id] = true;
    moduleEntitlementModeById[node.module.id] = "enabled";
  }
  return {
    modulesExplicit: true,
    moduleEntitledById,
    featuresExplicitByModuleId: {},
    featureEntitledById: {},
    moduleEntitlementModeById,
  };
}

/**
 * @deprecated Use full catalog + {@link orgEntitlementUiStatus} for UI. Previously used to hide
 * unlicensed features from Access Management; the tree is no longer filtered by org entitlements.
 */
export function filterModuleFeatureTreeByOrgEntitlements(
  tree: ModuleFeatureTreeNode[],
  snap: OrgEntitlementSnapshot,
): ModuleFeatureTreeNode[] {
  if (!snap.modulesExplicit) {
    return [];
  }
  const out: ModuleFeatureTreeNode[] = [];
  for (const node of tree) {
    if (snap.moduleEntitled.get(node.module.id) !== true) continue;
    const featureExplicit = snap.featuresExplicitByModuleId.get(node.module.id) === true;
    const features = featureExplicit
      ? node.features.filter((b) => {
          const fid = b.feature && !String(b.feature.id).startsWith("synthetic:") ? b.feature.id : null;
          if (!fid) return false;
          return snap.featureEntitled.get(fid) === true;
        })
      : node.features;
    if (features.length === 0) continue;
    out.push({ ...node, features });
  }
  return out;
}

/**
 * Access Management: show only what the org has **enabled** (and, when in per-feature licensing
 * mode, **purchased/activated** at feature level in `organization_module_features`).
 *
 * - **Modules** — require `moduleEntitlementModeById[moduleId] === "enabled"`.
 * - **Features** — when `featuresExplicitByModuleId[moduleId] === true`, keep only real
 *   `module_feature` rows with `moduleFeatureEntitlementModeById[featureId] === "enabled"`.
 *   When the module is *not* in explicit per-feature mode, all feature buckets under that
 *   module are kept (module-level license only).
 * - **Synthetic** / placeholder buckets are dropped when per-feature mode applies (no purchasable id).
 * - Drops modules with zero features after filtering. If `moduleEntitlementModeById` is empty
 *   (e.g. org rows failed to load), returns `tree` unchanged.
 */
export function filterModuleFeatureTreeToOrgLicensing(
  tree: ModuleFeatureTreeNode[],
  orgEntitlements: OrgEntitlementsPayload,
): ModuleFeatureTreeNode[] {
  const modModes = orgEntitlements.moduleEntitlementModeById;
  if (!modModes || Object.keys(modModes).length === 0) {
    return tree;
  }
  const featModes = orgEntitlements.moduleFeatureEntitlementModeById ?? {};
  const featuresExplicit = orgEntitlements.featuresExplicitByModuleId ?? {};

  return tree
    .filter((node) => modModes[node.module.id] === "enabled")
    .map((node) => {
      if (featuresExplicit[node.module.id] === true) {
        const features = node.features.filter((b) => {
          const feat = b.feature;
          if (!feat || String(feat.id).startsWith("synthetic:")) {
            return false;
          }
          return featModes[String(feat.id)] === "enabled";
        });
        return { ...node, features };
      }
      /** Per-feature org disables in `moduleFeatureEntitlementModeById` must apply even in module-level-only mode. */
      const features = node.features.filter((b) => {
        const feat = b.feature;
        if (!feat || String(feat.id).startsWith("synthetic:")) {
          return false;
        }
        if (featModes[String(feat.id)] === "disabled") {
          return false;
        }
        return true;
      });
      return { ...node, features };
    })
    .filter((node) => node.features.length > 0);
}

/**
 * License state for a module/feature from `organization_modules` / `organization_module_features`.
 * In Access Management, the module/feature tree is also filtered to {@link filterModuleFeatureTreeToOrgLicensing};
 * per-feature status still comes from this helper for badges and org checks.
 */
export function orgEntitlementUiStatus(args: {
  snapshot: OrgEntitlementSnapshot | OrgEntitlementsPayload | null;
  moduleId: string;
  moduleFeatureId: string | null;
  featureKey: string;
}): OrgEntitlementUiStatus {
  if (!args.snapshot) {
    return "not_configured";
  }
  const p = isEntitlementsPayload(args.snapshot) ? args.snapshot : null;
  const { moduleId, moduleFeatureId, featureKey } = args;
  if (p?.moduleEntitlementModeById?.[moduleId] === "disabled") {
    return "disabled";
  }
  /** Module row only: no per-feature id (Access Management module header). */
  if (!moduleFeatureId) {
    const m = p?.moduleEntitlementModeById?.[moduleId];
    if (m) return m;
  }
  if (moduleFeatureId && p?.moduleFeatureEntitlementModeById?.[moduleFeatureId] != null) {
    return p.moduleFeatureEntitlementModeById[moduleFeatureId]!;
  }
  if (p?.moduleEntitlementModeById?.[moduleId] === "not_configured") {
    return "not_configured";
  }
  if (p?.moduleEntitlementModeById?.[moduleId] === "enabled" && !moduleFeatureId) {
    return "enabled";
  }
  const snap = isEntitlementsPayload(args.snapshot)
    ? payloadToSnapshot(args.snapshot)
    : args.snapshot;

  if (!snap.modulesExplicit) {
    return "not_configured";
  }

  if (
    orgAllowsPermission({
      snapshot: args.snapshot,
      moduleId,
      moduleFeatureId,
      featureKey,
    }).entitled
  ) {
    return "enabled";
  }

  if (snap.moduleEntitled.get(args.moduleId) !== true) {
    return "disabled";
  }

  const featureExplicit = snap.featuresExplicitByModuleId.get(args.moduleId) === true;
  if (!featureExplicit || !moduleFeatureId || featureKey === GENERAL_FEATURE_KEY) {
    return "disabled";
  }
  if (!snap.featureEntitled.has(moduleFeatureId)) {
    return "not_configured";
  }
  return "disabled";
}

/**
 * `organization_modules` / `organization_module_features` rows: primary flag is
 * `is_enabled`. When that key is present, false means disabled; true means not disabled.
 * If `is_enabled` is null/undefined, fall back to legacy columns used by older data.
 */
export function orgEntitlementRowIsDisabled(r: Record<string, unknown>): boolean {
  if (Object.prototype.hasOwnProperty.call(r, "is_enabled")) {
    if (r.is_enabled === true) return false;
    if (r.is_enabled === false) return true;
  }
  return r.enabled === false || r.is_active === false || r.active === false;
}

/**
 * Per-module / per-feature entitlement row modes for the access tree, from raw DB rows.
 */
function entitlementMapIdKey(id: string): string {
  return id.trim().toLowerCase();
}

export function buildEntitlementModeMapsForTree(
  orgModuleRows: Record<string, unknown>[],
  orgFeatureRows: Record<string, unknown>[],
  tree: ModuleFeatureTreeNode[],
  _snap: OrgEntitlementSnapshot,
): {
  moduleEntitlementModeById: Record<string, OrgEntitlementUiStatus>;
  moduleFeatureEntitlementModeById: Record<string, OrgEntitlementUiStatus>;
} {
  const byMod = new Map<string, Record<string, unknown>>();
  for (const r of orgModuleRows) {
    const mid = String(r.module_id ?? (r as { moduleId?: unknown }).moduleId ?? "").trim();
    if (mid) byMod.set(entitlementMapIdKey(mid), r);
  }
  const moduleEntitlementModeById: Record<string, OrgEntitlementUiStatus> = {};
  for (const node of tree) {
    const row = byMod.get(entitlementMapIdKey(node.module.id));
    if (!row) {
      moduleEntitlementModeById[node.module.id] = "not_configured";
    } else {
      moduleEntitlementModeById[node.module.id] = orgEntitlementRowIsDisabled(row) ? "disabled" : "enabled";
    }
  }
  const byFeat = new Map<string, Record<string, unknown>>();
  for (const r of orgFeatureRows) {
    const fid = String(
      r.module_feature_id ?? (r as { moduleFeatureId?: unknown }).moduleFeatureId ?? "",
    ).trim();
    if (fid) byFeat.set(entitlementMapIdKey(fid), r);
  }
  const moduleFeatureEntitlementModeById: Record<string, OrgEntitlementUiStatus> = {};
  for (const node of tree) {
    for (const b of node.features) {
      const feat = b.feature;
      if (!feat || String(feat.id).startsWith("synthetic:")) {
        continue;
      }
      const fid = String(feat.id);
      const r = byFeat.get(entitlementMapIdKey(fid));
      if (!r) {
        moduleFeatureEntitlementModeById[fid] = "not_configured";
      } else {
        moduleFeatureEntitlementModeById[fid] = orgEntitlementRowIsDisabled(r) ? "disabled" : "enabled";
      }
    }
  }
  return { moduleEntitlementModeById, moduleFeatureEntitlementModeById };
}

function titleizeKey(raw: string): string {
  const s = raw.trim() || "general";
  return s
    .split(/[_\s]+/)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ");
}

/** `module.feature.action` → feature segment when first segment matches `permissions.module`. */
export function inferFeatureKeyFromPermission(perm: AccessPermissionRow): string {
  const mk = normalizedPermissionModule(perm).toLowerCase();
  const parts = perm.key.trim().toLowerCase().split(".").filter(Boolean);
  if (parts.length >= 3 && parts[0] === mk) {
    return parts[1] ?? GENERAL_FEATURE_KEY;
  }
  return GENERAL_FEATURE_KEY;
}

const READ_VERBS = new Set(["read", "view"]);
const WRITE_VERBS = new Set(["write", "edit", "create", "use", "submit", "update"]);
const MANAGE_VERBS = new Set(["manage", "admin", "adjust", "delete", "remove"]);

/**
 * Classify permission key into Read / Write / Manage / Other from the action segment.
 * Prefers `module.feature.<action>` when the first segment matches `permissions.module`.
 */
export function inferPermissionActionGroup(perm: AccessPermissionRow): PermissionActionGroup {
  const mk = normalizedPermissionModule(perm).toLowerCase();
  const parts = perm.key.trim().toLowerCase().split(".").filter(Boolean);
  let action = "";
  if (parts.length >= 3 && parts[0] === mk) {
    action = parts[parts.length - 1] ?? "";
  } else if (parts.length === 2 && parts[0] === mk) {
    action = parts[1] ?? "";
  } else if (parts.length >= 1) {
    action = parts[parts.length - 1] ?? "";
  }
  if (action === "access") return "read";
  if (READ_VERBS.has(action)) return "read";
  if (WRITE_VERBS.has(action)) return "write";
  if (MANAGE_VERBS.has(action)) return "manage";
  return "other";
}

export function partitionPermissionsByAction(perms: AccessPermissionRow[]): PermissionActionBuckets {
  const byAction: PermissionActionBuckets = { read: [], write: [], manage: [], other: [] };
  for (const p of perms) {
    byAction[inferPermissionActionGroup(p)].push(p);
  }
  for (const g of PERMISSION_ACTION_GROUP_ORDER) {
    byAction[g].sort(sortPerms);
  }
  return byAction;
}

export function totalPermissionsInBucket(bucket: ModuleFeatureBucket): number {
  return PERMISSION_ACTION_GROUP_ORDER.reduce((n, g) => n + bucket.byAction[g].length, 0);
}

export function buildOrgEntitlementSnapshot(
  orgModuleRows: Record<string, unknown>[],
  orgFeatureRows: Record<string, unknown>[],
  moduleFeatures: Pick<AccessModuleFeatureRow, "id" | "module_id">[],
): OrgEntitlementSnapshot {
  const moduleEntitled = new Map<string, boolean>();
  for (const r of orgModuleRows) {
    const mid = String(r.module_id ?? (r as { moduleId?: unknown }).moduleId ?? "").trim();
    if (!mid) continue;
    const disabled = orgEntitlementRowIsDisabled(r);
    moduleEntitled.set(mid, !disabled);
  }
  const modulesExplicit = orgModuleRows.length > 0;

  const featureEntitled = new Map<string, boolean>();
  const featuresExplicitByModuleId = new Map<string, boolean>();

  for (const r of orgFeatureRows) {
    const fid = String(
      r.module_feature_id ?? (r as { moduleFeatureId?: unknown }).moduleFeatureId ?? "",
    ).trim();
    if (!fid) continue;
    const disabled = orgEntitlementRowIsDisabled(r);
    featureEntitled.set(fid, !disabled);
    const mf = moduleFeatures.find((x) => x.id === fid);
    if (mf?.module_id) {
      featuresExplicitByModuleId.set(mf.module_id, true);
    }
  }

  return { modulesExplicit, moduleEntitled, featuresExplicitByModuleId, featureEntitled };
}

/**
 * A permission is usable only when org module + (if configured) org feature entitlements pass.
 * When no org module rows exist, modules are treated as entitled (legacy / rollout).
 */
function isEntitlementsPayload(
  s: OrgEntitlementSnapshot | OrgEntitlementsPayload,
): s is OrgEntitlementsPayload {
  return "moduleEntitledById" in s;
}

export function orgAllowsPermission(args: {
  snapshot: OrgEntitlementSnapshot | OrgEntitlementsPayload | null;
  moduleId: string;
  moduleFeatureId: string | null;
  featureKey: string;
}): { moduleOk: boolean; featureOk: boolean; entitled: boolean } {
  if (!args.snapshot) {
    return { moduleOk: true, featureOk: true, entitled: true };
  }
  const snap = isEntitlementsPayload(args.snapshot)
    ? payloadToSnapshot(args.snapshot)
    : args.snapshot;
  const { modulesExplicit, moduleEntitled, featuresExplicitByModuleId, featureEntitled } = snap;

  const moduleOk = !modulesExplicit || moduleEntitled.get(args.moduleId) === true;
  if (!moduleOk) {
    return { moduleOk: false, featureOk: false, entitled: false };
  }

  const featExplicitForModule = featuresExplicitByModuleId.get(args.moduleId) === true;
  if (!featExplicitForModule) {
    return { moduleOk: true, featureOk: true, entitled: true };
  }

  if (!args.moduleFeatureId || args.featureKey === GENERAL_FEATURE_KEY) {
    return { moduleOk: true, featureOk: true, entitled: true };
  }

  const featureOk = featureEntitled.get(args.moduleFeatureId) === true;
  return { moduleOk: true, featureOk, entitled: featureOk };
}

function normalizedPermissionModule(perm: AccessPermissionRow): string {
  return perm.module.trim() || "general";
}

export function resolveModuleRowForPermission(
  modules: AccessModuleRow[],
  perm: AccessPermissionRow,
): AccessModuleRow | null {
  const mk = normalizedPermissionModule(perm).toLowerCase();
  return modules.find((m) => m.key.trim().toLowerCase() === mk) ?? null;
}

export function buildModuleFeatureTree(
  modules: AccessModuleRow[],
  moduleFeatures: AccessModuleFeatureRow[],
  permissions: AccessPermissionRow[],
): ModuleFeatureTreeNode[] {
  const featuresByModule = new Map<string, AccessModuleFeatureRow[]>();
  for (const f of moduleFeatures) {
    const list = featuresByModule.get(f.module_id) ?? [];
    list.push(f);
    featuresByModule.set(f.module_id, list);
  }
  for (const [, list] of featuresByModule) {
    list.sort((a, b) => {
      const sa = a.sort_order ?? 1_000_000;
      const sb = b.sort_order ?? 1_000_000;
      if (sa !== sb) return sa - sb;
      return a.key.localeCompare(b.key) || a.name.localeCompare(b.name);
    });
  }

  type BucketKey = string;
  const bucketPerms = new Map<BucketKey, AccessPermissionRow[]>();

  function bucketKey(moduleId: string, featureKey: string): BucketKey {
    return `${moduleId}::${featureKey}`;
  }

  const unmapped: AccessPermissionRow[] = [];
  for (const p of permissions) {
    const mod = resolveModuleRowForPermission(modules, p);
    if (!mod) {
      unmapped.push(p);
      continue;
    }

    const fKey = inferFeatureKeyFromPermission(p);
    const bk = bucketKey(mod.id, fKey);
    const list = bucketPerms.get(bk) ?? [];
    list.push(p);
    bucketPerms.set(bk, list);
  }

  const sortedMods = [...modules].sort((a, b) => {
    const sa = a.sort_order ?? 1_000_000;
    const sb = b.sort_order ?? 1_000_000;
    if (sa !== sb) return sa - sb;
    return a.key.localeCompare(b.key);
  });
  const out: ModuleFeatureTreeNode[] = [];

  for (const mod of sortedMods) {
    const featsForMod = featuresByModule.get(mod.id) ?? [];
    const bucketKeysForMod = [...bucketPerms.keys()].filter((k) => k.startsWith(`${mod.id}::`));
    const seenFeatureKeys = new Set<string>();

    const features: ModuleFeatureBucket[] = [];

    for (const f of featsForMod) {
      seenFeatureKeys.add(f.key);
      const bk = bucketKey(mod.id, f.key);
      const perms = bucketPerms.get(bk) ?? [];
      features.push({
        feature: f,
        featureKey: f.key,
        byAction: partitionPermissionsByAction(perms),
      });
    }

    for (const bk of bucketKeysForMod.sort((a, b) => a.localeCompare(b))) {
      const featureKey = bk.split("::")[1] ?? "";
      if (!featureKey) continue;
      if (seenFeatureKeys.has(featureKey)) continue;
      const perms = bucketPerms.get(bk) ?? [];
      if (perms.length === 0) continue;

      const synthetic: AccessModuleFeatureRow = {
        id: `synthetic:${mod.id}:${featureKey}`,
        module_id: mod.id,
        key: featureKey,
        name:
          featureKey === GENERAL_FEATURE_KEY ? "General" : titleizeKey(featureKey),
        sort_order: null,
      };
      features.push({
        feature: synthetic,
        featureKey,
        byAction: partitionPermissionsByAction(perms),
      });
    }

    if (features.length > 0) {
      features.sort((a, b) => a.featureKey.localeCompare(b.featureKey));
      out.push({ module: mod, features });
    }
  }

  if (unmapped.length > 0) {
    const um = new Map<string, AccessPermissionRow[]>();
    for (const p of unmapped) {
      const k = normalizedPermissionModule(p);
      const list = um.get(k) ?? [];
      list.push(p);
      um.set(k, list);
    }
    for (const [modKey, plist] of [...um.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
      const mod: AccessModuleRow = {
        id: `unmapped:${modKey}`,
        key: modKey,
        name: `Unmapped (${modKey})`,
        sort_order: 9_999_999,
      };
      const synthetic: AccessModuleFeatureRow = {
        id: `unmapped:${modKey}:general`,
        module_id: mod.id,
        key: GENERAL_FEATURE_KEY,
        name: "Catalog (no matching public.modules row)",
        sort_order: null,
      };
      out.push({
        module: mod,
        features: [
          {
            feature: synthetic,
            featureKey: GENERAL_FEATURE_KEY,
            byAction: partitionPermissionsByAction(plist),
          },
        ],
      });
    }
  }

  return out;
}

function sortPerms(a: AccessPermissionRow, b: AccessPermissionRow): number {
  return a.name.localeCompare(b.name) || a.key.localeCompare(b.key);
}

/** Keep only permission rows whose ids are in the set (prunes empty features/modules). */
export function filterModuleFeatureTree(
  tree: ModuleFeatureTreeNode[],
  permissionIds: ReadonlySet<string>,
): ModuleFeatureTreeNode[] {
  const out: ModuleFeatureTreeNode[] = [];
  for (const node of tree) {
    const features: ModuleFeatureBucket[] = [];
    for (const bucket of node.features) {
      const filtered: AccessPermissionRow[] = [];
      for (const g of PERMISSION_ACTION_GROUP_ORDER) {
        for (const p of bucket.byAction[g]) {
          if (permissionIds.has(p.id)) filtered.push(p);
        }
      }
      if (filtered.length > 0) {
        features.push({ ...bucket, byAction: partitionPermissionsByAction(filtered) });
      }
    }
    if (features.length > 0) {
      features.sort((a, b) => a.featureKey.localeCompare(b.featureKey));
      out.push({ module: node.module, features });
    }
  }
  return out;
}

/**
 * When `modules` is empty in DB, synthesize module rows from distinct `permissions.module`
 * so the UI still renders (org entitlements by module UUID will not apply until catalog exists).
 */
export function virtualModulesFromPermissions(perms: AccessPermissionRow[]): AccessModuleRow[] {
  const keys = [...new Set(perms.map((p) => normalizedPermissionModule(p)).filter(Boolean))];
  return keys.sort((a, b) => a.localeCompare(b)).map((key) => ({
    id: `virt:${key}`,
    key,
    name: titleizeKey(key),
    sort_order: null,
  }));
}

/** Merge DB modules with virtual module rows for any `permissions.module` keys missing from DB. */
export function mergeModulesWithPermissionKeys(
  dbModules: AccessModuleRow[],
  perms: AccessPermissionRow[],
): AccessModuleRow[] {
  const byKey = new Map<string, AccessModuleRow>();
  for (const m of dbModules) {
    byKey.set(m.key.trim().toLowerCase(), m);
  }
  for (const p of perms) {
    const k = normalizedPermissionModule(p);
    const lk = k.toLowerCase();
    if (byKey.has(lk)) continue;
    byKey.set(lk, { id: `virt:${k}`, key: k, name: titleizeKey(k), sort_order: null });
  }
  return [...byKey.values()].sort((a, b) => a.key.localeCompare(b.key));
}
