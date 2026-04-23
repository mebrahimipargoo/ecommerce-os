import {
  totalPermissionsInBucket,
  type ModuleFeatureBucket,
  type ModuleFeatureTreeNode,
  PERMISSION_ACTION_GROUP_ORDER,
} from "./access-entitlements";
import { isSidebarModuleFeatureKey } from "./sidebar-access-ordering";

export type UiAccessLevel = "none" | "read" | "write" | "manage";

export const UI_ACCESS_LEVELS: readonly UiAccessLevel[] = [
  "none",
  "read",
  "write",
  "manage",
] as const;

export const UI_ACCESS_LEVEL_LABEL: Record<UiAccessLevel, string> = {
  none: "None",
  read: "Read",
  write: "Write",
  manage: "Manage",
};

/** All permission ids under a feature bucket (read/write/manage/other). */
export function allPermissionIdsInFeatureBucket(bucket: ModuleFeatureBucket): string[] {
  const out: string[] = [];
  for (const g of PERMISSION_ACTION_GROUP_ORDER) {
    for (const p of bucket.byAction[g]) {
      if (p.id) out.push(p.id);
    }
  }
  return out;
}

/**
 * From granted ids, derive UI level: Read if only read; Write if read+write; Manage if R+W+Manage; Other grants lift minimum.
 */
export function effectiveLevelFromFeatureBucket(
  bucket: ModuleFeatureBucket,
  granted: ReadonlySet<string>,
): UiAccessLevel {
  const r = bucket.byAction.read.some((p) => granted.has(p.id));
  const w = bucket.byAction.write.some((p) => granted.has(p.id));
  const m = bucket.byAction.manage.some((p) => granted.has(p.id));
  const o = bucket.byAction.other.some((p) => granted.has(p.id));

  if (r && w && m) return "manage";
  if (r && w) return "write";
  if (r) return "read";
  if (w && m) return "manage";
  if (w) return "write";
  if (m) return "manage";
  if (o) return "read";
  return "none";
}

/**
 * Ids to grant (subset of bucket) for the chosen level; "none" = revoke all in bucket.
 */
export function permissionIdsForUiLevel(
  bucket: ModuleFeatureBucket,
  level: UiAccessLevel,
): Set<string> {
  const out = new Set<string>();
  if (level === "none") return out;
  for (const p of bucket.byAction.read) {
    out.add(p.id);
  }
  if (level === "read") return out;
  for (const p of bucket.byAction.write) {
    out.add(p.id);
  }
  if (level === "write") return out;
  for (const p of bucket.byAction.manage) {
    out.add(p.id);
  }
  for (const p of bucket.byAction.other) {
    out.add(p.id);
  }
  return out;
}

export function isSyntheticOrUnmappedModule(node: { module: { id: string } }): boolean {
  return (
    node.module.id.startsWith("unmapped:")
    || node.module.id.startsWith("virt:")
  );
}

export function isSyntheticFeature(
  f: { feature: { id: string } | null; featureKey: string },
): boolean {
  if (!f.feature) return true;
  return String(f.feature.id).startsWith("synthetic:");
}

/**
 * Main Access Management tree: only real `modules` + `module_feature` rows; drop key-only synthetics and unmapped blocks.
 */
export function filterAccessManagementPrimaryTree(
  tree: ModuleFeatureTreeNode[],
): ModuleFeatureTreeNode[] {
  return tree
    .filter((n) => !isSyntheticOrUnmappedModule(n))
    .map((n) => ({
      ...n,
      features: n.features.filter((b) => {
        if (!isSyntheticFeature(b)) return true;
        // Keep synthetic buckets that have real permissions for a known sidebar product feature
        // (e.g. before module_features sync); role/group perm updates still work on real permission ids.
        if (totalPermissionsInBucket(b) === 0) return false;
        return isSidebarModuleFeatureKey(n.module.key, b.featureKey);
      }),
    }))
    .filter((n) => n.features.length > 0);
}

export function findFeatureBucket(
  tree: ModuleFeatureTreeNode[],
  moduleId: string,
  featureKey: string,
): ModuleFeatureBucket | null {
  const node = tree.find((n) => n.module.id === moduleId);
  if (!node) return null;
  return node.features.find((b) => b.featureKey === featureKey) ?? null;
}

/** All feature buckets in a module from the access-management primary tree. */
export function findAllFeatureBucketsInModule(
  tree: ModuleFeatureTreeNode[],
  moduleId: string,
): ModuleFeatureBucket[] {
  const node = tree.find((n) => n.module.id === moduleId);
  if (!node) return [];
  return node.features;
}
