/**
 * Align Access Management (module / feature tree) with `lib/sidebar-config.ts` — product sections,
 * order, and which rows must not be over-filtered for orgs.
 */

import type { ModuleFeatureBucket, ModuleFeatureTreeNode } from "./access-entitlements";
import {
  type SidebarGroup,
  flattenSidebarLeaves,
  parsePermissionBase,
  MAIN_SIDEBAR,
  WMS_ONLY_NAV,
  TECH_DEBUG_LEAF,
} from "./sidebar-config";

const MF_KEY = (moduleKey: string, featureKey: string) =>
  `${String(moduleKey).trim().toLowerCase()}::${String(featureKey).trim().toLowerCase()}`;

type SidebarIndexEntry = {
  order: number;
  groupLabel: string;
};

let cachedIndex: Map<string, SidebarIndexEntry> | null = null;
let cachedKeySet: Set<string> | null = null;

function buildSidebarLookup(): { index: Map<string, SidebarIndexEntry>; keySet: Set<string> } {
  const index = new Map<string, SidebarIndexEntry>();
  let order = 0;

  const addGroup = (g: SidebarGroup) => {
    for (const leaf of g.children) {
      if (leaf.kind !== "leaf") continue;
      const { module, featureKey } = parsePermissionBase(leaf.permissionBase);
      const k = MF_KEY(module, featureKey);
      if (!index.has(k)) {
        index.set(k, { order, groupLabel: g.label || "" });
        order += 1;
      }
    }
  };

  for (const sec of MAIN_SIDEBAR) {
    for (const g of sec.groups) {
      addGroup(g);
    }
  }
  for (const leaf of WMS_ONLY_NAV.leaves) {
    const { module, featureKey } = parsePermissionBase(leaf.permissionBase);
    const k = MF_KEY(module, featureKey);
    if (!index.has(k)) {
      index.set(k, { order, groupLabel: WMS_ONLY_NAV.label });
      order += 1;
    }
  }
  {
    const { module, featureKey } = parsePermissionBase(TECH_DEBUG_LEAF.permissionBase);
    const k = MF_KEY(module, featureKey);
    if (!index.has(k)) {
      index.set(k, { order, groupLabel: "Tech" });
      order += 1;
    }
  }

  const keySet = new Set(index.keys());
  for (const leaf of flattenSidebarLeaves()) {
    const { module, featureKey } = parsePermissionBase(leaf.permissionBase);
    keySet.add(MF_KEY(module, featureKey));
  }
  return { index, keySet };
}

function getIndex(): { index: Map<string, SidebarIndexEntry>; keySet: Set<string> } {
  if (!cachedIndex || !cachedKeySet) {
    const b = buildSidebarLookup();
    cachedIndex = b.index;
    cachedKeySet = b.keySet;
  }
  return { index: cachedIndex, keySet: cachedKeySet };
}

/** `module::feature` keys that correspond to a real product leaf in the sidebar. */
export function isSidebarModuleFeatureKey(moduleKey: string, featureKey: string): boolean {
  return getIndex().keySet.has(MF_KEY(moduleKey, featureKey));
}

export function getSidebarGroupLabelForModuleFeature(
  moduleKey: string,
  featureKey: string,
): string | undefined {
  return getIndex().index.get(MF_KEY(moduleKey, featureKey))?.groupLabel;
}

/**
 * Get sort order (lower first). Non-sidebar features get a high fallback so they list last.
 */
export function getSidebarOrderForModuleFeature(moduleKey: string, featureKey: string): number {
  return getIndex().index.get(MF_KEY(moduleKey, featureKey))?.order ?? 1_000_000;
}

type BucketWithMeta = ModuleFeatureBucket & {
  sidebarGroupLabel?: string;
  sidebarOrder?: number;
};

/**
 * Sort modules and features to match main navigation / WMS, and attach `sidebarGroupLabel` / `sidebarOrder`
 * for Access Management display.
 */
export function sortAndAnnotateModuleFeatureTreeBySidebar(
  tree: ModuleFeatureTreeNode[],
): ModuleFeatureTreeNode[] {
  const nodes: ModuleFeatureTreeNode[] = tree.map((node) => {
    const features: BucketWithMeta[] = node.features.map((b) => {
      const meta = getIndex().index.get(MF_KEY(node.module.key, b.featureKey));
      return {
        ...b,
        sidebarGroupLabel: meta?.groupLabel,
        sidebarOrder: meta?.order ?? 1_000_000,
      };
    });
    features.sort((a, b) => (a.sidebarOrder ?? 0) - (b.sidebarOrder ?? 0) || a.featureKey.localeCompare(b.featureKey));
    return { ...node, features };
  });

  nodes.sort((a, b) => {
    const oa = a.features.map((x) => (x as BucketWithMeta).sidebarOrder ?? 1_000_000);
    const ob = b.features.map((x) => (x as BucketWithMeta).sidebarOrder ?? 1_000_000);
    const minA = oa.length > 0 ? Math.min(...oa) : 1_000_000;
    const minB = ob.length > 0 ? Math.min(...ob) : 1_000_000;
    if (minA !== minB) return minA - minB;
    return a.module.key.localeCompare(b.module.key);
  });

  return nodes;
}
