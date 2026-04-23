import { SITE_FEATURE_TREE, type SiteFeatureNode } from "./site-feature-tree";

export type AccessSource = "role" | "database" | "both";

export type FeatureAccessRow = {
  id: string;
  label: string;
  path?: string;
  depth: number;
  isGroup: boolean;
  read: boolean;
  write: boolean;
  readSource: AccessSource;
  writeSource: AccessSource;
};

function hasKey(keys: string[] | undefined, perm: ReadonlySet<string>): boolean {
  if (!keys?.length) return false;
  return keys.some((k) => perm.has(k));
}

/**
 * Flattens `SITE_FEATURE_TREE` into table rows. Access is derived from `permissionKeys` (explicit DB keys).
 * `roleKey` is reserved for future per-role default hints; today grants are “database” when keys match.
 */
export function computeFeatureAccessRows(roleKey: string, permissionKeys: string[]): FeatureAccessRow[] {
  void roleKey;
  const perm = new Set(permissionKeys);
  const out: FeatureAccessRow[] = [];

  const visit = (nodes: SiteFeatureNode[], depth: number) => {
    for (const n of nodes) {
      if (n.children && n.children.length > 0) {
        out.push({
          id: n.id,
          label: n.label,
          path: n.path,
          depth,
          isGroup: true,
          read: false,
          write: false,
          readSource: "role",
          writeSource: "role",
        });
        visit(n.children, depth + 1);
        continue;
      }
      const r = hasKey(n.readKeys, perm) || hasKey(n.writeKeys, perm);
      const w = hasKey(n.writeKeys, perm);
      out.push({
        id: n.id,
        label: n.label,
        path: n.path,
        depth,
        isGroup: false,
        read: r,
        write: w,
        readSource: r ? "database" : "role",
        writeSource: w ? "database" : "role",
      });
    }
  };

  visit(SITE_FEATURE_TREE, 0);
  return out;
}
