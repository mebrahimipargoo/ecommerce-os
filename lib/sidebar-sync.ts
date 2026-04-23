import type { SupabaseClient } from "@supabase/supabase-js";
import { EXTRA_CATALOG_PERMISSIONS } from "./sidebar-catalog-extras";
import {
  flattenSidebarLeaves,
  getModuleRowsForSync,
  parsePermissionBase,
} from "./sidebar-config";

export type SidebarCatalogSyncResult =
  | { ok: true; modules: number; moduleFeatures: number; permissions: number }
  | { ok: false; error: string };

const RWM: { suffix: "read" | "write" | "manage"; name: string; desc: (label: string) => string }[] = [
  { suffix: "read", name: "View", desc: (label) => `View ${label}` },
  { suffix: "write", name: "Edit", desc: (label) => `Edit ${label}` },
  { suffix: "manage", name: "Manage", desc: (label) => `Manage ${label}` },
];

type SidebarPermissionRow = {
  key: string;
  name: string;
  module: string;
  description: string;
  /** `public.permissions.action` (NOT NULL). RWM from sidebar use read|write|manage; legacy extras use the catalog action string. */
  action: string;
};

function isValidRwmAction(x: string): x is "read" | "write" | "manage" {
  return x === "read" || x === "write" || x === "manage";
}

/**
 * Inserts/updates `public.modules`, `public.module_features`, and `public.permissions` from
 * the sidebar tree (`lib/sidebar-config.ts`) and merged legacy keys (`lib/sidebar-catalog-extras.ts`).
 * Sidebar RWM keys win on duplicate `key`. Does not delete rows or touch role / group / user override tables.
 */
export async function syncSidebarCatalogToDatabase(
  supabase: SupabaseClient,
): Promise<SidebarCatalogSyncResult> {
  const leaves = flattenSidebarLeaves();
  const modRows = getModuleRowsForSync();
  const moduleIdByKey = new Map<string, string>();

  for (const m of modRows) {
    const { data: existing, error: selErr } = await supabase
      .from("modules")
      .select("id")
      .eq("key", m.moduleKey)
      .maybeSingle();
    if (selErr) {
      return { ok: false, error: `modules read: ${selErr.message}` };
    }
    if (existing?.id) {
      const { error: up } = await supabase
        .from("modules")
        .update({ name: m.name, sort_order: m.sort_order })
        .eq("id", existing.id);
      if (up) {
        return { ok: false, error: `modules update: ${up.message}` };
      }
      moduleIdByKey.set(m.moduleKey, String(existing.id));
    } else {
      const { data: ins, error: inErr } = await supabase
        .from("modules")
        .insert({ key: m.moduleKey, name: m.name, sort_order: m.sort_order })
        .select("id")
        .single();
      if (inErr) {
        return { ok: false, error: `modules insert: ${inErr.message}` };
      }
      if (ins?.id) {
        moduleIdByKey.set(m.moduleKey, String(ins.id));
      }
    }
  }

  const seenFeature = new Set<string>();
  for (const leaf of leaves) {
    const { module, featureKey } = parsePermissionBase(leaf.permissionBase);
    const modId = moduleIdByKey.get(module);
    if (!modId) {
      return { ok: false, error: `No module id for "${module}" — run module sync or check MODULE_CATALOG.` };
    }
    const fk = `${modId}::${featureKey}`;
    if (seenFeature.has(fk)) continue;
    seenFeature.add(fk);

    const { data: exf, error: e1 } = await supabase
      .from("module_features")
      .select("id")
      .eq("module_id", modId)
      .eq("key", featureKey)
      .maybeSingle();
    if (e1) {
      return { ok: false, error: `module_features read: ${e1.message}` };
    }
    const sortOrder = leaf.order * 10;
    if (exf?.id) {
      const { error: u2 } = await supabase
        .from("module_features")
        .update({ name: leaf.label, sort_order: sortOrder })
        .eq("id", exf.id);
      if (u2) {
        return { ok: false, error: `module_features update: ${u2.message}` };
      }
    } else {
      const { error: i2 } = await supabase.from("module_features").insert({
        module_id: modId,
        key: featureKey,
        name: leaf.label,
        sort_order: sortOrder,
      });
      if (i2) {
        return { ok: false, error: `module_features insert: ${i2.message}` };
      }
    }
  }

  const permByKey = new Map<string, SidebarPermissionRow>();
  for (const leaf of leaves) {
    const baseRaw = leaf.permissionBase != null ? String(leaf.permissionBase) : "";
    const base = baseRaw.trim();
    if (!base) {
      return {
        ok: false,
        error: `sidebar-sync: leaf "${leaf.id}" has empty permissionBase; cannot derive permission keys.`,
      };
    }
    const { module } = parsePermissionBase(base);
    if (!module) {
      return {
        ok: false,
        error: `sidebar-sync: leaf "${leaf.id}" permissionBase "${base}" has no module segment.`,
      };
    }
    for (const row of RWM) {
      const action = row.suffix;
      const key = `${base}.${row.suffix}`;
      if (permByKey.has(key)) {
        continue;
      }
      permByKey.set(key, {
        key,
        name: row.name,
        module,
        description: row.desc(leaf.label),
        action,
      });
    }
  }

  for (const ex of EXTRA_CATALOG_PERMISSIONS) {
    const key = String(ex.key ?? "").trim();
    if (!key) {
      // eslint-disable-next-line no-console
      console.error("[sidebar-sync] extra catalog entry missing key:", ex);
      return { ok: false, error: "sidebar-sync: EXTRA_CATALOG_PERMISSIONS has an entry with an empty `key`." };
    }
    if (permByKey.has(key)) {
      continue;
    }
    const action = String(ex.action ?? "").trim();
    if (!action) {
      // eslint-disable-next-line no-console
      console.error("[sidebar-sync] extra catalog entry has empty action:", ex);
      return { ok: false, error: `sidebar-sync: extra catalog key "${key}" has empty \`action\` (NOT NULL in DB).` };
    }
    const mod = String(ex.module ?? "").trim() || "general";
    const name = String(ex.name ?? "").trim() || "Permission";
    const description = ex.description != null ? String(ex.description) : "";
    permByKey.set(key, { key, name, module: mod, description, action });
  }

  const permRows = [...permByKey.values()].sort((a, b) => a.key.localeCompare(b.key));
  for (const pr of permRows) {
    if (!pr.key?.trim() || !String(pr.action).trim()) {
      // eslint-disable-next-line no-console
      console.error("[sidebar-sync] invalid permission row (should not happen):", pr);
      return {
        ok: false,
        error: `sidebar-sync: invalid built row for key "${pr.key}": \`key\` and \`action\` must be non-empty strings.`,
      };
    }
  }

  for (const pr of permRows) {
    const segs = pr.key.toLowerCase().split(".").filter(Boolean);
    if (segs.length < 2) {
      continue;
    }
    const last = segs[segs.length - 1] ?? "";
    if (isValidRwmAction(last) && pr.action.toLowerCase() !== last) {
      // eslint-disable-next-line no-console
      console.error("[sidebar-sync] RWM-style key has mismatched action:", pr);
      return {
        ok: false,
        error: `sidebar-sync: key "${pr.key}" ends with a RWM segment but \`action\` is "${pr.action}" (expected "${last}").`,
      };
    }
  }

  for (let i = 0; i < permRows.length; i += 150) {
    const chunk = permRows.slice(i, i + 150);
    const { error: pe } = await supabase.from("permissions").upsert(chunk, { onConflict: "key" });
    if (pe) {
      return { ok: false, error: `permissions: ${pe.message}` };
    }
  }

  return {
    ok: true,
    modules: modRows.length,
    moduleFeatures: seenFeature.size,
    permissions: permRows.length,
  };
}
