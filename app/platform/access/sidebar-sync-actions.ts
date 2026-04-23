"use server";

import { syncSidebarCatalogToDatabase } from "../../../lib/sidebar-sync";
import { supabaseServer } from "../../../lib/supabase-server";
import { assertManagePlatformAccess } from "./server-gate";

function deniedMessage(d: "not_authenticated" | "forbidden"): string {
  return d === "not_authenticated" ? "Not authenticated." : "Forbidden.";
}

export type SyncSidebarCatalogActionResult =
  | { ok: true; modules: number; moduleFeatures: number; permissions: number }
  | { ok: false; error: string };

/**
 * Upserts `public.modules`, `public.module_features`, and `public.permissions` from
 * `lib/sidebar-config.ts` (read/write/manage per `permissionBase`). Does not delete rows
 * or touch role/group/user-override tables.
 */
export async function syncSidebarCatalogFromConfigAction(): Promise<SyncSidebarCatalogActionResult> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) {
    return { ok: false, error: deniedMessage(g.denied) };
  }
  const r = await syncSidebarCatalogToDatabase(supabaseServer);
  if (!r.ok) {
    return { ok: false, error: r.error };
  }
  return {
    ok: true,
    modules: r.modules,
    moduleFeatures: r.moduleFeatures,
    permissions: r.permissions,
  };
}
