"use server";

import { canManagePlatformAccessCatalog } from "../../../lib/platform-access-management";
import { loadTenantProfile } from "../../../lib/server-tenant";
import { getSessionUserIdFromCookies } from "../../../lib/supabase-server-auth";
import { isUuidString } from "../../../lib/uuid";

export type PlatformAccessDenied = "not_authenticated" | "forbidden";

/**
 * Session user must be signed in and have a catalog role that may use `/platform/access`
 * (`super_admin`, `programmer`, `system_admin` — canonical keys).
 */
export async function assertManagePlatformAccess(): Promise<
  { ok: true; actorProfileId: string } | { ok: false; denied: PlatformAccessDenied }
> {
  const sid = await getSessionUserIdFromCookies();
  if (!sid || !isUuidString(sid)) return { ok: false, denied: "not_authenticated" };
  const profile = await loadTenantProfile(sid);
  if (!profile) return { ok: false, denied: "not_authenticated" };
  if (!canManagePlatformAccessCatalog(profile.role)) {
    return { ok: false, denied: "forbidden" };
  }
  return { ok: true, actorProfileId: sid };
}
