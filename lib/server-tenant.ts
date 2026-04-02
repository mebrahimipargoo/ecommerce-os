import "server-only";

import { supabaseServer } from "./supabase-server";
import { resolveOrganizationId } from "./organization";
import { isUuidString } from "./uuid";

/** Workspace user directory (`public.profiles`). Roles: super_admin | admin | operator */
export type TenantProfileRow = {
  id: string;
  company_id: string;
  full_name: string;
  role: string;
  photo_url: string | null;
  /** Email is stored in auth.users, not profiles — may be absent on legacy rows. */
  email?: string;
};

export type TenantListScope =
  | { mode: "all" }
  | { mode: "single"; organizationId: string };

export type TenantQueryOpts = {
  /** `profiles.id` from localStorage / client — resolves role and home org server-side */
  actorProfileId: string | null | undefined;
  /**
   * Super Admin only: when set, restrict lists to this org.
   * When null/undefined and role is super_admin → all organizations (no org filter).
   */
  filterOrganizationId?: string | null;
};

const DEFAULT_ORG = resolveOrganizationId();

export async function loadTenantProfile(
  actorProfileId: string | null | undefined,
): Promise<TenantProfileRow | null> {
  const id = actorProfileId?.trim();
  if (!id || !isUuidString(id)) return null;
  const { data, error } = await supabaseServer
    .from("profiles")
    .select("id, company_id, full_name, name, role, photo_url")
    .eq("id", id)
    .maybeSingle();
  if (error || !data) return null;
  const row = data as Record<string, unknown>;
  const cid = row.company_id;
  return {
    id: String(row.id),
    company_id: typeof cid === "string" && cid.trim() ? cid : "",
    full_name: String(row.full_name ?? row.name ?? "").trim(),
    role: String(row.role ?? "operator"),
    photo_url: row.photo_url != null ? String(row.photo_url) : null,
  };
}

export function isSuperAdminRole(role: string | null | undefined): boolean {
  return (role ?? "").trim() === "super_admin";
}

/**
 * Resolves list queries: non–super-admins always scope to their profile org (ignores client org filters).
 * Super Admins: all rows unless `filterOrganizationId` is set (then that org only).
 * Legacy: no `actorProfileId` → use `filterOrganizationId` if valid, else env default org.
 */
export async function resolveTenantListScope(
  opts: TenantQueryOpts | undefined,
): Promise<TenantListScope> {
  const forced = opts?.filterOrganizationId?.trim();
  const forcedOk = forced && isUuidString(forced) ? forced : null;

  if (!opts?.actorProfileId) {
    return { mode: "single", organizationId: forcedOk ?? DEFAULT_ORG };
  }
  const profile = await loadTenantProfile(opts.actorProfileId);
  if (!profile) {
    return { mode: "single", organizationId: forcedOk ?? DEFAULT_ORG };
  }
  if (isSuperAdminRole(profile.role)) {
    if (forcedOk) return { mode: "single", organizationId: forcedOk };
    return { mode: "all" };
  }
  return { mode: "single", organizationId: profile.company_id };
}

/**
 * Writes: non–super-admins always use their profile organization.
 * Super Admins may set `requestedOrganizationId` to create data for a chosen tenant.
 */
export async function resolveWriteOrganizationId(
  actorProfileId: string | null | undefined,
  requestedOrganizationId: string | null | undefined,
): Promise<string> {
  const profile = await loadTenantProfile(actorProfileId);
  const req = requestedOrganizationId?.trim();
  const reqOk = req && isUuidString(req) ? req : null;
  if (!profile) {
    return reqOk ?? resolveOrganizationId();
  }
  if (isSuperAdminRole(profile.role)) {
    return reqOk ?? profile.company_id;
  }
  return profile.company_id;
}

/**
 * Ensures a row’s `company_id` is visible to the actor (for updates targeting an existing row).
 */
export async function assertRowOrgAccess(
  actorProfileId: string | null | undefined,
  rowOrganizationId: string | null | undefined,
): Promise<void> {
  const scope = await resolveTenantListScope({ actorProfileId, filterOrganizationId: null });
  if (scope.mode === "all") return;
  const oid = (rowOrganizationId ?? "").trim();
  if (!oid || !isUuidString(oid)) {
    throw new Error("Invalid organization on record.");
  }
  if (oid !== scope.organizationId) {
    throw new Error("Forbidden: record belongs to another organization.");
  }
}
