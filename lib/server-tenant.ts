import "server-only";

import { supabaseServer } from "./supabase-server";
import { getSessionUserIdFromCookies } from "./supabase-server-auth";
import { resolveOrganizationId } from "./organization";
import { isUuidString } from "./uuid";

/**
 * Workspace user directory (`public.profiles`).
 * Roles (5-tier RBAC): super_admin | system_employee | admin | employee | operator
 */
export type TenantProfileRow = {
  id: string;
  organization_id: string;
  full_name: string;
  role: string;
  photo_url: string | null;
  /** Email is stored in auth.users, not profiles — may be absent on legacy rows. */
  email?: string;
  /**
   * GBAC foundation — JSONB array of group/team slugs the user belongs to.
   * Used for future object-level access checks (e.g. ["warehouse-a", "returns-team"]).
   * Gracefully empty when the column is not yet populated.
   */
  team_groups: string[];
};

export type TenantListScope =
  | { mode: "all" }
  | { mode: "single"; organizationId: string };

export type TenantQueryOpts = {
  /** `profiles.id` from localStorage / client — resolves role and home org server-side */
  actorProfileId: string | null | undefined;
  /**
   * Super Admin only: when set, restrict lists to this organization (`organization_id`).
   * When null/undefined and role is super_admin → all organizations (no tenant filter).
   */
  filterOrganizationId?: string | null;
  /** @deprecated Use filterOrganizationId */
  filterCompanyId?: string | null;
};

/** Passed from client Server Actions: profile id + optional tenant override (super_admin). */
export type TenantWriteContext = {
  actorProfileId?: string | null;
  organizationId?: string | null;
};

export async function resolveTenantOrganizationId(
  ctx?: TenantWriteContext | null,
): Promise<string> {
  return resolveWriteOrganizationId(ctx?.actorProfileId, ctx?.organizationId);
}

/** @deprecated Use resolveTenantOrganizationId */
export const resolveTenantCompanyId = resolveTenantOrganizationId;

const DEFAULT_ORG = resolveOrganizationId();

export async function loadTenantProfile(
  actorProfileId: string | null | undefined,
): Promise<TenantProfileRow | null> {
  const id = actorProfileId?.trim();
  if (!id || !isUuidString(id)) return null;
  const { data, error } = await supabaseServer
    .from("profiles")
    .select("id, organization_id, full_name, name, role, photo_url, team_groups")
    .eq("id", id)
    .maybeSingle();
  if (error || !data) return null;
  const row = data as Record<string, unknown>;
  const oid = row.organization_id;
  const rawGroups = row.team_groups;
  const team_groups: string[] = Array.isArray(rawGroups)
    ? rawGroups.map(String).filter(Boolean)
    : [];
  return {
    id: String(row.id),
    organization_id: typeof oid === "string" && oid.trim() ? oid : "",
    full_name: String(row.full_name ?? row.name ?? "").trim(),
    role: String(row.role ?? "operator"),
    photo_url: row.photo_url != null ? String(row.photo_url) : null,
    team_groups,
  };
}

export function isSuperAdminRole(role: string | null | undefined): boolean {
  return (role ?? "").trim() === "super_admin";
}

/**
 * Resolves list queries: non–super-admins always scope to their profile organization (ignores client filters).
 * Super Admins: all rows unless `filterOrganizationId` is set (then that tenant only).
 * Legacy: no `actorProfileId` → use `filterOrganizationId` if valid, else env default org.
 */
export async function resolveTenantListScope(
  opts: TenantQueryOpts | undefined,
): Promise<TenantListScope> {
  const forced =
    opts?.filterOrganizationId?.trim() ?? opts?.filterCompanyId?.trim();
  const forcedOk = forced && isUuidString(forced) ? forced : null;

  if (!opts?.actorProfileId) {
    if (forcedOk) return { mode: "single", organizationId: forcedOk };
    return { mode: "all" };
  }
  const profile = await loadTenantProfile(opts.actorProfileId);
  if (!profile) {
    if (forcedOk) return { mode: "single", organizationId: forcedOk };
    return { mode: "all" };
  }
  if (isSuperAdminRole(profile.role)) {
    if (forcedOk) return { mode: "single", organizationId: forcedOk };
    return { mode: "all" };
  }
  return { mode: "single", organizationId: profile.organization_id };
}

/**
 * Writes: non–super-admins always use their profile `organization_id`.
 * Super Admins may set `requestedOrganizationId` to create data for a chosen tenant.
 *
 * Resolution order:
 *   1. Profile looked up by `actorProfileId` (client-supplied, safest).
 *   2. Profile looked up by auth-session user id from HTTP cookies (server-side).
 *   3. Explicitly supplied `requestedOrganizationId` (super_admin override).
 *   4. Env-var / fallback organization id (single-tenant deployments only).
 */
export async function resolveWriteOrganizationId(
  actorProfileId: string | null | undefined,
  requestedOrganizationId: string | null | undefined,
): Promise<string> {
  const req = requestedOrganizationId?.trim();
  const reqOk = req && isUuidString(req) ? req : null;

  // 1. Resolve via explicitly-passed profile id.
  const profile = await loadTenantProfile(actorProfileId);
  if (profile) {
    if (isSuperAdminRole(profile.role)) {
      return reqOk ?? profile.organization_id;
    }
    return profile.organization_id;
  }

  // 2. No client-supplied profile id — fall back to the Supabase auth session
  //    stored in HTTP cookies (covers the case where actor_profile_id was not
  //    forwarded from the UI but the user IS authenticated).
  try {
    const sessionUserId = await getSessionUserIdFromCookies();
    if (sessionUserId) {
      const sessionProfile = await loadTenantProfile(sessionUserId);
      if (sessionProfile) {
        if (isSuperAdminRole(sessionProfile.role)) {
          return reqOk ?? sessionProfile.organization_id;
        }
        return sessionProfile.organization_id;
      }
    }
  } catch {
    // Cookies may not be accessible in all Server Action contexts — continue.
  }

  // 3. Fall through to the requested org id or env-var default.
  //    This path is acceptable for single-tenant / seed deployments where the
  //    default organization row is guaranteed to exist in `organizations`.
  return reqOk ?? resolveOrganizationId();
}

/**
 * Ensures a row’s `organization_id` is visible to the actor (for updates targeting an existing row).
 */
export async function assertRowOrgAccess(
  actorProfileId: string | null | undefined,
  rowOrganizationId: string | null | undefined,
): Promise<void> {
  const scope = await resolveTenantListScope({ actorProfileId, filterOrganizationId: null });
  if (scope.mode === "all") return;
  const cid = (rowOrganizationId ?? "").trim();
  if (!cid || !isUuidString(cid)) {
    throw new Error("Invalid organization on record.");
  }
  if (cid !== scope.organizationId) {
    throw new Error("Forbidden: record belongs to another organization.");
  }
}
