"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { getSessionUserIdFromCookies } from "../../lib/supabase-server-auth";
import { loadTenantProfile } from "../../lib/server-tenant";
import {
  canPickWorkspaceOrganizationForTenantBranding,
  normalizeRoleKeyForBranding,
} from "../../lib/tenant-branding-permissions";
import { isUuidString } from "../../lib/uuid";

export type ViewAsProfileRow = {
  profile_id: string;
  full_name: string;
  role_key: string;
};

export type ViewAsProfileSnapshot = {
  profile_id: string;
  full_name: string;
  canonical_role_key: string;
  role_label: string;
  team_groups: string[];
};

function splitJoined<T>(raw: unknown): T | null {
  if (raw == null) return null;
  if (Array.isArray(raw)) return (raw[0] as T | undefined) ?? null;
  return raw as T;
}

function titleCaseRoleKey(key: string): string {
  const k = key.trim().toLowerCase();
  if (!k) return "User";
  return k
    .split("_")
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

async function requireViewAsCaller(): Promise<
  { ok: true; sessionUserId: string } | { ok: false; error: string }
> {
  const sessionUserId = await getSessionUserIdFromCookies();
  if (!sessionUserId || !isUuidString(sessionUserId)) {
    return { ok: false, error: "Not signed in." };
  }
  const profile = await loadTenantProfile(sessionUserId);
  if (!profile) {
    return { ok: false, error: "Profile not found." };
  }
  if (!canPickWorkspaceOrganizationForTenantBranding(profile.role)) {
    return { ok: false, error: "Forbidden." };
  }
  return { ok: true, sessionUserId };
}

function resolveRowRoleKey(row: Record<string, unknown>): string {
  const joined = splitJoined<{ key?: string | null }>(row.roles);
  const fromJoin = joined?.key != null ? String(joined.key).trim() : "";
  const fromCol = typeof row.role === "string" ? row.role.trim() : "";
  return normalizeRoleKeyForBranding(fromJoin || fromCol || null);
}

/**
 * Members of an organization for "view as" simulation (super_admin / programmer / system_admin only).
 */
export async function listViewAsProfilesForOrganization(
  organizationId: string | null | undefined,
): Promise<
  { ok: true; rows: ViewAsProfileRow[] } | { ok: false; error: string }
> {
  const gate = await requireViewAsCaller();
  if (!gate.ok) return { ok: false, error: gate.error };

  const oid = String(organizationId ?? "").trim();
  if (!isUuidString(oid)) {
    return { ok: false, error: "Invalid organization." };
  }

  const { data, error } = await supabaseServer
    .from("profiles")
    .select(
      [
        "id, full_name, role, role_id, team_groups",
        "roles!profiles_role_id_fkey(key, name)",
      ].join(", "),
    )
    .eq("organization_id", oid)
    .order("full_name", { ascending: true })
    .limit(10_000);

  if (error) {
    return { ok: false, error: error.message };
  }

  const rows: ViewAsProfileRow[] = [];
  for (const raw of data ?? []) {
    const row = raw as unknown as Record<string, unknown>;
    const id = String(row.id ?? "").trim();
    if (!isUuidString(id)) continue;
    const fullName = String(row.full_name ?? "").trim() || id.slice(0, 8) + "…";
    const role_key = resolveRowRoleKey(row);
    rows.push({ profile_id: id, full_name: fullName, role_key });
  }

  return { ok: true, rows };
}

/**
 * Snapshot for RBAC simulation; ensures target belongs to the workspace org.
 */
export async function getViewAsProfileSnapshot(
  profileId: string | null | undefined,
  organizationId: string | null | undefined,
): Promise<
  { ok: true; snapshot: ViewAsProfileSnapshot } | { ok: false; error: string }
> {
  const gate = await requireViewAsCaller();
  if (!gate.ok) return { ok: false, error: gate.error };

  const pid = String(profileId ?? "").trim();
  const oid = String(organizationId ?? "").trim();
  if (!isUuidString(pid) || !isUuidString(oid)) {
    return { ok: false, error: "Invalid id." };
  }
  if (pid === gate.sessionUserId) {
    return { ok: false, error: "Cannot view as yourself." };
  }

  const { data, error } = await supabaseServer
    .from("profiles")
    .select(
      [
        "id, full_name, organization_id, role, role_id, team_groups",
        "roles!profiles_role_id_fkey(key, name)",
      ].join(", "),
    )
    .eq("id", pid)
    .maybeSingle();

  if (error) return { ok: false, error: error.message };
  if (!data) return { ok: false, error: "Profile not found." };

  const row = data as unknown as Record<string, unknown>;
  const org = String(row.organization_id ?? "").trim();
  if (org !== oid) {
    return { ok: false, error: "User is not a member of this workspace organization." };
  }

  const joined = splitJoined<{ key?: string | null; name?: string | null }>(row.roles);
  const canonical_role_key = resolveRowRoleKey(row);
  const catalogName =
    joined?.name != null && String(joined.name).trim().length > 0
      ? String(joined.name).trim()
      : titleCaseRoleKey(canonical_role_key);

  const rawGroups = row.team_groups;
  const team_groups: string[] = Array.isArray(rawGroups)
    ? rawGroups.map(String).filter(Boolean)
    : [];

  return {
    ok: true,
    snapshot: {
      profile_id: pid,
      full_name: String(row.full_name ?? "").trim() || "User",
      canonical_role_key,
      role_label: catalogName,
      team_groups,
    },
  };
}
