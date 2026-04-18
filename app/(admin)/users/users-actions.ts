"use server";

import { randomBytes } from "crypto";
import { supabaseServer } from "../../../lib/supabase-server";
import { getSessionUserIdFromCookies } from "../../../lib/supabase-server-auth";
import { resolveOrganizationId } from "../../../lib/organization";
import {
  isSuperAdminRole,
  loadTenantProfile,
  resolveTenantListScope,
} from "../../../lib/server-tenant";
import { isUuidString } from "../../../lib/uuid";
import type { OrgGroupRow, ProfileRow, UserGroupAssignment } from "./users-types";

/**
 * Profile fields — `profiles.organization_id` references `public.organizations(id)`.
 * Company labels are resolved in `fetchOrgNameMap` (settings override + `organizations.name`).
 */
const PROFILE_LIST_SELECT =
  "id, organization_id, full_name, role, photo_url, created_at, updated_at, roles!profiles_role_id_fkey(key, name, scope)";

function splitJoined<T>(raw: unknown): T | null {
  if (raw == null) return null;
  if (Array.isArray(raw)) return (raw[0] as T | undefined) ?? null;
  return raw as T;
}

export type AssignableRoleRow = {
  id: string;
  key: string;
  name: string;
  description: string | null;
  scope: "system" | "tenant";
};

/**
 * Roles that may be assigned from the Users admin UI (catalog-driven).
 */
export async function listAssignableRolesForUsers(): Promise<
  { ok: true; rows: AssignableRoleRow[] } | { ok: false; error: string }
> {
  try {
    const { data, error } = await supabaseServer
      .from("roles")
      .select("id, key, name, description, scope")
      .eq("is_assignable", true)
      .order("scope", { ascending: true })
      .order("name", { ascending: true });
    if (error) return { ok: false, error: error.message };
    const rows: AssignableRoleRow[] = (data ?? []).map((raw) => {
      const r = raw as Record<string, unknown>;
      const scope = r.scope === "system" || r.scope === "tenant" ? r.scope : "tenant";
      return {
        id: String(r.id ?? ""),
        key: String(r.key ?? "").trim().toLowerCase(),
        name: String(r.name ?? "").trim() || String(r.key ?? ""),
        description: r.description != null ? String(r.description) : null,
        scope,
      };
    });
    return { ok: true, rows: rows.filter((x) => x.id && x.key) };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load roles." };
  }
}

/** Maps UI / joined role key to `profiles.role` text (must satisfy DB CHECK constraint). */
function profileRoleTextForStorage(roleKey: string): string {
  const k = roleKey.trim().toLowerCase();
  if (k === "tenant_admin") return "tenant_admin";
  if (k === "admin") return "tenant_admin";
  return k;
}

async function resolveRoleIdByKey(roleKey: string): Promise<string | null> {
  const key = roleKey.trim().toLowerCase();
  if (!key) return null;
  const { data, error } = await supabaseServer
    .from("roles")
    .select("id")
    .eq("key", key)
    .maybeSingle();
  if (error || !data?.id) return null;
  return String(data.id);
}

async function assertAssignableRoleKey(roleKey: string): Promise<boolean> {
  const key = roleKey.trim().toLowerCase();
  if (!key) return false;
  const { data, error } = await supabaseServer
    .from("roles")
    .select("id")
    .eq("key", key)
    .eq("is_assignable", true)
    .maybeSingle();
  return !error && !!data?.id;
}

/**
 * Fetches organization_id → display label: `organization_settings.company_display_name`
 * when set, otherwise `organizations.name`, otherwise the raw UUID.
 */
async function fetchOrgNameMap(orgIds: string[]): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  if (orgIds.length === 0) return map;
  try {
    const unique = [...new Set(orgIds.filter((id) => id && isUuidString(id)))];
    if (unique.length === 0) return map;

    const [settingsRes, orgsRes] = await Promise.all([
      supabaseServer
        .from("organization_settings")
        .select("organization_id, company_display_name")
        .in("organization_id", unique),
      supabaseServer
        .from("organizations")
        .select("id, name")
        .in("id", unique),
    ]);

    const orgNames: Record<string, string> = {};
    for (const o of orgsRes.data ?? []) {
      const id = String((o as { id?: unknown }).id ?? "").trim();
      const name = (o as { name?: unknown }).name;
      if (id && typeof name === "string" && name.trim()) {
        orgNames[id] = name.trim();
      }
    }

    for (const row of settingsRes.data ?? []) {
      const r = row as Record<string, unknown>;
      const oid = String(r.organization_id ?? "").trim();
      if (!oid) continue;
      const custom =
        typeof r.company_display_name === "string" && r.company_display_name.trim()
          ? r.company_display_name.trim()
          : "";
      map.set(oid, custom || orgNames[oid] || oid);
    }

    for (const id of unique) {
      if (!map.has(id)) {
        map.set(id, orgNames[id] || id);
      }
    }
  } catch {
    /* non-fatal — company_name will show as null */
  }
  return map;
}

/** Workspace tenant UUID (env / org) — use as default company when adding users. */
export async function getTenantCompanyIdForUsersPage(): Promise<string> {
  return resolveOrganizationId();
}

async function emailByUserIdMap(): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  try {
    const { data, error } = await supabaseServer.auth.admin.listUsers({ perPage: 1000 });
    if (error || !data?.users) return map;
    for (const u of data.users) {
      const e = u.email?.trim();
      if (e) map.set(u.id, e);
    }
  } catch {
    /* ignore */
  }
  return map;
}

function inferRoleScope(
  roleKey: string,
  joinedScope: string | null | undefined,
): "system" | "tenant" | null {
  if (joinedScope === "system" || joinedScope === "tenant") return joinedScope;
  const k = roleKey.trim().toLowerCase();
  if (
    k === "super_admin"
    || k === "system_admin"
    || k === "system_employee"
    || k === "programmer"
    || k === "customer_service"
  ) {
    return "system";
  }
  if (
    k === "tenant_admin"
    || k === "employee"
    || k === "operator"
    || k === "admin"
  ) {
    return "tenant";
  }
  return null;
}

type UsersActionCtx = {
  actorProfileId?: string | null;
  filterOrganizationId?: string | null;
};

async function resolveActorProfileId(ctx?: UsersActionCtx | null): Promise<string | null> {
  let actorId = ctx?.actorProfileId?.trim() ?? null;
  if (!actorId || !isUuidString(actorId)) {
    const fromSession = await getSessionUserIdFromCookies();
    actorId = fromSession && isUuidString(fromSession) ? fromSession : null;
  }
  return actorId;
}

/**
 * Groups for `public.groups.organization_id`, when the actor may manage that organization.
 * - Tenant admins: `organizationId` must match their tenant list scope.
 * - Super admins: any organization (for multi-org user directory); tenant path still enforced elsewhere.
 */
export async function listGroupsForOrganization(
  organizationId: string,
  ctx?: UsersActionCtx | null,
): Promise<{ ok: true; rows: OrgGroupRow[] } | { ok: false; error: string }> {
  const oid = organizationId.trim();
  if (!isUuidString(oid)) return { ok: false, error: "Invalid organization." };
  const actorId = await resolveActorProfileId(ctx);
  if (!actorId) return { ok: false, error: "Not authenticated." };

  const scope = await resolveTenantListScope({
    actorProfileId: actorId,
    filterOrganizationId: ctx?.filterOrganizationId?.trim() ?? null,
  });
  if (scope.mode === "single" && oid !== scope.organizationId) {
    return { ok: false, error: "Forbidden: organization mismatch." };
  }
  if (scope.mode === "all") {
    const actorProfile = await loadTenantProfile(actorId);
    if (!actorProfile || !isSuperAdminRole(actorProfile.role)) {
      return { ok: false, error: "Forbidden." };
    }
  }

  try {
    const { data, error } = await supabaseServer
      .from("groups")
      .select("id, key, name, description")
      .eq("organization_id", oid)
      .order("name", { ascending: true });
    if (error) return { ok: false, error: error.message };
    const rows: OrgGroupRow[] = (data ?? []).map((raw) => {
      const r = raw as Record<string, unknown>;
      return {
        id: String(r.id ?? ""),
        key: String(r.key ?? "").trim(),
        name: String(r.name ?? "").trim() || String(r.key ?? ""),
        description: r.description != null ? String(r.description) : null,
      };
    });
    return { ok: true, rows: rows.filter((x) => x.id && x.key) };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load groups." };
  }
}

/** Catalog for the effective single-tenant workspace (empty when list scope is `all`). */
export async function listOrganizationGroupsForUsers(
  ctx?: UsersActionCtx | null,
): Promise<{ ok: true; rows: OrgGroupRow[] } | { ok: false; error: string }> {
  const actorId = await resolveActorProfileId(ctx);
  if (!actorId) return { ok: false, error: "Not authenticated." };
  const scope = await resolveTenantListScope({
    actorProfileId: actorId,
    filterOrganizationId: ctx?.filterOrganizationId?.trim() ?? null,
  });
  if (scope.mode !== "single") {
    return { ok: true, rows: [] };
  }
  return listGroupsForOrganization(scope.organizationId, ctx);
}

export async function listUserGroupAssignmentsForProfiles(
  profileIds: string[],
  ctx?: UsersActionCtx | null,
): Promise<
  { ok: true; byProfileId: Record<string, UserGroupAssignment[]> } | { ok: false; error: string }
> {
  const actorId = await resolveActorProfileId(ctx);
  if (!actorId) return { ok: false, error: "Not authenticated." };

  const ids = [...new Set(profileIds.map((x) => x.trim()).filter((x) => isUuidString(x)))];
  if (ids.length === 0) {
    return { ok: true, byProfileId: {} };
  }

  const scope = await resolveTenantListScope({
    actorProfileId: actorId,
    filterOrganizationId: ctx?.filterOrganizationId?.trim() ?? null,
  });

  try {
    const { data: profileRows, error: pe } = await supabaseServer
      .from("profiles")
      .select("id, organization_id")
      .in("id", ids);
    if (pe) return { ok: false, error: pe.message };
    const profileOrg = new Map<string, string>();
    for (const p of profileRows ?? []) {
      const r = p as Record<string, unknown>;
      const id = String(r.id ?? "").trim();
      const oid = r.organization_id != null ? String(r.organization_id).trim() : "";
      if (id && oid && isUuidString(oid)) profileOrg.set(id, oid);
    }

    const { data, error } = await supabaseServer
      .from("user_groups")
      .select("id, profile_id, group_id, groups(id, organization_id, key, name)")
      .in("profile_id", ids);
    if (error) return { ok: false, error: error.message };

    const byProfileId: Record<string, UserGroupAssignment[]> = {};
    for (const id of ids) byProfileId[id] = [];

    for (const raw of data ?? []) {
      const row = raw as Record<string, unknown>;
      const profileId = String(row.profile_id ?? "").trim();
      const ugId = String(row.id ?? "").trim();
      const groupId = String(row.group_id ?? "").trim();
      const gJoin = splitJoined<{
        id?: unknown;
        organization_id?: unknown;
        key?: unknown;
        name?: unknown;
      }>(row.groups);
      const gOrg =
        gJoin?.organization_id != null ? String(gJoin.organization_id).trim() : "";
      const pOrg = profileOrg.get(profileId) ?? "";
      if (!ugId || !groupId || !profileId || !gOrg || pOrg !== gOrg) continue;
      if (scope.mode === "single" && gOrg !== scope.organizationId) continue;

      const key = String(gJoin?.key ?? "").trim();
      const name =
        gJoin?.name != null && String(gJoin.name).trim()
          ? String(gJoin.name).trim()
          : key;
      if (!key) continue;
      const list = byProfileId[profileId] ?? (byProfileId[profileId] = []);
      list.push({
        user_group_id: ugId,
        group_id: groupId,
        key,
        name,
      });
    }

    return { ok: true, byProfileId };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load group assignments." };
  }
}

async function assertUserGroupMutationAllowed(
  profileId: string,
  groupId: string,
  ctx?: UsersActionCtx | null,
): Promise<{ ok: true; profileOrg: string; groupOrg: string } | { ok: false; error: string }> {
  const actorId = await resolveActorProfileId(ctx);
  if (!actorId) return { ok: false, error: "Not authenticated." };
  if (!isUuidString(profileId) || !isUuidString(groupId)) {
    return { ok: false, error: "Invalid profile or group id." };
  }

  const [profileRes, groupRes] = await Promise.all([
    supabaseServer.from("profiles").select("organization_id").eq("id", profileId).maybeSingle(),
    supabaseServer.from("groups").select("organization_id").eq("id", groupId).maybeSingle(),
  ]);
  if (profileRes.error) return { ok: false, error: profileRes.error.message };
  if (groupRes.error) return { ok: false, error: groupRes.error.message };
  const pOrgRaw = profileRes.data?.organization_id;
  const gOrgRaw = groupRes.data?.organization_id;
  const profileOrg = pOrgRaw != null ? String(pOrgRaw).trim() : "";
  const groupOrg = gOrgRaw != null ? String(gOrgRaw).trim() : "";
  if (!profileOrg || !groupOrg || !isUuidString(profileOrg) || !isUuidString(groupOrg)) {
    return { ok: false, error: "Profile or group not found." };
  }
  if (profileOrg !== groupOrg) {
    return { ok: false, error: "Group belongs to a different organization than this user." };
  }

  const scope = await resolveTenantListScope({
    actorProfileId: actorId,
    filterOrganizationId: ctx?.filterOrganizationId?.trim() ?? null,
  });
  if (scope.mode === "single" && groupOrg !== scope.organizationId) {
    return { ok: false, error: "Forbidden: outside your organization." };
  }
  if (scope.mode === "all") {
    const actorProfile = await loadTenantProfile(actorId);
    if (!actorProfile || !isSuperAdminRole(actorProfile.role)) {
      return { ok: false, error: "Forbidden." };
    }
  }

  return { ok: true, profileOrg, groupOrg };
}

export async function assignUserGroup(
  profileId: string,
  groupId: string,
  ctx?: UsersActionCtx | null,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const gate = await assertUserGroupMutationAllowed(profileId, groupId, ctx);
  if (!gate.ok) return gate;

  try {
    const { error } = await supabaseServer.from("user_groups").insert({
      profile_id: profileId,
      group_id: groupId,
    });
    if (error) {
      if (error.code === "23505") {
        return { ok: true };
      }
      return { ok: false, error: error.message };
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Assign failed." };
  }
}

export async function removeUserGroup(
  profileId: string,
  groupId: string,
  ctx?: UsersActionCtx | null,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const gate = await assertUserGroupMutationAllowed(profileId, groupId, ctx);
  if (!gate.ok) return gate;

  try {
    const { error } = await supabaseServer
      .from("user_groups")
      .delete()
      .eq("profile_id", profileId)
      .eq("group_id", groupId);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Remove failed." };
  }
}

export async function listUserProfiles(ctx?: {
  actorProfileId?: string | null;
  /** Effective tenant (super_admin workspace); ignored for non–super-admins server-side. */
  filterOrganizationId?: string | null;
} | null): Promise<
  { ok: true; rows: ProfileRow[] } | { ok: false; error: string }
> {
  try {
    let actorId = ctx?.actorProfileId?.trim() ?? null;
    if (!actorId || !isUuidString(actorId)) {
      const fromSession = await getSessionUserIdFromCookies();
      actorId = fromSession && isUuidString(fromSession) ? fromSession : null;
    }
    if (!actorId) {
      return { ok: false, error: "Not authenticated." };
    }

    const scope = await resolveTenantListScope({
      actorProfileId: actorId,
      filterOrganizationId: ctx?.filterOrganizationId?.trim() ?? null,
    });

    let query = supabaseServer
      .from("profiles")
      .select(PROFILE_LIST_SELECT)
      .order("full_name", { ascending: true });

    if (scope.mode === "single") {
      const oid = String(scope.organizationId ?? "").trim();
      if (!oid || !isUuidString(oid)) {
        return {
          ok: false,
          error:
            "Your profile has no valid organization — assign `profiles.organization_id` or pick a workspace company.",
        };
      }
      query = query.eq("organization_id", oid);
    }

    const { data, error } = await query;
    if (error) return { ok: false, error: error.message };

    const rawRows = (data ?? []) as Record<string, unknown>[];

    /** Resolve display email: `auth.users` is source of truth (profiles may not store email). */
    const authEmails = await emailByUserIdMap();

    /** Resolve company display names from `organization_settings` (Rule 5 tenant table). */
    const distinctOrgIds = [
      ...new Set(
        rawRows
          .map((r) => (r.organization_id != null ? String(r.organization_id).trim() : ""))
          .filter(Boolean),
      ),
    ];
    const orgNameMap = await fetchOrgNameMap(distinctOrgIds);

    const rows: ProfileRow[] = rawRows.map((r) => {
      const id = String(r.id ?? "");
      const fromAuth = authEmails.get(id)?.trim() ?? "";
      const fn = r.full_name != null ? String(r.full_name).trim() : "";
      const displayName = fn.length > 0 ? fn : null;
      const oid = r.organization_id != null ? String(r.organization_id).trim() : null;
      const companyName = oid ? (orgNameMap.get(oid) ?? null) : null;
      const join = splitJoined<{ key?: string | null; name?: string | null; scope?: string | null }>(r.roles);
      const resolvedKey = String(
        (join?.key ?? r.role ?? ""),
      ).trim().toLowerCase() || null;
      const roleLabel =
        join?.name != null && String(join.name).trim().length > 0
          ? String(join.name).trim()
          : null;
      const scopeKey =
        resolvedKey === "admin"
          ? "tenant_admin"
          : resolvedKey;
      return {
        id,
        organization_id: oid,
        company_name: companyName,
        full_name: displayName,
        email: fromAuth,
        role: resolvedKey,
        role_display_name: roleLabel,
        role_scope: inferRoleScope(scopeKey ?? "", join?.scope),
        photo_url: r.photo_url != null ? String(r.photo_url) : null,
        created_at: String(r.created_at ?? ""),
        updated_at: r.updated_at != null ? String(r.updated_at) : null,
      };
    });

    return { ok: true, rows };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load users." };
  }
}

export async function createUserProfile(input: {
  full_name: string;
  email: string;
  role: string;
  /** Target tenant — stored on `profiles.organization_id`. */
  organization_id: string;
}): Promise<{ ok: true; id: string } | { ok: false; error: string }> {
  const email = input.email.trim().toLowerCase();
  if (!email) return { ok: false, error: "Email is required." };
  const cid = input.organization_id.trim();
  if (!isUuidString(cid)) return { ok: false, error: "Select a valid company." };
  const fullName = input.full_name.trim();
  const roleKey = input.role.trim().toLowerCase();
  const okKey = await assertAssignableRoleKey(roleKey);
  if (!okKey) return { ok: false, error: "Invalid or non-assignable role." };
  try {
    const tempPassword = `${randomBytes(24).toString("base64url")}Aa1!`;
    const { data: authCreated, error: authErr } = await supabaseServer.auth.admin.createUser({
      email,
      password: tempPassword,
      email_confirm: true,
      user_metadata: { full_name: fullName },
    });
    if (authErr || !authCreated.user?.id) {
      return { ok: false, error: authErr?.message ?? "Could not create auth user for this email." };
    }
    const uid = authCreated.user.id;
    const roleId = await resolveRoleIdByKey(roleKey);
    const roleText = profileRoleTextForStorage(roleKey);
    const { data, error } = await supabaseServer
      .from("profiles")
      .insert({
        id: uid,
        organization_id: cid,
        full_name: fullName,
        role_id: roleId,
        role: roleText,
      })
      .select("id")
      .single();
    if (error) {
      await supabaseServer.auth.admin.deleteUser(uid);
      return { ok: false, error: error.message };
    }
    if (!data?.id) {
      await supabaseServer.auth.admin.deleteUser(uid);
      return { ok: false, error: "Insert failed." };
    }
    return { ok: true, id: data.id as string };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Create failed." };
  }
}

export async function updateUserProfile(
  id: string,
  patch: { full_name?: string; role?: string; photo_url?: string | null },
): Promise<{ ok: boolean; error?: string }> {
  if (!isUuidString(id)) return { ok: false, error: "Invalid user id." };
  const row: Record<string, unknown> = {
    updated_at: new Date().toISOString(),
  };
  if (typeof patch.full_name === "string") row.full_name = patch.full_name.trim();
  if (typeof patch.role === "string") {
    const roleKey = patch.role.trim().toLowerCase();
    const okKey = await assertAssignableRoleKey(roleKey);
    if (!okKey) return { ok: false, error: "Invalid or non-assignable role." };
    row.role = profileRoleTextForStorage(roleKey);
    const roleId = await resolveRoleIdByKey(roleKey);
    if (roleId) row.role_id = roleId;
  }
  if ("photo_url" in patch) row.photo_url = patch.photo_url;
  try {
    const { error } = await supabaseServer.from("profiles").update(row).eq("id", id);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

export async function deleteUserProfile(id: string): Promise<{ ok: boolean; error?: string }> {
  if (!isUuidString(id)) return { ok: false, error: "Invalid user id." };
  try {
    const { error } = await supabaseServer.from("profiles").delete().eq("id", id);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Delete failed." };
  }
}
