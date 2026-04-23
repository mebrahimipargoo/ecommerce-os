"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { isUuidString } from "../../../lib/uuid";
import {
  collectGroupCreateErrors,
  collectGroupUpdateErrors,
  collectRoleCreateErrors,
  collectRoleUpdateErrors,
  normalizeAccessEntityKey,
} from "./access-validation";
import { assertManagePlatformAccess } from "./server-gate";

function splitJoined<T>(raw: unknown): T | null {
  if (raw == null) return null;
  if (Array.isArray(raw)) return (raw[0] as T | undefined) ?? null;
  return raw as T;
}

export type PlatformAccessPageAccess = {
  accessDenied: "not_authenticated" | "forbidden" | null;
};

async function gate(): Promise<
  { ok: true } | { ok: false; denied: "not_authenticated" | "forbidden" }
> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return g;
  return { ok: true };
}

export async function getPlatformAccessPageAccessAction(): Promise<PlatformAccessPageAccess> {
  const g = await gate();
  if (!g.ok) return { accessDenied: g.denied };
  return { accessDenied: null };
}

export type RoleCatalogRow = {
  id: string;
  key: string;
  name: string;
  description: string | null;
  scope: "system" | "tenant";
  is_system: boolean;
  is_assignable: boolean;
  created_at: string;
};

export async function listRolesCatalogAction(): Promise<
  { ok: true; rows: RoleCatalogRow[] } | { ok: false; error: string }
> {
  const g = await gate();
  if (!g.ok) {
    return { ok: false, error: g.denied === "not_authenticated" ? "Not authenticated." : "Forbidden." };
  }
  try {
    const { data, error } = await supabaseServer
      .from("roles")
      .select("id, key, name, description, scope, is_system, is_assignable, created_at")
      .order("scope", { ascending: true })
      .order("name", { ascending: true });
    if (error) return { ok: false, error: error.message };
    const rows: RoleCatalogRow[] = (data ?? []).map((raw) => {
      const r = raw as Record<string, unknown>;
      const scopeRaw = String(r.scope ?? "tenant").toLowerCase();
      const scope: "system" | "tenant" = scopeRaw === "system" ? "system" : "tenant";
      return {
        id: String(r.id ?? ""),
        key: String(r.key ?? "").trim(),
        name: String(r.name ?? "").trim(),
        description: r.description != null ? String(r.description) : null,
        scope,
        is_system: Boolean(r.is_system),
        is_assignable: Boolean(r.is_assignable),
        created_at: r.created_at != null ? String(r.created_at) : "",
      };
    }).filter((x) => x.id && x.key);
    return { ok: true, rows };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load roles." };
  }
}

export type OrganizationOptionRow = {
  id: string;
  name: string;
  /** organizations.name (registry). */
  type: string | null;
  /**
   * Label for the UI: organization_settings.company_display_name when set, else `name`.
   */
  displayName: string;
};

export async function listOrganizationsForAccessAction(): Promise<
  { ok: true; rows: OrganizationOptionRow[] } | { ok: false; error: string }
> {
  const g = await gate();
  if (!g.ok) {
    return { ok: false, error: g.denied === "not_authenticated" ? "Not authenticated." : "Forbidden." };
  }
  try {
    const { data, error } = await supabaseServer
      .from("organizations")
      .select("id, name, type")
      .order("name", { ascending: true });
    if (error) return { ok: false, error: error.message };
    const orgRows = (data ?? [])
      .map((raw) => {
        const r = raw as Record<string, unknown>;
        return {
          id: String(r.id ?? ""),
          name: String(r.name ?? "").trim() || String(r.id ?? ""),
          type: r.type != null ? String(r.type).trim() : null,
        };
      })
      .filter((x) => x.id);
    if (orgRows.length === 0) {
      return { ok: true, rows: [] };
    }
    const ids = orgRows.map((o) => o.id);
    let stRows: Record<string, unknown>[] = [];
    {
      const a = await supabaseServer
        .from("organization_settings")
        .select("organization_id, company_id, company_display_name")
        .in("organization_id", ids);
      if (!a.error && a.data?.length) {
        stRows = (a.data as Record<string, unknown>[]) ?? [];
      } else {
        const b = await supabaseServer
          .from("organization_settings")
          .select("company_id, company_display_name")
          .in("company_id", ids);
        if (!b.error && b.data?.length) {
          stRows = (b.data as Record<string, unknown>[]) ?? [];
        }
      }
    }
    const displayByOrg = new Map<string, string>();
    for (const s of stRows) {
      const oid = String(s.organization_id ?? s.company_id ?? "").trim();
      if (!oid) continue;
      const d = s.company_display_name;
      if (d != null && String(d).trim()) {
        displayByOrg.set(oid, String(d).trim());
      }
    }
    const rows: OrganizationOptionRow[] = orgRows.map((o) => ({
      id: o.id,
      name: o.name,
      type: o.type,
      displayName: displayByOrg.get(o.id) ?? o.name,
    }));
    return { ok: true, rows };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load organizations." };
  }
}

export type GroupCatalogRow = {
  id: string;
  organization_id: string;
  organization_name: string | null;
  key: string;
  name: string;
  description: string | null;
  created_at: string;
};

export async function listGroupsForOrganizationAccessAction(
  organizationId: string,
): Promise<{ ok: true; rows: GroupCatalogRow[] } | { ok: false; error: string }> {
  const g = await gate();
  if (!g.ok) {
    return { ok: false, error: g.denied === "not_authenticated" ? "Not authenticated." : "Forbidden." };
  }
  const oid = organizationId.trim();
  if (!isUuidString(oid)) return { ok: false, error: "Invalid organization." };
  try {
    const { data, error } = await supabaseServer
      .from("groups")
      .select("id, organization_id, key, name, description, created_at, organizations(name)")
      .eq("organization_id", oid)
      .order("name", { ascending: true });
    if (error) return { ok: false, error: error.message };
    const rows: GroupCatalogRow[] = (data ?? []).map((raw) => {
      const r = raw as Record<string, unknown>;
      const orgJoin = splitJoined<{ name?: string | null }>(r.organizations);
      const orgName =
        orgJoin?.name != null && String(orgJoin.name).trim()
          ? String(orgJoin.name).trim()
          : null;
      return {
        id: String(r.id ?? ""),
        organization_id: String(r.organization_id ?? "").trim(),
        organization_name: orgName,
        key: String(r.key ?? "").trim(),
        name: String(r.name ?? "").trim(),
        description: r.description != null ? String(r.description) : null,
        created_at: r.created_at != null ? String(r.created_at) : "",
      };
    }).filter((x) => x.id && x.organization_id);
    return { ok: true, rows };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load groups." };
  }
}

export async function createRoleAccessAction(input: {
  name: string;
  key: string;
  description?: string | null;
  scope: string;
  is_assignable: boolean;
}): Promise<{ ok: true; id: string } | { ok: false; error: string; fieldErrors?: Record<string, string> }> {
  const g = await gate();
  if (!g.ok) {
    return { ok: false, error: g.denied === "not_authenticated" ? "Not authenticated." : "Forbidden." };
  }
  const fe = collectRoleCreateErrors({
    name: input.name,
    key: input.key,
    description: input.description,
    scope: input.scope,
  });
  if (fe) return { ok: false, error: "Validation failed.", fieldErrors: fe };
  const key = normalizeAccessEntityKey(input.key);
  const name = input.name.trim();
  const description =
    input.description != null && String(input.description).trim()
      ? String(input.description).trim().slice(0, 300)
      : null;
  const scope = input.scope === "system" ? "system" : "tenant";
  try {
    const { data, error } = await supabaseServer
      .from("roles")
      .insert({
        key,
        name,
        description,
        scope,
        is_assignable: Boolean(input.is_assignable),
        is_system: false,
      })
      .select("id")
      .maybeSingle();
    if (error) {
      if (error.code === "23505") {
        return { ok: false, error: "A role with this key already exists.", fieldErrors: { key: "Duplicate key." } };
      }
      return { ok: false, error: error.message };
    }
    const id = data && typeof (data as { id?: unknown }).id === "string" ? String((data as { id: string }).id) : "";
    if (!id) return { ok: false, error: "Create failed." };
    return { ok: true, id };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Create failed." };
  }
}

export async function updateRoleAccessAction(
  id: string,
  patch: {
    name: string;
    description?: string | null;
    scope: string;
    is_assignable: boolean;
  },
): Promise<{ ok: true } | { ok: false; error: string; fieldErrors?: Record<string, string> }> {
  const g = await gate();
  if (!g.ok) {
    return { ok: false, error: g.denied === "not_authenticated" ? "Not authenticated." : "Forbidden." };
  }
  if (!isUuidString(id)) return { ok: false, error: "Invalid role id." };
  const fe = collectRoleUpdateErrors({
    name: patch.name,
    description: patch.description,
    scope: patch.scope,
  });
  if (fe) return { ok: false, error: "Validation failed.", fieldErrors: fe };
  const name = patch.name.trim();
  const description =
    patch.description != null && String(patch.description).trim()
      ? String(patch.description).trim().slice(0, 300)
      : null;
  const scope = patch.scope === "system" ? "system" : "tenant";
  try {
    const { error } = await supabaseServer
      .from("roles")
      .update({
        name,
        description,
        scope,
        is_assignable: Boolean(patch.is_assignable),
      })
      .eq("id", id);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

export async function createGroupAccessAction(input: {
  organization_id: string;
  name: string;
  key: string;
  description?: string | null;
}): Promise<{ ok: true; id: string } | { ok: false; error: string; fieldErrors?: Record<string, string> }> {
  const g = await gate();
  if (!g.ok) {
    return { ok: false, error: g.denied === "not_authenticated" ? "Not authenticated." : "Forbidden." };
  }
  const fe = collectGroupCreateErrors(input);
  if (fe) return { ok: false, error: "Validation failed.", fieldErrors: fe };
  const oid = input.organization_id.trim();
  if (!isUuidString(oid)) return { ok: false, error: "Invalid organization.", fieldErrors: { organization_id: "Invalid." } };
  const key = normalizeAccessEntityKey(input.key);
  const name = input.name.trim();
  const description =
    input.description != null && String(input.description).trim()
      ? String(input.description).trim().slice(0, 300)
      : null;
  try {
    const { data: orgRow, error: orgErr } = await supabaseServer
      .from("organizations")
      .select("id")
      .eq("id", oid)
      .maybeSingle();
    if (orgErr) return { ok: false, error: orgErr.message };
    if (!orgRow) return { ok: false, error: "Organization not found." };
    const { data, error } = await supabaseServer
      .from("groups")
      .insert({
        organization_id: oid,
        key,
        name,
        description,
      })
      .select("id")
      .maybeSingle();
    if (error) {
      if (error.code === "23505") {
        return {
          ok: false,
          error: "A group with this key already exists in this organization.",
          fieldErrors: { key: "Duplicate key for this org." },
        };
      }
      return { ok: false, error: error.message };
    }
    const newId = data && typeof (data as { id?: unknown }).id === "string" ? String((data as { id: string }).id) : "";
    if (!newId) return { ok: false, error: "Create failed." };
    return { ok: true, id: newId };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Create failed." };
  }
}

export async function updateGroupAccessAction(
  id: string,
  patch: {
    name: string;
    key: string;
    description?: string | null;
  },
): Promise<{ ok: true } | { ok: false; error: string; fieldErrors?: Record<string, string> }> {
  const g = await gate();
  if (!g.ok) {
    return { ok: false, error: g.denied === "not_authenticated" ? "Not authenticated." : "Forbidden." };
  }
  if (!isUuidString(id)) return { ok: false, error: "Invalid group id." };
  const fe = collectGroupUpdateErrors(patch);
  if (fe) return { ok: false, error: "Validation failed.", fieldErrors: fe };
  const key = normalizeAccessEntityKey(patch.key);
  const name = patch.name.trim();
  const description =
    patch.description != null && String(patch.description).trim()
      ? String(patch.description).trim().slice(0, 300)
      : null;
  try {
    const { error } = await supabaseServer
      .from("groups")
      .update({ key, name, description })
      .eq("id", id);
    if (error) {
      if (error.code === "23505") {
        return {
          ok: false,
          error: "A group with this key already exists in this organization.",
          fieldErrors: { key: "Duplicate key for this org." },
        };
      }
      return { ok: false, error: error.message };
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

export async function deleteRoleAccessAction(
  id: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await gate();
  if (!g.ok) {
    return { ok: false, error: g.denied === "not_authenticated" ? "Not authenticated." : "Forbidden." };
  }
  if (!isUuidString(id)) return { ok: false, error: "Invalid role id." };
  try {
    const { data: row, error: re } = await supabaseServer
      .from("roles")
      .select("id, is_system")
      .eq("id", id)
      .maybeSingle();
    if (re) return { ok: false, error: re.message };
    if (!row) return { ok: false, error: "Role not found." };
    const r = row as { is_system?: unknown };
    if (Boolean(r.is_system)) {
      return { ok: false, error: "Cannot delete system catalog roles." };
    }
    const { count, error: ce } = await supabaseServer
      .from("profiles")
      .select("id", { count: "exact", head: true })
      .eq("role_id", id);
    if (ce) return { ok: false, error: ce.message };
    if ((count ?? 0) > 0) {
      return {
        ok: false,
        error: "Cannot delete: one or more users are assigned this role. Reassign them first.",
      };
    }
    const { error } = await supabaseServer.from("roles").delete().eq("id", id);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Delete failed." };
  }
}

export async function deleteGroupAccessAction(
  id: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await gate();
  if (!g.ok) {
    return { ok: false, error: g.denied === "not_authenticated" ? "Not authenticated." : "Forbidden." };
  }
  if (!isUuidString(id)) return { ok: false, error: "Invalid group id." };
  try {
    const { data: row, error: ge } = await supabaseServer
      .from("groups")
      .select("id")
      .eq("id", id)
      .maybeSingle();
    if (ge) return { ok: false, error: ge.message };
    if (!row) return { ok: false, error: "Group not found." };
    const { count, error: ce } = await supabaseServer
      .from("user_groups")
      .select("id", { count: "exact", head: true })
      .eq("group_id", id);
    if (ce) return { ok: false, error: ce.message };
    if ((count ?? 0) > 0) {
      return {
        ok: false,
        error: "Cannot delete: users are still assigned to this group. Remove memberships first.",
      };
    }
    const { error } = await supabaseServer.from("groups").delete().eq("id", id);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Delete failed." };
  }
}
