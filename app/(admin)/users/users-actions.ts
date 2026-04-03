"use server";

import { randomBytes } from "crypto";
import { supabaseServer } from "../../../lib/supabase-server";
import { resolveOrganizationId } from "../../../lib/organization";
import { isUuidString } from "../../../lib/uuid";
import type { ProfileRow } from "./users-types";

/**
 * Profile fields — no PostgREST join used here because `profiles.organization_id`
 * references `organization_settings.organization_id` (not a standalone `companies`
 * or `organizations` table). Company display names are resolved in a second round-trip
 * via `fetchOrgNameMap` below, keyed by `organization_id`.
 */
const PROFILE_LIST_SELECT =
  "id, organization_id, full_name, role, photo_url, created_at, updated_at";

/**
 * Fetches a map of organization_id → display_name from `organization_settings`.
 * Falls back to the raw UUID string when `company_display_name` is blank.
 */
async function fetchOrgNameMap(orgIds: string[]): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  if (orgIds.length === 0) return map;
  try {
    const { data } = await supabaseServer
      .from("organization_settings")
      .select("organization_id, company_display_name")
      .in("organization_id", orgIds);
    for (const row of data ?? []) {
      const r = row as Record<string, unknown>;
      const oid = String(r.organization_id ?? "").trim();
      if (!oid) continue;
      const label =
        typeof r.company_display_name === "string" && r.company_display_name.trim()
          ? r.company_display_name.trim()
          : oid;
      map.set(oid, label);
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

export async function listUserProfiles(): Promise<
  { ok: true; rows: ProfileRow[] } | { ok: false; error: string }
> {
  try {
    const { data, error } = await supabaseServer
      .from("profiles")
      .select(PROFILE_LIST_SELECT)
      .order("full_name", { ascending: true });
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
      return {
        id,
        organization_id: oid,
        company_name: companyName,
        full_name: displayName,
        email: fromAuth,
        role: r.role != null ? String(r.role) : null,
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
  const VALID_ROLES = ["super_admin", "system_employee", "admin", "employee", "operator"] as const;
  const role = VALID_ROLES.includes(input.role as typeof VALID_ROLES[number])
    ? input.role
    : "operator";
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
    const { data, error } = await supabaseServer
      .from("profiles")
      .insert({
        id: uid,
        organization_id: cid,
        full_name: fullName,
        role,
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
    const VALID_ROLES = ["super_admin", "system_employee", "admin", "employee", "operator"];
    row.role = VALID_ROLES.includes(patch.role) ? patch.role : "operator";
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
