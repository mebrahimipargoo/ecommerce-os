"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { resolveOrganizationId } from "../../../lib/organization";
import { isUuidString } from "../../../lib/uuid";

/** Workspace tenant UUID (env / org) — use as default company when adding users. */
export async function getTenantCompanyIdForUsersPage(): Promise<string> {
  return resolveOrganizationId();
}

/** One row from the `profiles` table (email from `auth.users` when available). */
export type ProfileRow = {
  id: string;
  company_id: string | null;
  full_name: string | null;
  email: string;
  role: string | null;
  photo_url: string | null;
  created_at: string;
  updated_at: string | null;
};

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
    const orgId = resolveOrganizationId();
    const { data, error } = await supabaseServer
      .from("profiles")
      .select("id, company_id, full_name, email, role, photo_url, created_at, updated_at")
      .eq("company_id", orgId)
      .order("full_name", { ascending: true });
    if (error) return { ok: false, error: error.message };

    const authEmails = await emailByUserIdMap();
    const rows: ProfileRow[] = (data ?? []).map((raw) => {
      const r = raw as Record<string, unknown>;
      const id = String(r.id ?? "");
      const fromAuth = authEmails.get(id)?.trim();
      const fromProfile = typeof r.email === "string" ? r.email.trim() : "";
      return {
        id,
        company_id: r.company_id != null ? String(r.company_id) : null,
        full_name: r.full_name != null ? String(r.full_name) : null,
        email: fromAuth || fromProfile || "",
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
  /** Target tenant — stored on `profiles.company_id`. */
  company_id: string;
}): Promise<{ ok: true; id: string } | { ok: false; error: string }> {
  const email = input.email.trim().toLowerCase();
  if (!email) return { ok: false, error: "Email is required." };
  const cid = input.company_id.trim();
  if (!isUuidString(cid)) return { ok: false, error: "Select a valid company." };
  const fullName = input.full_name.trim();
  const role = input.role === "admin" ? "admin" : "operator";
  try {
    const { data, error } = await supabaseServer
      .from("profiles")
      .insert({
        company_id: cid,
        full_name: fullName,
        email,
        role,
      })
      .select("id")
      .single();
    if (error) return { ok: false, error: error.message };
    if (!data?.id) return { ok: false, error: "Insert failed." };
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
  const orgId = resolveOrganizationId();
  const row: Record<string, unknown> = {
    updated_at: new Date().toISOString(),
  };
  if (typeof patch.full_name === "string") row.full_name = patch.full_name.trim();
  if (typeof patch.role === "string") row.role = patch.role === "admin" ? "admin" : "operator";
  if ("photo_url" in patch) row.photo_url = patch.photo_url;
  try {
    const { error } = await supabaseServer
      .from("profiles")
      .update(row)
      .eq("id", id)
      .eq("company_id", orgId);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

export async function deleteUserProfile(id: string): Promise<{ ok: boolean; error?: string }> {
  if (!isUuidString(id)) return { ok: false, error: "Invalid user id." };
  const orgId = resolveOrganizationId();
  try {
    const { error } = await supabaseServer
      .from("profiles")
      .delete()
      .eq("id", id)
      .eq("company_id", orgId);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Delete failed." };
  }
}
