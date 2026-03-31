"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { resolveOrganizationId } from "../../lib/organization";
import { isUuidString } from "../../lib/uuid";

/** One row from the `profiles` table. */
export type ProfileRow = {
  id: string;
  organization_id: string;
  full_name: string;
  email: string;
  role: string;
  photo_url: string | null;
  created_at: string;
  updated_at: string;
};

export async function listUserProfiles(): Promise<
  { ok: true; rows: ProfileRow[] } | { ok: false; error: string }
> {
  try {
    const orgId = resolveOrganizationId();
    const { data, error } = await supabaseServer
      .from("profiles")
      .select("*")
      .eq("organization_id", orgId)
      .order("full_name", { ascending: true });
    if (error) return { ok: false, error: error.message };
    return { ok: true, rows: (data ?? []) as ProfileRow[] };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load users." };
  }
}

export async function createUserProfile(input: {
  full_name: string;
  email: string;
  role: string;
}): Promise<{ ok: true; id: string } | { ok: false; error: string }> {
  const orgId = resolveOrganizationId();
  const email = input.email.trim().toLowerCase();
  if (!email) return { ok: false, error: "Email is required." };
  const fullName = input.full_name.trim();
  const role = input.role === "admin" ? "admin" : "operator";
  try {
    const { data, error } = await supabaseServer
      .from("profiles")
      .insert({
        organization_id: orgId,
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
      .eq("organization_id", orgId);
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
      .eq("organization_id", orgId);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Delete failed." };
  }
}
