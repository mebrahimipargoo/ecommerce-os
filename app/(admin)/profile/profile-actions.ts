"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { getSessionUserIdFromCookies } from "../../../lib/supabase-server-auth";
import { isUuidString } from "../../../lib/uuid";

export async function updateOwnProfileFullName(
  fullName: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const uid = await getSessionUserIdFromCookies();
  if (!uid) return { ok: false, error: "Not signed in." };
  if (!isUuidString(uid)) return { ok: false, error: "Invalid session." };
  const name = fullName.trim();
  if (!name) return { ok: false, error: "Name is required." };
  if (name.length > 200) return { ok: false, error: "Name is too long." };
  try {
    const { error } = await supabaseServer
      .from("profiles")
      .update({ full_name: name, updated_at: new Date().toISOString() })
      .eq("id", uid);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}
