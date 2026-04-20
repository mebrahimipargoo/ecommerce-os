"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { getSessionUserIdFromCookies } from "../../../lib/supabase-server-auth";

type AccessDenied = "not_authenticated" | "forbidden";

type PlatformSettingsView = {
  app_name: string;
  logo_url: string;
  accessDenied: AccessDenied | null;
};

type SavePlatformSettingsInput = {
  app_name: string;
  logo_url: string | null;
};

export async function getAuthenticatedPlatformRoleKey(): Promise<string | null> {
  const sessionUserId = await getSessionUserIdFromCookies();
  if (!sessionUserId) return null;

  const { data: profile } = await supabaseServer
    .from("profiles")
    .select("role, role_id")
    .eq("id", sessionUserId)
    .maybeSingle();

  if (!profile) return null;

  const roleId = typeof profile.role_id === "string" ? profile.role_id.trim() : "";
  if (roleId) {
    const { data: roleRow } = await supabaseServer
      .from("roles")
      .select("key")
      .eq("id", roleId)
      .maybeSingle();
    const roleKey = typeof roleRow?.key === "string" ? roleRow.key.trim().toLowerCase() : "";
    if (roleKey) return roleKey;
  }

  const legacyRole = typeof profile.role === "string" ? profile.role.trim().toLowerCase() : "";
  return legacyRole || null;
}

export async function getPlatformSettingsAction(): Promise<PlatformSettingsView> {
  const roleKey = await getAuthenticatedPlatformRoleKey();
  if (!roleKey) {
    return { app_name: "", logo_url: "", accessDenied: "not_authenticated" };
  }
  if (roleKey !== "super_admin") {
    return { app_name: "", logo_url: "", accessDenied: "forbidden" };
  }

  const { data } = await supabaseServer
    .from("platform_settings")
    .select("app_name, logo_url")
    .eq("id", true)
    .maybeSingle();

  return {
    app_name: typeof data?.app_name === "string" ? data.app_name : "",
    logo_url: typeof data?.logo_url === "string" ? data.logo_url : "",
    accessDenied: null,
  };
}

export async function savePlatformSettingsAction(
  input: SavePlatformSettingsInput,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const roleKey = await getAuthenticatedPlatformRoleKey();
  if (!roleKey) return { ok: false, error: "You must be signed in." };
  if (roleKey !== "super_admin") return { ok: false, error: "Only super_admin can edit platform settings." };

  const app_name = String(input.app_name ?? "").trim();
  if (!app_name) {
    return { ok: false, error: "App name is required." };
  }
  const logo_url =
    input.logo_url == null ? null : String(input.logo_url).trim() || null;

  const { error } = await supabaseServer
    .from("platform_settings")
    .update({ app_name, logo_url })
    .eq("id", true);
  if (error) return { ok: false, error: error.message };
  return { ok: true };
}
