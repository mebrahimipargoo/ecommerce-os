"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import {
  canManagePlatformSettings,
  loadTenantProfile,
} from "../../../lib/server-tenant";
import { isUuidString } from "../../../lib/uuid";

const DEBUG =
  process.env.BRANDING_DEBUG === "1" ||
  process.env.NEXT_PUBLIC_BRANDING_DEBUG === "1";
function dlog(...args: unknown[]) {
  if (DEBUG) console.log("[platform-settings-server]", ...args);
}

export type SavePlatformSettingsInput = {
  actorProfileId: string | null | undefined;
  app_name: string;
  logo_url: string | null;
};

export async function savePlatformSettingsAction(
  input: SavePlatformSettingsInput,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const aid = String(input.actorProfileId ?? "").trim();
  if (!aid || !isUuidString(aid)) {
    dlog("save → missing/invalid actor profile id", { aid });
    return { ok: false, error: "Missing or invalid session profile." };
  }
  const profile = await loadTenantProfile(aid);
  const allowed = canManagePlatformSettings(profile);
  dlog("save → permission check", {
    aid,
    role: profile?.role ?? null,
    role_scope: profile?.role_scope ?? null,
    canManagePlatformSettings: allowed,
  });
  if (!allowed) {
    return { ok: false, error: "You do not have permission to edit platform branding." };
  }
  const app_name = String(input.app_name ?? "").trim();
  if (!app_name) {
    return { ok: false, error: "App name is required." };
  }
  const logo_url =
    input.logo_url != null && String(input.logo_url).trim()
      ? String(input.logo_url).trim()
      : null;

  const { error } = await supabaseServer
    .from("platform_settings")
    .update({ app_name, logo_url })
    .eq("id", true);
  if (error) return { ok: false, error: error.message };
  return { ok: true };
}
