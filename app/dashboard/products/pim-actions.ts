"use server";

import { amazonSpCredentialsLookComplete } from "../../../lib/amazon-marketplace-credentials";
import { getSessionUserIdFromCookies } from "../../../lib/supabase-server-auth";
import { supabaseServer } from "../../../lib/supabase-server";
import { isUuidString } from "../../../lib/uuid";
import { listStoresForImports } from "../../(admin)/imports/companies-actions";
import type { CatalogModuleConfig, ModuleConfigs } from "../../settings/workspace-settings-types";

export type PimConnectionStatus = {
  amazonSpApi: boolean;
  openai: boolean;
  googleSheets: boolean;
};

export type PimIntegrationsSummary = {
  googleSheetId: string | null;
  /** Default store for ETL calls that require store_id (e.g. Google Sheets sync). */
  defaultStoreId: string | null;
  connectionStatus: PimConnectionStatus;
};

function extractGoogleSheetId(moduleConfigs: unknown): string | null {
  if (!moduleConfigs || typeof moduleConfigs !== "object") return null;
  const catalog = (moduleConfigs as ModuleConfigs).catalog as CatalogModuleConfig | undefined;
  const id = catalog?.google_sheet_id;
  return typeof id === "string" && id.trim() ? id.trim() : null;
}

/**
 * Validates session + tenant: home org must match `organizationId`, or user is platform staff
 * (super_admin / system_employee / system_admin) so workspace switching in the shell is honored.
 */
export async function assertUserCanAccessOrganization(
  organizationId: string,
): Promise<{ ok: true; userId: string } | { ok: false; error: string }> {
  if (!isUuidString(organizationId)) {
    return { ok: false, error: "Invalid organization." };
  }
  const uid = await getSessionUserIdFromCookies();
  if (!uid) return { ok: false, error: "Not signed in." };

  const { data: prof, error } = await supabaseServer
    .from("profiles")
    .select("organization_id, roles!profiles_role_id_fkey(key)")
    .eq("id", uid)
    .maybeSingle();

  if (error || !prof) {
    return { ok: false, error: "Profile not found." };
  }

  const homeOrg = String(prof.organization_id ?? "").trim();
  const rolesRel = (prof as { roles?: { key?: string } | { key?: string }[] | null }).roles;
  const roleKeyRaw =
    Array.isArray(rolesRel) ? rolesRel[0]?.key : rolesRel && typeof rolesRel === "object" ? rolesRel.key : undefined;
  const roleKey = String(roleKeyRaw ?? "").toLowerCase();
  const isPlatformStaff =
    roleKey === "super_admin" || roleKey === "system_employee" || roleKey === "system_admin";

  if (!isPlatformStaff && homeOrg !== organizationId) {
    return { ok: false, error: "You do not have access to this organization." };
  }

  return { ok: true, userId: uid };
}

export async function getPimIntegrationsSummary(
  organizationId: string,
): Promise<{ ok: true; data: PimIntegrationsSummary } | { ok: false; error: string }> {
  const gate = await assertUserCanAccessOrganization(organizationId);
  if (!gate.ok) return gate;

  let googleSheetId: string | null = null;

  const { data: wsOrg, error: wsOrgErr } = await supabaseServer
    .from("workspace_settings")
    .select("module_configs")
    .eq("organization_id", organizationId)
    .maybeSingle();

  if (!wsOrgErr && wsOrg) {
    googleSheetId = extractGoogleSheetId(wsOrg.module_configs);
  }

  if (!googleSheetId) {
    const { data: wsAny } = await supabaseServer
      .from("workspace_settings")
      .select("module_configs")
      .limit(1)
      .maybeSingle();
    googleSheetId = wsAny ? extractGoogleSheetId(wsAny.module_configs) : null;
  }

  const { data: keyRows } = await supabaseServer
    .from("organization_api_keys")
    .select("name, role")
    .eq("organization_id", organizationId);

  const names = new Set((keyRows ?? []).map((r) => String((r as { name?: string }).name ?? "").trim()));
  const roles = (keyRows ?? []).map((r) => ({
    name: String((r as { name?: string }).name ?? "").trim(),
    role: String((r as { role?: string }).role ?? "").trim(),
  }));

  let amazonFromMarketplaces = false;
  const { data: mpRows } = await supabaseServer
    .from("marketplaces")
    .select("credentials")
    .eq("organization_id", organizationId)
    .eq("provider", "amazon_sp_api");
  for (const row of mpRows ?? []) {
    if (amazonSpCredentialsLookComplete((row as { credentials?: unknown }).credentials)) {
      amazonFromMarketplaces = true;
      break;
    }
  }

  const amazonSpApi = names.has("amazon_sp_api") || amazonFromMarketplaces;
  const googleSheets = names.has("google_sheets_api");
  const openai =
    names.has("openai_api_key") ||
    roles.some((r) => r.role === "llm_provider" && (r.name === "OpenAI" || r.name.toLowerCase() === "openai"));

  const { data: osDefault } = await supabaseServer
    .from("organization_settings")
    .select("default_store_id")
    .eq("organization_id", organizationId)
    .maybeSingle();
  const rawDefault = (osDefault as { default_store_id?: string | null } | null)?.default_store_id;
  const defaultStoreId = typeof rawDefault === "string" && isUuidString(rawDefault) ? rawDefault : null;

  return {
    ok: true,
    data: {
      googleSheetId,
      defaultStoreId,
      connectionStatus: { amazonSpApi, openai, googleSheets },
    },
  };
}

export type PimStoreOption = {
  id: string;
  organization_id: string;
  display_name: string;
};

/** Active stores for manual product create (same rules as imports). */
export async function getPimManualProductFormDefaults(
  organizationId: string,
): Promise<
  { ok: true; stores: PimStoreOption[]; defaultStoreId: string | null } | { ok: false; error: string }
> {
  const gate = await assertUserCanAccessOrganization(organizationId);
  if (!gate.ok) return gate;

  const storesRes = await listStoresForImports(gate.userId, organizationId);
  if (!storesRes.ok) {
    return { ok: false, error: storesRes.error };
  }

  const { data: os } = await supabaseServer
    .from("organization_settings")
    .select("default_store_id")
    .eq("organization_id", organizationId)
    .maybeSingle();

  const raw = (os as { default_store_id?: string | null } | null)?.default_store_id;
  const defaultStoreId = typeof raw === "string" && isUuidString(raw) ? raw : null;

  return {
    ok: true,
    stores: storesRes.rows.map((r) => ({
      id: r.id,
      organization_id: r.organization_id,
      display_name: r.display_name,
    })),
    defaultStoreId,
  };
}
