"use server";

import {
  getOrganizationLogoUrlFromDb,
  upsertOrganizationLogoUrl,
} from "../../lib/organization-logo";
import { normalizeTenantLogoUrl } from "../../lib/tenant-logo-url";
import { supabaseServer } from "../../lib/supabase-server";
import type { TenantWriteContext } from "../../lib/server-tenant";
import { isUuidString } from "../../lib/uuid";
import {
  DEFAULT_CLAIM_AGENT_CONFIG,
  DEFAULT_CORE_SETTINGS,
  DEFAULT_FEFO,
  DEFAULT_WORKSPACE_SETTINGS,
  type ClaimAgentConfig,
  type CoreSettings,
  type InventoryModuleConfig,
  type ModuleConfigs,
  type WorkspaceSettings,
} from "./workspace-settings-types";

// ─── Read ─────────────────────────────────────────────────────────────────────

/**
 * Returns workspace settings, falling back gracefully to defaults if the
 * workspace_settings table hasn't been provisioned yet.
 */
export async function getWorkspaceSettings(): Promise<WorkspaceSettings> {
  try {
    const { data, error } = await supabaseServer
      .from("workspace_settings")
      .select("id, core_settings, module_configs")
      .order("id", { ascending: true })
      .limit(1)
      .maybeSingle();

    if (error || !data) return DEFAULT_WORKSPACE_SETTINGS;

    return {
      id: data.id as string,
      core_settings:  (data.core_settings  as Record<string, unknown>) ?? {},
      module_configs: (data.module_configs as ModuleConfigs) ?? { inventory: DEFAULT_FEFO },
    };
  } catch {
    return DEFAULT_WORKSPACE_SETTINGS;
  }
}

/**
 * Returns the core_settings (white-label / tenant branding). Always succeeds.
 * Pass `companyId` to resolve the logo for that tenant (sidebar multi-tenant branding).
 */
export async function getCoreSettings(companyId?: string): Promise<CoreSettings> {
  const ws = await getWorkspaceSettings();
  const merged = {
    ...DEFAULT_CORE_SETTINGS,
    ...(ws.core_settings as CoreSettings),
  };
  const orgLogo = await getOrganizationLogoUrlFromDb(companyId);
  let labelFromOrg = "";
  if (companyId?.trim()) {
    try {
      const { data: osRow } = await supabaseServer
        .from("organization_settings")
        .select("company_display_name")
        .eq("organization_id", companyId.trim())
        .maybeSingle();
      const dn = (osRow as { company_display_name?: string | null } | null)?.company_display_name;
      labelFromOrg = typeof dn === "string" ? dn.trim() : "";
    } catch {
      labelFromOrg = "";
    }
  }
  const rawLogo =
    orgLogo ||
    (typeof merged.company_logo_url === "string" && merged.company_logo_url) ||
    (typeof merged.logo_url === "string" && merged.logo_url) ||
    "";
  const logo = normalizeTenantLogoUrl(rawLogo);
  const companyName =
    labelFromOrg ||
    (typeof merged.company_name === "string" && merged.company_name.trim()) ||
    (typeof merged.workspace_name === "string" && String(merged.workspace_name).trim()) ||
    "";
  return {
    ...merged,
    company_name: companyName,
    company_logo_url: logo,
    logo_url: logo,
  };
}

/**
 * Returns the FEFO thresholds. Always succeeds — returns defaults on any error.
 */
export async function getFefoSettings(): Promise<InventoryModuleConfig> {
  const ws = await getWorkspaceSettings();
  return {
    fefo_critical_days: ws.module_configs.inventory?.fefo_critical_days ?? DEFAULT_FEFO.fefo_critical_days,
    fefo_warning_days:  ws.module_configs.inventory?.fefo_warning_days  ?? DEFAULT_FEFO.fefo_warning_days,
  };
}

export async function getClaimAgentConfig(): Promise<ClaimAgentConfig> {
  const ws = await getWorkspaceSettings();
  return {
    ...DEFAULT_CLAIM_AGENT_CONFIG,
    ...(ws.module_configs.claim_agent_config ?? {}),
  };
}

export async function saveClaimAgentConfig(
  patch: Partial<ClaimAgentConfig>,
): Promise<{ ok: boolean; error?: string }> {
  try {
    const existing = await getWorkspaceSettings();
    const merged: ClaimAgentConfig = {
      ...DEFAULT_CLAIM_AGENT_CONFIG,
      ...(existing.module_configs.claim_agent_config ?? {}),
      ...patch,
    };
    const newModuleConfigs: ModuleConfigs = {
      ...existing.module_configs,
      claim_agent_config: merged,
    };

    if (existing.id) {
      const { error } = await supabaseServer
        .from("workspace_settings")
        .update({ module_configs: newModuleConfigs })
        .eq("id", existing.id);
      if (error) return { ok: false, error: error.message };
    } else {
      const { error } = await supabaseServer
        .from("workspace_settings")
        .insert({
          core_settings: existing.core_settings ?? {},
          module_configs: newModuleConfigs,
        });
      if (error) return { ok: false, error: error.message };
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Unknown error" };
  }
}

// ─── Write ────────────────────────────────────────────────────────────────────

export async function saveInventoryFefoSettings(
  fefo: InventoryModuleConfig,
): Promise<{ ok: boolean; error?: string }> {
  try {
    // Deep-merge inventory key into existing module_configs
    const existing = await getWorkspaceSettings();
    const newModuleConfigs: ModuleConfigs = {
      ...existing.module_configs,
      inventory: {
        ...(existing.module_configs.inventory ?? {}),
        fefo_critical_days: fefo.fefo_critical_days,
        fefo_warning_days:  fefo.fefo_warning_days,
      },
    };

    if (existing.id) {
      // Update existing row
      const { error } = await supabaseServer
        .from("workspace_settings")
        .update({ module_configs: newModuleConfigs })
        .eq("id", existing.id);
      if (error) return { ok: false, error: error.message };
    } else {
      // Insert new row
      const { error } = await supabaseServer
        .from("workspace_settings")
        .insert({ core_settings: {}, module_configs: newModuleConfigs });
      if (error) return { ok: false, error: error.message };
    }

    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Unknown error" };
  }
}

/**
 * Persists tenant branding / white-label fields into the core_settings JSONB column.
 * Performs a deep-merge so unrelated keys in core_settings are preserved.
 */
export async function saveCoreSettings(
  data: Partial<CoreSettings>,
  tenant?: TenantWriteContext | null,
): Promise<{ ok: boolean; error?: string }> {
  try {
    const hasLogoPatch =
      Object.prototype.hasOwnProperty.call(data, "company_logo_url") ||
      Object.prototype.hasOwnProperty.call(data, "logo_url");
    if (hasLogoPatch) {
      const raw =
        (typeof data.company_logo_url === "string" ? data.company_logo_url : "") ||
        (typeof data.logo_url === "string" ? data.logo_url : "") ||
        "";
      const orgRes = await upsertOrganizationLogoUrl(raw.trim() || null, tenant);
      if (!orgRes.ok) return { ok: false, error: orgRes.error };
    }

    const existing = await getWorkspaceSettings();
    const newCoreSettings: CoreSettings = {
      ...DEFAULT_CORE_SETTINGS,
      ...(existing.core_settings as CoreSettings),
      ...data,
    };

    if (existing.id) {
      const { error } = await supabaseServer
        .from("workspace_settings")
        .update({ core_settings: newCoreSettings })
        .eq("id", existing.id);
      if (error) return { ok: false, error: error.message };
    } else {
      // First-time insert — also seed an empty reports placeholder in module_configs
      const newModuleConfigs: ModuleConfigs = {
        inventory: DEFAULT_FEFO,
        reports:   {},
        ...existing.module_configs,
      };
      const { error } = await supabaseServer
        .from("workspace_settings")
        .insert({ core_settings: newCoreSettings, module_configs: newModuleConfigs });
      if (error) return { ok: false, error: error.message };
    }

    // Keep organization_settings.company_display_name in sync so that
    // getCoreSettings(organizationId) reads back the name the user just typed,
    // and dropdowns across the app show the correct label instead of a raw UUID.
    const newName = typeof data.company_name === "string" ? data.company_name.trim() : "";
    const orgId = tenant?.organizationId?.trim() ?? "";
    if (newName && orgId && isUuidString(orgId)) {
      await supabaseServer
        .from("organization_settings")
        .upsert(
          { organization_id: orgId, company_display_name: newName },
          { onConflict: "organization_id" },
        );
    }

    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Unknown error" };
  }
}
