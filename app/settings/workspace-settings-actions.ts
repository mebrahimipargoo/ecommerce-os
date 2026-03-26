"use server";

import { supabaseServer } from "../../lib/supabase-server";
import {
  DEFAULT_CORE_SETTINGS,
  DEFAULT_FEFO,
  DEFAULT_WORKSPACE_SETTINGS,
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
      .limit(1)
      .single();

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
 */
export async function getCoreSettings(): Promise<CoreSettings> {
  const ws = await getWorkspaceSettings();
  return {
    ...DEFAULT_CORE_SETTINGS,
    ...(ws.core_settings as CoreSettings),
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
): Promise<{ ok: boolean; error?: string }> {
  try {
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

    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Unknown error" };
  }
}
