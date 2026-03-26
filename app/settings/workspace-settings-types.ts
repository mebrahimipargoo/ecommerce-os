// ─── Shared types & constants (usable in both server and client) ─────────────
// Keep in a separate file so client components can import without hitting
// the "server-only" boundary in workspace-settings-actions.ts.

export interface InventoryModuleConfig {
  fefo_critical_days: number;
  fefo_warning_days:  number;
}

/**
 * Tenant white-label / branding fields stored in core_settings JSONB.
 * Used for report generation and custom dashboard branding.
 */
export interface CoreSettings {
  company_name?:     string;
  company_logo_url?: string;
  [key: string]: unknown;
}

/**
 * Placeholder for custom table column preferences — populated by the
 * Reports & Analytics module when users configure their column layouts.
 */
export interface ReportsModuleConfig {
  [key: string]: unknown;
}

export interface ModuleConfigs {
  inventory?: InventoryModuleConfig;
  reports?:   ReportsModuleConfig;
  [key: string]: unknown;
}

export interface WorkspaceSettings {
  id?:            string;
  core_settings:  CoreSettings;
  module_configs: ModuleConfigs;
}

export const DEFAULT_FEFO: InventoryModuleConfig = {
  fefo_critical_days: 30,
  fefo_warning_days:  90,
};

export const DEFAULT_CORE_SETTINGS: CoreSettings = {
  company_name:     "",
  company_logo_url: "",
};

export const DEFAULT_WORKSPACE_SETTINGS: WorkspaceSettings = {
  core_settings:  DEFAULT_CORE_SETTINGS,
  module_configs: { inventory: DEFAULT_FEFO, reports: {} },
};
