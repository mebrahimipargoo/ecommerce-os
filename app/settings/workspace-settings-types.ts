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
  /** Primary logo URL for PDFs and UI (alias: `logo_url`). */
  company_logo_url?: string;
  /** Alternate key used in some workspace JSON payloads — treated like `company_logo_url`. */
  logo_url?: string;
  [key: string]: unknown;
}

/**
 * Placeholder for custom table column preferences — populated by the
 * Reports & Analytics module when users configure their column layouts.
 */
export interface ReportsModuleConfig {
  [key: string]: unknown;
}

/** Autonomous claim agent toggles (stored in `module_configs.claim_agent_config` JSONB). */
export interface ClaimAgentConfig {
  /** Default ON — auto-generate PDF reports for ready-for-claim items. */
  auto_generate_pdf_reports?: boolean;
  /** Default OFF — allow agent to file directly to marketplace. */
  allow_agent_direct_submit?: boolean;
  /** Upper bound (USD) for automated submission when direct submit is enabled. */
  max_auto_submit_amount_usd?: number;
  /** Default OFF — agent may auto-submit small claims (≤ $50) when policy allows. */
  autonomous_claim_submission_0_50_usd?: boolean;
  /** Default ON — bulk marketplace submission requires explicit approval in UI. */
  require_manual_approval_bulk_submission?: boolean;

  /** Logistics: periodic sync of `ready_for_claim` returns → `claim_submissions` (UI/cron hint). */
  logistics_background_sync_enabled?: boolean;
  /** Hours between background sync runs (e.g. 2). */
  logistics_sync_interval_hours?: number;
}

/** PIM / catalog integrations (e.g. Google Sheets ID read by FastAPI `etl/sync-google-sheets`). */
export interface CatalogModuleConfig {
  google_sheet_id?: string;
}

export interface ModuleConfigs {
  inventory?: InventoryModuleConfig;
  reports?:   ReportsModuleConfig;
  claim_agent_config?: ClaimAgentConfig;
  /** Optional: `google_sheet_id` for ETL Google Sheets sync. */
  catalog?: CatalogModuleConfig;
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

export const DEFAULT_CLAIM_AGENT_CONFIG: ClaimAgentConfig = {
  auto_generate_pdf_reports: true,
  allow_agent_direct_submit: false,
  max_auto_submit_amount_usd: 500,
  autonomous_claim_submission_0_50_usd: false,
  require_manual_approval_bulk_submission: true,
  logistics_background_sync_enabled: false,
  logistics_sync_interval_hours: 2,
};

export const DEFAULT_WORKSPACE_SETTINGS: WorkspaceSettings = {
  core_settings:  DEFAULT_CORE_SETTINGS,
  module_configs: { inventory: DEFAULT_FEFO, reports: {}, claim_agent_config: DEFAULT_CLAIM_AGENT_CONFIG },
};
