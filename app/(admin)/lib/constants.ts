/**
 * Central table names for admin workspace code paths.
 * Update here when renaming Supabase tables.
 */
export const DB_TABLES = {
  profiles: "profiles",
  stores: "stores",
  organizationSettings: "organization_settings",
  rawReportUploads: "raw_report_uploads",
  /** Phase 2 staging table (renamed from amazon_ledger_staging in 20260430 migration). */
  amazonLedgerStaging: "amazon_staging",

  // ── Amazon domain tables (amazon_ prefix standard, all have raw_data JSONB) ──
  amazonReturns: "amazon_returns",
  amazonRemovals: "amazon_removals",
  amazonInventoryLedger: "amazon_inventory_ledger",
  amazonReimbursements: "amazon_reimbursements",
  amazonSettlements: "amazon_settlements",
  amazonSafetClaims: "amazon_safet_claims",
  amazonTransactions: "amazon_transactions",
} as const;

/** Storage bucket for raw report CSV files. */
export const RAW_REPORTS_BUCKET = "raw-reports";

/** Column list for `raw_report_uploads` select queries (technical tracking lives in `metadata` JSONB). */
export const RAW_REPORT_UPLOADS_SELECT =
  "id, organization_id, file_name, report_type, status, column_mapping, metadata, created_at, updated_at, created_by";
