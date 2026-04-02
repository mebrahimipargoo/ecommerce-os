/**
 * Central table names for admin workspace code paths.
 * Update here when renaming Supabase tables.
 */
export const DB_TABLES = {
  profiles: "profiles",
  companies: "companies",
  organizationSettings: "organization_settings",
  rawReportUploads: "raw_report_uploads",
  amazonLedgerStaging: "amazon_ledger_staging",
} as const;
