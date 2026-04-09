/** Stored in `raw_report_uploads.report_type` — smart-import canonical + legacy Amazon slugs. */

/**
 * Canonical DB values (CHECK on `raw_report_uploads.report_type`).
 *
 * Smart-import canonical types (written by header classification):
 *   FBA_RETURNS | REMOVAL_ORDER | INVENTORY_LEDGER |
 *   REIMBURSEMENTS | SETTLEMENT | SAFET_CLAIMS | TRANSACTIONS | REPORTS_REPOSITORY | UNKNOWN
 *
 * Legacy rows may use fba_customer_returns, inventory_ledger, safe_t_claims, etc.
 */
export const RAW_REPORT_TYPES = [
  // ── Smart-import canonical (rule-based / AI detected) ────────────────────
  "FBA_RETURNS",
  "REMOVAL_ORDER",
  "REMOVAL_SHIPMENT",
  "INVENTORY_LEDGER",
  "REIMBURSEMENTS",
  "SETTLEMENT",
  "SAFET_CLAIMS",
  "TRANSACTIONS",
  "REPORTS_REPOSITORY",
  "UNKNOWN",
  // ── Legacy slugs (kept for backward-compat with older rows) ───────────────
  "fba_customer_returns",
  "reimbursements",
  "inventory_ledger",
  "safe_t_claims",
  "transaction_view",
  "settlement_repository",
] as const;

export type RawReportType = (typeof RAW_REPORT_TYPES)[number];

export const RAW_REPORT_TYPE_ORDER: RawReportType[] = [...RAW_REPORT_TYPES];
