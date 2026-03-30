/** Stored in `raw_report_uploads.report_type` — aligned with Amazon report families. */

/**
 * Canonical DB values (CHECK on `raw_report_uploads.report_type`).
 * User-facing labels map roughly to: returns, reimbursements, inventory, safe-t, transactions, settlement.
 */
export const RAW_REPORT_TYPES = [
  "fba_customer_returns",
  "reimbursements",
  "inventory_ledger",
  "safe_t_claims",
  "transaction_view",
  "settlement_repository",
] as const;

export type RawReportType = (typeof RAW_REPORT_TYPES)[number];

export const RAW_REPORT_TYPE_ORDER: RawReportType[] = [...RAW_REPORT_TYPES];
