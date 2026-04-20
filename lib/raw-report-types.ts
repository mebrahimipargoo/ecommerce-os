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
  // ── Additional report types (raw-archive landing tables) ─────────────────
  "ALL_ORDERS",
  "REPLACEMENTS",
  "FBA_GRADE_AND_RESELL",
  "MANAGE_FBA_INVENTORY",
  "FBA_INVENTORY",
  "INBOUND_PERFORMANCE",
  "AMAZON_FULFILLED_INVENTORY",
  "RESERVED_INVENTORY",
  "FEE_PREVIEW",
  "MONTHLY_STORAGE_FEES",
  "UNKNOWN",
  "CATEGORY_LISTINGS",
  "ALL_LISTINGS",
  "ACTIVE_LISTINGS",
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

/** Listing exports → `catalog_products` via `/api/settings/imports/process` (not staging/sync). */
export const LISTING_REPORT_TYPES = ["CATEGORY_LISTINGS", "ALL_LISTINGS", "ACTIVE_LISTINGS"] as const;

export function isListingReportType(rt: string | null | undefined): boolean {
  const s = String(rt ?? "").trim();
  return (LISTING_REPORT_TYPES as readonly string[]).includes(s);
}
