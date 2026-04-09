import type { RawReportType } from "./raw-report-types";

/** Values written to `raw_report_uploads.report_type` by the header rule engine + GPT fallback. */
export const CLASSIFIED_REPORT_TYPES = [
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
] as const;

/** Used by buildColumnMappingFromHeaders — hyphen-key form for column_mapping JSONB. */
function normHeader(h: string): string {
  return h
    .replace(/^\uFEFF/, "")
    .trim()
    .toLowerCase()
    .replace(/[\s_]+/g, "-");
}

/**
 * Used by classifyCsvHeadersRuleBased — space-based form for human-readable matching.
 * Converts hyphens, underscores, and repeated spaces to a single space, then trims.
 * This handles all Amazon export variants:
 *   "removal-order-id" / "Removal Order ID" / "removal_order_id" → "removal order id"
 *   "SAFE-T Claim ID" / "safe t claim id" / "Safe-T_Claim_ID" → "safe t claim id"
 */
function normForDetection(h: string): string {
  return (h ?? "")
    .replace(/^\uFEFF/, "")
    .toLowerCase()
    .replace(/[-_]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function headerSet(headers: string[]): Set<string> {
  return new Set(headers.map(normHeader));
}

function detectionSet(headers: string[]): Set<string> {
  return new Set(headers.map(normForDetection));
}

/**
 * Per-report-type canonical field definitions — used for auto column_mapping generation
 * and for rendering the manual mapping modal dropdowns.
 */
export type CanonicalField = {
  /** Key stored in `column_mapping` JSONB (underscore style, matches mapper alias). */
  key: string;
  /** Human-readable label shown in the mapping modal. */
  label: string;
  /** Required fields block processing if missing; optional fields skip gracefully. */
  required: boolean;
  /** All known Amazon spellings for auto-detection. */
  aliases: string[];
};

export const CANONICAL_FIELDS_PER_TYPE: Record<string, CanonicalField[]> = {
  FBA_RETURNS: [
    {
      key: "lpn",
      label: "License Plate Number (LPN)",
      required: true,
      aliases: ["license-plate-number", "license plate number", "lpn", "LPN"],
    },
    {
      key: "detailed_disposition",
      label: "Detailed Disposition",
      required: false,
      aliases: ["detailed-disposition", "detailed disposition", "disposition"],
    },
    {
      key: "asin",
      label: "ASIN",
      required: false,
      aliases: ["asin", "ASIN", "product-id", "product id"],
    },
    {
      key: "order_id",
      label: "Order ID",
      required: false,
      aliases: ["order-id", "amazon-order-id", "order id", "amazon order id"],
    },
    {
      key: "return_reason",
      label: "Return Reason Code",
      required: false,
      aliases: ["return-reason-code", "return reason code", "return reason", "reason"],
    },
  ],
  REMOVAL_ORDER: [
    {
      key: "order_id",
      label: "Removal Order ID",
      required: true,
      aliases: [
        "removal-order-id", "removal order id", "removal_order_id",
        "order-id", "order id",
      ],
    },
    {
      key: "sku",
      label: "SKU",
      required: false,
      aliases: ["sku", "SKU", "merchant-sku", "msku"],
    },
    {
      key: "fnsku",
      label: "FNSKU",
      required: false,
      aliases: ["fnsku", "FNSKU", "fulfillment-network-sku"],
    },
    {
      key: "disposition",
      label: "Disposition",
      required: false,
      aliases: ["disposition", "detailed-disposition", "detailed disposition"],
    },
    {
      key: "requested_quantity",
      label: "Requested Quantity",
      required: false,
      aliases: ["requested-quantity", "requested quantity", "quantity", "qty"],
    },
    {
      key: "shipped_quantity",
      label: "Shipped Quantity",
      required: false,
      aliases: ["shipped-quantity", "shipped quantity"],
    },
    {
      key: "disposed_quantity",
      label: "Disposed Quantity",
      required: false,
      aliases: ["disposed-quantity", "disposed quantity"],
    },
    {
      key: "cancelled_quantity",
      label: "Cancelled Quantity",
      required: false,
      aliases: ["cancelled-quantity", "cancelled quantity"],
    },
    {
      key: "status",
      label: "Order Status",
      required: false,
      aliases: ["order-status", "order status", "status"],
    },
    {
      key: "tracking_number",
      label: "Tracking Number",
      required: false,
      aliases: ["tracking-number", "tracking number", "tracking_number", "tracking-id", "tracking id"],
    },
    {
      key: "carrier",
      label: "Carrier",
      required: false,
      aliases: ["carrier", "carrier-name", "carrier name"],
    },
    {
      key: "shipment_date",
      label: "Shipment Date",
      required: false,
      aliases: ["carrier-shipment-date", "shipment-date", "shipment date", "ship-date", "shipped-date"],
    },
  ],
  REMOVAL_SHIPMENT: [
    {
      key: "order_id",
      label: "Removal Order ID",
      required: true,
      aliases: ["removal-order-id", "removal order id", "order-id", "order id"],
    },
    {
      key: "sku",
      label: "SKU",
      required: false,
      aliases: ["sku", "SKU", "merchant-sku", "msku"],
    },
    {
      key: "fnsku",
      label: "FNSKU",
      required: false,
      aliases: ["fnsku", "FNSKU", "fulfillment-network-sku"],
    },
    {
      key: "tracking_number",
      label: "Tracking Number",
      required: false,
      aliases: ["tracking-number", "tracking number", "tracking_number", "tracking-id", "tracking id"],
    },
    {
      key: "carrier",
      label: "Carrier",
      required: false,
      aliases: ["carrier", "carrier-name", "carrier name"],
    },
    {
      key: "shipment_date",
      label: "Shipment Date",
      required: false,
      aliases: ["carrier-shipment-date", "shipment-date", "shipment date", "ship-date", "shipped-date"],
    },
    {
      key: "shipped_quantity",
      label: "Shipped Quantity",
      required: false,
      aliases: ["shipped-quantity", "shipped quantity"],
    },
  ],
  INVENTORY_LEDGER: [
    {
      key: "fnsku",
      label: "FNSKU",
      required: true,
      aliases: ["fnsku", "FNSKU", "fulfillment-network-sku"],
    },
    {
      key: "ending_warehouse_balance",
      label: "Ending Warehouse Balance",
      required: false,
      aliases: ["ending-warehouse-balance", "ending warehouse balance"],
    },
    {
      key: "title",
      label: "Product Title / Name",
      required: false,
      aliases: ["product-name", "product name", "title", "item-name", "item name", "description"],
    },
    {
      key: "date",
      label: "Date / Snapshot Date",
      required: false,
      aliases: ["date", "snapshot-date", "event-date", "Date"],
    },
    {
      key: "asin",
      label: "ASIN",
      required: false,
      aliases: ["asin", "ASIN"],
    },
  ],
  REIMBURSEMENTS: [
    {
      key: "reimbursement_id",
      label: "Reimbursement ID",
      required: true,
      aliases: ["reimbursement-id", "reimbursement id", "reimbursement_id"],
    },
    {
      key: "quantity_reimbursed_total",
      label: "Quantity Reimbursed (Total)",
      required: true,
      aliases: [
        "quantity-reimbursed-total",
        "quantity reimbursed total",
        "quantity-reimbursed-(total)",
      ],
    },
    {
      key: "order_id",
      label: "Order ID",
      required: false,
      aliases: ["order-id", "amazon-order-id", "order id"],
    },
    {
      key: "asin",
      label: "ASIN",
      required: false,
      aliases: ["asin", "ASIN"],
    },
    {
      key: "amount_per_unit",
      label: "Amount Per Unit",
      required: false,
      aliases: ["amount-per-unit", "amount per unit"],
    },
    {
      key: "approval_date",
      label: "Approval Date",
      required: false,
      aliases: ["approval-date", "approval date"],
    },
  ],
  SETTLEMENT: [
    {
      key: "settlement_id",
      label: "Settlement ID",
      required: true,
      aliases: ["settlement-id", "settlement id", "Settlement ID"],
    },
    {
      key: "settlement_start_date",
      label: "Settlement Start Date",
      required: false,
      aliases: ["settlement-start-date", "settlement start date"],
    },
    {
      key: "settlement_end_date",
      label: "Settlement End Date",
      required: false,
      aliases: ["settlement-end-date", "settlement end date"],
    },
    {
      key: "deposit_date",
      label: "Deposit Date",
      required: false,
      aliases: ["deposit-date", "deposit date"],
    },
    {
      key: "total_amount",
      label: "Total Amount (header column)",
      required: false,
      aliases: ["total-amount", "total amount", "total", "amount", "net-proceeds", "net proceeds"],
    },
    {
      key: "currency",
      label: "Currency",
      required: false,
      aliases: ["currency", "Currency"],
    },
    {
      key: "transaction_status",
      label: "Transaction Status / Type (legacy CSV)",
      required: false,
      aliases: [
        "transaction-status",
        "transaction status",
        "transaction-type",
        "transaction type",
      ],
    },
    {
      key: "order_id",
      label: "Order ID",
      required: false,
      aliases: ["order-id", "amazon-order-id", "order id"],
    },
    {
      key: "total",
      label: "Total (legacy key)",
      required: false,
      aliases: ["net-proceeds", "net proceeds"],
    },
  ],
  SAFET_CLAIMS: [
    {
      key: "claim_id",
      label: "SAFE-T Claim ID",
      required: true,
      aliases: [
        "safe-t-claim-id", "safe-t claim id", "safe_t_claim_id",
        "safe t claim id", "claim-id", "claim id", "claim_id",
      ],
    },
    {
      key: "amount",
      label: "Reimbursement Amount",
      required: true,
      aliases: [
        "reimbursement-amount", "reimbursement amount",
        "amount", "reimburse-amount",
      ],
    },
    {
      key: "order_id",
      label: "Order ID",
      required: false,
      aliases: ["order-id", "amazon-order-id", "order id"],
    },
    {
      key: "claim_status",
      label: "Claim Status",
      required: false,
      aliases: ["claim-status", "claim status", "status"],
    },
  ],
  TRANSACTIONS: [
    {
      key: "settlement_id",
      label: "Settlement ID",
      required: false,
      aliases: ["settlement-id", "settlement id", "Settlement ID"],
    },
    {
      key: "transaction_type",
      label: "Transaction Type",
      required: true,
      aliases: ["transaction-type", "transaction type", "type"],
    },
    {
      key: "total_product_charges",
      label: "Total Product Charges",
      required: false,
      aliases: ["total-product-charges", "total product charges"],
    },
    {
      key: "order_id",
      label: "Order ID",
      required: false,
      aliases: ["order-id", "amazon-order-id", "order id"],
    },
    {
      key: "sku",
      label: "SKU",
      required: false,
      aliases: ["sku", "SKU", "merchant-sku", "msku"],
    },
    {
      key: "posted_date",
      label: "Posted Date",
      required: false,
      aliases: ["posted-date", "posted date", "date-time", "date/time"],
    },
    {
      key: "amount",
      label: "Amount",
      required: false,
      aliases: ["amount", "Amount", "total"],
    },
  ],
  REPORTS_REPOSITORY: [
    {
      key: "date_time",
      label: "Date / Time",
      required: true,
      aliases: ["date/time", "date-time", "datetime", "posted-date", "posted date"],
    },
    {
      key: "settlement_id",
      label: "Settlement ID",
      required: false,
      aliases: ["settlement-id", "settlement id", "Settlement ID"],
    },
    {
      key: "transaction_type",
      label: "Type",
      required: true,
      aliases: ["type", "transaction-type", "transaction type"],
    },
    {
      key: "order_id",
      label: "Order ID",
      required: false,
      aliases: ["order-id", "order id", "amazon-order-id", "amazon order id"],
    },
    {
      key: "sku",
      label: "SKU",
      required: false,
      aliases: ["sku", "SKU", "merchant-sku", "msku"],
    },
    {
      key: "description",
      label: "Description",
      required: false,
      aliases: ["description", "Description"],
    },
    {
      key: "total_amount",
      label: "Total",
      required: false,
      aliases: ["total", "Total", "total-amount", "total amount"],
    },
  ],
};

/**
 * Auto-generates `column_mapping` JSONB from actual CSV headers for a classified report type.
 * Returns e.g. `{ "lpn": "license-plate-number", "asin": "asin" }`.
 * Unknown or unmatched fields are omitted; the user can supply them via the mapping modal.
 */
export function buildColumnMappingFromHeaders(
  headers: string[],
  reportType: RawReportType,
): Record<string, string> {
  const fields = CANONICAL_FIELDS_PER_TYPE[reportType];
  if (!fields) return {};
  const mapping: Record<string, string> = {};
  for (const field of fields) {
    const match = headers.find((h) =>
      field.aliases.some((a) => normHeader(h) === normHeader(a)),
    );
    if (match) mapping[field.key] = match;
  }
  return mapping;
}

/**
 * Returns true when any required canonical field for the given report type has no mapping.
 * Used to decide whether to set status → `needs_mapping`.
 */
export function mappingHasRequiredGaps(
  mapping: Record<string, string>,
  reportType: RawReportType,
): boolean {
  const fields = CANONICAL_FIELDS_PER_TYPE[reportType];
  if (!fields) return false;
  return fields.filter((f) => f.required).some((f) => !mapping[f.key]);
}

/**
 * Rule-based CSV header detection — STRICT priority order, no AI involved.
 *
 * Rules applied in this exact order (first match wins):
 *   1. FBA_RETURNS      — contains "license-plate-number" AND "detailed-disposition"
 *   2. REMOVAL_ORDER    — contains "removal-order-id" OR
 *                         ("requested-quantity" AND "disposed-quantity")
 *   3. INVENTORY_LEDGER — contains "fnsku" AND "ending-warehouse-balance"
 *   4. REIMBURSEMENTS   — contains "reimbursement-id" AND "quantity-reimbursed-total"
 *   5. SETTLEMENT       — contains "settlement-id" AND "transaction-status"
 *   6. SAFET_CLAIMS     — contains "safe-t-claim-id" AND "reimbursement-amount"
 *   7. REPORTS_REPOSITORY — Amazon Reports Repository CSV (after 9-line preamble):
 *                           "date/time" + "settlement id" + "type" + "order id" + "sku" +
 *                           "description" + "total"; not the Fee Preview "transaction type" header
 *   8. TRANSACTIONS     — contains "transaction-type" AND "total-product-charges"
 *
 * Returns UNKNOWN only when no rule matches → triggers Mapping Memory → GPT fallback.
 */
/**
 * Rule-based CSV header detection — STRICT priority order, no AI involved.
 *
 * Uses space-based normalization so ALL Amazon export variants match correctly:
 *   "removal-order-id", "Removal Order ID", "removal_order_id" → "removal order id"
 *   "SAFE-T Claim ID", "safe t claim id", "Safe-T_Claim_ID"   → "safe t claim id"
 *
 * Rules (first match wins):
 *   1. FBA_RETURNS      — "license plate number" AND "detailed disposition"
 *   2. REMOVAL_ORDER    — "removal order id" OR ("requested quantity" AND "disposed quantity")
 *   3. INVENTORY_LEDGER — "fnsku" AND "ending warehouse balance"
 *   4. REIMBURSEMENTS   — "reimbursement id" AND "quantity reimbursed total"
 *   5. SETTLEMENT       — "settlement id" AND "transaction status"
 *   6. SAFET_CLAIMS     — "safe t claim id" AND "reimbursement amount"
 *   7. REPORTS_REPOSITORY — "date/time" + "settlement id" + "type" + "order id" + "sku" +
 *                           "description" + "total" (no "transaction type" header)
 *   8. TRANSACTIONS     — "transaction type" AND "total product charges"
 */
/** Same conditions as rule 7 — used to guard settlement flat-file detection. */
export function headersLookLikeReportsRepository(headers: string[]): boolean {
  const ds = detectionSet(headers);
  return (
    ds.has("date/time") &&
    ds.has("settlement id") &&
    ds.has("type") &&
    ds.has("order id") &&
    ds.has("sku") &&
    ds.has("description") &&
    ds.has("total") &&
    !ds.has("transaction type")
  );
}

export function classifyCsvHeadersRuleBased(headers: string[]): {
  reportType: RawReportType;
  matchedRule: string;
} {
  // Space-based set for matching — handles any combination of hyphens/underscores/spaces
  const ds = detectionSet(headers);

  // Rule 1: FBA Customer Returns
  if (ds.has("license plate number") && ds.has("detailed disposition")) {
    return { reportType: "FBA_RETURNS", matchedRule: "license plate number+detailed disposition" };
  }

  // Rule 2a: Removal Shipment Detail (tracking-number + carrier or carrier-shipment-date)
  // Must come BEFORE the generic Removal Order rule — a Shipment file contains order-id
  // but does NOT have removal-order-id, requested-quantity, or disposed-quantity.
  if (
    ds.has("tracking number") &&
    (ds.has("carrier") || ds.has("carrier shipment date") || ds.has("shipment date"))
  ) {
    return { reportType: "REMOVAL_SHIPMENT", matchedRule: "tracking number+carrier/shipment-date" };
  }

  // Rule 2b: Removal Order Detail
  if (
    ds.has("removal order id") ||
    (ds.has("requested quantity") && ds.has("disposed quantity"))
  ) {
    return {
      reportType: "REMOVAL_ORDER",
      matchedRule: ds.has("removal order id")
        ? "removal order id"
        : "requested quantity+disposed quantity",
    };
  }

  // Rule 3: Inventory Ledger
  if (ds.has("fnsku") && ds.has("ending warehouse balance")) {
    return { reportType: "INVENTORY_LEDGER", matchedRule: "fnsku+ending warehouse balance" };
  }

  // Rule 4: Reimbursements
  if (ds.has("reimbursement id") && ds.has("quantity reimbursed total")) {
    return {
      reportType: "REIMBURSEMENTS",
      matchedRule: "reimbursement id+quantity reimbursed total",
    };
  }

  // Rule 5: Settlement report
  // Guard: Reports Repository CSVs include "Transaction Status" as an extra column but
  // are NOT settlement reports — check Rule 7 fingerprint before committing to SETTLEMENT.
  if (
    ds.has("settlement id") &&
    ds.has("transaction status") &&
    !headersLookLikeReportsRepository(headers)
  ) {
    return { reportType: "SETTLEMENT", matchedRule: "settlement id+transaction status" };
  }

  // Rule 6: SAFE-T Claims
  // Flexible: any header containing "safe" + any header containing "claim"
  // catches Amazon variants: "SAFE-T Claim ID", "SafeT-Claim-Id", etc.
  {
    const headerList = [...ds];
    const hasSafeTClaimId =
      ds.has("safe t claim id") ||
      headerList.some((h) => h.includes("safe") && h.includes("claim"));
    const hasReimbursement = headerList.some((h) => h.includes("reimbursement") || h.includes("amount"));
    if (hasSafeTClaimId && hasReimbursement) {
      return { reportType: "SAFET_CLAIMS", matchedRule: "safe+claim+reimbursement (flexible)" };
    }
  }

  // Rule 7: Amazon Reports Repository transaction CSV (dynamic preamble; header row detected in Phase 1)
  if (headersLookLikeReportsRepository(headers)) {
    return {
      reportType: "REPORTS_REPOSITORY",
      matchedRule:
        "Reports Repository CSV: date/time + settlement id + type + order id + sku + description + total (no Fee Preview transaction type header)",
    };
  }

  // Rule 8: Transactions / Fee Preview (standard format)
  if (ds.has("transaction type") && ds.has("total product charges")) {
    return { reportType: "TRANSACTIONS", matchedRule: "transaction type+total product charges" };
  }

  // Rule 8b: Amazon settlement flat-file detail report (.txt TSV).
  //
  // Columns are hyphenated: settlement-id, transaction-type, order-id, total-amount,
  // price-amount, item-related-fee-amount, etc.  (NOT the Fee Preview "total product charges".)
  //
  // detectionSet entries are normForDetection(header) — e.g. "transaction type", "total amount".
  // Guard: classic SETTLEMENT reports use "transaction status"; this flat file uses "transaction type".
  const hasAmountLikeHeader = headers.some((h) => {
    const n = normForDetection(h);
    return (
      n === "total amount" ||
      n === "currency" ||
      (n.includes("amount") && !n.includes("quantity") && !n.includes("reimbursement"))
    );
  });
  if (
    ds.has("settlement id") &&
    ds.has("transaction type") &&
    ds.has("order id") &&
    hasAmountLikeHeader &&
    !ds.has("transaction status") &&
    !headersLookLikeReportsRepository(headers)
  ) {
    return {
      reportType: "SETTLEMENT",
      matchedRule:
        "Amazon settlement flat .txt TSV → SETTLEMENT (settlement id + transaction type + order id + amount columns); does not use TRANSACTIONS CSV path",
    };
  }

  // No rule matched — Mapping Memory → GPT fallback will run
  return { reportType: "UNKNOWN", matchedRule: "none" };
}

export function parseGptReportType(raw: string): RawReportType {
  const u = raw.toUpperCase();
  if (/\bFBA_RETURNS\b/.test(u)) return "FBA_RETURNS";
  if (/\bREMOVAL_ORDER\b/.test(u)) return "REMOVAL_ORDER";
  if (/\bINVENTORY_LEDGER\b/.test(u)) return "INVENTORY_LEDGER";
  if (/\bREIMBURSEMENTS\b/.test(u)) return "REIMBURSEMENTS";
  if (/\bSETTLEMENT\b/.test(u)) return "SETTLEMENT";
  if (/\bSAFET_CLAIMS\b/.test(u)) return "SAFET_CLAIMS";
  if (/\bTRANSACTIONS\b/.test(u)) return "TRANSACTIONS";
  if (/\bREPORTS_REPOSITORY\b/.test(u)) return "REPORTS_REPOSITORY";
  return "UNKNOWN";
}
