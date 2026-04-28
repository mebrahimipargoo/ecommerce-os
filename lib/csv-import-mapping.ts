/**
 * CSV header parsing and per-report-type primary column expectations (Amazon-aligned).
 */

import type { RawReportType } from "./raw-report-types";

export function parseCsvHeaderLine(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      inQuote = !inQuote;
    } else if ((c === "," && !inQuote) || c === "\r" || c === "\n") {
      if (cur.length || out.length) out.push(cur.trim());
      cur = "";
      if (c === "\n" || c === "\r") break;
    } else {
      cur += c;
    }
  }
  if (cur.length || out.length) out.push(cur.trim());
  return out.filter((s) => s.length > 0);
}

function norm(h: string): string {
  return h.trim().toLowerCase().replace(/[\s_]+/g, "-");
}

function headerMatchesAlias(header: string, alias: string): boolean {
  return norm(header) === norm(alias);
}

/** Per Amazon file family — primary column we expect + common export spellings. */
export const REPORT_TYPE_SPECS: Record<
  RawReportType,
  {
    canonicalKey: string;
    shortLabel: string;
    description: string;
    aliases: string[];
  }
> = {
  FBA_RETURNS: {
    canonicalKey: "license-plate-number",
    shortLabel: "FBA Returns (detected)",
    description: "LPN + return reason — routed to expected returns",
    aliases: [
      "license-plate-number",
      "return-reason-code",
      "license plate number",
      "LPN",
      "lpn",
    ],
  },
  REMOVAL_ORDER: {
    canonicalKey: "tracking-number",
    shortLabel: "Removal order (detected)",
    description: "Tracking + shipment id — routed to expected packages",
    aliases: ["tracking-number", "shipment-id", "Tracking Number", "Shipment ID"],
  },
  REMOVAL_SHIPMENT: {
    canonicalKey: "tracking-number",
    shortLabel: "Removal shipment (detected)",
    description: "Tracking + carrier — routed to amazon_removals",
    aliases: ["tracking-number", "carrier", "carrier-shipment-date"],
  },
  INVENTORY_LEDGER: {
    canonicalKey: "fnsku",
    shortLabel: "Inventory ledger (detected)",
    description: "Date + FNSKU — routed to product catalog",
    aliases: ["fnsku", "FNSKU", "snapshot-date", "date", "asin", "ASIN"],
  },
  UNKNOWN: {
    canonicalKey: "order-id",
    shortLabel: "Unknown / other",
    description: "Could not classify — set type manually or re-upload",
    aliases: ["order-id", "Order-ID", "description"],
  },
  REIMBURSEMENTS: {
    canonicalKey: "reimbursement-id",
    shortLabel: "Reimbursements (detected)",
    description: "Reimbursement ID + qty — routed to reimbursement records",
    aliases: [
      "reimbursement-id",
      "reimbursement id",
      "approval-date",
      "quantity-reimbursed-total",
      "quantity reimbursed total",
    ],
  },
  SETTLEMENT: {
    canonicalKey: "settlement-id",
    shortLabel: "Settlement (detected)",
    description: "Settlement flat .txt (TSV) or legacy CSV — routed to amazon_settlements",
    aliases: [
      "settlement-id",
      "settlement id",
      "Settlement ID",
      "settlement-start-date",
      "transaction-type",
      "transaction-status",
      "deposit-date",
      "total-amount",
    ],
  },
  SAFET_CLAIMS: {
    canonicalKey: "safe-t-claim-id",
    shortLabel: "SAFE-T Claims (detected)",
    description: "SAFE-T Claim ID + reimbursement amount — routed to claims",
    aliases: [
      "safe-t-claim-id",
      "safe-t claim id",
      "claim-id",
      "reimbursement-amount",
      "reimbursement amount",
    ],
  },
  TRANSACTIONS: {
    canonicalKey: "transaction-type",
    shortLabel: "Transactions (detected)",
    description: "Transaction type + product charges — fee / revenue breakdown",
    aliases: [
      "transaction-type",
      "transaction type",
      "total-product-charges",
      "total product charges",
      "posted-date",
    ],
  },
  REPORTS_REPOSITORY: {
    canonicalKey: "date/time",
    shortLabel: "Reports Repository (transactions CSV)",
    description: "9-line preamble; headers on row 10 — synced to amazon_reports_repository",
    aliases: [
      "date/time",
      "settlement id",
      "type",
      "order id",
      "sku",
      "description",
      "total",
    ],
  },
  PRODUCT_IDENTITY: {
    canonicalKey: "seller_sku",
    shortLabel: "Product Identity CSV",
    description: "Seller SKU identity file — upserts products, catalog_products, and identifier map",
    aliases: ["Seller SKU", "seller sku", "Product Name", "UPC", "Vendor", "Mfg #", "FNSKU", "ASIN"],
  },
  ALL_ORDERS: {
    canonicalKey: "amazon-order-id",
    shortLabel: "All Orders",
    description: "Order lines — staged then synced to amazon_all_orders",
    aliases: [
      "amazon-order-id",
      "amazon order id",
      "purchase-date",
      "purchase date",
      "order-status",
      "order status",
      "fulfillment-channel",
    ],
  },
  REPLACEMENTS: {
    canonicalKey: "replacement-order-id",
    shortLabel: "Replacements",
    description: "Replacement orders — synced to amazon_replacements",
    aliases: [
      "replacement-order-id",
      "replacement order id",
      "original-order-id",
      "original order id",
      "replacement-reason",
    ],
  },
  FBA_GRADE_AND_RESELL: {
    canonicalKey: "fnsku",
    shortLabel: "FBA Grade and Resell",
    description: "Graded units — synced to amazon_fba_grade_and_resell",
    aliases: ["fnsku", "FNSKU", "asin", "grade", "units", "merchant-sku"],
  },
  MANAGE_FBA_INVENTORY: {
    canonicalKey: "fnsku",
    shortLabel: "Manage FBA Inventory",
    description: "AFN quantities — synced to amazon_manage_fba_inventory",
    aliases: [
      "fnsku",
      "FNSKU",
      "afn-fulfillable-quantity",
      "afn fulfillable quantity",
      "afn-warehouse-quantity",
      "afn-inbound-working-quantity",
      "afn-inbound-receiving-quantity",
      "merchant-sku",
      "asin",
    ],
  },
  FBA_INVENTORY: {
    canonicalKey: "fnsku",
    shortLabel: "FBA Inventory (health)",
    description: "Inventory health — synced to amazon_fba_inventory",
    aliases: [
      "fnsku", "FNSKU", "snapshot-date", "snapshot date",
      "available", "asin", "merchant-sku", "sku",
      "inbound-quantity", "inbound-working", "inbound-received",
      "inventory-supply-at-fba", "total-reserved-quantity",
    ],
  },
  INBOUND_PERFORMANCE: {
    canonicalKey: "fba-shipment-id",
    shortLabel: "Inbound Performance",
    description: "Shipment problem report — synced to amazon_inbound_performance",
    aliases: [
      "fba-shipment-id", "fba shipment id",
      "problem-type", "problem type",
      "expected-quantity", "received-quantity", "problem-quantity",
      "fba-carton-id",
    ],
  },
  AMAZON_FULFILLED_INVENTORY: {
    canonicalKey: "seller-sku",
    shortLabel: "Amazon Fulfilled Inventory",
    description: "AFI snapshot — synced to amazon_amazon_fulfilled_inventory",
    aliases: [
      "seller-sku", "seller sku",
      "fulfillment-channel-sku", "fulfillment channel sku",
      "asin", "ASIN",
      "quantity-available", "quantity available",
      "condition-type", "warehouse-condition-code",
    ],
  },
  RESERVED_INVENTORY: {
    canonicalKey: "fnsku",
    shortLabel: "Reserved Inventory",
    description: "Reserved breakdown — synced to amazon_reserved_inventory",
    aliases: [
      "fnsku",
      "FNSKU",
      "reserved-customerorders",
      "reserved-fc-transfers",
      "asin",
    ],
  },
  FEE_PREVIEW: {
    canonicalKey: "sku",
    shortLabel: "Fee Preview",
    description: "Estimated fees — synced to amazon_fee_preview",
    aliases: [
      "sku",
      "estimated-fees",
      "estimated fees",
      "estimated-referral-fee-per-unit",
      "asin",
      "fnsku",
    ],
  },
  MONTHLY_STORAGE_FEES: {
    canonicalKey: "fnsku",
    shortLabel: "Monthly Storage Fees",
    description: "Storage charges — synced to amazon_monthly_storage_fees",
    aliases: [
      "fnsku",
      "FNSKU",
      "monthly-storage-fee",
      "monthly storage fee",
      "average-quantity-charged",
      "asin",
    ],
  },
  CATEGORY_LISTINGS: {
    canonicalKey: "seller-sku",
    shortLabel: "Category Listings (detected)",
    description: "Browse/category + seller SKU + ASIN — upserted to catalog_products",
    aliases: ["seller-sku", "seller sku", "asin1", "asin", "browse node", "item-name"],
  },
  ALL_LISTINGS: {
    canonicalKey: "seller-sku",
    shortLabel: "All Listings (detected)",
    description: "Merchant listing export — upserted to catalog_products",
    aliases: ["seller-sku", "seller sku", "asin1", "asin", "item-name", "item name", "open-date"],
  },
  ACTIVE_LISTINGS: {
    canonicalKey: "seller-sku",
    shortLabel: "Active Listings (detected)",
    description: "Listing status + SKU + ASIN — upserted to catalog_products",
    aliases: ["seller-sku", "seller sku", "asin1", "asin", "item-name", "status"],
  },
  fba_customer_returns: {
    canonicalKey: "license-plate-number",
    shortLabel: "FBA Customer Returns",
    description: "Source for LPN & Reasons",
    aliases: [
      "license-plate-number",
      "license plate number",
      "LPN",
      "lpn",
      "License Plate Number",
      "fulfillment-network-sku",
    ],
  },
  reimbursements: {
    canonicalKey: "order-id",
    shortLabel: "Reimbursements",
    description: "Source for Amazon payouts",
    aliases: [
      "order-id",
      "Order-ID",
      "OrderID",
      "amazon-order-id",
      "case-id",
      "reimbursement-id",
      "approval-date",
    ],
  },
  inventory_ledger: {
    canonicalKey: "fnsku",
    shortLabel: "Inventory Ledger",
    description: "Source for ASIN/FNSKU mapping & Dimensions",
    aliases: [
      "fnsku",
      "FNSKU",
      "sku",
      "MSKU",
      "merchant-sku",
      "asin",
      "ASIN",
    ],
  },
  safe_t_claims: {
    canonicalKey: "claim-id",
    shortLabel: "SAFE-T Claims",
    description: "Source for manual claim status",
    aliases: ["claim-id", "Claim ID", "case-id", "Case ID", "safe-t-id", "SAFE-T ID"],
  },
  transaction_view: {
    canonicalKey: "order-id",
    shortLabel: "Transaction View",
    description: "Source for exact fees and profit/loss",
    aliases: [
      "order-id",
      "Order-ID",
      "OrderID",
      "description",
      "transaction-type",
      "amount-type",
    ],
  },
  settlement_repository: {
    canonicalKey: "settlement-id",
    shortLabel: "Settlement Repository",
    description: "Source for bulk accounting",
    aliases: [
      "settlement-id",
      "Settlement ID",
      "standard-order-id",
      "Standard Order ID",
      "order-id",
      "deposit-date",
    ],
  },
};

export function needsMappingPreviewForType(
  headers: string[],
  reportType: RawReportType,
): boolean {
  if (headers.length === 0) return false;
  const spec = REPORT_TYPE_SPECS[reportType];
  for (const h of headers) {
    for (const a of spec.aliases) {
      if (headerMatchesAlias(h, a)) return false;
    }
  }
  return true;
}

/** Fuzzy suggestion: map canonical key → actual header column. */
export function suggestMappingForType(
  headers: string[],
  reportType: RawReportType,
): Record<string, string> {
  const spec = REPORT_TYPE_SPECS[reportType];
  const mapping: Record<string, string> = {};

  const direct = headers.find((h) => spec.aliases.some((a) => headerMatchesAlias(h, a)));
  if (direct) {
    mapping[spec.canonicalKey] = direct;
    return mapping;
  }

  const key = spec.canonicalKey.toLowerCase();
  const fuzzy =
    headers.find((h) => norm(h).includes(key.replace(/-/g, ""))) ??
    headers.find((h) => {
      const n = norm(h);
      return spec.aliases.some((a) => n.includes(norm(a).replace(/-/g, "")));
    });
  if (fuzzy) {
    mapping[spec.canonicalKey] = fuzzy;
  }
  return mapping;
}
