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
  "PRODUCT_IDENTITY",
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
  PRODUCT_IDENTITY: [
    {
      key: "seller_sku",
      label: "Seller SKU",
      required: true,
      aliases: ["Seller SKU", "seller-sku", "seller sku", "sku", "MSKU", "msku"],
    },
    {
      key: "product_name",
      label: "Product Name",
      required: false,
      aliases: ["Product Name", "product-name", "item-name", "item name", "title", "description"],
    },
    {
      key: "vendor",
      label: "Vendor",
      required: false,
      aliases: ["Vendor", "vendor", "vendor-name", "vendor name", "brand"],
    },
    {
      key: "mfg_part_number",
      label: "Mfg #",
      required: false,
      aliases: ["Mfg #", "Mfg#", "Mfg No", "Mfg Number", "Manufacturer Part Number", "mfg_part_number"],
    },
    {
      key: "upc",
      label: "UPC",
      required: false,
      aliases: ["UPC", "upc", "UPC Code", "upc_code", "barcode", "gtin"],
    },
    {
      key: "fnsku",
      label: "FNSKU",
      required: false,
      aliases: ["FNSKU", "fnsku", "fulfillment-network-sku", "fulfillment channel sku"],
    },
    {
      key: "asin",
      label: "ASIN",
      required: false,
      aliases: ["ASIN", "asin", "asin1", "product-id", "product id"],
    },
  ],
  CATEGORY_LISTINGS: [
    {
      key: "seller_sku",
      label: "Seller SKU",
      required: true,
      aliases: ["seller-sku", "seller sku", "sku", "SKU"],
    },
    {
      key: "asin",
      label: "ASIN",
      required: true,
      aliases: ["asin1", "asin", "ASIN", "product-id", "product id"],
    },
    {
      key: "item_name",
      label: "Item name",
      required: false,
      aliases: ["item-name", "item name", "product-name", "product name", "title"],
    },
    {
      key: "item_description",
      label: "Item description",
      required: false,
      aliases: ["item-description", "item description"],
    },
    {
      key: "fulfillment_channel",
      label: "Fulfillment channel",
      required: false,
      aliases: ["fulfillment-channel", "fulfillment channel", "fulfilment-channel"],
    },
    {
      key: "listing_status",
      label: "Status",
      required: false,
      aliases: ["status", "listing-status", "listing status"],
    },
    {
      key: "price",
      label: "Price",
      required: false,
      aliases: ["price", "your-price", "your price"],
    },
    {
      key: "quantity",
      label: "Quantity",
      required: false,
      aliases: ["quantity", "qty", "available"],
    },
    {
      key: "open_date",
      label: "Open date",
      required: false,
      aliases: ["open-date", "open date", "open_date", "listing-created-date"],
    },
    {
      key: "fnsku",
      label: "FNSKU",
      required: false,
      aliases: ["fnsku", "FNSKU", "fulfillment-network-sku"],
    },
  ],
  ALL_LISTINGS: [
    {
      key: "seller_sku",
      label: "Seller SKU",
      required: true,
      aliases: ["seller-sku", "seller sku", "sku", "SKU"],
    },
    {
      key: "asin",
      label: "ASIN",
      required: true,
      aliases: ["asin1", "asin", "ASIN", "product-id", "product id"],
    },
    {
      key: "item_name",
      label: "Item name",
      required: false,
      aliases: ["item-name", "item name", "product-name", "product name", "title"],
    },
    {
      key: "item_description",
      label: "Item description",
      required: false,
      aliases: ["item-description", "item description"],
    },
    {
      key: "fulfillment_channel",
      label: "Fulfillment channel",
      required: false,
      aliases: ["fulfillment-channel", "fulfillment channel", "fulfilment-channel"],
    },
    {
      key: "listing_status",
      label: "Status",
      required: false,
      aliases: ["status", "listing-status", "listing status"],
    },
    {
      key: "price",
      label: "Price",
      required: false,
      aliases: ["price", "your-price", "your price"],
    },
    {
      key: "quantity",
      label: "Quantity",
      required: false,
      aliases: ["quantity", "qty", "available"],
    },
    {
      key: "open_date",
      label: "Open date",
      required: false,
      aliases: ["open-date", "open date", "open_date", "listing-created-date"],
    },
    {
      key: "fnsku",
      label: "FNSKU",
      required: false,
      aliases: ["fnsku", "FNSKU", "fulfillment-network-sku"],
    },
  ],
  ACTIVE_LISTINGS: [
    {
      key: "seller_sku",
      label: "Seller SKU",
      required: true,
      aliases: ["seller-sku", "seller sku", "sku", "SKU"],
    },
    {
      key: "asin",
      label: "ASIN",
      required: true,
      aliases: ["asin1", "asin", "ASIN", "product-id", "product id"],
    },
    {
      key: "item_name",
      label: "Item name",
      required: false,
      aliases: ["item-name", "item name", "product-name", "product name", "title"],
    },
    {
      key: "item_description",
      label: "Item description",
      required: false,
      aliases: ["item-description", "item description"],
    },
    {
      key: "fulfillment_channel",
      label: "Fulfillment channel",
      required: false,
      aliases: ["fulfillment-channel", "fulfillment channel", "fulfilment-channel"],
    },
    {
      key: "listing_status",
      label: "Status",
      required: false,
      aliases: ["status", "listing-status", "listing status"],
    },
    {
      key: "price",
      label: "Price",
      required: false,
      aliases: ["price", "your-price", "your price"],
    },
    {
      key: "quantity",
      label: "Quantity",
      required: false,
      aliases: ["quantity", "qty", "available"],
    },
    {
      key: "open_date",
      label: "Open date",
      required: false,
      aliases: ["open-date", "open date", "open_date", "listing-created-date"],
    },
    {
      key: "fnsku",
      label: "FNSKU",
      required: false,
      aliases: ["fnsku", "FNSKU", "fulfillment-network-sku"],
    },
  ],
  MANAGE_FBA_INVENTORY: [
    { key: "fnsku", label: "FNSKU", required: true,
      aliases: ["fnsku", "FNSKU", "fulfillment-network-sku"] },
    { key: "afn_fulfillable_quantity", label: "AFN Fulfillable Quantity", required: true,
      aliases: ["afn-fulfillable-quantity", "afn fulfillable quantity"] },
    { key: "sku", label: "SKU (merchant)", required: false,
      aliases: ["sku", "SKU", "merchant-sku", "msku"] },
    { key: "asin", label: "ASIN", required: false, aliases: ["asin", "ASIN"] },
    { key: "product_name", label: "Product Name", required: false,
      aliases: ["product-name", "product name", "title"] },
    { key: "condition", label: "Condition", required: false,
      aliases: ["condition", "item-condition"] },
    { key: "your_price", label: "Your Price", required: false,
      aliases: ["your-price", "your price", "price"] },
    { key: "afn_warehouse_quantity", label: "AFN Warehouse Quantity", required: false,
      aliases: ["afn-warehouse-quantity", "afn warehouse quantity"] },
    { key: "afn_unsellable_quantity", label: "AFN Unsellable Quantity", required: false,
      aliases: ["afn-unsellable-quantity", "afn unsellable quantity"] },
    { key: "afn_reserved_quantity", label: "AFN Reserved Quantity", required: false,
      aliases: ["afn-reserved-quantity", "afn reserved quantity"] },
    { key: "afn_total_quantity", label: "AFN Total Quantity", required: false,
      aliases: ["afn-total-quantity", "afn total quantity"] },
    { key: "afn_inbound_working_quantity", label: "AFN Inbound Working", required: false,
      aliases: ["afn-inbound-working-quantity", "afn inbound working quantity"] },
    { key: "afn_inbound_shipped_quantity", label: "AFN Inbound Shipped", required: false,
      aliases: ["afn-inbound-shipped-quantity", "afn inbound shipped quantity"] },
    { key: "afn_inbound_receiving_quantity", label: "AFN Inbound Receiving", required: false,
      aliases: ["afn-inbound-receiving-quantity", "afn inbound receiving quantity"] },
  ],
  FBA_INVENTORY: [
    { key: "fnsku", label: "FNSKU", required: true,
      aliases: ["fnsku", "FNSKU", "fulfillment-network-sku"] },
    { key: "available", label: "Available", required: true,
      aliases: ["available", "afn-fulfillable-quantity"] },
    { key: "snapshot_date", label: "Snapshot Date", required: false,
      aliases: ["snapshot-date", "snapshot date", "Snapshot-Date"] },
    { key: "sku", label: "SKU (merchant)", required: false,
      aliases: ["sku", "SKU", "merchant-sku", "msku"] },
    { key: "asin", label: "ASIN", required: false, aliases: ["asin", "ASIN"] },
    { key: "product_name", label: "Product Name", required: false,
      aliases: ["product-name", "product name", "title"] },
    { key: "condition", label: "Condition", required: false,
      aliases: ["condition", "item-condition"] },
    { key: "inbound_quantity", label: "Inbound Quantity", required: false,
      aliases: ["inbound-quantity", "inbound quantity"] },
    { key: "inbound_working", label: "Inbound Working", required: false,
      aliases: ["inbound-working", "inbound working"] },
    { key: "inbound_received", label: "Inbound Received", required: false,
      aliases: ["inbound-received", "inbound received"] },
    { key: "inventory_supply_at_fba", label: "Inventory Supply at FBA", required: false,
      aliases: ["inventory-supply-at-fba", "inventory supply at fba"] },
    { key: "total_reserved_quantity", label: "Total Reserved Quantity", required: false,
      aliases: ["total-reserved-quantity", "total reserved quantity"] },
  ],
  INBOUND_PERFORMANCE: [
    { key: "fba_shipment_id", label: "FBA Shipment ID", required: true,
      aliases: ["fba-shipment-id", "fba shipment id", "shipment-id", "shipment id"] },
    { key: "problem_type", label: "Problem Type", required: true,
      aliases: ["problem-type", "problem type"] },
    { key: "fba_carton_id", label: "FBA Carton ID", required: false,
      aliases: ["fba-carton-id", "fba carton id", "carton-id"] },
    { key: "expected_quantity", label: "Expected Quantity", required: false,
      aliases: ["expected-quantity", "expected quantity"] },
    { key: "received_quantity", label: "Received Quantity", required: false,
      aliases: ["received-quantity", "received quantity"] },
    { key: "problem_quantity", label: "Problem Quantity", required: false,
      aliases: ["problem-quantity", "problem quantity"] },
    { key: "sku", label: "SKU", required: false,
      aliases: ["sku", "SKU", "merchant-sku", "msku"] },
    { key: "fnsku", label: "FNSKU", required: false,
      aliases: ["fnsku", "FNSKU", "fulfillment-network-sku"] },
    { key: "asin", label: "ASIN", required: false, aliases: ["asin", "ASIN"] },
  ],
  AMAZON_FULFILLED_INVENTORY: [
    { key: "seller_sku", label: "Seller SKU", required: true,
      aliases: ["seller-sku", "seller sku", "sku", "SKU"] },
    { key: "fulfillment_channel_sku", label: "Fulfillment Channel SKU", required: true,
      aliases: ["fulfillment-channel-sku", "fulfillment channel sku", "fnsku"] },
    { key: "asin", label: "ASIN", required: true,
      aliases: ["asin", "ASIN"] },
    { key: "quantity_available", label: "Quantity Available", required: true,
      aliases: ["quantity-available", "quantity available"] },
    { key: "condition_type", label: "Condition Type", required: false,
      aliases: ["condition-type", "condition type", "condition"] },
    { key: "warehouse_condition_code", label: "Warehouse Condition Code", required: false,
      aliases: ["warehouse-condition-code", "warehouse condition code"] },
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

  // Rule 8a: Product Identity CSV (custom item identity import).
  // Must run before listing rules because it also has seller sku + ASIN, but
  // its vendor / mfg / UPC fingerprint routes directly to identity tables.
  {
    const headerList = [...ds];
    const hasSellerSku = ds.has("seller sku") || ds.has("sku") || ds.has("msku");
    const hasProductName = ds.has("product name") || ds.has("item name") || ds.has("title");
    const hasIdentityFingerprint =
      ds.has("upc") ||
      ds.has("upc code") ||
      ds.has("vendor") ||
      headerList.some((h) => h === "mfg #" || h === "mfg#" || h.includes("manufacturer part") || h.startsWith("mfg "));
    if (hasSellerSku && hasProductName && hasIdentityFingerprint) {
      return {
        reportType: "PRODUCT_IDENTITY",
        matchedRule: "seller sku+product name+vendor/mfg/upc identity columns",
      };
    }
  }

  // ── Rule INV-A: Manage FBA Inventory (AFN) ──────────────────────────────
  // Anchor: fnsku + afn-fulfillable-quantity, plus at least one AFN flow column.
  // Must run BEFORE listing rules so AFN exports never collapse into ALL/ACTIVE LISTINGS.
  if (
    ds.has("fnsku") &&
    ds.has("afn fulfillable quantity") &&
    (
      ds.has("afn warehouse quantity") ||
      ds.has("afn inbound working quantity") ||
      ds.has("afn inbound receiving quantity")
    )
  ) {
    return {
      reportType: "MANAGE_FBA_INVENTORY",
      matchedRule: "fnsku+afn fulfillable quantity+afn warehouse/inbound flow",
    };
  }

  // ── Rule INV-B: FBA Inventory (Health) ──────────────────────────────────
  // Anchor: snapshot-date + fnsku + available, plus at least one inbound/supply column.
  if (
    ds.has("snapshot date") &&
    ds.has("fnsku") &&
    ds.has("available") &&
    (
      ds.has("inbound quantity") ||
      ds.has("inbound working") ||
      ds.has("inbound received") ||
      ds.has("inventory supply at fba") ||
      ds.has("total reserved quantity")
    )
  ) {
    return {
      reportType: "FBA_INVENTORY",
      matchedRule: "snapshot date+fnsku+available+inbound/supply column",
    };
  }

  // ── Rule INV-C: Inbound Performance ─────────────────────────────────────
  // Anchor: fba-shipment-id + problem-type, plus at least one quantity / carton column.
  if (
    ds.has("fba shipment id") &&
    ds.has("problem type") &&
    (
      ds.has("expected quantity") ||
      ds.has("received quantity") ||
      ds.has("problem quantity") ||
      ds.has("fba carton id")
    )
  ) {
    return {
      reportType: "INBOUND_PERFORMANCE",
      matchedRule: "fba shipment id+problem type+quantity/carton column",
    };
  }

  // ── Rule INV-D: Amazon Fulfilled Inventory ──────────────────────────────
  // Anchor: seller-sku + fulfillment-channel-sku + asin + quantity available.
  // MUST run BEFORE listing rules so this never falls into ALL_LISTINGS / ACTIVE_LISTINGS.
  if (
    ds.has("seller sku") &&
    ds.has("fulfillment channel sku") &&
    ds.has("asin") &&
    ds.has("quantity available")
  ) {
    return {
      reportType: "AMAZON_FULFILLED_INVENTORY",
      matchedRule:
        "seller sku+fulfillment channel sku+asin+quantity available (Amazon Fulfilled Inventory; not a listing export)",
    };
  }

  // Rule 9: Category Listings Report — browse / category columns plus seller SKU + ASIN
  if (
    ds.has("seller sku") &&
    (ds.has("asin1") || ds.has("asin")) &&
    (ds.has("browse node") || ds.has("product category") || ds.has("browse tree"))
  ) {
    return {
      reportType: "CATEGORY_LISTINGS",
      matchedRule: "seller sku+asin+browse node or product category",
    };
  }

  // Rule 10: Active Listings — status + item name + identity (no browse node fingerprint)
  if (
    ds.has("seller sku") &&
    (ds.has("asin1") || ds.has("asin")) &&
    ds.has("item name") &&
    ds.has("status") &&
    !ds.has("browse node")
  ) {
    return {
      reportType: "ACTIVE_LISTINGS",
      matchedRule: "seller sku+asin+item name+status (no browse node)",
    };
  }

  // Rule 11: All Listings Report — broad merchant listing export
  if (
    ds.has("seller sku") &&
    (ds.has("asin1") || ds.has("asin")) &&
    (ds.has("item name") || ds.has("open date") || ds.has("product name"))
  ) {
    return {
      reportType: "ALL_LISTINGS",
      matchedRule: "seller sku+asin+item name or open date (all listings superset)",
    };
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
  if (/\bREMOVAL_SHIPMENT\b/.test(u)) return "REMOVAL_SHIPMENT";
  if (/\bINVENTORY_LEDGER\b/.test(u)) return "INVENTORY_LEDGER";
  if (/\bREIMBURSEMENTS\b/.test(u)) return "REIMBURSEMENTS";
  if (/\bSETTLEMENT\b/.test(u)) return "SETTLEMENT";
  if (/\bSAFET_CLAIMS\b/.test(u)) return "SAFET_CLAIMS";
  if (/\bTRANSACTIONS\b/.test(u)) return "TRANSACTIONS";
  if (/\bREPORTS_REPOSITORY\b/.test(u)) return "REPORTS_REPOSITORY";
  if (/\bPRODUCT_IDENTITY(?:_CSV)?\b/.test(u)) return "PRODUCT_IDENTITY";
  if (/\bALL_ORDERS\b/.test(u)) return "ALL_ORDERS";
  if (/\bREPLACEMENTS\b/.test(u)) return "REPLACEMENTS";
  if (/\bFBA_GRADE_AND_RESELL\b/.test(u)) return "FBA_GRADE_AND_RESELL";
  if (/\bMANAGE_FBA_INVENTORY\b/.test(u)) return "MANAGE_FBA_INVENTORY";
  if (/\bFBA_INVENTORY\b/.test(u)) return "FBA_INVENTORY";
  if (/\bINBOUND_PERFORMANCE\b/.test(u)) return "INBOUND_PERFORMANCE";
  if (/\bAMAZON_FULFILLED_INVENTORY\b/.test(u)) return "AMAZON_FULFILLED_INVENTORY";
  if (/\bRESERVED_INVENTORY\b/.test(u)) return "RESERVED_INVENTORY";
  if (/\bFEE_PREVIEW\b/.test(u)) return "FEE_PREVIEW";
  if (/\bMONTHLY_STORAGE_FEES\b/.test(u)) return "MONTHLY_STORAGE_FEES";
  if (/\bCATEGORY_LISTINGS\b/.test(u)) return "CATEGORY_LISTINGS";
  if (/\bALL_LISTINGS\b/.test(u)) return "ALL_LISTINGS";
  if (/\bACTIVE_LISTINGS\b/.test(u)) return "ACTIVE_LISTINGS";
  return "UNKNOWN";
}
