import { isUuidString } from "./uuid";

/**
 * Map CSV rows to amazon_ domain tables for the Phase-3 Sync pipeline.
 *
 * ── JSONB Packing Standard ────────────────────────────────────────────────────
 *
 *  NATIVE_COLUMNS_<TABLE>  — the exact physical columns that exist in the DB.
 *  packPayloadForSupabase() — call this BEFORE every supabase.upsert():
 *    • Keys that ARE in NATIVE_COLUMNS → kept at the root level.
 *    • Keys that are NOT in NATIVE_COLUMNS → merged into raw_data JSONB.
 *
 *  This guarantees that even if a mapper accidentally emits an unknown key, it
 *  never reaches Supabase as a root column → permanently prevents
 *  "could not find column X in schema cache" errors.
 *
 * ── SAFE-T Hard-coded overrides ───────────────────────────────────────────────
 *   CSV "Reimbursement Amount" → DB `total_reimbursement_amount`
 *   CSV "SAFE-T Claim ID"      → DB `safet_claim_id`
 */

function normKey(s: string): string {
  return s.replace(/^\uFEFF/, "").trim().toLowerCase().replace(/[\s_]+/g, "-");
}

/** Resolve a cell when CSV headers use hyphens but keys were normalized to underscores (or vice versa). */
function resolveMappedCell(row: Record<string, string>, csvHeader: string): string | undefined {
  const h = String(csvHeader).trim().replace(/^\uFEFF/, "");
  if (h === "") return undefined;
  if (row[h] !== undefined) return row[h];
  const unders = h.replace(/-/g, "_");
  if (row[unders] !== undefined) return row[unders];
  const hyph = h.replace(/_/g, "-");
  if (row[hyph] !== undefined) return row[hyph];
  return undefined;
}

/**
 * Amazon .txt flat files use hyphenated headers (order-id). Normalizing keys to
 * snake_case (order_id) keeps staging rows and mappers aligned.
 */
export function normalizeAmazonReportRowKeys(row: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(row)) {
    const clean = k.replace(/^\uFEFF/, "").trim().replace(/-/g, "_");
    out[clean] = v;
  }
  return out;
}

/**
 * Applies user-verified `column_mapping` onto a raw CSV row before the fuzzy
 * alias matchers run.
 */
export function applyColumnMappingToRow(
  row: Record<string, string>,
  columnMapping: Record<string, string> | null | undefined,
): Record<string, string> {
  if (!columnMapping || Object.keys(columnMapping).length === 0) return row;
  const enhanced = { ...row };
  for (const [canonicalKey, csvHeader] of Object.entries(columnMapping)) {
    const value = resolveMappedCell(row, csvHeader);
    if (value !== undefined && !(canonicalKey in enhanced)) {
      enhanced[canonicalKey] = value;
    }
  }
  return enhanced;
}

/**
 * Variant of `pick` that records which CSV header was consumed.
 * Used to build the `raw_data` JSONB fallback from whatever is left over.
 */
function pickT(
  row: Record<string, string>,
  aliases: string[],
  consumed: Set<string>,
): string {
  const want = new Set(aliases.map(normKey));
  for (const [k, v] of Object.entries(row)) {
    if (want.has(normKey(k))) {
      consumed.add(k);
      return String(v ?? "").trim();
    }
  }
  for (const [k, v] of Object.entries(row)) {
    const nk = normKey(k);
    for (const a of aliases) {
      const na = normKey(a);
      if (nk === na || nk.includes(na) || na.includes(nk)) {
        consumed.add(k);
        return String(v ?? "").trim();
      }
    }
  }
  return "";
}

/** Non-tracking version — kept for backward compat with legacy mappers. */
function pick(row: Record<string, string>, aliases: string[]): string {
  const dummy = new Set<string>();
  return pickT(row, aliases, dummy);
}

/**
 * Collects all CSV columns that were NOT consumed by alias matching into a
 * `raw_data` JSONB object. Empty/blank values are omitted to keep the column lean.
 */
function buildRawData(
  row: Record<string, string>,
  consumed: Set<string>,
): Record<string, string> | null {
  const rd: Record<string, string> = {};
  for (const [k, v] of Object.entries(row)) {
    if (!consumed.has(k) && v !== null && v !== undefined && String(v).trim() !== "") {
      rd[k] = String(v);
    }
  }
  return Object.keys(rd).length > 0 ? rd : null;
}

// =============================================================================
// ── NATIVE_COLUMNS — physical DB columns per amazon_ table ───────────────────
//    These are the EXACT column names that exist in the live Supabase schema.
//    packPayloadForSupabase() uses these sets to separate root keys from JSONB.
// =============================================================================

/** amazon_returns — physical DB columns */
export const NATIVE_COLUMNS_RETURNS = new Set([
  "id", "organization_id", "upload_id", "return_date",
  "source_file_sha256", "source_physical_row_number",
  "order_id", "sku", "asin", "lpn",
  "product_name", "disposition", "reason", "status",
  "created_at", "raw_data",
]);

/** amazon_removals — physical DB columns */
export const NATIVE_COLUMNS_REMOVALS = new Set([
  "id", "organization_id", "store_id", "upload_id", "source_staging_id", "order_date",
  "order_id", "sku", "fnsku", "disposition",
  "shipped_quantity", "cancelled_quantity", "disposed_quantity", "requested_quantity",
  "status", "tracking_number", "carrier", "shipment_date",
  "order_source",
  "order_type",
  "last_updated_date",
  "in_process_quantity",
  "removal_fee",
  "currency",
  "created_at", "raw_data",
]);

/**
 * amazon_inventory_ledger — physical DB columns ONLY.
 *
 * Physical schema: organization_id · upload_id · fnsku · disposition ·
 *                  location · event_type · quantity · id · created_at · raw_data
 *
 * NON-physical columns from the CSV (asin, sku/msku, country, title, reference_id,
 * reconciled_qty, unreconciled_qty, reason, etc.) are NOT listed here so that
 * packPayloadForSupabase() automatically redirects them into the raw_data JSONB.
 */
export const NATIVE_COLUMNS_LEDGER = new Set([
  "id", "organization_id", "upload_id",
  "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "fnsku", "disposition", "location", "event_type", "quantity",
  // Migration 20260642: positional columns from headerless ledger export.
  "event_date", "event_timestamp", "sku", "asin", "product_name", "country",
  // Migration 20260620: identifier resolution.
  "resolved_product_id", "resolved_catalog_product_id",
  "identifier_resolution_status", "identifier_resolution_confidence",
  "created_at", "raw_data",
]);

/** amazon_reimbursements — physical DB columns */
export const NATIVE_COLUMNS_REIMBURSEMENTS = new Set([
  "id", "organization_id", "upload_id",
  "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "order_id", "reimbursement_id", "sku", "amount_reimbursed",
  "created_at", "raw_data",
]);

/** amazon_settlements — physical DB columns (legacy + flat .txt + transaction-detail report) */
export const NATIVE_COLUMNS_SETTLEMENTS = new Set([
  "id",
  "organization_id",
  "upload_id",
  "source_file_sha256",
  "source_physical_row_number",
  "amazon_line_key",
  "settlement_id",
  "settlement_start_date",
  "settlement_end_date",
  "deposit_date",
  "total_amount",
  "currency",
  "posted_date",
  "order_id",
  "sku",
  "transaction_type",
  "amount_total",
  "product_sales",
  "selling_fees",
  "fba_fees",
  "description",
  // Migration 20260642 — transaction-detail report typed columns.
  "quantity",
  "marketplace",
  "account_type",
  "fulfillment_channel",
  "product_sales_tax",
  "shipping_credits",
  "shipping_credits_tax",
  "gift_wrap_credits",
  "giftwrap_credits_tax",
  "regulatory_fee",
  "tax_on_regulatory_fee",
  "promotional_rebates",
  "promotional_rebates_tax",
  "marketplace_withheld_tax",
  "other_transaction_fees",
  "other_amount",
  "transaction_status",
  "transaction_release_date",
  "resolved_product_id",
  "resolved_catalog_product_id",
  "identifier_resolution_status",
  "identifier_resolution_confidence",
  "created_at",
  "raw_data",
]);

/** amazon_safet_claims — physical DB columns */
export const NATIVE_COLUMNS_SAFET = new Set([
  "id", "organization_id", "upload_id", "claim_date",
  "source_file_sha256", "source_physical_row_number",
  "safet_claim_id", "order_id", "asin", "item_name",
  "claim_reason", "claim_status", "claim_amount", "total_reimbursement_amount",
  "created_at", "raw_data",
]);

/** amazon_transactions — physical DB columns (source_line_hash added in migration 20260605) */
export const NATIVE_COLUMNS_TRANSACTIONS = new Set([
  "id", "organization_id", "upload_id",
  "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "settlement_id", "order_id", "transaction_type", "amount", "sku", "posted_date",
  // Migration 20260642 — identifier resolution columns (joined via amazon_all_orders for the simple summary file).
  "resolved_product_id", "resolved_catalog_product_id",
  "identifier_resolution_status", "identifier_resolution_confidence",
  "created_at", "raw_data",
]);

// ── New raw-archive tables (migration 20260604) ───────────────────────────────

/** amazon_all_orders — physical DB columns. Migration 20260642 adds typed Fulfilled-Shipments columns. */
export const NATIVE_COLUMNS_ALL_ORDERS = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "order_id", "purchase_date", "order_status", "fulfillment_channel", "sales_channel",
  // Migration 20260642 — Fulfilled Shipments typed columns.
  "amazon_order_id", "merchant_order_id", "sku", "product_name", "quantity",
  "currency", "item_price", "item_tax", "shipping_price", "ship_country",
  "resolved_product_id", "resolved_catalog_product_id",
  "identifier_resolution_status", "identifier_resolution_confidence",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_replacements — physical DB columns */
export const NATIVE_COLUMNS_REPLACEMENTS = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "order_id", "replacement_order_id", "asin", "sku", "order_date",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_fba_grade_and_resell — physical DB columns */
export const NATIVE_COLUMNS_FBA_GRADE_AND_RESELL = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "asin", "fnsku", "sku", "grade", "units",
  "raw_data", "created_at", "updated_at",
]);

/**
 * amazon_manage_fba_inventory — physical DB columns.
 * Wave 4 expansion adds AFN flow + listing context columns; older deployments
 * have only the original (id/org/store/upload/hash/sku/asin/fnsku/afn_fulfillable_quantity)
 * subset — packPayloadForSupabase will redirect any column missing in the DB to raw_data,
 * so this expanded set is forward-compatible with the column-add migration.
 */
export const NATIVE_COLUMNS_MANAGE_FBA_INVENTORY = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "sku", "fnsku", "asin", "product_name", "condition", "your_price",
  "mfn_listing_exists", "mfn_fulfillable_quantity",
  "afn_listing_exists",
  "afn_warehouse_quantity", "afn_fulfillable_quantity", "afn_unsellable_quantity",
  "afn_reserved_quantity", "afn_total_quantity", "per_unit_volume",
  "afn_inbound_working_quantity", "afn_inbound_shipped_quantity", "afn_inbound_receiving_quantity",
  "afn_researching_quantity", "afn_reserved_future_supply", "afn_future_supply_buyable",
  "store",
  // Migration 20260642 — Restock Inventory typed columns.
  "inbound_quantity", "fc_transfer_quantity", "fc_processing_quantity",
  "customer_order_quantity", "recommended_replenishment_qty",
  "recommended_ship_date", "recommended_action", "unit_storage_size",
  "resolved_product_id", "resolved_catalog_product_id",
  "identifier_resolution_status", "identifier_resolution_confidence",
  "raw_data", "created_at", "updated_at",
]);

/**
 * amazon_fba_inventory (Inventory Health) — physical DB columns.
 * Same forward-compatibility rule applies.
 */
export const NATIVE_COLUMNS_FBA_INVENTORY = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "snapshot_date", "sku", "fnsku", "asin", "product_name", "condition",
  "available", "pending_removal_quantity",
  "inv_age_0_to_90_days", "inv_age_91_to_180_days", "inv_age_181_to_270_days",
  "inv_age_271_to_365_days", "inv_age_366_to_455_days", "inv_age_456_plus_days",
  "currency",
  "units_shipped_t7", "units_shipped_t30", "units_shipped_t60", "units_shipped_t90",
  "alert", "your_price", "sales_price",
  "recommended_action", "sell_through",
  "item_volume", "volume_unit_measurement", "storage_type", "storage_volume",
  "marketplace", "product_group", "sales_rank", "days_of_supply", "estimated_excess_quantity",
  "weeks_of_cover_t30", "weeks_of_cover_t90",
  "estimated_storage_cost_next_month",
  "inbound_quantity", "inbound_working", "inbound_shipped", "inbound_received",
  "no_sale_last_6_months", "total_reserved_quantity", "unfulfillable_quantity",
  "historical_days_of_supply", "fba_minimum_inventory_level", "fba_inventory_level_health_status",
  "recommended_ship_in_quantity", "recommended_ship_in_date",
  "inventory_age_snapshot_date", "inventory_supply_at_fba",
  "reserved_fc_transfer", "reserved_fc_processing", "reserved_customer_order",
  "total_days_of_supply_including_open_shipments",
  "supplier", "is_seasonal_in_next_3_months", "season_name", "season_start_date", "season_end_date",
  "quantity",
  "raw_data", "created_at", "updated_at",
]);

/**
 * amazon_inbound_performance — physical DB columns (NEW table; migration 20260622).
 */
export const NATIVE_COLUMNS_INBOUND_PERFORMANCE = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "issue_reported_date", "shipment_creation_date",
  "fba_shipment_id", "fba_carton_id", "fulfillment_center_id",
  "sku", "fnsku", "asin", "product_name",
  "problem_type", "problem_quantity", "expected_quantity", "received_quantity",
  "performance_measurement_unit", "coaching_level", "fee_type", "currency", "fee_total",
  "problem_level", "alert_status",
  "raw_data", "created_at", "updated_at",
]);

/**
 * amazon_amazon_fulfilled_inventory — physical DB columns (NEW table; migration 20260623).
 * Table name doubled to match the registry domain table convention `amazon_<report-slug>`.
 */
export const NATIVE_COLUMNS_AMAZON_FULFILLED_INVENTORY = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "seller_sku", "fulfillment_channel_sku", "asin",
  "condition_type", "warehouse_condition_code", "quantity_available",
  // Migration 20260642 — identifier resolution.
  "resolved_product_id", "resolved_catalog_product_id",
  "identifier_resolution_status", "identifier_resolution_confidence",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_reserved_inventory — physical DB columns */
export const NATIVE_COLUMNS_RESERVED_INVENTORY = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "asin", "fnsku", "sku", "reserved_quantity",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_fee_preview — physical DB columns */
export const NATIVE_COLUMNS_FEE_PREVIEW = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "asin", "fnsku", "sku", "price", "estimated_fee", "currency",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_monthly_storage_fees — physical DB columns */
export const NATIVE_COLUMNS_MONTHLY_STORAGE_FEES = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "asin", "fnsku", "sku", "storage_month", "storage_rate", "currency",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_reports_repository — physical DB columns */
export const NATIVE_COLUMNS_REPORTS_REPOSITORY = new Set([
  "id", "organization_id", "upload_id",
  "source_line_hash",
  "source_file_sha256", "source_physical_row_number",
  "date_time", "settlement_id", "transaction_type", "order_id", "sku", "description",
  "total_amount", "created_at", "raw_data",
]);

// =============================================================================
// ── packPayloadForSupabase ────────────────────────────────────────────────────
// =============================================================================

/**
 * Strict JSONB packing interceptor — call this BEFORE every supabase.upsert().
 *
 * For each row:
 *   • Keys in `nativeColumns`  → kept at the root (real DB columns).
 *   • Keys NOT in `nativeColumns` (and not "raw_data") → merged into raw_data.
 *   • Existing raw_data content is preserved and merged with the overflow.
 *
 * This is the permanent fix for Supabase "could not find column X in schema
 * cache" errors: unknown CSV-derived keys can never reach Postgres as root
 * columns because they are intercepted here and packed into the JSONB bucket.
 *
 * @param rows         Array of mapper output objects.
 * @param nativeColumns Set of column names that physically exist in the DB table.
 * @returns            Array of safe, schema-aligned upsert payloads.
 */
export function packPayloadForSupabase(
  rows: Record<string, unknown>[],
  nativeColumns: Set<string>,
): Record<string, unknown>[] {
  return rows.map((row) => {
    const packed: Record<string, unknown> = {};
    const overflow: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(row)) {
      if (key === "raw_data") continue; // handled separately below
      if (nativeColumns.has(key)) {
        packed[key] = value;
      } else {
        // Non-native key: redirect to JSONB overflow
        if (value !== null && value !== undefined && String(value).trim() !== "") {
          overflow[key] = value;
        }
      }
    }

    // Merge existing raw_data (from mapper) with any overflow keys from above
    const existingRawData =
      row.raw_data && typeof row.raw_data === "object" && !Array.isArray(row.raw_data)
        ? (row.raw_data as Record<string, unknown>)
        : {};

    const mergedRawData = { ...overflow, ...existingRawData };

    if (nativeColumns.has("raw_data")) {
      packed.raw_data = Object.keys(mergedRawData).length > 0 ? mergedRawData : null;
    }

    return packed;
  });
}

// =============================================================================
// ── Shared alias arrays ───────────────────────────────────────────────────────
// =============================================================================

const LPN_ALIASES        = ["license-plate-number", "license plate number", "lpn", "LPN"];
const ASIN_ALIASES       = ["asin", "product-id", "product id", "ASIN"];
const ORDER_ALIASES      = ["order-id", "order id", "amazon-order-id", "amazon order id"];
const REMOVAL_ORDER_ID_ALIASES = [
  "removal-order-id", "removal order id", "removal_order_id",
  "order-id", "order id",
];
const TRACK_ALIASES         = ["tracking-number", "tracking number", "tracking id", "tracking-id"];
const CARRIER_ALIASES       = ["carrier", "carrier-name", "carrier name"];
const SHIPMENT_DATE_ALIASES = ["carrier-shipment-date", "shipment-date", "shipment date", "ship-date", "shipped-date"];
const SKU_ALIASES        = ["sku", "merchant-sku", "msku", "SKU"];
const QTY_ALIASES        = ["requested-quantity", "requested quantity", "quantity", "qty"];
const SHIPPED_QTY_ALIASES   = ["shipped-quantity", "shipped quantity"];
const DISPOSED_QTY_ALIASES  = ["disposed-quantity", "disposed quantity"];
const CANCELLED_QTY_ALIASES = ["cancelled-quantity", "cancelled quantity"];
const ORDER_STATUS_ALIASES  = ["order-status", "order status", "status"];
const DISPOSITION_ALIASES   = ["disposition", "detailed-disposition", "detailed disposition"];
const ORDER_DATE_ALIASES    = ["order-date", "order date", "request-date", "requested-date"];
const ORDER_SOURCE_ALIASES = ["order-source", "order source", "order_source"];
const ORDER_TYPE_ALIASES = ["order-type", "order type", "order_type"];
const LAST_UPDATED_DATE_ALIASES = [
  "last-updated-date",
  "last updated date",
  "last_updated_date",
];
const IN_PROCESS_QTY_ALIASES = [
  "in-process-quantity",
  "in process quantity",
  "in_process_quantity",
];
const REMOVAL_FEE_ALIASES = ["removal-fee", "removal fee", "removal_fee"];
const CURRENCY_ALIASES = ["currency"];
const FNSKU_ALIASES      = ["fnsku", "FNSKU", "fulfillment-network-sku"];
const LOCATION_ALIASES   = [
  "fulfillment-center", "fulfillment center", "fc", "warehouse", "location",
  "Fulfillment Center", "Location",
];
const EVENT_TYPE_ALIASES = [
  "event-type", "event type", "Event Type", "event_type",
  "transaction-type", "transaction type",
];
const TITLE_ALIASES      = [
  "product-name", "product name", "title", "item-name", "item name", "description",
];
const DATE_ALIASES       = ["date", "snapshot-date", "event-date", "Date"];
const ENDING_WH_BAL_ALIASES = ["ending-warehouse-balance", "ending warehouse balance"];
const RETURN_REASON_ALIASES = [
  "return-reason-code", "return reason code", "return reason", "reason",
];

// ── SAFE-T hard-coded aliases (Task requirement) ──────────────────────────────
const SAFET_CLAIM_ID_ALIASES = [
  "safe-t-claim-id", "safe-t claim id", "safe t claim id", "safe_t_claim_id",
  "safet-claim-id", "safet claim id", "claim-id", "claim id", "claim_id",
  "SAFE-T Claim ID", "Claim ID",
];
const SAFET_REIMBURSEMENT_AMOUNT_ALIASES = [
  "reimbursement-amount", "reimbursement amount",
  "total-reimbursement-amount", "total reimbursement amount",
  "amount", "reimburse-amount",
];
const CLAIM_STATUS_ALIASES   = ["claim-status", "claim status", "status"];
const CLAIM_REASON_ALIASES   = ["claim-reason", "claim reason", "reason"];
const CLAIM_AMOUNT_ALIASES   = ["claim-amount", "claim amount", "amount-claimed"];

// ── Reimbursements aliases ────────────────────────────────────────────────────
const REIMBURSEMENT_ID_ALIASES = [
  "reimbursement-id", "reimbursement id", "reimbursement_id",
];
const AMOUNT_REIMBURSED_ALIASES = [
  "amount-reimbursed", "amount reimbursed",
  "amount-per-unit", "amount per unit",
  "total-amount", "total amount",
];

// ── Settlement aliases ────────────────────────────────────────────────────────
const SETTLEMENT_ID_ALIASES    = ["settlement-id", "settlement id", "Settlement ID"];
const TX_TYPE_ALIASES          = ["transaction-type", "transaction type", "type", "Type"];
const DEPOSIT_DATE_ALIASES     = ["deposit-date", "deposit date", "posted-date", "posted date", "date/time", "Date/Time"];
const AMOUNT_TOTAL_ALIASES     = ["total", "amount", "net-proceeds", "net proceeds", "amount-total", "amount total"];

// Transaction / Payment Detail report columns (Amazon).
const SETTLEMENT_QUANTITY_ALIASES        = ["quantity", "Quantity"];
const SETTLEMENT_MARKETPLACE_ALIASES     = ["marketplace", "Marketplace"];
const SETTLEMENT_ACCOUNT_TYPE_ALIASES    = ["account type", "account-type", "account_type"];
const SETTLEMENT_FULFILLMENT_ALIASES     = [
  "fulfillment", "Fulfillment", "fulfillment-channel", "fulfillment channel",
];
const SETTLEMENT_PRODUCT_SALES_ALIASES   = ["product sales", "product-sales", "product_sales"];
const SETTLEMENT_PRODUCT_SALES_TAX_ALIASES = ["product sales tax", "product-sales-tax"];
const SETTLEMENT_SHIPPING_CREDITS_ALIASES = ["shipping credits", "shipping-credits"];
const SETTLEMENT_SHIPPING_CREDITS_TAX_ALIASES = ["shipping credits tax", "shipping-credits-tax"];
const SETTLEMENT_GIFT_WRAP_CREDITS_ALIASES = ["gift wrap credits", "gift-wrap-credits", "giftwrap credits"];
const SETTLEMENT_GIFTWRAP_CREDITS_TAX_ALIASES = [
  "giftwrap credits tax", "giftwrap-credits-tax", "gift wrap credits tax", "gift-wrap-credits-tax",
];
const SETTLEMENT_REGULATORY_FEE_ALIASES  = ["Regulatory Fee", "regulatory fee", "regulatory-fee"];
const SETTLEMENT_TAX_ON_REG_FEE_ALIASES  = [
  "Tax On Regulatory Fee", "tax on regulatory fee", "tax-on-regulatory-fee",
];
const SETTLEMENT_PROMOTIONAL_REBATES_ALIASES = ["promotional rebates", "promotional-rebates"];
const SETTLEMENT_PROMO_REBATES_TAX_ALIASES = [
  "promotional rebates tax", "promotional-rebates-tax",
];
const SETTLEMENT_MARKETPLACE_WITHHELD_TAX_ALIASES = [
  "marketplace withheld tax", "marketplace-withheld-tax",
];
const SETTLEMENT_SELLING_FEES_ALIASES    = ["selling fees", "selling-fees"];
const SETTLEMENT_FBA_FEES_ALIASES        = ["fba fees", "fba-fees"];
const SETTLEMENT_OTHER_TX_FEES_ALIASES   = ["other transaction fees", "other-transaction-fees"];
const SETTLEMENT_OTHER_AMOUNT_ALIASES    = ["other", "Other", "other-amount"];
const SETTLEMENT_TX_STATUS_ALIASES       = [
  "Transaction Status", "transaction-status", "transaction status", "transaction_status",
];
const SETTLEMENT_TX_RELEASE_DATE_ALIASES = [
  "Transaction Release Date", "transaction-release-date", "transaction release date",
];
const SETTLEMENT_DESCRIPTION_ALIASES     = ["description", "Description"];

/** Settlement flat-file: settlement id on transaction lines */
const TX_SETTLEMENT_ID_ALIASES = ["settlement-id", "settlement id", "Settlement ID"];
const ORDER_ITEM_CODE_ALIASES = ["order-item-code", "order item code"];
const PRICE_TYPE_ALIASES = ["price-type", "price type"];
const ITEM_RELATED_FEE_TYPE_ALIASES = ["item-related-fee-type", "item related fee type"];
const SHIPMENT_FEE_TYPE_ALIASES = ["shipment-fee-type", "shipment fee type"];
const PROMOTION_TYPE_ALIASES = ["promotion-type", "promotion type"];

/** First non-empty numeric among Amazon settlement line amount columns */
const TX_LINE_AMOUNT_ALIAS_GROUPS = [
  ["price-amount", "price amount"],
  ["item-related-fee-amount", "item related fee amount"],
  ["shipment-fee-amount", "shipment fee amount"],
  ["promotion-amount", "promotion amount"],
  ["direct-payment-amount", "direct payment amount"],
  ["other-amount", "other amount"],
  ["misc-fee-amount", "misc fee amount"],
  ["total-amount", "total amount"],
];

// ── Transaction aliases ───────────────────────────────────────────────────────
// Hard-coded: CSV "amount" column → DB amount (prevents raw_data burial).
// Includes "Total (USD)" / "Total" from the simple Transactions Summary report.
const TX_AMOUNT_ALIASES = [
  "amount", "Amount", "transaction-amount", "transaction amount",
  "total (usd)", "Total (USD)", "total-usd", "total usd", "total", "Total",
];
const POSTED_DATE_ALIASES = [
  "posted-date", "posted date", "date-time", "Date/Time", "date/time", "date", "Date",
];

// ── Helpers ───────────────────────────────────────────────────────────────────

function parseQty(v: string): number | null {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
}

function parseNum(v: string): number | null {
  if (!v) return null;
  const n = parseFloat(v.replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

function parseIsoDate(v: string): string | null {
  if (!v) return null;
  const s = v.trim();
  // Amazon request-date is often "2026-04-02T20:01:41-07:00" — use calendar date as in file,
  // not UTC conversion (avoids wrong day near midnight in local TZ).
  const ymd = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (ymd) return ymd[1];
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function parseIsoDateTime(v: string): string | null {
  if (!v?.trim()) return null;
  const d = new Date(v.trim());
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

// =============================================================================
// ── LEGACY mappers (kept for /api/settings/imports/process backward compat) ──
// =============================================================================

export type ExpectedReturnInsert = {
  organization_id: string;
  lpn: string;
  asin: string | null;
  order_id: string | null;
  upload_id: string;
};

export function mapRowToExpectedReturn(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): ExpectedReturnInsert | null {
  const lpn = pick(row, LPN_ALIASES);
  if (!lpn) return null;
  return {
    organization_id: orgId,
    lpn,
    asin: pick(row, ASIN_ALIASES) || null,
    order_id: pick(row, ORDER_ALIASES) || null,
    upload_id: uploadId,
  };
}

export type ExpectedPackageInsert = {
  organization_id: string;
  upload_id: string;
  source_staging_id?: string | null;
  order_id: string;
  sku: string;
  /** Distinguishes multiple UNKNOW merchant-sku lines (Amazon FNSKU). */
  fnsku: string | null;
  tracking_number: string | null;
  requested_quantity: number | null;
  shipped_quantity: number | null;
  disposed_quantity: number | null;
  cancelled_quantity: number | null;
  order_status: string | null;
  disposition: string | null;
  order_date: string | null;
};

/**
 * Exact schema for `expected_removals` table.
 * NOTE: tracking_number and order_date are intentionally absent — removed on 2026-04-28.
 */
export type ExpectedRemovalInsert = {
  organization_id: string;
  upload_id: string;
  order_id: string;
  sku: string;
  fnsku: string | null;
  disposition: string | null;
  shipped_quantity: number | null;
  cancelled_quantity: number | null;
  disposed_quantity: number | null;
  requested_quantity: number | null;
  status: string | null;
};

export function mapRowToExpectedPackage(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): ExpectedPackageInsert | null {
  const order_id = pick(row, REMOVAL_ORDER_ID_ALIASES);
  if (!order_id) return null;
  return {
    organization_id: orgId,
    upload_id: uploadId,
    order_id,
    sku: pick(row, SKU_ALIASES) || "",
    fnsku: pick(row, FNSKU_ALIASES) || null,
    tracking_number: pick(row, TRACK_ALIASES) || null,
    requested_quantity: parseQty(pick(row, QTY_ALIASES)),
    shipped_quantity: parseQty(pick(row, SHIPPED_QTY_ALIASES)),
    disposed_quantity: parseQty(pick(row, DISPOSED_QTY_ALIASES)),
    cancelled_quantity: parseQty(pick(row, CANCELLED_QTY_ALIASES)),
    order_status: pick(row, ORDER_STATUS_ALIASES) || null,
    disposition: pick(row, DISPOSITION_ALIASES) || null,
    order_date: parseIsoDate(pick(row, ORDER_DATE_ALIASES)),
  };
}

export function mapRowToExpectedRemoval(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): ExpectedRemovalInsert | null {
  const order_id = pick(row, REMOVAL_ORDER_ID_ALIASES);
  if (!order_id) return null;
  return {
    organization_id: orgId,
    upload_id: uploadId,
    order_id,
    sku: pick(row, SKU_ALIASES) || "",
    fnsku: pick(row, FNSKU_ALIASES) || null,
    disposition: pick(row, DISPOSITION_ALIASES) || null,
    shipped_quantity: parseQty(pick(row, SHIPPED_QTY_ALIASES)),
    cancelled_quantity: parseQty(pick(row, CANCELLED_QTY_ALIASES)),
    disposed_quantity: parseQty(pick(row, DISPOSED_QTY_ALIASES)),
    requested_quantity: parseQty(pick(row, QTY_ALIASES)),
    status: pick(row, ORDER_STATUS_ALIASES) || null,
  };
}

export type ProductLedgerUpsert = {
  organization_id: string;
  barcode: string;
  product_name: string;
  source: string;
};

export function mapRowToProductFromLedger(
  row: Record<string, string>,
  orgId: string,
): ProductLedgerUpsert | null {
  const fnsku = pick(row, FNSKU_ALIASES);
  if (!fnsku) return null;
  const title = pick(row, TITLE_ALIASES);
  return {
    organization_id: orgId,
    barcode: fnsku,
    product_name: title || fnsku,
    source: "Amazon Inventory Ledger",
  };
}

/** Physical columns on `public.catalog_products` (listing export upserts). */
export const NATIVE_COLUMNS_CATALOG_PRODUCTS = new Set([
  "id",
  "organization_id",
  "store_id",
  "source_report_type",
  "source_upload_id",
  "seller_sku",
  "asin",
  "fnsku",
  "item_name",
  "item_description",
  "fulfillment_channel",
  "listing_status",
  "listing_id",
  "product_id",
  "product_id_type",
  "item_condition",
  "merchant_shipping_group",
  "price",
  "quantity",
  "open_date",
  "raw_payload",
  "first_seen_at",
  "last_seen_at",
  "created_at",
  "updated_at",
]);

const SELLER_SKU_LISTING_ALIASES = [
  "seller-sku",
  "seller sku",
  "seller_sku",
  "sku",
  "SKU",
];
/** ASIN for the canonical key — use asin1 / asin, not product-id (stored separately). */
const ASIN_LISTING_ALIASES = ["asin1", "asin1-value", "asin", "ASIN"];
const PRODUCT_ID_LISTING_ALIASES = ["product-id", "product id", "product_id"];
const LISTING_ID_ALIASES = ["listing-id", "listing id", "listing_id"];
const PRODUCT_ID_TYPE_ALIASES = ["product-id-type", "product id type", "product_id_type"];
const ITEM_CONDITION_ALIASES = [
  "item-condition",
  "item condition",
  "item_condition",
  "condition",
];
const MERCHANT_SHIPPING_GROUP_ALIASES = [
  "merchant-shipping-group",
  "merchant shipping group",
  "merchant_shipping_group",
];
const ITEM_NAME_LISTING_ALIASES = [
  "item-name",
  "item name",
  "item_name",
  "product-name",
  "product name",
  "title",
];
const ITEM_DESC_LISTING_ALIASES = ["item-description", "item description", "item_description"];
const FULFILLMENT_CHANNEL_ALIASES = [
  "fulfillment-channel",
  "fulfillment channel",
  "fulfillment_channel",
  "fulfilment-channel",
];
const LISTING_STATUS_ALIASES = ["status", "listing-status", "listing status", "listing_status"];
const PRICE_ALIASES = ["price", "your-price", "your price", "standard-price", "standard price"];
const LISTING_QTY_ALIASES = ["quantity", "qty", "available"];
const OPEN_DATE_ALIASES = [
  "open-date",
  "open date",
  "open_date",
  "start-date",
  "start date",
  "listing-created-date",
];

export type CatalogListingSource = "category_listings" | "all_listings" | "active_listings";

export type CatalogProductUpsert = {
  organization_id: string;
  store_id: string | null;
  source_report_type: CatalogListingSource;
  /** Provenance: raw_report_uploads.id for this import when available. */
  source_upload_id: string | null;
  seller_sku: string;
  asin: string;
  fnsku: string | null;
  item_name: string | null;
  item_description: string | null;
  fulfillment_channel: string | null;
  listing_status: string | null;
  listing_id: string | null;
  product_id: string | null;
  product_id_type: string | null;
  item_condition: string | null;
  merchant_shipping_group: string | null;
  price: number | null;
  quantity: number | null;
  open_date: string | null;
  raw_payload: Record<string, string>;
};

/**
 * Maps Amazon Category / All / Active Listings CSV rows → `catalog_products`.
 * FNSKU is optional. Rows without both seller_sku and asin are skipped.
 */
/** Maps `raw_report_uploads.report_type` → `catalog_products.source_report_type`. */
export function catalogReportTypeToSource(reportType: string | null | undefined): CatalogListingSource {
  const rt = String(reportType ?? "").trim();
  if (rt === "CATEGORY_LISTINGS") return "category_listings";
  if (rt === "ACTIVE_LISTINGS") return "active_listings";
  return "all_listings";
}

/** Denormalized identifiers for amazon_listing_report_rows_raw (every file line; optional columns). */
export function extractListingIdentifiersForRawRow(row: Record<string, string>): {
  seller_sku: string | null;
  asin: string | null;
  listing_id: string | null;
} {
  const sku = pick(row, SELLER_SKU_LISTING_ALIASES)?.trim();
  const asin = pick(row, ASIN_LISTING_ALIASES)?.trim();
  const lid = pick(row, LISTING_ID_ALIASES)?.trim();
  return {
    seller_sku: sku ? sku : null,
    asin: asin ? asin : null,
    listing_id: lid ? lid : null,
  };
}

/** Full normalized row for raw_payload JSONB (string values per cell). */
export function listingMappedRowToRawPayload(row: Record<string, string>): Record<string, string> {
  const o: Record<string, string> = {};
  for (const [k, v] of Object.entries(row)) {
    o[k] = v ?? "";
  }
  return o;
}

export function mapRowToCatalogProduct(
  row: Record<string, string>,
  orgId: string,
  storeId: string | null,
  source: CatalogListingSource,
  sourceUploadId: string | null,
): CatalogProductUpsert | null {
  const seller_sku = pick(row, SELLER_SKU_LISTING_ALIASES);
  const asin = pick(row, ASIN_LISTING_ALIASES);
  if (!seller_sku?.trim() || !asin?.trim()) {
    return null;
  }

  const raw_payload: Record<string, string> = {};
  for (const [k, v] of Object.entries(row)) {
    raw_payload[k] = v ?? "";
  }

  const qtyStr = pick(row, LISTING_QTY_ALIASES);
  let quantity: number | null = parseQty(qtyStr);
  if (quantity === null && qtyStr) {
    const n = parseNum(qtyStr);
    if (n !== null) quantity = Math.round(n);
  }

  const priceStr = pick(row, PRICE_ALIASES);
  const price = priceStr ? parseNum(priceStr) : null;

  const openRaw = pick(row, OPEN_DATE_ALIASES);
  const open_date = openRaw ? parseIsoDateTime(openRaw) : null;

  const nz = (s: string | undefined) => {
    const t = (s ?? "").trim();
    return t === "" ? null : t;
  };

  return {
    organization_id: orgId,
    store_id: storeId,
    source_report_type: source,
    source_upload_id: sourceUploadId && isUuidString(sourceUploadId) ? sourceUploadId : null,
    seller_sku: seller_sku.trim(),
    asin: asin.trim(),
    fnsku: (pick(row, FNSKU_ALIASES) || "").trim() || null,
    item_name: pick(row, ITEM_NAME_LISTING_ALIASES) || null,
    item_description: pick(row, ITEM_DESC_LISTING_ALIASES) || null,
    fulfillment_channel: pick(row, FULFILLMENT_CHANNEL_ALIASES) || null,
    listing_status: pick(row, LISTING_STATUS_ALIASES) || null,
    listing_id: nz(pick(row, LISTING_ID_ALIASES)),
    product_id: nz(pick(row, PRODUCT_ID_LISTING_ALIASES)),
    product_id_type: nz(pick(row, PRODUCT_ID_TYPE_ALIASES)),
    item_condition: nz(pick(row, ITEM_CONDITION_ALIASES)),
    merchant_shipping_group: nz(pick(row, MERCHANT_SHIPPING_GROUP_ALIASES)),
    price,
    quantity,
    open_date,
    raw_payload,
  };
}

// =============================================================================
// ── amazon_ table mappers (Phase-3 Sync → amazon_ domain tables) ─────────────
//
//  All mappers ONLY use column names that exist in NATIVE_COLUMNS_<TABLE>.
//  packPayloadForSupabase() is called in sync/route.ts as an additional guard.
// =============================================================================

// ── amazon_returns ────────────────────────────────────────────────────────────
// DB columns: id · organization_id · upload_id · return_date · order_id · sku ·
//             asin · lpn · product_name · disposition · reason · status ·
//             created_at · raw_data
// ─────────────────────────────────────────────────────────────────────────────

export type AmazonReturnInsert = {
  organization_id: string;
  upload_id: string;
  /** Lookup only — import identity is (organization_id, source_file_sha256, source_physical_row_number). */
  lpn: string | null;
  order_id: string | null;
  sku: string | null;
  asin: string | null;
  product_name: string | null;
  /** DB column: `disposition`  (was mistakenly `detailed_disposition` — root cause of schema error). */
  disposition: string | null;
  /** DB column: `reason`  (was mistakenly `return_reason`). */
  reason: string | null;
  status: string | null;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonReturn(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonReturnInsert {
  const consumed = new Set<string>();
  const lpnRaw = pickT(row, LPN_ALIASES, consumed);
  const lpn = lpnRaw === "" ? null : lpnRaw;
  return {
    organization_id: orgId,
    upload_id: uploadId,
    lpn,
    order_id:     pickT(row, ORDER_ALIASES, consumed) || null,
    sku:          pickT(row, SKU_ALIASES, consumed) || null,
    asin:         pickT(row, ASIN_ALIASES, consumed) || null,
    product_name: pickT(row, TITLE_ALIASES, consumed) || null,
    disposition:  pickT(row, DISPOSITION_ALIASES, consumed) || null,
    reason:       pickT(row, RETURN_REASON_ALIASES, consumed) || null,
    status:       pickT(row, ORDER_STATUS_ALIASES, consumed) || null,
    raw_data:     buildRawData(row, consumed),
  };
}

// ── amazon_removals ───────────────────────────────────────────────────────────
// DB columns include order_date, order_source, order_type, last_updated_date,
// in_process_quantity, removal_fee, currency (see migration 20260516).
// ─────────────────────────────────────────────────────────────────────────────

export type AmazonRemovalInsert = {
  organization_id: string;
  /** Imports Target Store — required for Wave 1 business dedupe. */
  store_id: string;
  upload_id: string;
  /** Set in Phase 3 sync from amazon_staging.id — one DB row per CSV line (migration 60519). */
  source_staging_id?: string;
  order_id: string;
  sku: string;
  fnsku: string | null;
  disposition: string | null;
  /** Request / order date from CSV — part of DB unique key (see migration 20260515). */
  order_date: string | null;
  order_source: string | null;
  /** Disposal | Liquidations | Return — worklist (expected_packages) uses Return rows only. */
  order_type: string | null;
  last_updated_date: string | null;
  in_process_quantity: number | null;
  removal_fee: number | null;
  currency: string | null;
  shipped_quantity: number | null;
  cancelled_quantity: number | null;
  disposed_quantity: number | null;
  requested_quantity: number | null;
  status: string | null;
  tracking_number: string | null;
  carrier: string | null;
  shipment_date: string | null;
  raw_data: Record<string, string> | null;
};

/**
 * Amazon SP-API `GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA` — tab-delimited attributes (hyphens in file).
 * Staging uses `normalizeAmazonReportRowKeys`: hyphens → underscores (e.g. `order-id` → `order_id`,
 * `removal-order-type` → `removal_order_type`).
 */
const REMOVAL_SHIPMENT_ORDER_ID_PRIORITY = [
  "order-id",
  "order_id",
  "order id",
  "removal-order-id",
  "removal_order_id",
  "removal order id",
  "amazon-order-id",
  "amazon_order_id",
  "amazon order id",
];

/** Shipment report uses `request-date` for the order/request line date (not only `order-date`). */
const REMOVAL_SHIPMENT_REQUEST_OR_ORDER_DATE_PRIORITY = [
  "request-date",
  "request_date",
  "request date",
  "order-date",
  "order_date",
  "order date",
  "requested-date",
  "requested_date",
];

const REMOVAL_SHIPMENT_SHIPMENT_DATE_PRIORITY = [
  "shipment-date",
  "shipment_date",
  "shipment date",
  "carrier-shipment-date",
  "carrier_shipment_date",
  "carrier shipment date",
  "ship-date",
  "shipped-date",
];

/** Shipment detail report uses `removal-order-type`; removal order detail uses `order-type`. */
const REMOVAL_SHIPMENT_ORDER_TYPE_PRIORITY = [
  "removal-order-type",
  "removal_order_type",
  "removal order type",
  "order-type",
  "order_type",
  "order type",
];

const REMOVAL_SHIPMENT_TRACKING_PRIORITY = [
  "tracking-number",
  "tracking_number",
  "tracking number",
  "tracking-id",
  "tracking_id",
  "tracking id",
];

const REMOVAL_SHIPMENT_CARRIER_PRIORITY = [
  "carrier",
  "carrier-name",
  "carrier_name",
  "carrier name",
];

/**
 * Prefer the first alias that maps to a non-empty cell (exact header match after normKey).
 * Avoids an empty `removal-order-id` / `carrier-shipment-date` winning before `order-id` / `shipment-date`
 * when `Object.entries` order varies.
 */
function pickFirstNonEmptyForAliases(
  row: Record<string, string>,
  aliases: string[],
  consumed: Set<string>,
): string {
  for (const a of aliases) {
    const na = normKey(a);
    for (const [k, v] of Object.entries(row)) {
      if (normKey(k) !== na) continue;
      const s = String(v ?? "").trim();
      if (!s) continue;
      consumed.add(k);
      return s;
    }
  }
  return "";
}

/**
 * REMOVAL_SHIPMENT (FBA Removal Shipment Detail) — same DB shape as removal orders but column names differ;
 * uses priority picks + `removal-order-type` / `request-date` / `shipment-date` per SP-API report schema.
 */
export function mapRowToAmazonRemovalShipment(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
  storeId: string,
): AmazonRemovalInsert | null {
  const consumed = new Set<string>();

  let order_id = pickFirstNonEmptyForAliases(row, REMOVAL_SHIPMENT_ORDER_ID_PRIORITY, consumed);
  if (!order_id) order_id = pickT(row, REMOVAL_ORDER_ID_ALIASES, consumed);
  if (!order_id) return null;

  const sku = pickT(row, SKU_ALIASES, consumed) || "";
  const fnsku = pickT(row, FNSKU_ALIASES, consumed) || null;
  const disposition = pickT(row, DISPOSITION_ALIASES, consumed) || null;

  let orderDateRaw = pickFirstNonEmptyForAliases(row, REMOVAL_SHIPMENT_REQUEST_OR_ORDER_DATE_PRIORITY, consumed);
  if (!orderDateRaw) orderDateRaw = pickT(row, ORDER_DATE_ALIASES, consumed);
  const order_date = parseIsoDate(orderDateRaw);

  const order_source = pickT(row, ORDER_SOURCE_ALIASES, consumed) || null;

  let order_type_raw = pickFirstNonEmptyForAliases(row, REMOVAL_SHIPMENT_ORDER_TYPE_PRIORITY, consumed);
  if (!order_type_raw) order_type_raw = pickT(row, ORDER_TYPE_ALIASES, consumed);

  const last_updated_date = parseIsoDate(pickT(row, LAST_UPDATED_DATE_ALIASES, consumed));
  const in_process_quantity = parseQty(pickT(row, IN_PROCESS_QTY_ALIASES, consumed));
  const removal_fee = parseNum(pickT(row, REMOVAL_FEE_ALIASES, consumed));
  const currency = pickT(row, CURRENCY_ALIASES, consumed) || null;
  const shipped_quantity = parseQty(pickT(row, SHIPPED_QTY_ALIASES, consumed));
  const cancelled_quantity = parseQty(pickT(row, CANCELLED_QTY_ALIASES, consumed));
  const disposed_quantity = parseQty(pickT(row, DISPOSED_QTY_ALIASES, consumed));
  const requested_quantity = parseQty(pickT(row, QTY_ALIASES, consumed));
  const status = pickT(row, ORDER_STATUS_ALIASES, consumed) || null;

  let tracking_raw = pickFirstNonEmptyForAliases(row, REMOVAL_SHIPMENT_TRACKING_PRIORITY, consumed);
  if (!tracking_raw) tracking_raw = pickT(row, TRACK_ALIASES, consumed);

  let carrier_raw = pickFirstNonEmptyForAliases(row, REMOVAL_SHIPMENT_CARRIER_PRIORITY, consumed);
  if (!carrier_raw) carrier_raw = pickT(row, CARRIER_ALIASES, consumed);

  let shipDateRaw = pickFirstNonEmptyForAliases(row, REMOVAL_SHIPMENT_SHIPMENT_DATE_PRIORITY, consumed);
  if (!shipDateRaw) shipDateRaw = pickT(row, SHIPMENT_DATE_ALIASES, consumed);
  const shipment_date = parseIsoDate(shipDateRaw);

  return {
    organization_id: orgId,
    store_id: storeId,
    upload_id: uploadId,
    order_id,
    sku,
    fnsku,
    disposition,
    order_date,
    order_source,
    order_type: order_type_raw || null,
    last_updated_date,
    in_process_quantity,
    removal_fee,
    currency,
    shipped_quantity,
    cancelled_quantity,
    disposed_quantity,
    requested_quantity,
    status,
    tracking_number: tracking_raw || null,
    carrier: carrier_raw || null,
    shipment_date,
    raw_data: buildRawData(row, consumed),
  };
}

export function mapRowToAmazonRemoval(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
  storeId: string,
): AmazonRemovalInsert | null {
  const consumed = new Set<string>();
  const order_id = pickT(row, REMOVAL_ORDER_ID_ALIASES, consumed);
  if (!order_id) return null;
  return {
    organization_id: orgId,
    store_id: storeId,
    upload_id: uploadId,
    order_id,
    sku:                pickT(row, SKU_ALIASES, consumed) || "",
    fnsku:              pickT(row, FNSKU_ALIASES, consumed) || null,
    disposition:        pickT(row, DISPOSITION_ALIASES, consumed) || null,
    order_date:         parseIsoDate(pickT(row, ORDER_DATE_ALIASES, consumed)),
    order_source:       pickT(row, ORDER_SOURCE_ALIASES, consumed) || null,
    order_type:         pickT(row, ORDER_TYPE_ALIASES, consumed) || null,
    last_updated_date:  parseIsoDate(pickT(row, LAST_UPDATED_DATE_ALIASES, consumed)),
    in_process_quantity: parseQty(pickT(row, IN_PROCESS_QTY_ALIASES, consumed)),
    removal_fee:        parseNum(pickT(row, REMOVAL_FEE_ALIASES, consumed)),
    currency:           pickT(row, CURRENCY_ALIASES, consumed) || null,
    shipped_quantity:   parseQty(pickT(row, SHIPPED_QTY_ALIASES, consumed)),
    cancelled_quantity: parseQty(pickT(row, CANCELLED_QTY_ALIASES, consumed)),
    disposed_quantity:  parseQty(pickT(row, DISPOSED_QTY_ALIASES, consumed)),
    requested_quantity: parseQty(pickT(row, QTY_ALIASES, consumed)),
    status:             pickT(row, ORDER_STATUS_ALIASES, consumed) || null,
    tracking_number:    pickT(row, TRACK_ALIASES, consumed) || null,
    carrier:            pickT(row, CARRIER_ALIASES, consumed) || null,
    shipment_date:      pickT(row, SHIPMENT_DATE_ALIASES, consumed) || null,
    raw_data:           buildRawData(row, consumed),
  };
}

// ── amazon_inventory_ledger ───────────────────────────────────────────────────
// Physical DB columns: id · organization_id · upload_id · fnsku · disposition ·
//                      location · event_type · quantity · created_at · raw_data
//
// Non-physical CSV columns (asin, sku/msku, title, country, reference_id,
// reconciled_qty, unreconciled_qty, reason, etc.) → raw_data JSONB
// ─────────────────────────────────────────────────────────────────────────────

export type AmazonInventoryLedgerInsert = {
  organization_id: string;
  upload_id: string;
  /** Line-level dedupe — full-row fingerprint (migration 20260613). */
  source_line_hash: string;
  fnsku: string;
  /** Physical DB column: `disposition` */
  disposition: string | null;
  /** Physical DB column: `location` (maps from CSV "Fulfillment Center") */
  location: string | null;
  /** Physical DB column: `event_type` (maps from CSV "Event Type") */
  event_type: string | null;
  quantity: number | null;
  /** Headerless ledger col1 — Amazon event date (NOT created_at). Migration 20260642. */
  event_date: string | null;
  /** Headerless ledger col15 — Amazon event timestamp. Migration 20260642. */
  event_timestamp: string | null;
  sku: string | null;
  asin: string | null;
  product_name: string | null;
  country: string | null;
  /** All non-physical CSV columns land here. */
  raw_data: Record<string, string> | null;
};

const LEDGER_EVENT_DATE_ALIASES = [
  "event-date", "event date", "event_date",
  "date", "Date", "snapshot-date", "snapshot date",
];
const LEDGER_EVENT_TIMESTAMP_ALIASES = [
  "event-timestamp", "event timestamp", "event_timestamp",
  "timestamp", "Timestamp",
];
const LEDGER_COUNTRY_ALIASES = [
  "country", "Country", "country-code", "country code", "marketplace-country",
];
const LEDGER_PRODUCT_NAME_ALIASES = [
  "product-name", "product name", "title", "item-name", "item name", "description", "product_name",
];

export function mapRowToAmazonInventoryLedger(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonInventoryLedgerInsert | null {
  const source_line_hash = computeSourceLineHash(orgId, row);
  const consumed = new Set<string>();
  // fnsku is the primary anchor — skip row if missing
  const fnsku = pickT(row, FNSKU_ALIASES, consumed).trim();
  if (!fnsku) return null;

  const asin = pickT(row, ASIN_ALIASES, consumed) || null;
  const sku = pickT(row, SKU_ALIASES, consumed) || null;
  const product_name = pickT(row, LEDGER_PRODUCT_NAME_ALIASES, consumed) || null;
  // Date — interpret as event_date (NOT created_at).
  const event_date_raw = pickT(row, LEDGER_EVENT_DATE_ALIASES, consumed);
  const event_date = event_date_raw ? parseIsoDate(event_date_raw) : null;
  const event_timestamp_raw = pickT(row, LEDGER_EVENT_TIMESTAMP_ALIASES, consumed);
  const event_timestamp = event_timestamp_raw ? parseIsoDateTime(event_timestamp_raw) : null;
  const country = pickT(row, LEDGER_COUNTRY_ALIASES, consumed) || null;

  // Normalise the three constraint columns to trimmed-lowercase so the value
  // stored in Postgres exactly matches the JS dedup key in deduplicateByConflictKey().
  const disposition = (pickT(row, DISPOSITION_ALIASES, consumed) || "").trim().toLowerCase() || null;
  const location    = (pickT(row, LOCATION_ALIASES, consumed)    || "").trim().toLowerCase() || null;
  const event_type  = (pickT(row, EVENT_TYPE_ALIASES, consumed)  || "").trim().toLowerCase() || null;

  return {
    organization_id: orgId,
    upload_id: uploadId,
    source_line_hash,
    fnsku,
    disposition,
    location,
    event_type,
    quantity: parseQty(pickT(row, [...QTY_ALIASES, ...ENDING_WH_BAL_ALIASES], consumed)),
    event_date,
    event_timestamp,
    sku,
    asin,
    product_name,
    country,
    raw_data: buildRawData(row, consumed),
  };
}

// ── amazon_reimbursements ─────────────────────────────────────────────────────
// DB columns: id · organization_id · upload_id · order_id · reimbursement_id ·
//             amount_reimbursed · created_at · raw_data
// (asin / qty_reimbursed_total / approval_date → raw_data)
// ─────────────────────────────────────────────────────────────────────────────

export type AmazonReimbursementInsert = {
  organization_id: string;
  upload_id: string;
  source_line_hash: string;
  reimbursement_id: string;
  order_id: string | null;
  /** DB column: `sku` — part of the unique constraint (organization_id, reimbursement_id, sku). */
  sku: string | null;
  /** DB column: `amount_reimbursed`  (was mistakenly `amount_per_unit`). */
  amount_reimbursed: number | null;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonReimbursement(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonReimbursementInsert | null {
  const source_line_hash = computeSourceLineHash(orgId, row);
  const consumed = new Set<string>();
  const reimbursement_id = pickT(row, REIMBURSEMENT_ID_ALIASES, consumed);
  if (!reimbursement_id) return null;
  return {
    organization_id:  orgId,
    upload_id:        uploadId,
    source_line_hash,
    reimbursement_id,
    order_id:         pickT(row, ORDER_ALIASES, consumed) || null,
    sku:              pickT(row, SKU_ALIASES, consumed) || null,
    amount_reimbursed: parseNum(pickT(row, AMOUNT_REIMBURSED_ALIASES, consumed)),
    raw_data:         buildRawData(row, consumed), // asin/qty/approval_date land here
  };
}

// ── amazon_settlements ────────────────────────────────────────────────────────
// Legacy CSV + Amazon flat settlement .txt (TSV): header fields on physical cols;
// all other columns (transaction-type, order-id, sku, fee columns, …) → raw_data.
// Upsert: (organization_id, upload_id, amazon_line_key).
// ─────────────────────────────────────────────────────────────────────────────

/** Normalized keys populated from settlement flat-file header columns (Phase 2). */
const SETTLEMENT_TXT_PHYSICAL_KEYS = new Set([
  "settlement_id",
  "settlement_start_date",
  "settlement_end_date",
  "deposit_date",
  "total_amount",
  "currency",
]);

/** Deterministic line id (no node:crypto — this module is imported from client + server). */
function settlementAmazonLineKey(parts: string[]): string {
  const s = parts.join("\x1e");
  let h1 = 2166136261 >>> 0;
  let h2 = 374761393 >>> 0;
  for (let i = 0; i < s.length; i++) {
    h1 ^= s.charCodeAt(i);
    h1 = Math.imul(h1, 16777619) >>> 0;
    h2 += (s.charCodeAt(i) * (i + 1)) >>> 0;
    h2 = Math.imul(h2, 2654435761) >>> 0;
  }
  return `${h1.toString(16).padStart(8, "0")}${h2.toString(16).padStart(8, "0")}_${s.length.toString(16)}`;
}

/**
 * Deterministic FNV content fingerprint for a CSV row.
 * Combines organization_id + sorted key=value pairs of the full raw row.
 * Same row from any upload/file → same hash → idempotent upsert.
 * Different rows (different amounts, SKUs, fee types) → different hashes.
 *
 * Used as `source_line_hash` in amazon_transactions and all raw-archive tables.
 * Does NOT use node:crypto so it is safe in both client and server contexts.
 */
export function computeSourceLineHash(orgId: string, row: Record<string, string>): string {
  const parts = Object.keys(row)
    .sort()
    .map((k) => `${k}=${String(row[k] ?? "").trim()}`);
  parts.unshift(orgId);
  const s = parts.join("\x1f");
  let h1 = 2166136261 >>> 0;
  let h2 = 374761393 >>> 0;
  for (let i = 0; i < s.length; i++) {
    h1 ^= s.charCodeAt(i);
    h1 = Math.imul(h1, 16777619) >>> 0;
    h2 += (s.charCodeAt(i) * (i + 1)) >>> 0;
    h2 = Math.imul(h2, 2654435761) >>> 0;
  }
  return `${h1.toString(16).padStart(8, "0")}${h2.toString(16).padStart(8, "0")}_${s.length.toString(16)}`;
}

function isAmazonSettlementTxtFlatRow(row: Record<string, string>): boolean {
  return Object.prototype.hasOwnProperty.call(row, "settlement_start_date");
}

function mapRowToAmazonSettlementTxtFlat(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonSettlementInsert | null {
  const settlement_id = (row.settlement_id ?? "").trim();
  if (!settlement_id) return null;

  const raw_data: Record<string, string> = {};
  for (const [k, v] of Object.entries(row)) {
    if (SETTLEMENT_TXT_PHYSICAL_KEYS.has(k)) continue;
    const jsonKey = k.replace(/-/g, "_");
    if (v != null && String(v).trim() !== "") raw_data[jsonKey] = String(v);
  }

  const totalRaw = (row.total_amount ?? "").trim();
  const total_amount = totalRaw === "" ? null : parseNum(totalRaw);

  const lineKey = settlementAmazonLineKey([
    orgId,
    uploadId,
    settlement_id,
    row.settlement_start_date ?? "",
    row.settlement_end_date ?? "",
    row.deposit_date ?? "",
    totalRaw,
    row.currency ?? "",
    JSON.stringify(
      Object.keys(raw_data)
        .sort()
        .reduce<Record<string, string>>((acc, key) => {
          acc[key] = raw_data[key];
          return acc;
        }, {}),
    ),
  ]);

  return {
    organization_id: orgId,
    upload_id: uploadId,
    settlement_id,
    amazon_line_key: lineKey,
    settlement_start_date: parseIsoDateTime(row.settlement_start_date ?? "") ?? null,
    settlement_end_date: parseIsoDateTime(row.settlement_end_date ?? "") ?? null,
    deposit_date: parseIsoDateTime(row.deposit_date ?? "") ?? null,
    total_amount,
    currency: (row.currency ?? "").trim() || null,
    raw_data: Object.keys(raw_data).length > 0 ? raw_data : null,
  };
}

/**
 * Settlement CSV shape (legacy + Transaction / Payment Detail report).
 * Extracts the wide column set defined by migration 20260642 so the typed
 * physical columns are populated when present, and the original raw row is
 * preserved in raw_data.
 */
function mapRowToAmazonSettlementLegacyCsv(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonSettlementInsert | null {
  const consumed = new Set<string>();
  const settlement_id = pickT(row, SETTLEMENT_ID_ALIASES, consumed);
  if (!settlement_id) return null;
  const order_id = pickT(row, ORDER_ALIASES, consumed) || null;
  const sku = pickT(row, SKU_ALIASES, consumed) || null;
  const transaction_type = pickT(row, TX_TYPE_ALIASES, consumed) || null;
  const amount_total = parseNum(pickT(row, AMOUNT_TOTAL_ALIASES, consumed));
  const posted_date = parseIsoDateTime(pickT(row, DEPOSIT_DATE_ALIASES, consumed));

  // Transaction / Payment Detail report typed columns.
  const quantity = parseQty(pickT(row, SETTLEMENT_QUANTITY_ALIASES, consumed));
  const marketplace = pickT(row, SETTLEMENT_MARKETPLACE_ALIASES, consumed) || null;
  const account_type = pickT(row, SETTLEMENT_ACCOUNT_TYPE_ALIASES, consumed) || null;
  const fulfillment_channel = pickT(row, SETTLEMENT_FULFILLMENT_ALIASES, consumed) || null;
  const description = pickT(row, SETTLEMENT_DESCRIPTION_ALIASES, consumed) || null;
  const product_sales = parseNum(pickT(row, SETTLEMENT_PRODUCT_SALES_ALIASES, consumed));
  const product_sales_tax = parseNum(pickT(row, SETTLEMENT_PRODUCT_SALES_TAX_ALIASES, consumed));
  const shipping_credits = parseNum(pickT(row, SETTLEMENT_SHIPPING_CREDITS_ALIASES, consumed));
  const shipping_credits_tax = parseNum(pickT(row, SETTLEMENT_SHIPPING_CREDITS_TAX_ALIASES, consumed));
  const gift_wrap_credits = parseNum(pickT(row, SETTLEMENT_GIFT_WRAP_CREDITS_ALIASES, consumed));
  const giftwrap_credits_tax = parseNum(pickT(row, SETTLEMENT_GIFTWRAP_CREDITS_TAX_ALIASES, consumed));
  const regulatory_fee = parseNum(pickT(row, SETTLEMENT_REGULATORY_FEE_ALIASES, consumed));
  const tax_on_regulatory_fee = parseNum(pickT(row, SETTLEMENT_TAX_ON_REG_FEE_ALIASES, consumed));
  const promotional_rebates = parseNum(pickT(row, SETTLEMENT_PROMOTIONAL_REBATES_ALIASES, consumed));
  const promotional_rebates_tax = parseNum(pickT(row, SETTLEMENT_PROMO_REBATES_TAX_ALIASES, consumed));
  const marketplace_withheld_tax = parseNum(pickT(row, SETTLEMENT_MARKETPLACE_WITHHELD_TAX_ALIASES, consumed));
  const selling_fees = parseNum(pickT(row, SETTLEMENT_SELLING_FEES_ALIASES, consumed));
  const fba_fees = parseNum(pickT(row, SETTLEMENT_FBA_FEES_ALIASES, consumed));
  const other_transaction_fees = parseNum(pickT(row, SETTLEMENT_OTHER_TX_FEES_ALIASES, consumed));
  const other_amount = parseNum(pickT(row, SETTLEMENT_OTHER_AMOUNT_ALIASES, consumed));
  const transaction_status = pickT(row, SETTLEMENT_TX_STATUS_ALIASES, consumed) || null;
  const transaction_release_date = parseIsoDateTime(
    pickT(row, SETTLEMENT_TX_RELEASE_DATE_ALIASES, consumed),
  );

  const lineKey = settlementAmazonLineKey([
    "legacy",
    orgId,
    uploadId,
    settlement_id,
    order_id ?? "",
    sku ?? "",
    transaction_type ?? "",
    String(amount_total ?? ""),
    posted_date ?? "",
    description ?? "",
    transaction_status ?? "",
  ]);

  return {
    organization_id: orgId,
    upload_id: uploadId,
    settlement_id,
    amazon_line_key: lineKey,
    order_id,
    sku,
    transaction_type,
    amount_total,
    posted_date,
    quantity,
    marketplace,
    account_type,
    fulfillment_channel,
    description,
    product_sales,
    product_sales_tax,
    shipping_credits,
    shipping_credits_tax,
    gift_wrap_credits,
    giftwrap_credits_tax,
    regulatory_fee,
    tax_on_regulatory_fee,
    promotional_rebates,
    promotional_rebates_tax,
    marketplace_withheld_tax,
    selling_fees,
    fba_fees,
    other_transaction_fees,
    other_amount,
    transaction_status,
    transaction_release_date,
    raw_data: buildRawData(row, consumed),
  };
}

export type AmazonSettlementInsert = {
  organization_id: string;
  upload_id: string;
  settlement_id: string;
  /** Dedupe key for upsert — required on all inserts (migration backfills legacy to id::text). */
  amazon_line_key: string;
  settlement_start_date?: string | null;
  settlement_end_date?: string | null;
  deposit_date?: string | null;
  total_amount?: number | null;
  currency?: string | null;
  order_id?: string | null;
  sku?: string | null;
  transaction_type?: string | null;
  amount_total?: number | null;
  posted_date?: string | null;
  // Migration 20260642 — Transaction / Payment Detail report.
  quantity?: number | null;
  marketplace?: string | null;
  account_type?: string | null;
  fulfillment_channel?: string | null;
  description?: string | null;
  product_sales?: number | null;
  product_sales_tax?: number | null;
  shipping_credits?: number | null;
  shipping_credits_tax?: number | null;
  gift_wrap_credits?: number | null;
  giftwrap_credits_tax?: number | null;
  regulatory_fee?: number | null;
  tax_on_regulatory_fee?: number | null;
  promotional_rebates?: number | null;
  promotional_rebates_tax?: number | null;
  marketplace_withheld_tax?: number | null;
  selling_fees?: number | null;
  fba_fees?: number | null;
  other_transaction_fees?: number | null;
  other_amount?: number | null;
  transaction_status?: string | null;
  transaction_release_date?: string | null;
  raw_data?: Record<string, string> | null;
};

export function mapRowToAmazonSettlement(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonSettlementInsert | null {
  if (isAmazonSettlementTxtFlatRow(row)) {
    return mapRowToAmazonSettlementTxtFlat(row, orgId, uploadId);
  }
  return mapRowToAmazonSettlementLegacyCsv(row, orgId, uploadId);
}

// ── amazon_safet_claims ───────────────────────────────────────────────────────
// DB columns: id · organization_id · upload_id · claim_date · safet_claim_id ·
//             order_id · asin · item_name · claim_reason · claim_status ·
//             claim_amount · total_reimbursement_amount · created_at · raw_data
//
// Hard-coded overrides (Task requirement):
//   CSV "SAFE-T Claim ID"      → DB safet_claim_id        (NOT claim_id)
//   CSV "Reimbursement Amount" → DB total_reimbursement_amount  (NOT amount)
// ─────────────────────────────────────────────────────────────────────────────

export type AmazonSafetClaimInsert = {
  organization_id: string;
  upload_id: string;
  safet_claim_id: string;
  order_id: string | null;
  asin: string | null;
  item_name: string | null;
  claim_reason: string | null;
  claim_status: string | null;
  claim_amount: number | null;
  total_reimbursement_amount: number | null;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonSafetClaim(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonSafetClaimInsert | null {
  const consumed = new Set<string>();
  const safet_claim_id = pickT(row, SAFET_CLAIM_ID_ALIASES, consumed);
  if (!safet_claim_id) return null;
  const total_reimbursement_amount = parseNum(
    pickT(row, SAFET_REIMBURSEMENT_AMOUNT_ALIASES, consumed),
  );
  return {
    organization_id: orgId,
    upload_id: uploadId,
    safet_claim_id,
    order_id:                  pickT(row, ORDER_ALIASES, consumed) || null,
    asin:                      pickT(row, ASIN_ALIASES, consumed) || null,
    item_name:                 pickT(row, TITLE_ALIASES, consumed) || null,
    claim_reason:              pickT(row, CLAIM_REASON_ALIASES, consumed) || null,
    claim_status:              pickT(row, CLAIM_STATUS_ALIASES, consumed) || null,
    claim_amount:              parseNum(pickT(row, CLAIM_AMOUNT_ALIASES, consumed)),
    total_reimbursement_amount,
    raw_data:                  buildRawData(row, consumed),
  };
}

// ── amazon_transactions ───────────────────────────────────────────────────────
// DB columns: id · organization_id · upload_id · settlement_id · order_id ·
//             transaction_type · amount · sku · posted_date · created_at · raw_data
// ─────────────────────────────────────────────────────────────────────────────

export type AmazonTransactionInsert = {
  organization_id: string;
  upload_id: string;
  /** Deterministic content fingerprint — new dedup key replacing the old (org, order_id, tx_type, amount) constraint. */
  source_line_hash: string;
  settlement_id: string | null;
  transaction_type: string;
  order_id: string | null;
  sku: string | null;
  posted_date: string | null;
  /** DB column: `amount` — settlement lines use price-amount / fee columns. */
  amount: number | null;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonTransaction(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonTransactionInsert | null {
  const consumed = new Set<string>();
  const base_tx = pickT(row, TX_TYPE_ALIASES, consumed);
  if (!base_tx?.trim()) return null;

  // Compute hash from FULL raw row BEFORE extracting fields — prevents collapse
  // of distinct rows that share order_id + transaction_type + amount.
  const source_line_hash = computeSourceLineHash(orgId, row);

  const settlement_id = pickT(row, TX_SETTLEMENT_ID_ALIASES, consumed) || null;
  const order_id = pickT(row, ORDER_ALIASES, consumed) || null;
  const sku = pickT(row, SKU_ALIASES, consumed) || null;
  const posted_raw = pickT(row, POSTED_DATE_ALIASES, consumed);
  const posted_date = parseIsoDateTime(posted_raw) ?? null;

  const price_type = pickT(row, PRICE_TYPE_ALIASES, consumed);
  const item_fee_type = pickT(row, ITEM_RELATED_FEE_TYPE_ALIASES, consumed);
  const ship_fee_type = pickT(row, SHIPMENT_FEE_TYPE_ALIASES, consumed);
  const promo_type = pickT(row, PROMOTION_TYPE_ALIASES, consumed);
  const order_item_code = pickT(row, ORDER_ITEM_CODE_ALIASES, consumed);

  const discrim_parts = [price_type, item_fee_type, ship_fee_type, promo_type, sku, order_item_code].filter(
    (p) => p && String(p).trim(),
  );
  const transaction_type =
    discrim_parts.length > 0 ? `${base_tx} | ${discrim_parts.join(" | ")}` : base_tx;

  let amount: number | null = null;
  for (const aliases of TX_LINE_AMOUNT_ALIAS_GROUPS) {
    const v = pickT(row, aliases, consumed);
    const n = parseNum(v);
    if (n != null) {
      amount = n;
      break;
    }
  }
  if (amount == null) {
    amount = parseNum(pickT(row, TX_AMOUNT_ALIASES, consumed));
  }

  return {
    organization_id: orgId,
    upload_id: uploadId,
    source_line_hash,
    settlement_id,
    transaction_type,
    order_id,
    sku,
    posted_date,
    amount,
    raw_data: buildRawData(row, consumed),
  };
}

// ── amazon_reports_repository ─────────────────────────────────────────────────
// Amazon Reports Repository CSV: date/time, settlement id, type, order id, sku,
// description, total — remaining columns → raw_data
// ─────────────────────────────────────────────────────────────────────────────

const REPORTS_REPO_DATE_ALIASES = [
  "date/time",
  "date-time",
  "datetime",
  "date_time",
  "posted-date",
  "posted date",
];
const REPORTS_REPO_SETTLEMENT_ALIASES = [
  "settlement-id",
  "settlement id",
  "settlement_id",
  "Settlement ID",
];
const REPORTS_REPO_TYPE_ALIASES = ["type", "transaction-type", "transaction type", "transaction_type"];
const REPORTS_REPO_DESC_ALIASES = ["description", "Description"];
const REPORTS_REPO_TOTAL_ALIASES = ["total", "total-amount", "total amount", "total_amount"];

export type AmazonReportsRepositoryInsert = {
  organization_id: string;
  upload_id: string;
  source_line_hash: string;
  date_time: string | null;
  settlement_id: string | null;
  transaction_type: string;
  order_id: string | null;
  sku: string | null;
  description: string | null;
  total_amount: number;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonReportsRepository(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonReportsRepositoryInsert | null {
  const source_line_hash = computeSourceLineHash(orgId, row);
  const consumed = new Set<string>();
  const transaction_type = pickT(row, REPORTS_REPO_TYPE_ALIASES, consumed).trim();
  if (!transaction_type) return null;

  const date_raw = pickT(row, REPORTS_REPO_DATE_ALIASES, consumed);
  const date_time = parseIsoDateTime(date_raw) ?? null;

  const settlement_id = pickT(row, REPORTS_REPO_SETTLEMENT_ALIASES, consumed) || null;
  const order_id = pickT(row, ORDER_ALIASES, consumed) || null;
  const sku = pickT(row, SKU_ALIASES, consumed) || null;
  const description = pickT(row, REPORTS_REPO_DESC_ALIASES, consumed) || null;

  const total_raw = pickT(row, REPORTS_REPO_TOTAL_ALIASES, consumed);
  const total_amount = parseNum(total_raw) ?? 0;

  return {
    organization_id: orgId,
    upload_id: uploadId,
    source_line_hash,
    date_time,
    settlement_id,
    transaction_type,
    order_id,
    sku,
    description,
    total_amount,
    raw_data: buildRawData(row, consumed),
  };
}

// =============================================================================
// ── Generic raw-archive mapper (ALL_ORDERS / REPLACEMENTS / FBA_GRADE_AND_RESELL /
//    MANAGE_FBA_INVENTORY / FBA_INVENTORY / RESERVED_INVENTORY / FEE_PREVIEW /
//    MONTHLY_STORAGE_FEES)
//
// Strategy:
//   • source_line_hash computed from full row — idempotent dedup key.
//   • A handful of universal columns (order_id, asin, fnsku, sku) extracted for
//     direct querying; all other values land in raw_data JSONB.
//   • packPayloadForSupabase() in sync/route.ts ensures only native columns reach
//     Postgres; extras stay in raw_data.
//   • Mapper NEVER returns null for these append-only reports — every non-blank
//     row is archived regardless of missing anchor fields.
// =============================================================================

export type AmazonRawArchiveInsert = {
  organization_id: string;
  store_id: string | null;
  source_upload_id: string;
  source_line_hash: string;
  order_id: string | null;
  asin: string | null;
  fnsku: string | null;
  sku: string | null;
  raw_data: Record<string, string>;
};

/**
 * Amazon Fulfilled Shipments report → amazon_all_orders typed mapper.
 *
 * Replaces the generic raw-archive path for ALL_ORDERS so the Fulfilled
 * Shipments-specific columns land on physical columns (Migration 20260642).
 *
 * Shipment-level fields (Shipment ID, Shipment Item Id, Shipment Date,
 * Carrier, Tracking Number, Estimated Arrival Date, FC, Reporting Date,
 * Payments Date, Buyer*, Recipient*, Shipping Address*, Billing Address*,
 * Item Promo Discount, Shipment Promo Discount, etc.) are intentionally NOT
 * pulled out as physical columns and remain in raw_data so the import is
 * lossless without bloating the `amazon_all_orders` table.
 */
const SHIPMENTS_AMAZON_ORDER_ID_ALIASES = ["amazon-order-id", "amazon order id", "Amazon Order Id"];
const SHIPMENTS_MERCHANT_ORDER_ID_ALIASES = ["merchant-order-id", "merchant order id", "Merchant Order Id"];
const SHIPMENTS_PURCHASE_DATE_ALIASES = ["purchase-date", "purchase date", "Purchase Date"];
const SHIPMENTS_TITLE_ALIASES = ["title", "Title", "product-name", "product name"];
const SHIPMENTS_QTY_ALIASES = ["shipped-quantity", "shipped quantity", "Shipped Quantity", "quantity"];
const SHIPMENTS_CURRENCY_ALIASES = ["currency", "Currency"];
const SHIPMENTS_ITEM_PRICE_ALIASES = ["item-price", "item price", "Item Price"];
const SHIPMENTS_ITEM_TAX_ALIASES = ["item-tax", "item tax", "Item Tax"];
const SHIPMENTS_SHIPPING_PRICE_ALIASES = ["shipping-price", "shipping price", "Shipping Price"];
const SHIPMENTS_SHIP_COUNTRY_ALIASES = [
  "shipping-country-code", "shipping country code", "Shipping Country Code",
  "ship-country", "ship country",
];
const SHIPMENTS_FULFILLMENT_ALIASES = ["fulfillment-channel", "fulfillment channel", "Fulfillment Channel"];
const SHIPMENTS_SALES_CHANNEL_ALIASES = ["sales-channel", "sales channel", "Sales Channel"];

export type AmazonAllOrdersInsert = {
  organization_id: string;
  store_id: string | null;
  source_upload_id: string;
  source_line_hash: string;
  amazon_order_id: string | null;
  merchant_order_id: string | null;
  /** Mirrored from amazon_order_id for legacy joins/queries. */
  order_id: string | null;
  purchase_date: string | null;
  sku: string | null;
  product_name: string | null;
  quantity: number | null;
  currency: string | null;
  item_price: number | null;
  item_tax: number | null;
  shipping_price: number | null;
  ship_country: string | null;
  fulfillment_channel: string | null;
  sales_channel: string | null;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonAllOrders(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
  storeId: string | null,
): AmazonAllOrdersInsert | null {
  const hasContent = Object.values(row).some((v) => v?.trim() !== "");
  if (!hasContent) return null;
  const source_line_hash = computeSourceLineHash(orgId, row);
  const consumed = new Set<string>();

  const amazon_order_id = pickT(row, SHIPMENTS_AMAZON_ORDER_ID_ALIASES, consumed) || null;
  const merchant_order_id = pickT(row, SHIPMENTS_MERCHANT_ORDER_ID_ALIASES, consumed) || null;
  const purchaseRaw = pickT(row, SHIPMENTS_PURCHASE_DATE_ALIASES, consumed);
  const purchase_date = purchaseRaw ? parseIsoDateTime(purchaseRaw) : null;
  const sku = pickT(row, SKU_ALIASES, consumed) || null;
  const product_name = pickT(row, SHIPMENTS_TITLE_ALIASES, consumed) || null;
  const quantity = parseQty(pickT(row, SHIPMENTS_QTY_ALIASES, consumed));
  const currency = pickT(row, SHIPMENTS_CURRENCY_ALIASES, consumed) || null;
  const item_price = parseNum(pickT(row, SHIPMENTS_ITEM_PRICE_ALIASES, consumed));
  const item_tax = parseNum(pickT(row, SHIPMENTS_ITEM_TAX_ALIASES, consumed));
  const shipping_price = parseNum(pickT(row, SHIPMENTS_SHIPPING_PRICE_ALIASES, consumed));
  const ship_country = pickT(row, SHIPMENTS_SHIP_COUNTRY_ALIASES, consumed) || null;
  const fulfillment_channel = pickT(row, SHIPMENTS_FULFILLMENT_ALIASES, consumed) || null;
  const sales_channel = pickT(row, SHIPMENTS_SALES_CHANNEL_ALIASES, consumed) || null;

  return {
    organization_id: orgId,
    store_id: storeId || null,
    source_upload_id: uploadId,
    source_line_hash,
    amazon_order_id,
    merchant_order_id,
    order_id: amazon_order_id,
    purchase_date,
    sku,
    product_name,
    quantity,
    currency,
    item_price,
    item_tax,
    shipping_price,
    ship_country,
    fulfillment_channel,
    sales_channel,
    raw_data: buildRawData(row, consumed),
  };
}

/**
 * Generic raw-archive mapper shared by all 8 new report types.
 * Returns null only if the entire row is blank (skip empty CSV lines).
 */
export function mapRowToAmazonRawArchive(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
  storeId: string | null,
): AmazonRawArchiveInsert | null {
  const hasContent = Object.values(row).some((v) => v?.trim() !== "");
  if (!hasContent) return null;

  const source_line_hash = computeSourceLineHash(orgId, row);

  const raw_data: Record<string, string> = {};
  for (const [k, v] of Object.entries(row)) {
    if (v != null && String(v).trim() !== "") raw_data[k] = String(v);
  }

  return {
    organization_id: orgId,
    store_id: storeId || null,
    source_upload_id: uploadId,
    source_line_hash,
    order_id: pick(row, ORDER_ALIASES) || null,
    asin: pick(row, ASIN_ALIASES) || null,
    fnsku: pick(row, FNSKU_ALIASES) || null,
    sku: pick(row, SKU_ALIASES) || null,
    raw_data,
  };
}

// =============================================================================
// ── Specialised mappers for the four FBA-inventory report families
//    (MANAGE_FBA_INVENTORY, FBA_INVENTORY, INBOUND_PERFORMANCE,
//     AMAZON_FULFILLED_INVENTORY).
//
// Strategy:
//   • Every column listed in the spec is consumed via pickT() so it lands in
//     a NATIVE column; everything else falls through to raw_data via
//     buildRawData(). packPayloadForSupabase() in sync/route.ts is the final
//     safety net that re-buckets unknown columns even after the mapper runs.
//   • Mappers never throw on a missing optional column. They return null only
//     for fully blank rows.
// =============================================================================

const PRODUCT_NAME_ALIASES = ["product-name", "product name", "title", "item-name", "item name"];
const CONDITION_ALIASES = ["condition", "item-condition", "condition-type"];

/** integer parser that tolerates blanks, "—", commas; returns null if empty/non-numeric. */
function parseIntSafe(v: string | null | undefined): number | null {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s || s === "—" || s === "-") return null;
  const n = parseInt(s.replace(/,/g, ""), 10);
  return Number.isFinite(n) ? n : null;
}

/** numeric parser tolerating blanks, currency symbols, "—". */
function parseNumSafe(v: string | null | undefined): number | null {
  if (v == null) return null;
  const s = String(v).trim().replace(/[,$\u00A0]/g, "");
  if (!s || s === "—" || s === "-") return null;
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : null;
}

/** "yes"/"true"/"1"/"y" → true · "no"/"false"/"0"/"n" → false · else null. */
function parseBoolLoose(v: string | null | undefined): boolean | null {
  if (v == null) return null;
  const s = String(v).trim().toLowerCase();
  if (!s) return null;
  if (["yes", "y", "true", "t", "1"].includes(s)) return true;
  if (["no", "n", "false", "f", "0"].includes(s)) return false;
  return null;
}

/** Pass through trimmed string or null — Amazon dates appear as "YYYY-MM-DD" in these reports. */
function passThroughDateText(v: string | null | undefined): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  return s || null;
}

// ── MANAGE_FBA_INVENTORY ──────────────────────────────────────────────────────

export type AmazonManageFbaInventoryInsert = {
  organization_id: string;
  store_id: string | null;
  source_upload_id: string;
  source_line_hash: string;
  sku: string | null;
  fnsku: string | null;
  asin: string | null;
  product_name: string | null;
  condition: string | null;
  your_price: number | null;
  mfn_listing_exists: boolean | null;
  mfn_fulfillable_quantity: number | null;
  afn_listing_exists: boolean | null;
  afn_warehouse_quantity: number | null;
  afn_fulfillable_quantity: number | null;
  afn_unsellable_quantity: number | null;
  afn_reserved_quantity: number | null;
  afn_total_quantity: number | null;
  per_unit_volume: number | null;
  afn_inbound_working_quantity: number | null;
  afn_inbound_shipped_quantity: number | null;
  afn_inbound_receiving_quantity: number | null;
  afn_researching_quantity: number | null;
  afn_reserved_future_supply: number | null;
  afn_future_supply_buyable: number | null;
  store: string | null;
  // Migration 20260642 — Restock Inventory columns.
  inbound_quantity: number | null;
  fc_transfer_quantity: number | null;
  fc_processing_quantity: number | null;
  customer_order_quantity: number | null;
  recommended_replenishment_qty: number | null;
  recommended_ship_date: string | null;
  recommended_action: string | null;
  unit_storage_size: string | null;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonManageFbaInventory(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
  storeId: string | null,
): AmazonManageFbaInventoryInsert | null {
  const hasContent = Object.values(row).some((v) => v != null && String(v).trim() !== "");
  if (!hasContent) return null;

  const source_line_hash = computeSourceLineHash(orgId, row);
  const consumed = new Set<string>();

  return {
    organization_id: orgId,
    store_id: storeId || null,
    source_upload_id: uploadId,
    source_line_hash,
    sku:                pickT(row, ["sku", "merchant-sku", "msku", "seller-sku", "seller sku"], consumed) || null,
    fnsku:              pickT(row, FNSKU_ALIASES, consumed) || null,
    asin:               pickT(row, ASIN_ALIASES, consumed) || null,
    product_name:       pickT(row, PRODUCT_NAME_ALIASES, consumed) || null,
    condition:          pickT(row, CONDITION_ALIASES, consumed) || null,
    your_price:         parseNumSafe(pickT(row, ["your-price", "your price", "price"], consumed)),
    mfn_listing_exists: parseBoolLoose(pickT(row, ["mfn-listing-exists", "mfn listing exists"], consumed)),
    mfn_fulfillable_quantity: parseIntSafe(pickT(row, ["mfn-fulfillable-quantity", "mfn fulfillable quantity"], consumed)),
    afn_listing_exists: parseBoolLoose(pickT(row, ["afn-listing-exists", "afn listing exists"], consumed)),
    afn_warehouse_quantity: parseIntSafe(pickT(row, ["afn-warehouse-quantity", "afn warehouse quantity"], consumed)),
    afn_fulfillable_quantity: parseIntSafe(pickT(row, ["afn-fulfillable-quantity", "afn fulfillable quantity"], consumed)),
    afn_unsellable_quantity: parseIntSafe(pickT(row, ["afn-unsellable-quantity", "afn unsellable quantity"], consumed)),
    afn_reserved_quantity: parseIntSafe(pickT(row, ["afn-reserved-quantity", "afn reserved quantity"], consumed)),
    afn_total_quantity: parseIntSafe(pickT(row, ["afn-total-quantity", "afn total quantity"], consumed)),
    per_unit_volume: parseNumSafe(pickT(row, ["per-unit-volume", "per unit volume"], consumed)),
    afn_inbound_working_quantity: parseIntSafe(pickT(row, ["afn-inbound-working-quantity", "afn inbound working quantity"], consumed)),
    afn_inbound_shipped_quantity: parseIntSafe(pickT(row, ["afn-inbound-shipped-quantity", "afn inbound shipped quantity"], consumed)),
    afn_inbound_receiving_quantity: parseIntSafe(pickT(row, ["afn-inbound-receiving-quantity", "afn inbound receiving quantity"], consumed)),
    afn_researching_quantity: parseIntSafe(pickT(row, ["afn-researching-quantity", "afn researching quantity"], consumed)),
    afn_reserved_future_supply: parseIntSafe(pickT(row, ["afn-reserved-future-supply", "afn reserved future supply"], consumed)),
    afn_future_supply_buyable: parseIntSafe(pickT(row, ["afn-future-supply-buyable", "afn future supply buyable"], consumed)),
    store:              pickT(row, ["store"], consumed) || null,
    // Restock Inventory typed columns (Restock report).
    inbound_quantity: parseIntSafe(pickT(row, ["Inbound", "inbound", "inbound-quantity", "inbound quantity"], consumed)),
    fc_transfer_quantity: parseIntSafe(pickT(row, ["FC transfer", "fc transfer", "fc-transfer-quantity"], consumed)),
    fc_processing_quantity: parseIntSafe(pickT(row, ["FC Processing", "fc processing", "fc-processing-quantity"], consumed)),
    customer_order_quantity: parseIntSafe(pickT(row, ["Customer Order", "customer order", "customer-order-quantity"], consumed)),
    recommended_replenishment_qty: parseIntSafe(pickT(row, [
      "Recommended replenishment qty", "recommended replenishment qty", "recommended-replenishment-qty",
    ], consumed)),
    recommended_ship_date: passThroughDateText(pickT(row, [
      "Recommended ship date", "recommended ship date", "recommended-ship-date",
    ], consumed)),
    recommended_action: pickT(row, [
      "Recommended action", "recommended action", "recommended-action",
    ], consumed) || null,
    unit_storage_size: pickT(row, [
      "Unit storage size", "unit storage size", "unit-storage-size",
    ], consumed) || null,
    raw_data:           buildRawData(row, consumed),
  };
}

// ── FBA_INVENTORY (Inventory Health) ──────────────────────────────────────────

export type AmazonFbaInventoryInsert = {
  organization_id: string;
  store_id: string | null;
  source_upload_id: string;
  source_line_hash: string;
  snapshot_date: string | null;
  sku: string | null;
  fnsku: string | null;
  asin: string | null;
  product_name: string | null;
  condition: string | null;
  available: number | null;
  pending_removal_quantity: number | null;
  inv_age_0_to_90_days: number | null;
  inv_age_91_to_180_days: number | null;
  inv_age_181_to_270_days: number | null;
  inv_age_271_to_365_days: number | null;
  inv_age_366_to_455_days: number | null;
  inv_age_456_plus_days: number | null;
  currency: string | null;
  units_shipped_t7: number | null;
  units_shipped_t30: number | null;
  units_shipped_t60: number | null;
  units_shipped_t90: number | null;
  alert: string | null;
  your_price: number | null;
  sales_price: number | null;
  recommended_action: string | null;
  sell_through: number | null;
  item_volume: number | null;
  volume_unit_measurement: string | null;
  storage_type: string | null;
  storage_volume: number | null;
  marketplace: string | null;
  product_group: string | null;
  sales_rank: number | null;
  days_of_supply: number | null;
  estimated_excess_quantity: number | null;
  weeks_of_cover_t30: number | null;
  weeks_of_cover_t90: number | null;
  estimated_storage_cost_next_month: number | null;
  inbound_quantity: number | null;
  inbound_working: number | null;
  inbound_shipped: number | null;
  inbound_received: number | null;
  no_sale_last_6_months: number | null;
  total_reserved_quantity: number | null;
  unfulfillable_quantity: number | null;
  historical_days_of_supply: number | null;
  fba_minimum_inventory_level: number | null;
  fba_inventory_level_health_status: string | null;
  recommended_ship_in_quantity: number | null;
  recommended_ship_in_date: string | null;
  inventory_age_snapshot_date: string | null;
  inventory_supply_at_fba: number | null;
  reserved_fc_transfer: number | null;
  reserved_fc_processing: number | null;
  reserved_customer_order: number | null;
  total_days_of_supply_including_open_shipments: number | null;
  supplier: string | null;
  is_seasonal_in_next_3_months: boolean | null;
  season_name: string | null;
  season_start_date: string | null;
  season_end_date: string | null;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonFbaInventory(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
  storeId: string | null,
): AmazonFbaInventoryInsert | null {
  const hasContent = Object.values(row).some((v) => v != null && String(v).trim() !== "");
  if (!hasContent) return null;

  const source_line_hash = computeSourceLineHash(orgId, row);
  const consumed = new Set<string>();
  const num = (a: string[]) => parseNumSafe(pickT(row, a, consumed));
  const int = (a: string[]) => parseIntSafe(pickT(row, a, consumed));
  const txt = (a: string[]) => pickT(row, a, consumed) || null;
  const dt  = (a: string[]) => passThroughDateText(pickT(row, a, consumed));

  return {
    organization_id: orgId,
    store_id: storeId || null,
    source_upload_id: uploadId,
    source_line_hash,
    snapshot_date: dt(["snapshot-date", "snapshot date"]),
    sku: txt(["sku", "merchant-sku", "msku"]),
    fnsku: txt(FNSKU_ALIASES),
    asin: txt(ASIN_ALIASES),
    product_name: txt(PRODUCT_NAME_ALIASES),
    condition: txt(CONDITION_ALIASES),
    available: int(["available", "afn-fulfillable-quantity"]),
    pending_removal_quantity: int(["pending-removal-quantity", "pending removal quantity"]),
    inv_age_0_to_90_days: int(["inv-age-0-to-90-days", "inv age 0 to 90 days"]),
    inv_age_91_to_180_days: int(["inv-age-91-to-180-days", "inv age 91 to 180 days"]),
    inv_age_181_to_270_days: int(["inv-age-181-to-270-days", "inv age 181 to 270 days"]),
    inv_age_271_to_365_days: int(["inv-age-271-to-365-days", "inv age 271 to 365 days"]),
    inv_age_366_to_455_days: int(["inv-age-366-to-455-days", "inv age 366 to 455 days"]),
    inv_age_456_plus_days: int(["inv-age-456-plus-days", "inv age 456 plus days"]),
    currency: txt(["currency"]),
    units_shipped_t7: int(["units-shipped-t7", "units shipped t7"]),
    units_shipped_t30: int(["units-shipped-t30", "units shipped t30"]),
    units_shipped_t60: int(["units-shipped-t60", "units shipped t60"]),
    units_shipped_t90: int(["units-shipped-t90", "units shipped t90"]),
    alert: txt(["alert"]),
    your_price: num(["your-price", "your price"]),
    sales_price: num(["sales-price", "sales price"]),
    recommended_action: txt(["recommended-action", "recommended action"]),
    sell_through: num(["sell-through", "sell through"]),
    item_volume: num(["item-volume", "item volume"]),
    volume_unit_measurement: txt(["volume-unit-measurement", "volume unit measurement"]),
    storage_type: txt(["storage-type", "storage type"]),
    storage_volume: num(["storage-volume", "storage volume"]),
    marketplace: txt(["marketplace"]),
    product_group: txt(["product-group", "product group"]),
    sales_rank: int(["sales-rank", "sales rank"]),
    days_of_supply: int(["days-of-supply", "days of supply"]),
    estimated_excess_quantity: int(["estimated-excess-quantity", "estimated excess quantity"]),
    weeks_of_cover_t30: num(["weeks-of-cover-t30", "weeks of cover t30"]),
    weeks_of_cover_t90: num(["weeks-of-cover-t90", "weeks of cover t90"]),
    estimated_storage_cost_next_month: num(["estimated-storage-cost-next-month", "estimated storage cost next month"]),
    inbound_quantity: int(["inbound-quantity", "inbound quantity"]),
    inbound_working: int(["inbound-working", "inbound working"]),
    inbound_shipped: int(["inbound-shipped", "inbound shipped"]),
    inbound_received: int(["inbound-received", "inbound received"]),
    no_sale_last_6_months: int(["no-sale-last-6-months", "no sale last 6 months"]),
    total_reserved_quantity: int(["total-reserved-quantity", "total reserved quantity"]),
    unfulfillable_quantity: int(["unfulfillable-quantity", "unfulfillable quantity"]),
    historical_days_of_supply: int(["historical-days-of-supply", "historical days of supply"]),
    fba_minimum_inventory_level: int(["fba-minimum-inventory-level", "fba minimum inventory level"]),
    fba_inventory_level_health_status: txt(["fba-inventory-level-health-status", "fba inventory level health status"]),
    recommended_ship_in_quantity: int(["recommended-ship-in-quantity", "recommended ship in quantity"]),
    recommended_ship_in_date: dt(["recommended-ship-in-date", "recommended ship in date"]),
    inventory_age_snapshot_date: dt(["inventory-age-snapshot-date", "inventory age snapshot date"]),
    inventory_supply_at_fba: int(["inventory-supply-at-fba", "inventory supply at fba"]),
    reserved_fc_transfer: int(["reserved-fc-transfer", "reserved fc transfer"]),
    reserved_fc_processing: int(["reserved-fc-processing", "reserved fc processing"]),
    reserved_customer_order: int(["reserved-customer-order", "reserved customer order"]),
    total_days_of_supply_including_open_shipments: int([
      "total-days-of-supply-including-open-shipments",
      "total days of supply including open shipments",
    ]),
    supplier: txt(["supplier"]),
    is_seasonal_in_next_3_months: parseBoolLoose(pickT(row, ["is-seasonal-in-next-3-months", "is seasonal in next 3 months"], consumed)),
    season_name: txt(["season-name", "season name"]),
    season_start_date: dt(["season-start-date", "season start date"]),
    season_end_date: dt(["season-end-date", "season end date"]),
    raw_data: buildRawData(row, consumed),
  };
}

// ── INBOUND_PERFORMANCE ───────────────────────────────────────────────────────

export type AmazonInboundPerformanceInsert = {
  organization_id: string;
  store_id: string | null;
  source_upload_id: string;
  source_line_hash: string;
  issue_reported_date: string | null;
  shipment_creation_date: string | null;
  fba_shipment_id: string | null;
  fba_carton_id: string | null;
  fulfillment_center_id: string | null;
  sku: string | null;
  fnsku: string | null;
  asin: string | null;
  product_name: string | null;
  problem_type: string | null;
  problem_quantity: number | null;
  expected_quantity: number | null;
  received_quantity: number | null;
  performance_measurement_unit: string | null;
  coaching_level: string | null;
  fee_type: string | null;
  currency: string | null;
  fee_total: number | null;
  problem_level: string | null;
  alert_status: string | null;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonInboundPerformance(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
  storeId: string | null,
): AmazonInboundPerformanceInsert | null {
  const hasContent = Object.values(row).some((v) => v != null && String(v).trim() !== "");
  if (!hasContent) return null;

  const source_line_hash = computeSourceLineHash(orgId, row);
  const consumed = new Set<string>();
  const num = (a: string[]) => parseNumSafe(pickT(row, a, consumed));
  const int = (a: string[]) => parseIntSafe(pickT(row, a, consumed));
  const txt = (a: string[]) => pickT(row, a, consumed) || null;
  const dt  = (a: string[]) => passThroughDateText(pickT(row, a, consumed));

  return {
    organization_id: orgId,
    store_id: storeId || null,
    source_upload_id: uploadId,
    source_line_hash,
    issue_reported_date: dt(["issue-reported-date", "issue reported date"]),
    shipment_creation_date: dt(["shipment-creation-date", "shipment creation date"]),
    fba_shipment_id: txt(["fba-shipment-id", "fba shipment id", "shipment-id", "shipment id"]),
    fba_carton_id: txt(["fba-carton-id", "fba carton id", "carton-id"]),
    fulfillment_center_id: txt(["fulfillment-center-id", "fulfillment center id", "fulfillment-center"]),
    sku: txt(["sku", "merchant-sku", "msku"]),
    fnsku: txt(FNSKU_ALIASES),
    asin: txt(ASIN_ALIASES),
    product_name: txt(PRODUCT_NAME_ALIASES),
    problem_type: txt(["problem-type", "problem type"]),
    problem_quantity: int(["problem-quantity", "problem quantity"]),
    expected_quantity: int(["expected-quantity", "expected quantity"]),
    received_quantity: int(["received-quantity", "received quantity"]),
    performance_measurement_unit: txt(["performance-measurement-unit", "performance measurement unit"]),
    coaching_level: txt(["coaching-level", "coaching level"]),
    fee_type: txt(["fee-type", "fee type"]),
    currency: txt(["currency"]),
    fee_total: num(["fee-total", "fee total"]),
    problem_level: txt(["problem-level", "problem level"]),
    alert_status: txt(["alert-status", "alert status"]),
    raw_data: buildRawData(row, consumed),
  };
}

// ── AMAZON_FULFILLED_INVENTORY ────────────────────────────────────────────────

export type AmazonAmazonFulfilledInventoryInsert = {
  organization_id: string;
  store_id: string | null;
  source_upload_id: string;
  source_line_hash: string;
  seller_sku: string | null;
  fulfillment_channel_sku: string | null;
  asin: string | null;
  condition_type: string | null;
  warehouse_condition_code: string | null;
  quantity_available: number | null;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonAmazonFulfilledInventory(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
  storeId: string | null,
): AmazonAmazonFulfilledInventoryInsert | null {
  const hasContent = Object.values(row).some((v) => v != null && String(v).trim() !== "");
  if (!hasContent) return null;

  const source_line_hash = computeSourceLineHash(orgId, row);
  const consumed = new Set<string>();

  return {
    organization_id: orgId,
    store_id: storeId || null,
    source_upload_id: uploadId,
    source_line_hash,
    seller_sku: pickT(row, ["seller-sku", "seller sku", "sku", "merchant-sku"], consumed) || null,
    fulfillment_channel_sku: pickT(row, ["fulfillment-channel-sku", "fulfillment channel sku", "fnsku"], consumed) || null,
    asin: pickT(row, ASIN_ALIASES, consumed) || null,
    condition_type: pickT(row, ["condition-type", "condition type", "condition"], consumed) || null,
    warehouse_condition_code: pickT(row, ["warehouse-condition-code", "warehouse condition code"], consumed) || null,
    quantity_available: parseIntSafe(pickT(row, ["quantity-available", "quantity available"], consumed)),
    raw_data: buildRawData(row, consumed),
  };
}
