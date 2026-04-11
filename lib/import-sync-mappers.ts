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
  "fnsku", "disposition", "location", "event_type", "quantity",
  "created_at", "raw_data",
]);

/** amazon_reimbursements — physical DB columns */
export const NATIVE_COLUMNS_REIMBURSEMENTS = new Set([
  "id", "organization_id", "upload_id",
  "order_id", "reimbursement_id", "sku", "amount_reimbursed",
  "created_at", "raw_data",
]);

/** amazon_settlements — physical DB columns (legacy + flat .txt settlement report) */
export const NATIVE_COLUMNS_SETTLEMENTS = new Set([
  "id",
  "organization_id",
  "upload_id",
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
  "created_at",
  "raw_data",
]);

/** amazon_safet_claims — physical DB columns */
export const NATIVE_COLUMNS_SAFET = new Set([
  "id", "organization_id", "upload_id", "claim_date",
  "safet_claim_id", "order_id", "asin", "item_name",
  "claim_reason", "claim_status", "claim_amount", "total_reimbursement_amount",
  "created_at", "raw_data",
]);

/** amazon_transactions — physical DB columns (source_line_hash added in migration 20260605) */
export const NATIVE_COLUMNS_TRANSACTIONS = new Set([
  "id", "organization_id", "upload_id",
  "source_line_hash",
  "settlement_id", "order_id", "transaction_type", "amount", "sku", "posted_date",
  "created_at", "raw_data",
]);

// ── New raw-archive tables (migration 20260604) ───────────────────────────────

/** amazon_all_orders — physical DB columns */
export const NATIVE_COLUMNS_ALL_ORDERS = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "order_id", "purchase_date", "order_status", "fulfillment_channel", "sales_channel",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_replacements — physical DB columns */
export const NATIVE_COLUMNS_REPLACEMENTS = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "order_id", "replacement_order_id", "asin", "sku", "order_date",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_fba_grade_and_resell — physical DB columns */
export const NATIVE_COLUMNS_FBA_GRADE_AND_RESELL = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "asin", "fnsku", "sku", "grade", "units",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_manage_fba_inventory — physical DB columns */
export const NATIVE_COLUMNS_MANAGE_FBA_INVENTORY = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "asin", "fnsku", "sku", "afn_fulfillable_quantity",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_fba_inventory — physical DB columns */
export const NATIVE_COLUMNS_FBA_INVENTORY = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "asin", "fnsku", "sku", "quantity",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_reserved_inventory — physical DB columns */
export const NATIVE_COLUMNS_RESERVED_INVENTORY = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "asin", "fnsku", "sku", "reserved_quantity",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_fee_preview — physical DB columns */
export const NATIVE_COLUMNS_FEE_PREVIEW = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "asin", "fnsku", "sku", "price", "estimated_fee", "currency",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_monthly_storage_fees — physical DB columns */
export const NATIVE_COLUMNS_MONTHLY_STORAGE_FEES = new Set([
  "id", "organization_id", "store_id", "source_upload_id", "source_line_hash",
  "asin", "fnsku", "sku", "storage_month", "storage_rate", "currency",
  "raw_data", "created_at", "updated_at",
]);

/** amazon_reports_repository — physical DB columns */
export const NATIVE_COLUMNS_REPORTS_REPOSITORY = new Set([
  "id", "organization_id", "upload_id",
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
const TX_TYPE_ALIASES          = ["transaction-type", "transaction type"];
const DEPOSIT_DATE_ALIASES     = ["deposit-date", "deposit date", "posted-date", "posted date"];
const AMOUNT_TOTAL_ALIASES     = ["total", "amount", "net-proceeds", "net proceeds", "amount-total", "amount total"];

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
// Hard-coded: CSV "amount" column → DB amount (prevents raw_data burial)
const TX_AMOUNT_ALIASES = ["amount", "Amount", "transaction-amount", "transaction amount"];
const POSTED_DATE_ALIASES = ["posted-date", "posted date", "date-time", "Date/Time", "date/time"];

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
  lpn: string;
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
): AmazonReturnInsert | null {
  const consumed = new Set<string>();
  const lpn = pickT(row, LPN_ALIASES, consumed);
  if (!lpn) return null;
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
  fnsku: string;
  /** Physical DB column: `disposition` */
  disposition: string | null;
  /** Physical DB column: `location` (maps from CSV "Fulfillment Center") */
  location: string | null;
  /** Physical DB column: `event_type` (maps from CSV "Event Type") */
  event_type: string | null;
  quantity: number | null;
  /** All non-physical CSV columns (asin, sku, country, title, …) land here. */
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonInventoryLedger(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonInventoryLedgerInsert | null {
  const consumed = new Set<string>();
  // fnsku is the primary anchor — skip row if missing
  const fnsku = pickT(row, FNSKU_ALIASES, consumed).trim();
  if (!fnsku) return null;

  // These columns are NOT physical DB columns — consume them so buildRawData()
  // captures them in the JSONB bucket rather than leaving them unconsumed.
  pickT(row, ASIN_ALIASES, consumed);
  pickT(row, SKU_ALIASES, consumed);
  pickT(row, TITLE_ALIASES, consumed);
  pickT(row, DATE_ALIASES, consumed);

  // Normalise the three constraint columns to trimmed-lowercase so the value
  // stored in Postgres exactly matches the JS dedup key in deduplicateByConflictKey().
  // This prevents "Sellable" / "SELLABLE" / " Sellable " from creating phantom
  // duplicate-key collisions at the Postgres layer.
  const disposition = (pickT(row, DISPOSITION_ALIASES, consumed) || "").trim().toLowerCase() || null;
  const location    = (pickT(row, LOCATION_ALIASES, consumed)    || "").trim().toLowerCase() || null;
  const event_type  = (pickT(row, EVENT_TYPE_ALIASES, consumed)  || "").trim().toLowerCase() || null;

  return {
    organization_id: orgId,
    upload_id: uploadId,
    fnsku,
    disposition,
    location,
    event_type,
    quantity: parseQty(pickT(row, [...QTY_ALIASES, ...ENDING_WH_BAL_ALIASES], consumed)),
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
  const consumed = new Set<string>();
  const reimbursement_id = pickT(row, REIMBURSEMENT_ID_ALIASES, consumed);
  if (!reimbursement_id) return null;
  return {
    organization_id:  orgId,
    upload_id:        uploadId,
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

/** Older settlement CSV shape (order/sku/amount_total on physical columns). */
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
  const posted_date = parseIsoDate(pickT(row, DEPOSIT_DATE_ALIASES, consumed));
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
