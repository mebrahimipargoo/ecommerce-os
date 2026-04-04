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
    const value = row[csvHeader];
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
  "id", "organization_id", "upload_id", "request_date",
  "order_id", "sku", "fnsku", "disposition",
  "shipped_quantity", "cancelled_quantity", "disposed_quantity", "requested_quantity",
  "status", "tracking_number", "carrier",
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

/** amazon_settlements — physical DB columns */
export const NATIVE_COLUMNS_SETTLEMENTS = new Set([
  "id", "organization_id", "upload_id", "posted_date",
  "settlement_id", "order_id", "sku", "transaction_type",
  "amount_total", "product_sales", "selling_fees", "fba_fees", "description",
  "created_at", "raw_data",
]);

/** amazon_safet_claims — physical DB columns */
export const NATIVE_COLUMNS_SAFET = new Set([
  "id", "organization_id", "upload_id", "claim_date",
  "safet_claim_id", "order_id", "asin", "item_name",
  "claim_reason", "claim_status", "claim_amount", "total_reimbursement_amount",
  "created_at", "raw_data",
]);

/** amazon_transactions — physical DB columns */
export const NATIVE_COLUMNS_TRANSACTIONS = new Set([
  "id", "organization_id", "upload_id",
  "order_id", "transaction_type", "amount",
  "created_at", "raw_data",
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
const TRACK_ALIASES      = ["tracking-number", "tracking number", "tracking id", "tracking-id"];
const SKU_ALIASES        = ["sku", "merchant-sku", "msku", "SKU"];
const QTY_ALIASES        = ["requested-quantity", "requested quantity", "quantity", "qty"];
const SHIPPED_QTY_ALIASES   = ["shipped-quantity", "shipped quantity"];
const DISPOSED_QTY_ALIASES  = ["disposed-quantity", "disposed quantity"];
const CANCELLED_QTY_ALIASES = ["cancelled-quantity", "cancelled quantity"];
const ORDER_STATUS_ALIASES  = ["order-status", "order status", "status"];
const DISPOSITION_ALIASES   = ["disposition", "detailed-disposition", "detailed disposition"];
const ORDER_DATE_ALIASES    = ["order-date", "order date", "request-date", "requested-date"];
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
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
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
  order_id: string;
  sku: string;
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
// DB columns: id · organization_id · upload_id · request_date · order_id · sku ·
//             fnsku · disposition · shipped_quantity · cancelled_quantity ·
//             disposed_quantity · requested_quantity · status · tracking_number ·
//             carrier · created_at · raw_data
// ─────────────────────────────────────────────────────────────────────────────

export type AmazonRemovalInsert = {
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
  tracking_number: string | null;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonRemoval(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonRemovalInsert | null {
  const consumed = new Set<string>();
  const order_id = pickT(row, REMOVAL_ORDER_ID_ALIASES, consumed);
  if (!order_id) return null;
  return {
    organization_id: orgId,
    upload_id: uploadId,
    order_id,
    sku:                pickT(row, SKU_ALIASES, consumed) || "",
    fnsku:              pickT(row, FNSKU_ALIASES, consumed) || null,
    disposition:        pickT(row, DISPOSITION_ALIASES, consumed) || null,
    shipped_quantity:   parseQty(pickT(row, SHIPPED_QTY_ALIASES, consumed)),
    cancelled_quantity: parseQty(pickT(row, CANCELLED_QTY_ALIASES, consumed)),
    disposed_quantity:  parseQty(pickT(row, DISPOSED_QTY_ALIASES, consumed)),
    requested_quantity: parseQty(pickT(row, QTY_ALIASES, consumed)),
    status:             pickT(row, ORDER_STATUS_ALIASES, consumed) || null,
    tracking_number:    pickT(row, TRACK_ALIASES, consumed) || null,
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
// DB columns: id · organization_id · upload_id · posted_date · settlement_id ·
//             order_id · sku · transaction_type · amount_total · product_sales ·
//             selling_fees · fba_fees · description · created_at · raw_data
// ─────────────────────────────────────────────────────────────────────────────

export type AmazonSettlementInsert = {
  organization_id: string;
  upload_id: string;
  settlement_id: string;
  order_id: string | null;
  sku: string | null;
  /** DB column: `transaction_type`  (was mistakenly `transaction_status`). */
  transaction_type: string | null;
  /** DB column: `amount_total`  (was mistakenly `total`). */
  amount_total: number | null;
  /** DB column: `posted_date`  (was mistakenly `deposit_date`). */
  posted_date: string | null;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonSettlement(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonSettlementInsert | null {
  const consumed = new Set<string>();
  const settlement_id = pickT(row, SETTLEMENT_ID_ALIASES, consumed);
  if (!settlement_id) return null;
  return {
    organization_id: orgId,
    upload_id: uploadId,
    settlement_id,
    order_id:         pickT(row, ORDER_ALIASES, consumed) || null,
    sku:              pickT(row, SKU_ALIASES, consumed) || null,
    transaction_type: pickT(row, TX_TYPE_ALIASES, consumed) || null,
    amount_total:     parseNum(pickT(row, AMOUNT_TOTAL_ALIASES, consumed)),
    posted_date:      parseIsoDate(pickT(row, DEPOSIT_DATE_ALIASES, consumed)),
    raw_data:         buildRawData(row, consumed),
  };
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
// DB columns: id · organization_id · upload_id · order_id · transaction_type ·
//             amount · created_at · raw_data
// (sku / posted_date / total_product_charges → raw_data)
// ─────────────────────────────────────────────────────────────────────────────

export type AmazonTransactionInsert = {
  organization_id: string;
  upload_id: string;
  transaction_type: string;
  order_id: string | null;
  /** DB column: `amount`  Hard-coded fallback: CSV "amount" → amount. */
  amount: number | null;
  raw_data: Record<string, string> | null;
};

export function mapRowToAmazonTransaction(
  row: Record<string, string>,
  orgId: string,
  uploadId: string,
): AmazonTransactionInsert | null {
  const consumed = new Set<string>();
  const transaction_type = pickT(row, TX_TYPE_ALIASES, consumed);
  if (!transaction_type) return null;
  // Consume (but do not surface) sku / posted_date / total_product_charges — they go to raw_data
  pickT(row, SKU_ALIASES, consumed);
  pickT(row, POSTED_DATE_ALIASES, consumed);
  return {
    organization_id: orgId,
    upload_id: uploadId,
    transaction_type,
    order_id: pickT(row, ORDER_ALIASES, consumed) || null,
    amount:   parseNum(pickT(row, TX_AMOUNT_ALIASES, consumed)),
    raw_data: buildRawData(row, consumed),
  };
}
