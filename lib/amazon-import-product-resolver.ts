/**
 * Post-import product resolver for Amazon `amazon_*` raw-archive tables.
 *
 * Reads rows for a specific (organization_id, upload_id) pair, looks up
 * `product_identifier_map` in batches using the same fnsku → seller_sku/asin →
 * sku/msku → asin priority, and patches resolved_product_id /
 * resolved_catalog_product_id / identifier_resolution_status /
 * identifier_resolution_confidence on each row.
 *
 * Idempotent — running twice yields the same writes (last_seen_at on the bridge
 * is only touched when a NEW match is created elsewhere; this resolver does not
 * mutate `product_identifier_map`).
 *
 * The resolver is order-aware:
 *   • For tables that lack any direct identifier (amazon_transactions in
 *     "Simple Transactions Summary" mode), pass `joinAllOrders: true` and the
 *     resolver will attempt to inherit a resolved_product_id from
 *     amazon_all_orders matched by order_id.
 */

import type { SupabaseClient } from "@supabase/supabase-js";

import {
  pickBestProductIdentifierMatch,
  prefetchIdentifierMapCandidatesForBatch,
  type ProductIdentifierMapRow,
} from "./product-identifier-match";

export type ResolveTargetTable =
  | "amazon_all_orders"
  | "amazon_settlements"
  | "amazon_transactions"
  | "amazon_inventory_ledger"
  | "amazon_manage_fba_inventory"
  | "amazon_amazon_fulfilled_inventory";

export type ResolveOptions = {
  supabase: SupabaseClient;
  organizationId: string;
  uploadId: string;
  /** Imports target store — required so identifier resolution stays store-scoped. */
  storeId: string;
  table: ResolveTargetTable;
  /** Batch page size for SELECT/UPDATE chunks. */
  pageSize?: number;
  /**
   * For tables without a direct identifier (amazon_transactions: simple summary
   * report). When true, after the standard resolve pass the resolver also tries
   * to inherit resolved_product_id from amazon_all_orders by order_id.
   */
  joinAllOrders?: boolean;
};

export type ResolveMetrics = {
  table: ResolveTargetTable;
  rows_scanned: number;
  rows_resolved: number;
  rows_resolved_via_fnsku: number;
  rows_resolved_via_sku_asin: number;
  rows_resolved_via_sku: number;
  rows_resolved_via_asin: number;
  rows_inherited_from_all_orders: number;
  rows_ambiguous: number;
  rows_unresolved: number;
};

const UPLOAD_ID_COLUMN: Record<ResolveTargetTable, "upload_id" | "source_upload_id"> = {
  amazon_all_orders: "source_upload_id",
  amazon_settlements: "upload_id",
  amazon_transactions: "upload_id",
  amazon_inventory_ledger: "upload_id",
  amazon_manage_fba_inventory: "source_upload_id",
  amazon_amazon_fulfilled_inventory: "source_upload_id",
};

type ResolverRow = {
  id: string;
  fnsku: string | null;
  sku: string | null;
  asin: string | null;
  order_id: string | null;
  raw_data: Record<string, unknown> | null;
};

const SELECT_COLUMNS: Record<ResolveTargetTable, string> = {
  amazon_all_orders: "id, sku, amazon_order_id, order_id, raw_data",
  amazon_settlements: "id, sku, order_id, raw_data",
  amazon_transactions: "id, sku, order_id, raw_data",
  amazon_inventory_ledger: "id, fnsku, sku, asin, raw_data",
  amazon_manage_fba_inventory: "id, fnsku, sku, asin, raw_data",
  amazon_amazon_fulfilled_inventory: "id, fulfillment_channel_sku, seller_sku, asin, raw_data",
};

function n(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function pickFromRaw(raw: Record<string, unknown> | null, keys: string[]): string | null {
  if (!raw || typeof raw !== "object") return null;
  for (const k of keys) {
    const v = (raw as Record<string, unknown>)[k];
    if (v !== null && v !== undefined && String(v).trim() !== "") return String(v).trim();
  }
  return null;
}

function adaptRow(table: ResolveTargetTable, raw: Record<string, unknown>): ResolverRow {
  const rd = (raw.raw_data && typeof raw.raw_data === "object" && !Array.isArray(raw.raw_data))
    ? (raw.raw_data as Record<string, unknown>)
    : null;
  switch (table) {
    case "amazon_all_orders":
      return {
        id: String(raw.id ?? ""),
        fnsku: pickFromRaw(rd, ["fnsku", "FNSKU", "fulfillment-network-sku"]),
        sku: n(raw.sku) ?? pickFromRaw(rd, ["sku", "Merchant SKU", "merchant-sku"]),
        asin: pickFromRaw(rd, ["asin", "ASIN"]),
        order_id: n(raw.amazon_order_id) ?? n(raw.order_id) ?? null,
        raw_data: rd,
      };
    case "amazon_settlements":
    case "amazon_transactions":
      return {
        id: String(raw.id ?? ""),
        fnsku: pickFromRaw(rd, ["fnsku", "FNSKU", "fulfillment-network-sku"]),
        sku: n(raw.sku) ?? pickFromRaw(rd, ["sku", "Merchant SKU", "merchant-sku"]),
        asin: pickFromRaw(rd, ["asin", "ASIN"]),
        order_id: n(raw.order_id) ?? null,
        raw_data: rd,
      };
    case "amazon_inventory_ledger":
      return {
        id: String(raw.id ?? ""),
        fnsku: n(raw.fnsku),
        sku: n(raw.sku) ?? pickFromRaw(rd, ["sku", "MSKU", "msku", "merchant-sku"]),
        asin: n(raw.asin) ?? pickFromRaw(rd, ["asin", "ASIN"]),
        order_id: null,
        raw_data: rd,
      };
    case "amazon_manage_fba_inventory":
      return {
        id: String(raw.id ?? ""),
        fnsku: n(raw.fnsku),
        sku: n(raw.sku) ?? pickFromRaw(rd, ["Merchant SKU", "merchant-sku", "sku"]),
        asin: n(raw.asin),
        order_id: null,
        raw_data: rd,
      };
    case "amazon_amazon_fulfilled_inventory":
      return {
        id: String(raw.id ?? ""),
        fnsku: n(raw.fulfillment_channel_sku),
        sku: n(raw.seller_sku),
        asin: n(raw.asin),
        order_id: null,
        raw_data: rd,
      };
  }
}

function statusFromMatch(
  match: ReturnType<typeof pickBestProductIdentifierMatch>,
  m: ResolveMetrics,
): { status: string; productId: string | null; catalogProductId: string | null; confidence: number } {
  if (match.status === "ambiguous") {
    m.rows_ambiguous += 1;
    return { status: "ambiguous", productId: null, catalogProductId: null, confidence: match.confidence };
  }
  if (match.status === "resolved" && match.row && match.tier != null) {
    if (match.tier === 1) m.rows_resolved_via_fnsku += 1;
    else if (match.tier === 2) m.rows_resolved_via_sku_asin += 1;
    else if (match.tier === 3) m.rows_resolved_via_sku += 1;
    else if (match.tier === 4) m.rows_resolved_via_asin += 1;
    m.rows_resolved += 1;
    return {
      status: match.tier === 1 ? "resolved" : "matched",
      productId: n(match.row.product_id),
      catalogProductId: n(match.row.catalog_product_id),
      confidence: match.confidence,
    };
  }
  m.rows_unresolved += 1;
  return { status: "unresolved", productId: null, catalogProductId: null, confidence: 0 };
}

async function fetchPage(
  supabase: SupabaseClient,
  table: ResolveTargetTable,
  organizationId: string,
  uploadId: string,
  offset: number,
  pageSize: number,
): Promise<Record<string, unknown>[]> {
  const uploadCol = UPLOAD_ID_COLUMN[table];
  const { data, error } = await supabase
    .from(table)
    .select(SELECT_COLUMNS[table])
    .eq("organization_id", organizationId)
    .eq(uploadCol, uploadId)
    .range(offset, offset + pageSize - 1);
  if (error) throw new Error(`[product-resolver] read ${table}: ${error.message}`);
  return (data ?? []) as unknown as Record<string, unknown>[];
}

async function patchRow(
  supabase: SupabaseClient,
  table: ResolveTargetTable,
  organizationId: string,
  rowId: string,
  patch: Record<string, unknown>,
): Promise<void> {
  const { error } = await supabase
    .from(table)
    .update(patch)
    .eq("organization_id", organizationId)
    .eq("id", rowId);
  if (error) {
    console.warn(`[product-resolver] update ${table} id=${rowId}: ${error.message}`);
  }
}

/**
 * Standard resolution pass — uses the row's own fnsku / sku / asin against
 * `product_identifier_map` and writes resolved_product_id back onto the row.
 */
async function runStandardResolve(opts: ResolveOptions): Promise<ResolveMetrics> {
  const { supabase, organizationId, uploadId, storeId, table } = opts;
  const pageSize = opts.pageSize ?? 500;
  if (!String(storeId ?? "").trim()) {
    throw new Error("[product-resolver] storeId is required (metadata.import_store_id on the upload).");
  }

  const m: ResolveMetrics = {
    table,
    rows_scanned: 0,
    rows_resolved: 0,
    rows_resolved_via_fnsku: 0,
    rows_resolved_via_sku_asin: 0,
    rows_resolved_via_sku: 0,
    rows_resolved_via_asin: 0,
    rows_inherited_from_all_orders: 0,
    rows_ambiguous: 0,
    rows_unresolved: 0,
  };

  let offset = 0;
  while (true) {
    const rawRows = await fetchPage(supabase, table, organizationId, uploadId, offset, pageSize);
    if (rawRows.length === 0) break;
    const rows = rawRows.map((r) => adaptRow(table, r));

    const keyBatch = rows.map((r) => ({ fnsku: r.fnsku, msku: r.sku, asin: r.asin }));
    const pool: ProductIdentifierMapRow[] = await prefetchIdentifierMapCandidatesForBatch(
      supabase,
      organizationId,
      storeId,
      keyBatch,
    );

    for (const row of rows) {
      m.rows_scanned += 1;
      if (!row.fnsku && !row.sku && !row.asin) {
        m.rows_unresolved += 1;
        // Don't write status when we never had identifiers on this row.
        continue;
      }
      const match = pickBestProductIdentifierMatch(pool, {
        organizationId,
        storeId,
        fnsku: row.fnsku,
        msku: row.sku,
        asin: row.asin,
      });
      const out = statusFromMatch(match, m);
      await patchRow(supabase, table, organizationId, row.id, {
        resolved_product_id: out.productId,
        resolved_catalog_product_id: out.catalogProductId,
        identifier_resolution_status: out.status,
        identifier_resolution_confidence: out.confidence,
      });
    }

    offset += pageSize;
    if (rawRows.length < pageSize) break;
  }
  return m;
}

/**
 * Inherit resolved_product_id from `amazon_all_orders` rows that share order_id
 * for the same organization. Used by amazon_transactions (Simple Transactions
 * Summary file) which has no SKU/FNSKU/ASIN of its own.
 */
async function runJoinAllOrdersResolve(opts: ResolveOptions, base: ResolveMetrics): Promise<ResolveMetrics> {
  const { supabase, organizationId, uploadId, table, storeId } = opts;
  if (table !== "amazon_transactions") return base;

  const pageSize = opts.pageSize ?? 500;
  let offset = 0;

  while (true) {
    const { data, error } = await supabase
      .from(table)
      .select("id, order_id")
      .eq("organization_id", organizationId)
      .eq("upload_id", uploadId)
      .is("resolved_product_id", null)
      .not("order_id", "is", null)
      .range(offset, offset + pageSize - 1);
    if (error) throw new Error(`[product-resolver] join all_orders read: ${error.message}`);
    const rows = (data ?? []) as { id: string; order_id: string }[];
    if (rows.length === 0) break;

    const orderIds = [...new Set(rows.map((r) => r.order_id).filter(Boolean))];
    if (orderIds.length === 0) {
      offset += pageSize;
      if (rows.length < pageSize) break;
      continue;
    }

    const orderResolved = new Map<string, { productId: string | null; catalogProductId: string | null }>();
    for (let i = 0; i < orderIds.length; i += 80) {
      const slice = orderIds.slice(i, i + 80);
      const { data: aoData, error: aoErr } = await supabase
        .from("amazon_all_orders")
        .select("amazon_order_id, order_id, resolved_product_id, resolved_catalog_product_id")
        .eq("organization_id", organizationId)
        .eq("store_id", storeId)
        .or(`amazon_order_id.in.(${slice.map((s) => `"${s}"`).join(",")}),order_id.in.(${slice.map((s) => `"${s}"`).join(",")})`)
        .not("resolved_product_id", "is", null);
      if (aoErr) {
        console.warn(`[product-resolver] all_orders read for join: ${aoErr.message}`);
        continue;
      }
      for (const r of (aoData ?? []) as Array<{
        amazon_order_id: string | null;
        order_id: string | null;
        resolved_product_id: string | null;
        resolved_catalog_product_id: string | null;
      }>) {
        const keys = [r.amazon_order_id, r.order_id].filter((k): k is string => !!k);
        for (const k of keys) {
          if (!orderResolved.has(k) && r.resolved_product_id) {
            orderResolved.set(k, {
              productId: r.resolved_product_id,
              catalogProductId: r.resolved_catalog_product_id,
            });
          }
        }
      }
    }

    for (const r of rows) {
      const hit = orderResolved.get(r.order_id);
      if (!hit?.productId) continue;
      base.rows_inherited_from_all_orders += 1;
      await patchRow(supabase, "amazon_transactions", organizationId, r.id, {
        resolved_product_id: hit.productId,
        resolved_catalog_product_id: hit.catalogProductId,
        identifier_resolution_status: "matched_via_all_orders",
        identifier_resolution_confidence: 0.85,
      });
    }

    offset += pageSize;
    if (rows.length < pageSize) break;
  }
  return base;
}

export async function resolveAmazonImportProducts(opts: ResolveOptions): Promise<ResolveMetrics> {
  if (!String(opts.storeId ?? "").trim()) {
    throw new Error("[product-resolver] resolveAmazonImportProducts requires storeId.");
  }
  const base = await runStandardResolve(opts);
  if (opts.joinAllOrders && opts.table === "amazon_transactions") {
    await runJoinAllOrdersResolve(opts, base);
  }
  console.log(
    `[product-resolver] table=${opts.table} upload=${opts.uploadId} ` +
      `scanned=${base.rows_scanned} resolved=${base.rows_resolved} ` +
      `(fnsku=${base.rows_resolved_via_fnsku} sku_asin=${base.rows_resolved_via_sku_asin} ` +
      `sku=${base.rows_resolved_via_sku} asin=${base.rows_resolved_via_asin} ` +
      `inherited=${base.rows_inherited_from_all_orders}) ambiguous=${base.rows_ambiguous} ` +
      `unresolved=${base.rows_unresolved}`,
  );
  return base;
}
