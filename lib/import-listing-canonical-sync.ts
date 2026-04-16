/**
 * Pass-2 listing import: replay raw_payload rows into catalog_products (canonical layer).
 * Raw preservation is amazon_listing_report_rows_raw (Phase 3). Canonical upsert is catalog_products
 * (organization_id, store_id, seller_sku, asin — NULLS NOT DISTINCT), invoked from Phase 4 Generic only.
 */

import type { SupabaseClient } from "@supabase/supabase-js";

import {
  catalogReportTypeToSource,
  mapRowToCatalogProduct,
  type CatalogProductUpsert,
} from "./import-sync-mappers";
import { isListingAmazonSyncKind, resolveAmazonImportSyncKind } from "./pipeline/amazon-report-registry";
import { upsertProductIdentifierMapFromCatalogRows, type CatalogRowForIdentifierMap } from "./product-identifier-map-sync";

export type ListingCanonicalSyncMetrics = {
  /** (seller_sku, asin) was not in catalog_products before this run — true inserts only. */
  canonical_rows_new: number;
  /** Business fields changed vs prior DB row or vs a prior line with the same key in this file. */
  canonical_rows_updated: number;
  /** Same as DB snapshot, or duplicate file line with no field change vs prior same-key line. */
  canonical_rows_unchanged: number;
  /** Raw rows where mapRowToCatalogProduct returned null (missing canonical identifiers). */
  canonical_rows_invalid_for_merge: number;
  /**
   * Set when `product_identifier_map` sync fails after successful `catalog_products` upserts.
   * Phase 4 can still complete; operators can retry Generic or fix map sync separately.
   */
  identifier_map_sync_error?: string | null;
};

/** Smaller batches reduce statement timeouts on large `catalog_products` upserts. */
const BATCH = 250;

function canonKey(sellerSku: string, asin: string): string {
  return `${String(sellerSku).trim()}\x1f${String(asin).trim()}`;
}

function numOrNull(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

/** Compare canonical listing columns (excludes raw_payload). */
export function listingCanonicalBusinessFieldsEqual(a: CatalogProductUpsert, b: CatalogProductUpsert): boolean {
  const fields: (keyof CatalogProductUpsert)[] = [
    "source_report_type",
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
  ];
  for (const k of fields) {
    const va = a[k];
    const vb = b[k];
    if (k === "price") {
      const na = va as number | null | undefined;
      const nb = vb as number | null | undefined;
      if (na === null || na === undefined) {
        if (nb !== null && nb !== undefined) return false;
      } else if (nb === null || nb === undefined) {
        return false;
      } else if (Math.abs(na - nb) > 1e-9) return false;
      continue;
    }
    if (k === "quantity") {
      const qa = va === null || va === undefined ? null : Math.round(Number(va));
      const qb = vb === null || vb === undefined ? null : Math.round(Number(vb));
      if (qa !== qb) return false;
      continue;
    }
    const sa = va === null || va === undefined ? "" : String(va);
    const sb = vb === null || vb === undefined ? "" : String(vb);
    if (sa !== sb) return false;
  }
  return true;
}

function dbRowToComparableUpsert(
  r: Record<string, unknown>,
  orgId: string,
  storeId: string | null,
  source: CatalogProductUpsert["source_report_type"],
): CatalogProductUpsert {
  return {
    organization_id: orgId,
    store_id: storeId,
    source_report_type: source,
    source_upload_id: null,
    seller_sku: String(r.seller_sku ?? ""),
    asin: String(r.asin ?? ""),
    fnsku: r.fnsku != null ? String(r.fnsku) : null,
    item_name: r.item_name != null ? String(r.item_name) : null,
    item_description: r.item_description != null ? String(r.item_description) : null,
    fulfillment_channel: r.fulfillment_channel != null ? String(r.fulfillment_channel) : null,
    listing_status: r.listing_status != null ? String(r.listing_status) : null,
    listing_id: r.listing_id != null ? String(r.listing_id) : null,
    product_id: r.product_id != null ? String(r.product_id) : null,
    product_id_type: r.product_id_type != null ? String(r.product_id_type) : null,
    item_condition: r.item_condition != null ? String(r.item_condition) : null,
    merchant_shipping_group: r.merchant_shipping_group != null ? String(r.merchant_shipping_group) : null,
    price: numOrNull(r.price),
    quantity: r.quantity != null ? Math.round(Number(r.quantity)) : null,
    open_date: r.open_date != null ? String(r.open_date) : null,
    raw_payload: {},
  };
}

export type ListingCanonicalSyncParams = {
  supabase: SupabaseClient;
  organizationId: string;
  storeId: string | null;
  sourceUploadId: string;
  /**
   * When set (normal path), raw rows are read by physical file identity so re-uploads of the same
   * bytes still see every line even if `source_upload_id` on archived rows was updated by a later sync.
   */
  sourceFileSha256?: string | null;
  reportTypeRaw: string;
  /** Second arg is raw rows processed in pass 2 (for processed_rows / metadata). */
  onProgress?: (processPct: number, pass2RowsProcessed: number) => void | Promise<void>;
  /** Physical data lines after header (pass 1 file_rows_seen). */
  fileRowsSeen: number;
  /** Rows stored in amazon_listing_report_rows_raw (pass 1 raw_rows_stored). */
  storedRawRows: number;
  /**
   * `legacy`: onProgress pct is 50–99 (combined with a prior staging half-step).
   * `full`: onProgress pct is 0–100 (Phase 4 generic / catalog only).
   */
  progressScale?: "legacy" | "full";
};

async function prefetchCatalogSnapshots(
  supabase: SupabaseClient,
  organizationId: string,
  storeId: string | null,
  source: CatalogProductUpsert["source_report_type"],
): Promise<Map<string, CatalogProductUpsert>> {
  const dbCanon = new Map<string, CatalogProductUpsert>();
  let from = 0;
  for (;;) {
    let q = supabase
      .from("catalog_products")
      .select(
        "seller_sku, asin, fnsku, item_name, item_description, fulfillment_channel, listing_status, listing_id, product_id, product_id_type, item_condition, merchant_shipping_group, price, quantity, open_date",
      )
      .eq("organization_id", organizationId)
      .order("id", { ascending: true });
    q = storeId ? q.eq("store_id", storeId) : q.is("store_id", null);
    const { data: page, error: pgErr } = await q.range(from, from + 999);
    if (pgErr) throw new Error(pgErr.message);
    const rows = page ?? [];
    if (rows.length === 0) break;
    for (const r of rows) {
      const row = r as Record<string, unknown>;
      const sku = String(row.seller_sku ?? "").trim();
      const a = String(row.asin ?? "").trim();
      if (!sku || !a) continue;
      dbCanon.set(canonKey(sku, a), dbRowToComparableUpsert(row, organizationId, storeId, source));
    }
    from += rows.length;
    if (rows.length < 1000) break;
  }
  return dbCanon;
}

/**
 * Reads amazon_listing_report_rows_raw for the upload (ordered by row_number) and upserts catalog_products.
 */
export async function syncListingRawRowsToCatalogProducts(
  params: ListingCanonicalSyncParams,
): Promise<ListingCanonicalSyncMetrics> {
  const {
    supabase,
    organizationId,
    storeId,
    sourceUploadId,
    sourceFileSha256,
    reportTypeRaw,
    onProgress,
    storedRawRows,
    progressScale = "legacy",
  } = params;
  const rawSha = String(sourceFileSha256 ?? "").trim().toLowerCase();
  const listingKind = resolveAmazonImportSyncKind(reportTypeRaw);
  if (!isListingAmazonSyncKind(listingKind)) {
    throw new Error(
      `syncListingRawRowsToCatalogProducts refused: report_type "${reportTypeRaw}" resolves to ${listingKind} — only listing imports may write catalog_products.`,
    );
  }
  const source = catalogReportTypeToSource(reportTypeRaw);

  const dbCanon = await prefetchCatalogSnapshots(supabase, organizationId, storeId, source);

  const metrics: ListingCanonicalSyncMetrics = {
    canonical_rows_new: 0,
    canonical_rows_updated: 0,
    canonical_rows_unchanged: 0,
    canonical_rows_invalid_for_merge: 0,
  };

  const seenInFile = new Set<string>();
  const lastEmittedByCanon = new Map<string, CatalogProductUpsert>();

  let pass2Done = 0;
  let batch: Record<string, unknown>[] = [];
  /** Defer identifier-map writes to end of run — fewer round trips so large listings finish within the server window. */
  const pendingIdentifierRows: CatalogRowForIdentifierMap[] = [];

  const flushCatalog = async () => {
    if (batch.length === 0) return;
    const nowIso = new Date().toISOString();
    const payload = batch.map((row) => ({
      ...row,
      last_listing_sync_at: nowIso,
    }));
    const { data: upsertedCatalog, error: upErr } = await supabase
      .from("catalog_products")
      .upsert(payload, {
        onConflict: "organization_id,store_id,seller_sku,asin",
      })
      .select("id, organization_id, store_id, seller_sku, asin, fnsku, listing_id");
    if (upErr) {
      throw new Error(
        `catalog_products upsert failed after ~${pass2Done} listing raw rows processed: ${upErr.message}`,
      );
    }
    batch = [];
    const catRows = (upsertedCatalog ?? []) as CatalogRowForIdentifierMap[];
    if (catRows.length > 0) pendingIdentifierRows.push(...catRows);
  };

  let rawOffset = 0;
  while (true) {
    let rq = supabase
      .from("amazon_listing_report_rows_raw")
      .select("raw_payload")
      .eq("organization_id", organizationId)
      .order("row_number", { ascending: true });
    rq = rawSha !== "" ? rq.eq("source_file_sha256", rawSha) : rq.eq("source_upload_id", sourceUploadId);
    const { data: chunk, error } = await rq.range(rawOffset, rawOffset + BATCH - 1);

    if (error) throw new Error(error.message);
    const page = chunk ?? [];
    if (page.length === 0) break;

    for (const raw of page) {
      pass2Done += 1;
      const payload = raw.raw_payload;
      const mappedRow =
        payload && typeof payload === "object" && !Array.isArray(payload)
          ? (payload as Record<string, string>)
          : {};

      const catalog = mapRowToCatalogProduct(
        mappedRow,
        organizationId,
        storeId,
        source,
        sourceUploadId,
      );

      const shouldEmitProgress =
        onProgress &&
        storedRawRows > 0 &&
        (pass2Done % 15 === 0 || pass2Done === 1 || pass2Done === storedRawRows);
      if (shouldEmitProgress) {
        const pct =
          progressScale === "full"
            ? Math.min(100, Math.max(0, Math.ceil((pass2Done / storedRawRows) * 100)))
            : 50 + Math.min(49, Math.ceil((pass2Done / storedRawRows) * 49));
        await onProgress(pct, pass2Done);
      }

      if (!catalog) {
        metrics.canonical_rows_invalid_for_merge += 1;
        continue;
      }

      const k = canonKey(catalog.seller_sku, catalog.asin);

      if (seenInFile.has(k)) {
        const prev = lastEmittedByCanon.get(k)!;
        if (listingCanonicalBusinessFieldsEqual(catalog, prev)) {
          metrics.canonical_rows_unchanged += 1;
        } else {
          metrics.canonical_rows_updated += 1;
        }
        lastEmittedByCanon.set(k, catalog);
      } else {
        seenInFile.add(k);
        const dbPrev = dbCanon.get(k);
        if (dbPrev) {
          if (listingCanonicalBusinessFieldsEqual(catalog, dbPrev)) {
            metrics.canonical_rows_unchanged += 1;
          } else {
            metrics.canonical_rows_updated += 1;
          }
        } else {
          metrics.canonical_rows_new += 1;
        }
        lastEmittedByCanon.set(k, catalog);
      }

      batch.push(catalog as unknown as Record<string, unknown>);
      if (batch.length >= BATCH) {
        await flushCatalog();
        if (onProgress && storedRawRows > 0) {
          const pct =
            progressScale === "full"
              ? Math.min(100, Math.max(0, Math.ceil((pass2Done / storedRawRows) * 100)))
              : 50 + Math.min(49, Math.ceil((pass2Done / storedRawRows) * 49));
          await onProgress(pct, pass2Done);
        }
      }
    }

    rawOffset += page.length;
    if (page.length < BATCH) break;
  }

  await flushCatalog();

  /** One row per `catalog_products.id` — avoids redundant identifier work when the same key appears in multiple batches. */
  const dedupedIdentifierRows: CatalogRowForIdentifierMap[] = (() => {
    const byId = new Map<string, CatalogRowForIdentifierMap>();
    for (const r of pendingIdentifierRows) {
      const id = String(r.id ?? "").trim();
      if (id) byId.set(id, r);
    }
    return [...byId.values()];
  })();

  let identifierMapSyncError: string | null = null;
  const MAP_CHUNK = 200;
  try {
    for (let i = 0; i < dedupedIdentifierRows.length; i += MAP_CHUNK) {
      const chunk = dedupedIdentifierRows.slice(i, i + MAP_CHUNK);
      await upsertProductIdentifierMapFromCatalogRows(supabase, chunk, sourceUploadId, source);
    }
  } catch (err) {
    identifierMapSyncError = err instanceof Error ? err.message : String(err);
    console.error(
      "[listing-canonical-sync] product_identifier_map sync failed after catalog_products upsert:",
      identifierMapSyncError,
    );
  }

  console.log(
    `[listing-canonical-sync] Phase 4 catalog summary: catalog new ${metrics.canonical_rows_new}, ` +
      `catalog updated ${metrics.canonical_rows_updated}, catalog unchanged ${metrics.canonical_rows_unchanged}`,
  );

  return { ...metrics, identifier_map_sync_error: identifierMapSyncError };
}
