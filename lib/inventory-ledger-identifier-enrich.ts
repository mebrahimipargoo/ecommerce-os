/**
 * Phase 4 (Generic) for INVENTORY_LEDGER: enrich `product_identifier_map` with FNSKU
 * using ledger rows landed in `amazon_inventory_ledger` for this upload.
 *
 * Listing imports establish sku+asin → catalog; ledger rows add fnsku when catalog rows exist.
 */

import type { SupabaseClient } from "@supabase/supabase-js";

import type { CatalogRowForIdentifierMap } from "./product-identifier-map-sync";
import { upsertProductIdentifierMapFromCatalogRows } from "./product-identifier-map-sync";

type LedgerRow = {
  fnsku: string;
  raw_data: Record<string, string> | null;
};

function pickRaw(raw: Record<string, string> | null | undefined, keys: string[]): string {
  if (!raw) return "";
  const lower = new Map<string, string>();
  for (const [k, v] of Object.entries(raw)) {
    lower.set(String(k).trim().toLowerCase(), String(v ?? "").trim());
  }
  for (const key of keys) {
    const v = lower.get(key.toLowerCase());
    if (v) return v;
  }
  return "";
}

function pairKey(msku: string, asin: string): string {
  return `${msku.trim().toLowerCase()}\x1f${asin.trim().toLowerCase()}`;
}

/**
 * Returns rows scanned + catalog rows used for map upserts.
 */
export async function enrichIdentifierMapFromInventoryLedgerUpload(params: {
  supabase: SupabaseClient;
  organizationId: string;
  uploadId: string;
  storeId: string | null;
  pageSize?: number;
}): Promise<{ ledger_rows_scanned: number; catalog_hits: number; map_upserts: number }> {
  const { supabase, organizationId, uploadId, storeId } = params;
  const pageSize = params.pageSize ?? 500;

  let offset = 0;
  let scanned = 0;
  let catalogHits = 0;
  let mapUpserts = 0;

  while (true) {
    const { data, error } = await supabase
      .from("amazon_inventory_ledger")
      .select("fnsku, raw_data")
      .eq("organization_id", organizationId)
      .eq("upload_id", uploadId)
      .range(offset, offset + pageSize - 1);

    if (error) throw new Error(`ledger generic: read amazon_inventory_ledger failed: ${error.message}`);
    const rows = (data ?? []) as LedgerRow[];
    if (rows.length === 0) break;

    /** Last fnsku wins per (msku, asin) */
    const fnskuByPair = new Map<string, string>();
    for (const r of rows) {
      const fnsku = String(r.fnsku ?? "").trim();
      if (!fnsku) continue;
      const raw = r.raw_data && typeof r.raw_data === "object" && !Array.isArray(r.raw_data) ? r.raw_data : {};
      const msku = pickRaw(raw, ["MSKU", "msku", "sku", "Seller SKU", "seller-sku", "seller_sku"]);
      const asin = pickRaw(raw, ["ASIN", "asin", "asin1"]);
      if (!msku || !asin) continue;
      fnskuByPair.set(pairKey(msku, asin), fnsku);
    }

    const pairs = [...fnskuByPair.entries()].map(([k, fnsku]) => {
      const [msku, asin] = k.split("\x1f");
      return { msku, asin, fnsku };
    });

    const skuBatch = 80;
    for (let i = 0; i < pairs.length; i += skuBatch) {
      const slice = pairs.slice(i, i + skuBatch);
      const mskus = [...new Set(slice.map((p) => p.msku))];
      if (mskus.length === 0) continue;

      let q = supabase
        .from("catalog_products")
        .select("id, organization_id, store_id, seller_sku, asin, fnsku, listing_id")
        .eq("organization_id", organizationId)
        .in("seller_sku", mskus);

      if (storeId) {
        q = q.eq("store_id", storeId);
      }

      const { data: catPage, error: catErr } = await q;

      if (catErr) {
        console.warn(`[ledger-generic] catalog batch failed: ${catErr.message}`);
        continue;
      }

      const want = new Set(slice.map((p) => pairKey(p.msku, p.asin)));
      const catalogRows: CatalogRowForIdentifierMap[] = [];

      for (const c of catPage ?? []) {
        const row = c as Record<string, unknown>;
        const msku = String(row.seller_sku ?? "").trim();
        const asin = String(row.asin ?? "").trim();
        if (!msku || !asin) continue;
        if (!want.has(pairKey(msku, asin))) continue;
        const fnsku = fnskuByPair.get(pairKey(msku, asin));
        if (!fnsku) continue;
        const id = String(row.id ?? "");
        if (!id) continue;
        catalogHits++;
        catalogRows.push({
          id,
          organization_id: organizationId,
          store_id: (row.store_id as string | null) ?? storeId,
          seller_sku: msku,
          asin,
          fnsku,
          listing_id: (row.listing_id as string | null) ?? null,
        });
      }

      if (catalogRows.length > 0) {
        await upsertProductIdentifierMapFromCatalogRows(supabase, catalogRows, uploadId, "INVENTORY_LEDGER");
        mapUpserts += catalogRows.length;
      }
    }

    scanned += rows.length;
    offset += pageSize;
    if (rows.length < pageSize) break;
  }

  return { ledger_rows_scanned: scanned, catalog_hits: catalogHits, map_upserts: mapUpserts };
}
