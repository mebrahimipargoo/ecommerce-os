/**
 * Bridge rows between `catalog_products` (listing snapshot) and future `products`
 * (internal master). Populated only from listing canonical sync — never from
 * operational/financial imports.
 */

import type { SupabaseClient } from "@supabase/supabase-js";

export type CatalogRowForIdentifierMap = {
  id: string;
  organization_id: string;
  store_id: string | null;
  seller_sku: string | null;
  asin: string | null;
  fnsku: string | null;
  listing_id: string | null;
};

/** Matches `uq_product_identifier_map_org_store_ids` COALESCE semantics. */
function identifierTupleKey(p: {
  store_id: string | null;
  seller_sku: string | null;
  asin: string | null;
  fnsku: string | null;
  external_listing_id: string | null;
}): string {
  const store = p.store_id ?? "";
  return [store, p.seller_sku ?? "", p.asin ?? "", p.fnsku ?? "", p.external_listing_id ?? ""].join("\x1f");
}

/**
 * Upserts `product_identifier_map` rows for listing-derived catalog rows.
 * Non-destructive: updates `last_seen_at` / provenance on refresh; never deletes.
 */
export async function upsertProductIdentifierMapFromCatalogRows(
  supabase: SupabaseClient,
  rows: CatalogRowForIdentifierMap[],
  sourceUploadId: string,
  /** Provenance slug (listing report or operational enricher, e.g. INVENTORY_LEDGER). */
  sourceReportType: string,
): Promise<void> {
  if (rows.length === 0) return;

  const orgId = String(rows[0]?.organization_id ?? "").trim();
  if (!orgId) return;

  const catalogIds = [...new Set(rows.map((r) => r.id).filter(Boolean))];
  if (catalogIds.length === 0) return;

  const now = new Date().toISOString();
  const reportSlug = sourceReportType;

  /** PostgREST `.in()` is sent on the query string; keep chunks small to avoid 400 Bad Request. */
  const PREFETCH_ID_CHUNK = 50;
  const existing: Record<string, unknown>[] = [];
  for (let o = 0; o < catalogIds.length; o += PREFETCH_ID_CHUNK) {
    const idSlice = catalogIds.slice(o, o + PREFETCH_ID_CHUNK);
    const { data: page, error: exErr } = await supabase
      .from("product_identifier_map")
      .select("id, catalog_product_id, store_id, seller_sku, asin, fnsku, external_listing_id")
      .eq("organization_id", orgId)
      .in("catalog_product_id", idSlice);
    if (exErr) throw new Error(`product_identifier_map prefetch failed: ${exErr.message}`);
    if (page?.length) existing.push(...(page as Record<string, unknown>[]));
  }

  const existingByTuple = new Map<string, string>();
  for (const e of existing) {
    const row = e as Record<string, unknown>;
    const id = String(row.id ?? "");
    const cid = String(row.catalog_product_id ?? "");
    if (!id || !cid) continue;
    const k = identifierTupleKey({
      store_id: (row.store_id as string | null) ?? null,
      seller_sku: (row.seller_sku as string | null) ?? null,
      asin: (row.asin as string | null) ?? null,
      fnsku: (row.fnsku as string | null) ?? null,
      external_listing_id: (row.external_listing_id as string | null) ?? null,
    });
    existingByTuple.set(`${cid}\x1f${k}`, id);
  }

  const toInsert: Record<string, unknown>[] = [];
  const toUpdate: { id: string }[] = [];

  for (const r of rows) {
    const seller_sku = r.seller_sku != null ? String(r.seller_sku).trim() : null;
    const asin = r.asin != null ? String(r.asin).trim() : null;
    if (!seller_sku || !asin) continue;

    const fnsku = r.fnsku != null && String(r.fnsku).trim() !== "" ? String(r.fnsku).trim() : null;
    const external_listing_id =
      r.listing_id != null && String(r.listing_id).trim() !== "" ? String(r.listing_id).trim() : null;

    const tupleKey = identifierTupleKey({
      store_id: r.store_id,
      seller_sku,
      asin,
      fnsku,
      external_listing_id,
    });
    const mapKey = `${r.id}\x1f${tupleKey}`;
    const existingId = existingByTuple.get(mapKey);

    if (existingId) {
      toUpdate.push({ id: existingId });
    } else {
      toInsert.push({
        organization_id: orgId,
        catalog_product_id: r.id,
        store_id: r.store_id,
        seller_sku,
        asin,
        fnsku,
        external_listing_id,
        source_upload_id: sourceUploadId,
        source_report_type: reportSlug,
        is_primary: true,
        last_seen_at: now,
      });
    }
  }

  const CHUNK = 100;

  for (let i = 0; i < toInsert.length; i += CHUNK) {
    const chunk = toInsert.slice(i, i + CHUNK);
    const { error } = await supabase.from("product_identifier_map").insert(chunk);
    if (error) throw new Error(`product_identifier_map insert failed: ${error.message}`);
  }

  const updateIds = [...new Set(toUpdate.map((u) => u.id))];
  const UPDATE_CHUNK = 75;
  for (let i = 0; i < updateIds.length; i += UPDATE_CHUNK) {
    const chunk = updateIds.slice(i, i + UPDATE_CHUNK);
    const { error } = await supabase
      .from("product_identifier_map")
      .update({
        last_seen_at: now,
        source_upload_id: sourceUploadId,
        source_report_type: reportSlug,
        is_primary: true,
      })
      .in("id", chunk);
    if (error) throw new Error(`product_identifier_map update failed: ${error.message}`);
  }
}
