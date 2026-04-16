/**
 * Read-only helpers to resolve operational `amazon_*` rows toward `product_identifier_map`
 * and `catalog_products` without mutating catalog or master products.
 */

import type { SupabaseClient } from "@supabase/supabase-js";

export type CatalogListingSnapshot = {
  id: string;
  seller_sku: string | null;
  asin: string | null;
  fnsku: string | null;
  item_name: string | null;
  listing_status: string | null;
  source_report_type: string | null;
  canonical_scope: string | null;
};

export type ProductIdentifierMapRow = {
  id: string;
  organization_id: string;
  product_id: string | null;
  catalog_product_id: string | null;
  store_id: string | null;
  seller_sku: string | null;
  asin: string | null;
  fnsku: string | null;
  external_listing_id: string | null;
};

export type OperationalProductBridge = {
  identifier_rows: ProductIdentifierMapRow[];
  catalog_products: CatalogListingSnapshot[];
};

function n(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

/**
 * Query identifier bridge rows for an org, optionally narrowed by store / SKU / ASIN / FNSKU.
 * Does not assume catalog title/status columns exist beyond what PostgREST returns.
 */
export async function fetchProductIdentifierBridge(
  supabase: SupabaseClient,
  organizationId: string,
  filters: {
    storeId?: string | null;
    sellerSku?: string | null;
    asin?: string | null;
    fnsku?: string | null;
    limit?: number;
  } = {},
): Promise<OperationalProductBridge> {
  const lim = typeof filters.limit === "number" && filters.limit > 0 ? Math.min(filters.limit, 50) : 20;

  let q = supabase
    .from("product_identifier_map")
    .select(
      "id, organization_id, product_id, catalog_product_id, store_id, seller_sku, asin, fnsku, external_listing_id",
    )
    .eq("organization_id", organizationId);

  if (filters.storeId != null && String(filters.storeId).trim() !== "") {
    q = q.eq("store_id", filters.storeId);
  }
  const sku = n(filters.sellerSku);
  const asin = n(filters.asin);
  const fnsku = n(filters.fnsku);
  if (sku) q = q.eq("seller_sku", sku);
  if (asin) q = q.eq("asin", asin);
  if (fnsku) q = q.eq("fnsku", fnsku);

  const { data: idRows, error } = await q.order("last_seen_at", { ascending: false }).limit(lim);
  if (error) throw new Error(`fetchProductIdentifierBridge: ${error.message}`);

  const identifier_rows = (idRows ?? []) as ProductIdentifierMapRow[];
  const catIds = [...new Set(identifier_rows.map((r) => r.catalog_product_id).filter(Boolean))] as string[];

  if (catIds.length === 0) {
    return { identifier_rows, catalog_products: [] };
  }

  const { data: catRows, error: cErr } = await supabase
    .from("catalog_products")
    .select("id, seller_sku, asin, fnsku, item_name, listing_status, source_report_type, canonical_scope")
    .eq("organization_id", organizationId)
    .in("id", catIds);

  if (cErr) throw new Error(`fetchProductIdentifierBridge catalog: ${cErr.message}`);

  return {
    identifier_rows,
    catalog_products: (catRows ?? []) as CatalogListingSnapshot[],
  };
}

/** Convenience wrapper for `amazon_returns`-style rows (org + sku + asin [+ store if known]). */
export function resolveBridgeForAmazonReturnRow(
  supabase: SupabaseClient,
  organizationId: string,
  row: { sku?: unknown; asin?: unknown; store_id?: unknown },
): Promise<OperationalProductBridge> {
  return fetchProductIdentifierBridge(supabase, organizationId, {
    storeId: n(row.store_id),
    sellerSku: n(row.sku),
    asin: n(row.asin),
  });
}

/** Convenience wrapper for removal / shipment lines that include `store_id`. */
export function resolveBridgeForRemovalLine(
  supabase: SupabaseClient,
  organizationId: string,
  row: { store_id?: unknown; sku?: unknown; asin?: unknown; fnsku?: unknown },
): Promise<OperationalProductBridge> {
  return fetchProductIdentifierBridge(supabase, organizationId, {
    storeId: n(row.store_id),
    sellerSku: n(row.sku),
    asin: n(row.asin),
    fnsku: n(row.fnsku),
  });
}

/** Ledger / reimbursement / settlement / transaction lines — usually no `store_id` on row. */
export function resolveBridgeForSkuAsin(
  supabase: SupabaseClient,
  organizationId: string,
  row: { sku?: unknown; asin?: unknown; fnsku?: unknown },
): Promise<OperationalProductBridge> {
  return fetchProductIdentifierBridge(supabase, organizationId, {
    sellerSku: n(row.sku),
    asin: n(row.asin),
    fnsku: n(row.fnsku),
  });
}
