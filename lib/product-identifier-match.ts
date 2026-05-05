/**
 * Priority matching for `product_identifier_map` rows (operational imports).
 *
 * Candidates are always scoped by organization_id + store_id.
 *
 * Priority 1: fnsku match
 * Priority 2: seller_sku + asin
 * Priority 3: seller_sku (or msku column)
 * Priority 4: asin
 *
 * Tie-break: exact fnsku > sku/msku + asin > weaker tiers. Multiple equal winners → ambiguous.
 */

import type { SupabaseClient } from "@supabase/supabase-js";

/** Shape returned from `product_identifier_map` selects used by matchers and resolvers. */
export type ProductIdentifierMapRow = {
  id: string;
  organization_id: string;
  product_id: string | null;
  catalog_product_id: string | null;
  store_id: string | null;
  seller_sku: string | null;
  asin: string | null;
  fnsku: string | null;
  msku: string | null;
  title: string | null;
  disposition: string | null;
  external_listing_id: string | null;
  confidence_score?: number | null;
  match_source?: string | null;
  inventory_source?: string | null;
  last_seen_at?: string | null;
  linked_from_report_family?: string | null;
  linked_from_target_table?: string | null;
};

export type IdentifierLookupHints = {
  organizationId: string;
  /** Required for DB prefetch; matching only considers rows for this store. */
  storeId?: string | null;
  fnsku?: string | null;
  /** MSKU / seller SKU */
  msku?: string | null;
  asin?: string | null;
};

export type IdentifierMatchTier = 1 | 2 | 3 | 4;

export type ProductIdentifierMatchResult = {
  row: ProductIdentifierMapRow | null;
  status: "resolved" | "ambiguous" | "unresolved";
  tier: IdentifierMatchTier | null;
  confidence: number;
  candidatesConsidered: number;
};

function n(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

type Scored = { row: ProductIdentifierMapRow; tier: IdentifierMatchTier; sub: number };

function scoreCandidate(row: ProductIdentifierMapRow, h: IdentifierLookupHints): Scored | null {
  const fnsku = n(h.fnsku);
  const msku = n(h.msku);
  const asin = n(h.asin);

  const rowF = n(row.fnsku);
  const rowSku = n(row.seller_sku) ?? n(row.msku);
  const rowMsku = n(row.msku);
  const rowAsin = n(row.asin);

  const fnskuEq = !!(fnsku && rowF && fnsku === rowF);
  const skuEq = !!(msku && rowSku && msku === rowSku);
  const mskuEq = !!(msku && rowMsku && msku === rowMsku);
  const skuOrMskuEq = skuEq || mskuEq;
  const asinEq = !!(asin && rowAsin && asin === rowAsin);

  let best: Scored | null = null;
  const consider = (cand: Scored) => {
    if (
      !best ||
      cand.tier < best.tier ||
      (cand.tier === best.tier && cand.sub < best.sub) ||
      (cand.tier === best.tier && cand.sub === best.sub && cand.row.id < best.row.id)
    ) {
      best = cand;
    }
  };

  if (fnsku && fnskuEq) {
    consider({ row, tier: 1, sub: 0 });
  }
  if (skuOrMskuEq && asinEq) {
    consider({ row, tier: 2, sub: fnskuEq ? 0 : 1 });
  }
  if (skuOrMskuEq) {
    consider({ row, tier: 3, sub: asinEq ? 0 : 1 });
  }
  if (asinEq) {
    consider({ row, tier: 4, sub: skuOrMskuEq ? 1 : 2 });
  }

  return best;
}

function tierConfidence(tier: IdentifierMatchTier): number {
  switch (tier) {
    case 1:
      return 1;
    case 2:
      return 0.95;
    case 3:
      return 0.85;
    case 4:
      return 0.7;
    default:
      return 0.65;
  }
}

/**
 * Pick the best matching identifier map row from an in-memory candidate pool.
 */
export function pickBestProductIdentifierMatch(
  candidates: ProductIdentifierMapRow[],
  hints: IdentifierLookupHints,
): ProductIdentifierMatchResult {
  const uniq = new Map<string, ProductIdentifierMapRow>();
  for (const c of candidates) {
    const id = String(c.id ?? "").trim();
    if (id) uniq.set(id, c);
  }
  const scopeStore = n(hints.storeId);
  let pool = [...uniq.values()];
  if (scopeStore) {
    pool = pool.filter((r) => n(r.store_id) === scopeStore);
  }
  if (pool.length === 0) {
    return { row: null, status: "unresolved", tier: null, confidence: 0, candidatesConsidered: 0 };
  }

  const scored: Scored[] = [];
  for (const row of pool) {
    const s = scoreCandidate(row, hints);
    if (s) scored.push(s);
  }

  if (scored.length === 0) {
    return { row: null, status: "unresolved", tier: null, confidence: 0, candidatesConsidered: pool.length };
  }

  scored.sort((a, b) => a.tier - b.tier || a.sub - b.sub || (a.row.id > b.row.id ? 1 : -1));
  const best = scored[0]!;
  const winners = scored.filter((s) => s.tier === best.tier && s.sub === best.sub);
  const distinctIds = new Set(winners.map((w) => w.row.id));
  if (distinctIds.size > 1) {
    return {
      row: null,
      status: "ambiguous",
      tier: best.tier,
      confidence: tierConfidence(best.tier),
      candidatesConsidered: pool.length,
    };
  }

  return {
    row: best.row,
    status: "resolved",
    tier: best.tier,
    confidence: tierConfidence(best.tier),
    candidatesConsidered: pool.length,
  };
}

const FETCH_CHUNK = 60;

const baseSelect =
  "id, organization_id, product_id, catalog_product_id, store_id, seller_sku, asin, fnsku, msku, external_listing_id, title, disposition, confidence_score, match_source, inventory_source, last_seen_at, linked_from_report_family, linked_from_target_table";

/**
 * Load a bounded candidate set for priority matching (organization + store).
 */
export async function fetchProductIdentifierMapCandidates(
  supabase: SupabaseClient,
  organizationId: string,
  hints: IdentifierLookupHints,
): Promise<ProductIdentifierMapRow[]> {
  const storeId = n(hints.storeId);
  if (!storeId) {
    throw new Error("fetchProductIdentifierMapCandidates requires hints.storeId (imports target store).");
  }

  const fnsku = n(hints.fnsku);
  const msku = n(hints.msku);
  const asin = n(hints.asin);

  const collected: ProductIdentifierMapRow[] = [];
  const seen = new Set<string>();

  const pushRows = (rows: unknown) => {
    for (const r of (rows as ProductIdentifierMapRow[]) ?? []) {
      const id = String((r as ProductIdentifierMapRow).id ?? "").trim();
      if (!id || seen.has(id)) continue;
      seen.add(id);
      collected.push(r as ProductIdentifierMapRow);
    }
  };

  if (fnsku) {
    const { data, error } = await supabase
      .from("product_identifier_map")
      .select(baseSelect)
      .eq("organization_id", organizationId)
      .eq("store_id", storeId)
      .eq("fnsku", fnsku)
      .limit(120);
    if (error) throw new Error(`fetchProductIdentifierMapCandidates fnsku: ${error.message}`);
    pushRows(data);
  }

  if (msku) {
    const { data, error } = await supabase
      .from("product_identifier_map")
      .select(baseSelect)
      .eq("organization_id", organizationId)
      .eq("store_id", storeId)
      .eq("seller_sku", msku)
      .limit(200);
    if (error) throw new Error(`fetchProductIdentifierMapCandidates msku: ${error.message}`);
    pushRows(data);
    const { data: d2, error: e2 } = await supabase
      .from("product_identifier_map")
      .select(baseSelect)
      .eq("organization_id", organizationId)
      .eq("store_id", storeId)
      .eq("msku", msku)
      .limit(200);
    if (e2) throw new Error(`fetchProductIdentifierMapCandidates msku2: ${e2.message}`);
    pushRows(d2);
  }

  if (asin) {
    const { data, error } = await supabase
      .from("product_identifier_map")
      .select(baseSelect)
      .eq("organization_id", organizationId)
      .eq("store_id", storeId)
      .eq("asin", asin)
      .limit(200);
    if (error) throw new Error(`fetchProductIdentifierMapCandidates asin: ${error.message}`);
    pushRows(data);
  }

  return collected;
}

/**
 * Batch prefetch: union identifiers from many operational rows (organization + store).
 */
export async function prefetchIdentifierMapCandidatesForBatch(
  supabase: SupabaseClient,
  organizationId: string,
  storeId: string | null,
  keys: { fnsku?: string | null; msku?: string | null; asin?: string | null }[],
): Promise<ProductIdentifierMapRow[]> {
  const sid = n(storeId);
  if (!sid) {
    return [];
  }

  const fnskus = [...new Set(keys.map((k) => n(k.fnsku)).filter(Boolean))] as string[];
  const mskus = [...new Set(keys.map((k) => n(k.msku)).filter(Boolean))] as string[];
  const asins = [...new Set(keys.map((k) => n(k.asin)).filter(Boolean))] as string[];

  const seen = new Set<string>();
  const out: ProductIdentifierMapRow[] = [];
  const pushRows = (rows: unknown) => {
    for (const r of (rows as ProductIdentifierMapRow[]) ?? []) {
      const id = String((r as ProductIdentifierMapRow).id ?? "").trim();
      if (!id || seen.has(id)) continue;
      seen.add(id);
      out.push(r as ProductIdentifierMapRow);
    }
  };

  for (let i = 0; i < fnskus.length; i += FETCH_CHUNK) {
    const slice = fnskus.slice(i, i + FETCH_CHUNK);
    const { data, error } = await supabase
      .from("product_identifier_map")
      .select(baseSelect)
      .eq("organization_id", organizationId)
      .eq("store_id", sid)
      .in("fnsku", slice);
    if (error) throw new Error(`prefetchIdentifierMapCandidatesForBatch fnsku: ${error.message}`);
    pushRows(data);
  }

  for (let i = 0; i < mskus.length; i += FETCH_CHUNK) {
    const slice = mskus.slice(i, i + FETCH_CHUNK);
    const { data, error } = await supabase
      .from("product_identifier_map")
      .select(baseSelect)
      .eq("organization_id", organizationId)
      .eq("store_id", sid)
      .in("seller_sku", slice);
    if (error) throw new Error(`prefetchIdentifierMapCandidatesForBatch msku: ${error.message}`);
    pushRows(data);
    const { data: d2, error: e2 } = await supabase
      .from("product_identifier_map")
      .select(baseSelect)
      .eq("organization_id", organizationId)
      .eq("store_id", sid)
      .in("msku", slice);
    if (e2) throw new Error(`prefetchIdentifierMapCandidatesForBatch msku col: ${e2.message}`);
    pushRows(d2);
  }

  for (let i = 0; i < asins.length; i += FETCH_CHUNK) {
    const slice = asins.slice(i, i + FETCH_CHUNK);
    const { data, error } = await supabase
      .from("product_identifier_map")
      .select(baseSelect)
      .eq("organization_id", organizationId)
      .eq("store_id", sid)
      .in("asin", slice);
    if (error) throw new Error(`prefetchIdentifierMapCandidatesForBatch asin: ${error.message}`);
    pushRows(data);
  }

  return out;
}
