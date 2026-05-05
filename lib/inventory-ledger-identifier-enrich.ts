/**
 * Phase 4 (Generic) for INVENTORY_LEDGER: enrich `product_identifier_map` from
 * `amazon_inventory_ledger` using org-scoped identifier priority (FNSKU-first).
 *
 * Uses typed columns `sku` / `asin` / `title` / `product_name` only (keyset pages; no bulk raw_data reads).
 */

import type { SupabaseClient } from "@supabase/supabase-js";

import type { ProductIdentifierMapRow } from "./product-identifier-match";
import { pickBestProductIdentifierMatch, prefetchIdentifierMapCandidatesForBatch } from "./product-identifier-match";

/** Canonical slug written on ledger-derived bridge rows. */
const INVENTORY_LEDGER_REPORT_SLUG = "inventory_ledger";

const MAP_ROW_SELECT =
  "id, organization_id, product_id, catalog_product_id, store_id, seller_sku, asin, fnsku, msku, external_listing_id, title, disposition, confidence_score, match_source, inventory_source, last_seen_at, linked_from_report_family, linked_from_target_table";

type LedgerRow = {
  id: string;
  fnsku: string;
  sku: string | null;
  asin: string | null;
  title: string | null;
  product_name: string | null;
  disposition: string | null;
};

export type InventoryLedgerIdentifierEnrichMetrics = {
  ledger_rows_scanned: number;
  ledger_bridge_rows_inserted: number;
  ledger_bridge_rows_enriched: number;
  ledger_rows_resolved_by_fnsku: number;
  ledger_rows_resolved_by_sku_asin: number;
  ledger_rows_resolved_sku_only: number;
  ledger_rows_resolved_asin_only: number;
  /** Sum of tier 2–4 resolutions. */
  ledger_rows_resolved_by_fallback: number;
  unresolved_ambiguous: number;
  unresolved_insert_failed: number;
  /** Ledger lines still problematic after this run (ambiguous + failed insert). */
  unresolved_ledger_rows_remaining: number;
};

function n(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function ledgerResolutionLabel(
  tier: 1 | 2 | 3 | 4,
  conflictFnsku: boolean,
): "resolved" | "matched" | "ambiguous" {
  if (conflictFnsku) return "ambiguous";
  if (tier === 1) return "resolved";
  return "matched";
}

function isMissingColumnError(message: string): boolean {
  const m = message.toLowerCase();
  return (
    m.includes("column") && (m.includes("does not exist") || m.includes("undefined column") || m.includes("schema cache"))
  );
}

async function fetchLedgerBatchKeyset(
  supabase: SupabaseClient,
  organizationId: string,
  uploadId: string,
  cursorId: string | null,
  pageSize: number,
): Promise<LedgerRow[]> {
  const fullSelect = "id, fnsku, sku, asin, title, product_name, disposition";
  let q = supabase
    .from("amazon_inventory_ledger")
    .select(fullSelect)
    .eq("organization_id", organizationId)
    .eq("upload_id", uploadId)
    .order("id", { ascending: true })
    .limit(pageSize);
  if (cursorId) q = q.gt("id", cursorId);
  const { data, error } = await q;

  if (!error) {
    return (data ?? []) as LedgerRow[];
  }

  if (isMissingColumnError(error.message)) {
    let fq = supabase
      .from("amazon_inventory_ledger")
      .select("id, fnsku, sku, asin, title, disposition")
      .eq("organization_id", organizationId)
      .eq("upload_id", uploadId)
      .order("id", { ascending: true })
      .limit(pageSize);
    if (cursorId) fq = fq.gt("id", cursorId);
    const { data: fallback, error: err2 } = await fq;
    if (err2) throw new Error(`ledger generic: read amazon_inventory_ledger failed: ${err2.message}`);
    return (fallback ?? []).map((r) => ({
      ...(r as LedgerRow),
      product_name: null,
    }));
  }

  throw new Error(`ledger generic: read amazon_inventory_ledger failed: ${error.message}`);
}

async function pushFreshMapRowToPool(
  supabase: SupabaseClient,
  organizationId: string,
  id: string,
  pool: ProductIdentifierMapRow[],
): Promise<void> {
  const { data, error } = await supabase
    .from("product_identifier_map")
    .select(MAP_ROW_SELECT)
    .eq("organization_id", organizationId)
    .eq("id", id)
    .maybeSingle();
  if (error || !data) return;
  pool.push(data as ProductIdentifierMapRow);
}

/**
 * Returns ledger rows scanned + identifier map write stats.
 */
export async function enrichIdentifierMapFromInventoryLedgerUpload(params: {
  supabase: SupabaseClient;
  organizationId: string;
  uploadId: string;
  storeId: string;
  pageSize?: number;
}): Promise<InventoryLedgerIdentifierEnrichMetrics> {
  const { supabase, organizationId, uploadId, storeId } = params;
  const pageSize = params.pageSize ?? 2000;

  const metrics: InventoryLedgerIdentifierEnrichMetrics = {
    ledger_rows_scanned: 0,
    ledger_bridge_rows_inserted: 0,
    ledger_bridge_rows_enriched: 0,
    ledger_rows_resolved_by_fnsku: 0,
    ledger_rows_resolved_by_sku_asin: 0,
    ledger_rows_resolved_sku_only: 0,
    ledger_rows_resolved_asin_only: 0,
    ledger_rows_resolved_by_fallback: 0,
    unresolved_ambiguous: 0,
    unresolved_insert_failed: 0,
    unresolved_ledger_rows_remaining: 0,
  };

  let cursorId: string | null = null;
  while (true) {
    const rows = await fetchLedgerBatchKeyset(supabase, organizationId, uploadId, cursorId, pageSize);
    if (rows.length === 0) break;
    cursorId = String(rows[rows.length - 1]?.id ?? "").trim() || null;

    const normalized = rows.map((r) => {
      const sku = n(r.sku);
      const asin = n(r.asin);
      const title = n(r.title) ?? n(r.product_name);
      const fnsku = n(r.fnsku);
      const disposition = n(r.disposition);
      return {
        row: r,
        sku: sku ? sku : null,
        asin: asin ? asin : null,
        title: title ? title : null,
        fnsku,
        disposition,
      };
    });

    const keyBatch = normalized.map((x) => ({
      fnsku: x.fnsku,
      msku: x.sku,
      asin: x.asin,
    }));
    const pool = await prefetchIdentifierMapCandidatesForBatch(supabase, organizationId, storeId, keyBatch);

    const now = new Date().toISOString();
    const mapPatches = new Map<string, Record<string, unknown>>();
    const ledgerPatches: { id: string; patch: Record<string, unknown> }[] = [];

    for (const item of normalized) {
      const { row, sku, asin, title, fnsku, disposition } = item;
      if (!fnsku) continue;

      const match = pickBestProductIdentifierMatch(pool, {
        organizationId,
        storeId,
        fnsku,
        msku: sku,
        asin,
      });

      if (match.status === "ambiguous") {
        metrics.unresolved_ambiguous += 1;
        ledgerPatches.push({
          id: row.id,
          patch: {
            resolved_product_id: null,
            resolved_catalog_product_id: null,
            identifier_resolution_status: "ambiguous",
            identifier_resolution_confidence: match.confidence,
          },
        });
        continue;
      }

      if (match.status === "resolved" && match.row && match.tier != null) {
        const tier = match.tier;
        if (tier === 1) metrics.ledger_rows_resolved_by_fnsku += 1;
        else if (tier === 2) metrics.ledger_rows_resolved_by_sku_asin += 1;
        else if (tier === 3) metrics.ledger_rows_resolved_sku_only += 1;
        else if (tier === 4) metrics.ledger_rows_resolved_asin_only += 1;
        if (tier >= 2 && tier <= 4) metrics.ledger_rows_resolved_by_fallback += 1;

        const existing = match.row;
        const exF = n(existing.fnsku);
        const conflictFnsku = !!(exF && exF !== fnsku);
        const resLabel = ledgerResolutionLabel(tier, conflictFnsku);

        const patch: Record<string, unknown> = {
          last_seen_at: now,
          source_upload_id: uploadId,
          inventory_source: "inventory_ledger",
          confidence_score: Math.max(Number(existing.confidence_score ?? 0) || 0, match.confidence),
        };

        if (!conflictFnsku) {
          patch.fnsku = exF ?? fnsku;
        }
        if (sku) {
          if (!n(existing.seller_sku)) patch.seller_sku = sku;
          patch.msku = sku;
        }
        if (asin && !n(existing.asin)) patch.asin = asin;
        if (title) patch.title = title;
        if (disposition) patch.disposition = disposition;

        mapPatches.set(existing.id, { ...(mapPatches.get(existing.id) ?? {}), ...patch });

        const conf =
          resLabel === "ambiguous"
            ? match.confidence * 0.9
            : resLabel === "resolved"
              ? match.confidence
              : Math.min(0.92, match.confidence);

        ledgerPatches.push({
          id: row.id,
          patch: {
            resolved_product_id: n(existing.product_id),
            resolved_catalog_product_id: n(existing.catalog_product_id),
            identifier_resolution_status: resLabel,
            identifier_resolution_confidence: conflictFnsku ? match.confidence * 0.9 : conf,
          },
        });
        continue;
      }

      /** No bridge row: create ledger-derived row (FNSKU-only allowed). */
      const insertRow: Record<string, unknown> = {
        organization_id: organizationId,
        store_id: storeId,
        catalog_product_id: null,
        product_id: null,
        seller_sku: sku,
        msku: sku,
        asin,
        fnsku,
        title,
        disposition,
        external_listing_id: null,
        source_upload_id: uploadId,
        source_report_type: INVENTORY_LEDGER_REPORT_SLUG,
        match_source: "inventory_ledger_fnsku",
        inventory_source: "inventory_ledger",
        confidence_score: 0.95,
        linked_from_report_family: "inventory",
        linked_from_target_table: "amazon_inventory_ledger",
        first_seen_at: now,
        last_seen_at: now,
        is_primary: true,
      };

      const { data: ins, error: insErr } = await supabase
        .from("product_identifier_map")
        .insert(insertRow)
        .select("id")
        .maybeSingle();

      if (insErr) {
        const code = (insErr as { code?: string }).code;
        if (code === "23505") {
          const { data: dup } = await supabase
            .from("product_identifier_map")
            .select(MAP_ROW_SELECT)
            .eq("organization_id", organizationId)
            .eq("fnsku", fnsku)
            .limit(3);
          const dupRow = (dup ?? [])[0] as ProductIdentifierMapRow | undefined;
          if (dupRow?.id) {
            await pushFreshMapRowToPool(supabase, organizationId, dupRow.id, pool);
            metrics.ledger_rows_resolved_by_fnsku += 1;
            ledgerPatches.push({
              id: row.id,
              patch: {
                resolved_product_id: n(dupRow.product_id),
                resolved_catalog_product_id: n(dupRow.catalog_product_id),
                identifier_resolution_status: "resolved",
                identifier_resolution_confidence: 0.95,
              },
            });
            continue;
          }
        }
        metrics.unresolved_insert_failed += 1;
        console.warn(`[inventory-ledger-identifier-enrich] map insert failed: ${insErr.message}`);
        ledgerPatches.push({
          id: row.id,
          patch: {
            resolved_product_id: null,
            resolved_catalog_product_id: null,
            identifier_resolution_status: "unresolved",
            identifier_resolution_confidence: 0,
          },
        });
        continue;
      }

      const newId = String((ins as { id?: string } | null)?.id ?? "").trim();
      if (newId) {
        metrics.ledger_bridge_rows_inserted += 1;
        metrics.ledger_rows_resolved_by_fnsku += 1;
        await pushFreshMapRowToPool(supabase, organizationId, newId, pool);
        ledgerPatches.push({
          id: row.id,
          patch: {
            resolved_product_id: null,
            resolved_catalog_product_id: null,
            identifier_resolution_status: "resolved",
            identifier_resolution_confidence: 0.95,
          },
        });
      }
    }

    for (const [id, patch] of mapPatches) {
      const { error: uErr } = await supabase.from("product_identifier_map").update(patch).eq("id", id);
      if (uErr) {
        console.warn(`[inventory-ledger-identifier-enrich] map update ${id}: ${uErr.message}`);
      } else {
        metrics.ledger_bridge_rows_enriched += 1;
      }
    }

    for (const lp of ledgerPatches) {
      const { error: lErr } = await supabase
        .from("amazon_inventory_ledger")
        .update(lp.patch)
        .eq("id", lp.id)
        .eq("organization_id", organizationId);
      if (lErr) {
        console.warn(`[inventory-ledger-identifier-enrich] ledger resolve ${lp.id}: ${lErr.message}`);
      }
    }

    metrics.ledger_rows_scanned += rows.length;
    if (rows.length < pageSize) break;
  }

  metrics.unresolved_ledger_rows_remaining = metrics.unresolved_ambiguous + metrics.unresolved_insert_failed;

  console.log(
    `[inventory-ledger-identifier-enrich] upload=${uploadId} scanned=${metrics.ledger_rows_scanned} ` +
      `ledger_bridge_inserted=${metrics.ledger_bridge_rows_inserted} ledger_bridge_enriched=${metrics.ledger_bridge_rows_enriched} ` +
      `resolved_fnsku=${metrics.ledger_rows_resolved_by_fnsku} resolved_fallback=${metrics.ledger_rows_resolved_by_fallback} ` +
      `(sku_asin=${metrics.ledger_rows_resolved_by_sku_asin} sku_only=${metrics.ledger_rows_resolved_sku_only} asin_only=${metrics.ledger_rows_resolved_asin_only}) ` +
      `ambiguous=${metrics.unresolved_ambiguous} insert_failed=${metrics.unresolved_insert_failed} ` +
      `unresolved_remaining=${metrics.unresolved_ledger_rows_remaining}`,
  );

  return metrics;
}
