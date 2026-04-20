/**
 * Wave 5 — Identifier-map enrichment from the four FBA-inventory report
 * families:
 *   • amazon_manage_fba_inventory          (sku + asin + fnsku)
 *   • amazon_fba_inventory                 (sku + asin + fnsku)
 *   • amazon_inbound_performance           (sku + asin + fnsku)
 *   • amazon_amazon_fulfilled_inventory    (seller_sku + asin + fulfillment_channel_sku as FNSKU)
 *
 * Mirrors the proven `inventory-ledger-identifier-enrich` pattern:
 *   1. Org-scoped four-tier matcher: FNSKU → seller_sku+ASIN → seller_sku/msku → ASIN.
 *   2. Never overwrites a non-null trusted FNSKU on the existing bridge row
 *      with a different value.
 *   3. confidence_score = max(existing, source_weight) — stronger sources
 *      (listing_catalog 1.0, inventory_ledger 0.95) are never weakened.
 *   4. Handles unique-violation 23505 by re-fetching and merging.
 *
 * This module deliberately does NOT modify `amazon_*` operational rows — the
 * inventory tables added in Wave 4 do not carry resolution columns and Wave 5
 * was scoped to identity enrichment only.
 *
 * Reuses, never duplicates:
 *   • `pickBestProductIdentifierMatch` + `prefetchIdentifierMapCandidatesForBatch`
 *   • `pickRawPayloadFields`
 *   • `product_identifier_map` schema (no new columns / no new tables / no new views)
 */

import type { SupabaseClient } from "@supabase/supabase-js";

import { pickRawPayloadFields } from "./amazon-raw-payload-pick";
import type { ProductIdentifierMapRow } from "./product-identifier-match";
import {
  pickBestProductIdentifierMatch,
  prefetchIdentifierMapCandidatesForBatch,
} from "./product-identifier-match";

// ── Source spec — one entry per supported family ─────────────────────────────

/** Slugs allowed for the report_type we enrich from. */
export type InventoryFamilyReportType =
  | "MANAGE_FBA_INVENTORY"
  | "FBA_INVENTORY"
  | "INBOUND_PERFORMANCE"
  | "AMAZON_FULFILLED_INVENTORY";

type InventoryFamilySpec = {
  reportType: InventoryFamilyReportType;
  table: string;
  /** Column on the operational table that links back to raw_report_uploads.id. */
  uploadIdColumn: "source_upload_id";
  fnskuColumn: string;
  skuColumn: string;
  asinColumn: string;
  /** Aliases checked in raw_data when the physical column is null/empty. */
  rawDataFnskuKeys: string[];
  rawDataSkuKeys: string[];
  rawDataAsinKeys: string[];
  /** Provenance written on inserts and Math.max'd on updates. Never above 0.95. */
  insertConfidence: number;
  matchSource: string;
  inventorySource: string;
  linkedReportFamily: "inventory";
  linkedTargetTable: string;
};

const SPECS: Record<InventoryFamilyReportType, InventoryFamilySpec> = {
  MANAGE_FBA_INVENTORY: {
    reportType: "MANAGE_FBA_INVENTORY",
    table: "amazon_manage_fba_inventory",
    uploadIdColumn: "source_upload_id",
    fnskuColumn: "fnsku",
    skuColumn: "sku",
    asinColumn: "asin",
    rawDataFnskuKeys: ["fnsku", "FNSKU", "fulfillment-network-sku"],
    rawDataSkuKeys: ["sku", "SKU", "merchant-sku", "msku", "seller-sku", "seller_sku"],
    rawDataAsinKeys: ["asin", "ASIN", "asin1"],
    insertConfidence: 0.92,
    matchSource: "manage_fba_inventory_fnsku",
    inventorySource: "manage_fba_inventory",
    linkedReportFamily: "inventory",
    linkedTargetTable: "amazon_manage_fba_inventory",
  },
  FBA_INVENTORY: {
    reportType: "FBA_INVENTORY",
    table: "amazon_fba_inventory",
    uploadIdColumn: "source_upload_id",
    fnskuColumn: "fnsku",
    skuColumn: "sku",
    asinColumn: "asin",
    rawDataFnskuKeys: ["fnsku", "FNSKU", "fulfillment-network-sku"],
    rawDataSkuKeys: ["sku", "SKU", "merchant-sku", "msku", "seller-sku", "seller_sku"],
    rawDataAsinKeys: ["asin", "ASIN", "asin1"],
    insertConfidence: 0.9,
    matchSource: "fba_inventory_fnsku",
    inventorySource: "fba_inventory",
    linkedReportFamily: "inventory",
    linkedTargetTable: "amazon_fba_inventory",
  },
  INBOUND_PERFORMANCE: {
    reportType: "INBOUND_PERFORMANCE",
    table: "amazon_inbound_performance",
    uploadIdColumn: "source_upload_id",
    fnskuColumn: "fnsku",
    skuColumn: "sku",
    asinColumn: "asin",
    rawDataFnskuKeys: ["fnsku", "FNSKU", "fulfillment-network-sku"],
    rawDataSkuKeys: ["sku", "SKU", "merchant-sku", "msku", "seller-sku", "seller_sku"],
    rawDataAsinKeys: ["asin", "ASIN", "asin1"],
    insertConfidence: 0.9,
    matchSource: "inbound_performance_fnsku",
    inventorySource: "inbound_performance",
    linkedReportFamily: "inventory",
    linkedTargetTable: "amazon_inbound_performance",
  },
  AMAZON_FULFILLED_INVENTORY: {
    /**
     * Amazon Fulfilled Inventory: `fulfillment-channel-sku` IS the FNSKU per
     * the Amazon Fulfilled Inventory report definition. The Wave-4 mapper
     * `mapRowToAmazonAmazonFulfilledInventory` already accepts both
     * `fulfillment-channel-sku` and `fnsku` as the same identifier slot, so
     * treating this column as FNSKU is consistent with current conventions.
     */
    reportType: "AMAZON_FULFILLED_INVENTORY",
    table: "amazon_amazon_fulfilled_inventory",
    uploadIdColumn: "source_upload_id",
    fnskuColumn: "fulfillment_channel_sku",
    skuColumn: "seller_sku",
    asinColumn: "asin",
    rawDataFnskuKeys: ["fulfillment-channel-sku", "fulfillment channel sku", "fnsku", "FNSKU"],
    rawDataSkuKeys: ["seller-sku", "seller sku", "sku", "SKU"],
    rawDataAsinKeys: ["asin", "ASIN"],
    insertConfidence: 0.88,
    matchSource: "amazon_fulfilled_inventory_fnsku",
    inventorySource: "amazon_fulfilled_inventory",
    linkedReportFamily: "inventory",
    linkedTargetTable: "amazon_amazon_fulfilled_inventory",
  },
};

export function getInventoryFamilySpec(rt: string): InventoryFamilySpec | null {
  return SPECS[rt as InventoryFamilyReportType] ?? null;
}

const MAP_ROW_SELECT =
  "id, organization_id, product_id, catalog_product_id, store_id, seller_sku, asin, fnsku, msku, external_listing_id, title, disposition, confidence_score, match_source, inventory_source, last_seen_at, linked_from_report_family, linked_from_target_table";

// ── Helpers ──────────────────────────────────────────────────────────────────

function n(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

type SourceRow = {
  id: string;
  fnsku: string;
  sku: string | null;
  asin: string | null;
  raw_data: Record<string, unknown> | null;
};

async function fetchSourceBatch(
  supabase: SupabaseClient,
  spec: InventoryFamilySpec,
  organizationId: string,
  uploadId: string,
  offset: number,
  pageSize: number,
): Promise<SourceRow[]> {
  /**
   * Selects only what we need. Aliases the table-specific FNSKU/SKU/ASIN
   * columns to canonical names so the rest of the pipeline can stay generic.
   */
  const select = [
    "id",
    `${spec.fnskuColumn}::text AS fnsku`,
    `${spec.skuColumn}::text AS sku`,
    `${spec.asinColumn}::text AS asin`,
    "raw_data",
  ].join(", ");

  const { data, error } = await supabase
    .from(spec.table)
    .select(select)
    .eq("organization_id", organizationId)
    .eq(spec.uploadIdColumn, uploadId)
    .range(offset, offset + pageSize - 1);

  if (error) {
    throw new Error(
      `[identity-enrich:${spec.reportType}] read ${spec.table} failed: ${error.message}`,
    );
  }
  return ((data ?? []) as unknown) as SourceRow[];
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

// ── Public metric type ───────────────────────────────────────────────────────

export type InventoryFamilyEnrichMetrics = {
  report_type: InventoryFamilyReportType;
  source_table: string;
  /** True when no Postgres writes were issued (dry-run or write-disabled family). */
  dry_run: boolean;
  /** True when the family is on the temporary write-blocklist (e.g. INBOUND_PERFORMANCE). */
  write_disabled: boolean;
  rows_scanned: number;
  rows_skipped_no_fnsku: number;
  /** Real run: rows actually inserted. Dry run: 0. See `existing_rows_inserted`. */
  bridge_rows_inserted: number;
  /** Real run: rows actually updated. Dry run: 0. See `existing_rows_updated`. */
  bridge_rows_enriched: number;
  resolved_by_fnsku: number;
  resolved_by_sku_asin: number;
  resolved_sku_only: number;
  resolved_asin_only: number;
  unresolved_ambiguous: number;
  unresolved_insert_failed: number;
  /** Total non-ambiguous candidates that would write (insert+update) — populated in both modes. */
  safe_candidates_to_write: number;
  /** Alias of unresolved_ambiguous, kept under the user-spec name. */
  ambiguous_candidates_skipped: number;
  /** Real run: equals bridge_rows_inserted. Dry run: would-have-been inserted. */
  existing_rows_inserted: number;
  /** Real run: equals bridge_rows_enriched. Dry run: would-have-been updated. */
  existing_rows_updated: number;
  /**
   * Counts cases where we kept a stronger existing value:
   *   • existing FNSKU non-null AND different from candidate (conflictFnsku=true), OR
   *   • existing confidence_score >= source weight.
   */
  stronger_rows_preserved: number;
};

/**
 * Families currently allowed to perform real writes. Per Wave-5 op review,
 * INBOUND_PERFORMANCE contributed 0 useful matches so it is excluded from
 * the real-write pass. It can still be invoked: the helper forces dry_run=true
 * and returns counts so coverage can be monitored.
 */
const WRITE_DISABLED_FAMILIES: ReadonlySet<InventoryFamilyReportType> = new Set<InventoryFamilyReportType>([
  "INBOUND_PERFORMANCE",
]);

export function isInventoryFamilyWriteDisabled(rt: InventoryFamilyReportType): boolean {
  return WRITE_DISABLED_FAMILIES.has(rt);
}

// ── Main entry — one upload at a time ────────────────────────────────────────

/**
 * Enriches `product_identifier_map` from rows of one Wave-4 inventory upload.
 * Idempotent: re-running on the same upload updates `last_seen_at` and never
 * weakens existing confidence or overwrites a stronger non-null FNSKU.
 *
 * `storeId` is informational only — matching is org-wide (consistent with the
 * existing IL enricher and `pickBestProductIdentifierMatch` semantics).
 */
export async function enrichIdentifierMapFromInventoryFamilyUpload(params: {
  supabase: SupabaseClient;
  organizationId: string;
  uploadId: string;
  storeId: string | null;
  reportType: InventoryFamilyReportType;
  pageSize?: number;
  /** When true (or when the family is write-disabled) no Postgres writes are issued. */
  dryRun?: boolean;
}): Promise<InventoryFamilyEnrichMetrics> {
  const { supabase, organizationId, uploadId, storeId, reportType } = params;
  const pageSize = params.pageSize ?? 500;
  const spec = SPECS[reportType];
  const writeDisabled = WRITE_DISABLED_FAMILIES.has(reportType);
  const dryRun = (params.dryRun === true) || writeDisabled;

  /** Per-row counters incremented in both modes; writes are skipped when dryRun=true. */
  let wouldInsert = 0;
  let wouldUpdate = 0;
  let strongerPreserved = 0;

  const metrics: InventoryFamilyEnrichMetrics = {
    report_type: reportType,
    source_table: spec.table,
    dry_run: dryRun,
    write_disabled: writeDisabled,
    rows_scanned: 0,
    rows_skipped_no_fnsku: 0,
    bridge_rows_inserted: 0,
    bridge_rows_enriched: 0,
    resolved_by_fnsku: 0,
    resolved_by_sku_asin: 0,
    resolved_sku_only: 0,
    resolved_asin_only: 0,
    unresolved_ambiguous: 0,
    unresolved_insert_failed: 0,
    safe_candidates_to_write: 0,
    ambiguous_candidates_skipped: 0,
    existing_rows_inserted: 0,
    existing_rows_updated: 0,
    stronger_rows_preserved: 0,
  };

  let offset = 0;
  while (true) {
    const rows = await fetchSourceBatch(supabase, spec, organizationId, uploadId, offset, pageSize);
    if (rows.length === 0) break;

    const normalized = rows.map((r) => {
      const raw =
        r.raw_data && typeof r.raw_data === "object" && !Array.isArray(r.raw_data)
          ? (r.raw_data as Record<string, unknown>)
          : {};
      const fnsku = n(r.fnsku) ?? (pickRawPayloadFields(raw, spec.rawDataFnskuKeys) || null);
      const sku = n(r.sku) ?? (pickRawPayloadFields(raw, spec.rawDataSkuKeys) || null);
      const asin = n(r.asin) ?? (pickRawPayloadFields(raw, spec.rawDataAsinKeys) || null);
      return { row: r, fnsku, sku, asin };
    });

    const keyBatch = normalized.map((x) => ({
      fnsku: x.fnsku,
      msku: x.sku,
      asin: x.asin,
    }));
    const pool = await prefetchIdentifierMapCandidatesForBatch(
      supabase,
      organizationId,
      storeId,
      keyBatch,
    );

    const now = new Date().toISOString();
    /** Per-bridge-row patch accumulator (last write wins per field). */
    const mapPatches = new Map<string, Record<string, unknown>>();

    for (const item of normalized) {
      const { fnsku, sku, asin } = item;

      // Hard rule: candidate FNSKU must be non-null. No fallback insert without FNSKU
      // (those rows are handled by the listing-side bridge upsert from catalog_products).
      if (!fnsku) {
        metrics.rows_skipped_no_fnsku += 1;
        continue;
      }

      const match = pickBestProductIdentifierMatch(pool, {
        organizationId,
        storeId,
        fnsku,
        msku: sku,
        asin,
      });

      if (match.status === "ambiguous") {
        metrics.unresolved_ambiguous += 1;
        continue;
      }

      if (match.status === "resolved" && match.row && match.tier != null) {
        const tier = match.tier;
        if (tier === 1) metrics.resolved_by_fnsku += 1;
        else if (tier === 2) metrics.resolved_by_sku_asin += 1;
        else if (tier === 3) metrics.resolved_sku_only += 1;
        else if (tier === 4) metrics.resolved_asin_only += 1;

        const existing = match.row;
        const exF = n(existing.fnsku);
        const conflictFnsku = !!(exF && exF !== fnsku);

        const existingConfidence = Number(existing.confidence_score ?? 0) || 0;
        const nextConfidence = Math.max(existingConfidence, spec.insertConfidence);

        // Count "stronger row preserved" cases — informational only, no behaviour change.
        if (conflictFnsku || existingConfidence >= spec.insertConfidence) {
          strongerPreserved += 1;
        }

        const patch: Record<string, unknown> = {
          last_seen_at: now,
          source_upload_id: uploadId,
          // Inventory_source provenance: only stamp when the existing row has no
          // inventory_source yet, OR is from a strictly weaker inventory source.
          // Never demote `inventory_ledger` (canonical strongest inventory source).
          inventory_source:
            existing.inventory_source && existing.inventory_source !== spec.inventorySource
              ? existing.inventory_source === "inventory_ledger"
                ? existing.inventory_source
                : existing.inventory_source
              : spec.inventorySource,
          confidence_score: nextConfidence,
        };

        // Safe FNSKU fill — never overwrite a different non-null FNSKU.
        if (!conflictFnsku) {
          patch.fnsku = exF ?? fnsku;
        }

        if (sku) {
          if (!n(existing.seller_sku)) patch.seller_sku = sku;
          // msku mirrors seller_sku per the existing convention but also
          // serves as a defensive write for legacy code paths.
          if (!n(existing.msku)) patch.msku = sku;
        }
        if (asin && !n(existing.asin)) patch.asin = asin;

        // Only stamp linked_from_* when bridge row has no provenance yet —
        // never overwrite listing_catalog or inventory_ledger provenance.
        if (!existing.linked_from_report_family) {
          patch.linked_from_report_family = spec.linkedReportFamily;
        }
        if (!existing.linked_from_target_table) {
          patch.linked_from_target_table = spec.linkedTargetTable;
        }

        // Count what the real run would update (one logical update per bridge row).
        if (!mapPatches.has(existing.id)) wouldUpdate += 1;
        mapPatches.set(existing.id, { ...(mapPatches.get(existing.id) ?? {}), ...patch });
        continue;
      }

      // No bridge row at all → would insert a new FNSKU-anchored row.
      wouldInsert += 1;
      if (dryRun) {
        // Dry-run / write-disabled: do not contact Postgres at all.
        // Still bump tier-1 counter so resolved_by_fnsku reflects projected coverage.
        metrics.resolved_by_fnsku += 1;
        continue;
      }
      const insertRow: Record<string, unknown> = {
        organization_id: organizationId,
        store_id: storeId,
        catalog_product_id: null,
        product_id: null,
        seller_sku: sku,
        msku: sku,
        asin,
        fnsku,
        title: null,
        disposition: null,
        external_listing_id: null,
        source_upload_id: uploadId,
        source_report_type: spec.reportType,
        match_source: spec.matchSource,
        inventory_source: spec.inventorySource,
        confidence_score: spec.insertConfidence,
        linked_from_report_family: spec.linkedReportFamily,
        linked_from_target_table: spec.linkedTargetTable,
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
          // Race or pre-existing FNSKU bridge row — fold into pool and treat as resolved.
          const { data: dup } = await supabase
            .from("product_identifier_map")
            .select(MAP_ROW_SELECT)
            .eq("organization_id", organizationId)
            .eq("fnsku", fnsku)
            .limit(3);
          const dupRow = (dup ?? [])[0] as ProductIdentifierMapRow | undefined;
          if (dupRow?.id) {
            await pushFreshMapRowToPool(supabase, organizationId, dupRow.id, pool);
            metrics.resolved_by_fnsku += 1;
            const exConf = Number(dupRow.confidence_score ?? 0) || 0;
            mapPatches.set(dupRow.id, {
              ...(mapPatches.get(dupRow.id) ?? {}),
              last_seen_at: now,
              source_upload_id: uploadId,
              inventory_source: dupRow.inventory_source ?? spec.inventorySource,
              confidence_score: Math.max(exConf, spec.insertConfidence),
            });
            continue;
          }
        }
        metrics.unresolved_insert_failed += 1;
        console.warn(
          `[identity-enrich:${spec.reportType}] map insert failed: ${insErr.message}`,
        );
        continue;
      }

      const newId = String((ins as { id?: string } | null)?.id ?? "").trim();
      if (newId) {
        metrics.bridge_rows_inserted += 1;
        metrics.resolved_by_fnsku += 1;
        await pushFreshMapRowToPool(supabase, organizationId, newId, pool);
      }
    }

    if (!dryRun) {
      for (const [id, patch] of mapPatches) {
        const { error: uErr } = await supabase
          .from("product_identifier_map")
          .update(patch)
          .eq("id", id);
        if (uErr) {
          console.warn(
            `[identity-enrich:${spec.reportType}] map update ${id}: ${uErr.message}`,
          );
        } else {
          metrics.bridge_rows_enriched += 1;
        }
      }
    }
    // dry_run note: mapPatches accumulator is intentionally discarded; the
    // would-update count is already in `wouldUpdate` and surfaced via
    // `existing_rows_updated`.

    metrics.rows_scanned += rows.length;
    offset += pageSize;
    if (rows.length < pageSize) break;
  }

  // Finalise alias / projection counters.
  metrics.ambiguous_candidates_skipped = metrics.unresolved_ambiguous;
  metrics.existing_rows_inserted = dryRun ? wouldInsert : metrics.bridge_rows_inserted;
  metrics.existing_rows_updated = dryRun ? wouldUpdate : metrics.bridge_rows_enriched;
  metrics.safe_candidates_to_write =
    metrics.existing_rows_inserted + metrics.existing_rows_updated;
  metrics.stronger_rows_preserved = strongerPreserved;

  console.log(
    `[identity-enrich:${spec.reportType}] upload=${uploadId} dry_run=${dryRun} ` +
      `write_disabled=${writeDisabled} scanned=${metrics.rows_scanned} ` +
      `would_insert=${metrics.existing_rows_inserted} would_update=${metrics.existing_rows_updated} ` +
      `safe_to_write=${metrics.safe_candidates_to_write} stronger_preserved=${strongerPreserved} ` +
      `inserted=${metrics.bridge_rows_inserted} enriched=${metrics.bridge_rows_enriched} ` +
      `resolved_fnsku=${metrics.resolved_by_fnsku} sku_asin=${metrics.resolved_by_sku_asin} ` +
      `sku_only=${metrics.resolved_sku_only} asin_only=${metrics.resolved_asin_only} ` +
      `ambiguous=${metrics.unresolved_ambiguous} insert_failed=${metrics.unresolved_insert_failed} ` +
      `skipped_no_fnsku=${metrics.rows_skipped_no_fnsku}`,
  );

  return metrics;
}
