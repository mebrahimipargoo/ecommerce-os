/**
 * POST /api/settings/imports/sync
 *
 * Phase 3 of the 3-phase ETL pipeline.
 *
 * Reads rows from amazon_staging (keyed by upload_id), routes them into the
 * correct amazon_ domain table based on report_type, then deletes the processed
 * staging rows.  Sets status → "synced" on completion.
 *
 * Domain table routing (amazon_ prefix standard):
 *   FBA_RETURNS      → amazon_returns          (upsert on organization_id, lpn)
 *   REMOVAL_ORDER    → amazon_removals         (upsert on organization_id, upload_id, source_staging_id)
 *   INVENTORY_LEDGER → amazon_inventory_ledger (upsert on organization_id, fnsku, disposition, location, event_type)
 *   REIMBURSEMENTS   → amazon_reimbursements   (upsert on organization_id, reimbursement_id)
 *   SETTLEMENT       → amazon_settlements      (upsert on organization_id, upload_id, amazon_line_key)
 *   SAFET_CLAIMS     → amazon_safet_claims     (upsert on organization_id, safet_claim_id)
 *   TRANSACTIONS        → amazon_transactions        (upsert on organization_id, order_id, transaction_type, amount)
 *   REPORTS_REPOSITORY  → amazon_reports_repository  (upsert on organization_id, date_time, transaction_type, order_id, sku, description)
 *
 * JSONB Fallback: any CSV column not matched by the typed mapper is stored in
 * the `raw_data` JSONB column — this permanently prevents schema cache crashes.
 *
 * Staging Preservation Rule:
 *   Staging rows are deleted ONLY after their domain batch is successfully upserted.
 *   If any upsert fails, the remaining staging rows are left intact so the user
 *   can fix the issue and retry Phase 3 without re-running Phase 2.
 *
 * Accepts: { upload_id: string }
 * Returns: { ok: true, rowsSynced: number, kind: string }
 */

import { NextResponse } from "next/server";

import {
  applyColumnMappingToRow,
  mapRowToAmazonInventoryLedger,
  mapRowToAmazonReimbursement,
  mapRowToAmazonRemoval,
  mapRowToAmazonRemovalShipment,
  mapRowToAmazonReturn,
  mapRowToAmazonSafetClaim,
  mapRowToAmazonSettlement,
  mapRowToAmazonTransaction,
  mapRowToAmazonReportsRepository,
  mapRowToAmazonRawArchive,
  packPayloadForSupabase,
  NATIVE_COLUMNS_RETURNS,
  NATIVE_COLUMNS_REMOVALS,
  NATIVE_COLUMNS_LEDGER,
  NATIVE_COLUMNS_REIMBURSEMENTS,
  NATIVE_COLUMNS_SETTLEMENTS,
  NATIVE_COLUMNS_SAFET,
  NATIVE_COLUMNS_TRANSACTIONS,
  NATIVE_COLUMNS_REPORTS_REPOSITORY,
  NATIVE_COLUMNS_ALL_ORDERS,
  NATIVE_COLUMNS_REPLACEMENTS,
  NATIVE_COLUMNS_FBA_GRADE_AND_RESELL,
  NATIVE_COLUMNS_MANAGE_FBA_INVENTORY,
  NATIVE_COLUMNS_FBA_INVENTORY,
  NATIVE_COLUMNS_RESERVED_INVENTORY,
  NATIVE_COLUMNS_FEE_PREVIEW,
  NATIVE_COLUMNS_MONTHLY_STORAGE_FEES,
} from "../../../../../lib/import-sync-mappers";
import { removeOlderRemovalImportsWithSameFileContent } from "@/app/(admin)/imports/import-actions";
import { mergeUploadMetadata } from "../../../../../lib/raw-report-upload-metadata";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";
export const maxDuration = 300;

const BATCH_SIZE = 500;
/** Max rows per Postgres upsert call — enables granular sync_progress updates. */
const UPSERT_CHUNK_SIZE = 500;
/** Page size for staging reads. Always use range(0, …); do not advance offset after rows are deleted. */
const STAGING_READ_BATCH = 1000;
const STAGING_TABLE = "amazon_staging";

type Body = { upload_id?: string };

type SyncKind =
  | "FBA_RETURNS"
  | "REMOVAL_ORDER"
  | "REMOVAL_SHIPMENT"
  | "INVENTORY_LEDGER"
  | "REIMBURSEMENTS"
  | "SETTLEMENT"
  | "SAFET_CLAIMS"
  | "TRANSACTIONS"
  | "REPORTS_REPOSITORY"
  | "ALL_ORDERS"
  | "REPLACEMENTS"
  | "FBA_GRADE_AND_RESELL"
  | "MANAGE_FBA_INVENTORY"
  | "FBA_INVENTORY"
  | "RESERVED_INVENTORY"
  | "FEE_PREVIEW"
  | "MONTHLY_STORAGE_FEES"
  | "UNKNOWN";

/** amazon_ domain table for each report kind (null = no table yet / UNKNOWN). */
const DOMAIN_TABLE: Record<SyncKind, string | null> = {
  FBA_RETURNS:           "amazon_returns",
  REMOVAL_ORDER:         "amazon_removals",
  REMOVAL_SHIPMENT:      "amazon_removal_shipments",
  INVENTORY_LEDGER:      "amazon_inventory_ledger",
  REIMBURSEMENTS:        "amazon_reimbursements",
  SETTLEMENT:            "amazon_settlements",
  SAFET_CLAIMS:          "amazon_safet_claims",
  TRANSACTIONS:          "amazon_transactions",
  REPORTS_REPOSITORY:    "amazon_reports_repository",
  ALL_ORDERS:            "amazon_all_orders",
  REPLACEMENTS:          "amazon_replacements",
  FBA_GRADE_AND_RESELL:  "amazon_fba_grade_and_resell",
  MANAGE_FBA_INVENTORY:  "amazon_manage_fba_inventory",
  FBA_INVENTORY:         "amazon_fba_inventory",
  RESERVED_INVENTORY:    "amazon_reserved_inventory",
  FEE_PREVIEW:           "amazon_fee_preview",
  MONTHLY_STORAGE_FEES:  "amazon_monthly_storage_fees",
  UNKNOWN:               null,
};

/**
 * Upsert conflict key for each domain table.
 * Each value must exactly match a UNIQUE constraint that exists in Postgres.
 *
 * Supabase translates onConflict: "col_a,col_b" →
 *   INSERT … ON CONFLICT (col_a, col_b) DO UPDATE SET …
 *
 * Constraints as of migration 20260502+:
 *   amazon_returns          → (organization_id, lpn)
 *   amazon_removals         → (organization_id, upload_id, source_staging_id) — one row per staging line (60519)
 *   amazon_inventory_ledger → (organization_id, fnsku, disposition, location, event_type)
 *   amazon_reimbursements   → (organization_id, reimbursement_id, sku)
 *   amazon_settlements      → (organization_id, upload_id, amazon_line_key)
 *   amazon_safet_claims     → (organization_id, safet_claim_id)
 *   amazon_transactions     → (organization_id, order_id, transaction_type, amount)
 *   amazon_reports_repository → (organization_id, date_time, transaction_type, order_id, sku, description)
 */
const CONFLICT_KEY: Record<SyncKind, string | null> = {
  FBA_RETURNS:        "organization_id,lpn",
  REMOVAL_ORDER:      "organization_id,store_id,order_id,sku,fnsku,disposition,requested_quantity,shipped_quantity,disposed_quantity,cancelled_quantity,order_date,order_type",
  REMOVAL_SHIPMENT:   "organization_id,upload_id,amazon_staging_id",
  INVENTORY_LEDGER:   "organization_id,fnsku,disposition,location,event_type",
  REIMBURSEMENTS:     "organization_id,reimbursement_id,sku",
  SETTLEMENT:         "organization_id,upload_id,amazon_line_key",
  SAFET_CLAIMS:       "organization_id,safet_claim_id",
  // Fixed in migration 20260605: was (org, order_id, tx_type, amount) — too narrow,
  // collapsed distinct rows with same order+type+amount but different SKU/dates.
  TRANSACTIONS:       "organization_id,source_line_hash",
  REPORTS_REPOSITORY: "organization_id,date_time,transaction_type,order_id,sku,description",
  // New raw-archive tables: idempotent dedup via content fingerprint
  ALL_ORDERS:           "organization_id,source_line_hash",
  REPLACEMENTS:         "organization_id,source_line_hash",
  FBA_GRADE_AND_RESELL: "organization_id,source_line_hash",
  MANAGE_FBA_INVENTORY: "organization_id,source_line_hash",
  FBA_INVENTORY:        "organization_id,source_line_hash",
  RESERVED_INVENTORY:   "organization_id,source_line_hash",
  FEE_PREVIEW:          "organization_id,source_line_hash",
  MONTHLY_STORAGE_FEES: "organization_id,source_line_hash",
  UNKNOWN:              null,
};

/** NATIVE_COLUMNS set for each sync kind — passed to packPayloadForSupabase(). */
const NATIVE_COLUMNS_MAP: Record<SyncKind, Set<string> | null> = {
  FBA_RETURNS:           NATIVE_COLUMNS_RETURNS,
  REMOVAL_ORDER:         NATIVE_COLUMNS_REMOVALS,
  REMOVAL_SHIPMENT:      NATIVE_COLUMNS_REMOVALS,
  INVENTORY_LEDGER:      NATIVE_COLUMNS_LEDGER,
  REIMBURSEMENTS:        NATIVE_COLUMNS_REIMBURSEMENTS,
  SETTLEMENT:            NATIVE_COLUMNS_SETTLEMENTS,
  SAFET_CLAIMS:          NATIVE_COLUMNS_SAFET,
  TRANSACTIONS:          NATIVE_COLUMNS_TRANSACTIONS,
  REPORTS_REPOSITORY:    NATIVE_COLUMNS_REPORTS_REPOSITORY,
  ALL_ORDERS:            NATIVE_COLUMNS_ALL_ORDERS,
  REPLACEMENTS:          NATIVE_COLUMNS_REPLACEMENTS,
  FBA_GRADE_AND_RESELL:  NATIVE_COLUMNS_FBA_GRADE_AND_RESELL,
  MANAGE_FBA_INVENTORY:  NATIVE_COLUMNS_MANAGE_FBA_INVENTORY,
  FBA_INVENTORY:         NATIVE_COLUMNS_FBA_INVENTORY,
  RESERVED_INVENTORY:    NATIVE_COLUMNS_RESERVED_INVENTORY,
  FEE_PREVIEW:           NATIVE_COLUMNS_FEE_PREVIEW,
  MONTHLY_STORAGE_FEES:  NATIVE_COLUMNS_MONTHLY_STORAGE_FEES,
  UNKNOWN:               null,
};

/** Match Postgres NULLS NOT DISTINCT text normalization (see Python _pg_text_unique_field). */
function pgTextUniqueField(val: unknown): string | null {
  if (val === null || val === undefined) return null;
  const s = String(val).trim();
  return s === "" ? null : s;
}

/** Stable string for integer quantity columns (matches Postgres NULL / int semantics). */
function qtyKey(v: unknown): string {
  if (v === null || v === undefined) return "\0n";
  if (typeof v === "number" && Number.isFinite(v)) return `i:${Math.trunc(v)}`;
  const s = String(v).trim();
  if (s === "") return "\0n";
  const n = parseInt(s, 10);
  if (!Number.isNaN(n)) return `i:${n}`;
  return `s:${s}`;
}

/** Date column — align with Postgres date / NULLS NOT DISTINCT (YYYY-MM-DD or null). */
function dateKey(v: unknown): string {
  if (v === null || v === undefined) return "\0n";
  const s = String(v).trim();
  if (s === "") return "\0n";
  return s.length >= 10 ? s.slice(0, 10) : s;
}

/**
 * Logical-line fields for REMOVAL_SHIPMENT ↔ NULL-tracking matching (not the DB upsert key after 60519).
 */
function removalLineKeyFromMapped(row: Record<string, unknown>): string {
  return [
    pgTextUniqueField(row.order_id) ?? "",
    pgTextUniqueField(row.sku) ?? "",
    pgTextUniqueField(row.fnsku) ?? "",
    pgTextUniqueField(row.disposition) ?? "",
    qtyKey(row.requested_quantity),
    qtyKey(row.shipped_quantity),
    qtyKey(row.disposed_quantity),
    qtyKey(row.cancelled_quantity),
    dateKey(row.order_date),
    pgTextUniqueField(row.order_type) ?? "",
  ].join("\x1e");
}

/** Fallback batch key when source_staging_id is missing (legacy); REMOVAL_ORDER normally uses staging id. */
function removalLogicalLineDedupKey(row: Record<string, unknown>): string {
  return [
    String(row.organization_id ?? "").trim(),
    pgTextUniqueField(row.order_id) ?? "",
    pgTextUniqueField(row.sku) ?? "",
    pgTextUniqueField(row.fnsku) ?? "",
    pgTextUniqueField(row.disposition) ?? "",
    qtyKey(row.requested_quantity),
    qtyKey(row.shipped_quantity),
    qtyKey(row.disposed_quantity),
    qtyKey(row.cancelled_quantity),
    dateKey(row.order_date),
    pgTextUniqueField(row.order_type) ?? "",
  ].join("|");
}

/**
 * Imports Target Store on `raw_report_uploads.metadata` (Wave 1).
 * Prefer `import_store_id`; fall back to `ledger_store_id` for older sessions.
 */
function resolveImportStoreId(meta: unknown): string | null {
  const m =
    meta && typeof meta === "object" && !Array.isArray(meta)
      ? (meta as Record<string, unknown>)
      : {};
  const a = typeof m.import_store_id === "string" ? m.import_store_id.trim() : "";
  if (a && isUuidString(a)) return a;
  const b = typeof m.ledger_store_id === "string" ? m.ledger_store_id.trim() : "";
  if (b && isUuidString(b)) return b;
  return null;
}

/**
 * Within-batch dedupe key aligned with `uq_amazon_removals_business_line` (store-scoped logical line).
 */
function removalBusinessDedupKey(row: Record<string, unknown>): string {
  return [
    String(row.organization_id ?? "").trim(),
    String(row.store_id ?? "").trim(),
    pgTextUniqueField(row.order_id) ?? "",
    pgTextUniqueField(row.sku) ?? "",
    pgTextUniqueField(row.fnsku) ?? "",
    pgTextUniqueField(row.disposition) ?? "",
    qtyKey(row.requested_quantity),
    qtyKey(row.shipped_quantity),
    qtyKey(row.disposed_quantity),
    qtyKey(row.cancelled_quantity),
    dateKey(row.order_date),
    pgTextUniqueField(row.order_type) ?? "",
  ].join("|");
}

/**
 * Raw shipment archive: one row per staging line. Arbiter matches
 * `uq_amazon_removal_shipments_org_upload_staging` (see 20260521_wave1_removal_store_dual_dedupe
 * and migration dropping business-line uniqueness for raw archive).
 */
const REMOVAL_SHIPMENT_STAGING_CONFLICT = "organization_id,upload_id,amazon_staging_id";

/**
 * Patch `amazon_removals` from typed `amazon_removal_shipments` — fill missing fields only
 * (no null/empty overwrites of existing values).
 */
function buildRemovalFillFromShipment(
  existing: Record<string, unknown>,
  shipment: Record<string, unknown>,
): Record<string, unknown> {
  const payload: Record<string, unknown> = {};
  const tn = pgTextUniqueField(shipment.tracking_number);
  if (tn && !pgTextUniqueField(existing.tracking_number as string | null)) {
    payload.tracking_number = tn;
  }
  const incC = shipment.carrier;
  if (incC !== undefined && incC !== null && String(incC).trim() !== "") {
    if (!pgTextUniqueField(existing.carrier as string | null)) payload.carrier = incC;
  }
  const incSd = shipment.shipment_date;
  if (incSd !== undefined && incSd !== null && String(incSd).trim() !== "") {
    if (existing.shipment_date == null || String(existing.shipment_date).trim() === "") {
      payload.shipment_date = incSd;
    }
  }
  const incOd = shipment.order_date;
  if (incOd !== undefined && incOd !== null && String(incOd).trim() !== "") {
    if (existing.order_date == null || String(existing.order_date).trim() === "") {
      payload.order_date = incOd;
    }
  }
  const incOt = shipment.order_type;
  if (incOt !== undefined && incOt !== null && String(incOt).trim() !== "") {
    if (!pgTextUniqueField(existing.order_type as string | null)) payload.order_type = incOt;
  }
  return payload;
}

function parseStagingRawRow(raw: unknown): Record<string, string> {
  if (raw && typeof raw === "object" && !Array.isArray(raw)) {
    const o = raw as Record<string, unknown>;
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(o)) {
      out[k] = v === null || v === undefined ? "" : String(v);
    }
    return out;
  }
  if (typeof raw === "string") {
    try {
      const j = JSON.parse(raw) as unknown;
      if (j && typeof j === "object" && !Array.isArray(j)) {
        return parseStagingRawRow(j);
      }
    } catch {
      /* ignore */
    }
  }
  return {};
}

type RemovalShipmentSyncOpts = {
  uploadId: string;
  orgId: string;
  storeId: string;
  totalStagingRows: number;
  columnMapping: Record<string, string> | null;
  syncUpserted: { value: number };
};

/**
 * REMOVAL_SHIPMENT: for each staging line, upsert into `amazon_removal_shipments` (raw archive) using
 * staging identity only (`organization_id`, `upload_id`, `amazon_staging_id`). After sync, shipment
 * tree + allocations are rebuilt via DB RPCs (legacy per-batch enrichment of `amazon_removals` is bypassed).
 */
async function runRemovalShipmentSync(opts: RemovalShipmentSyncOpts): Promise<{
  synced: number;
  mapperNullCount: number;
}> {
  const { uploadId, orgId, storeId, totalStagingRows, columnMapping, syncUpserted } = opts;

  let shipmentArchiveRowsAttempted = 0;
  let shipmentArchiveRowsWritten = 0;

  let archived = 0;
  let mapperNull = 0;
  const removalsUpdated = 0;
  let loggedShipmentRawKeys = false;
  let loggedShipmentMappedSample = false;

  const REMOVAL_ENRICH_VERIFY_FIELDS = [
    "tracking_number",
    "carrier",
    "shipment_date",
    "order_date",
    "order_type",
  ] as const;

  let verifyAStagingFetched = 0;
  let verifyAShipmentWritten = 0;
  let verifyAStagingDeleted = 0;
  let verifyBEnrichConsidered = 0;
  let verifyBEnrichMatched = 0;
  let verifyBEnrichUpdatedOk = 0;
  const verifyBEnrichFieldInPayload: Record<string, number> = {};
  for (const k of REMOVAL_ENRICH_VERIFY_FIELDS) verifyBEnrichFieldInPayload[k] = 0;

  let removalShipmentBatchIndex = 0;

  while (true) {
    removalShipmentBatchIndex++;
    const { data: stagingRows, error: readErr } = await supabaseServer
      .from(STAGING_TABLE)
      .select("id, raw_row")
      .eq("upload_id", uploadId)
      .eq("organization_id", orgId)
      .order("id", { ascending: true })
      .limit(STAGING_READ_BATCH);

    if (readErr) throw new Error(`Staging read failed: ${readErr.message}`);
    if (!stagingRows || stagingRows.length === 0) break;

    const ids = stagingRows.map((r) => r.id as string);

    const archiveRows: Record<string, unknown>[] = [];
    for (const sr of stagingRows) {
      const rawObj = parseStagingRawRow(sr.raw_row);
      if (!loggedShipmentRawKeys && Object.keys(rawObj).length > 0) {
        loggedShipmentRawKeys = true;
        console.log(
          JSON.stringify({
            phase: "REMOVAL_SHIPMENT_raw_row_keys",
            upload_id: uploadId,
            keys: Object.keys(rawObj).sort(),
          }),
        );
      }
      const mappedRow = applyColumnMappingToRow(rawObj, columnMapping);
      const mappedRemoval = mapRowToAmazonRemovalShipment(mappedRow, orgId, uploadId, storeId) as Record<
        string,
        unknown
      > | null;
      if (!loggedShipmentMappedSample && mappedRemoval) {
        loggedShipmentMappedSample = true;
        console.log(
          JSON.stringify({
            phase: "REMOVAL_SHIPMENT_mapped_sample_before_upsert",
            upload_id: uploadId,
            store_id: storeId,
            sample: {
              order_id: mappedRemoval.order_id,
              tracking_number: mappedRemoval.tracking_number,
              carrier: mappedRemoval.carrier,
              shipment_date: mappedRemoval.shipment_date,
              order_date: mappedRemoval.order_date,
              order_type: mappedRemoval.order_type,
              sku: mappedRemoval.sku,
              fnsku: mappedRemoval.fnsku,
              disposition: mappedRemoval.disposition,
              requested_quantity: mappedRemoval.requested_quantity,
              shipped_quantity: mappedRemoval.shipped_quantity,
              disposed_quantity: mappedRemoval.disposed_quantity,
              cancelled_quantity: mappedRemoval.cancelled_quantity,
            },
          }),
        );
      }
      const baseArchive: Record<string, unknown> = {
        organization_id: orgId,
        upload_id: uploadId,
        amazon_staging_id: sr.id,
        store_id: storeId,
        raw_row: rawObj,
      };
      if (mappedRemoval) {
        Object.assign(baseArchive, {
          order_id: mappedRemoval.order_id ?? null,
          sku: mappedRemoval.sku ?? null,
          fnsku: mappedRemoval.fnsku ?? null,
          disposition: mappedRemoval.disposition ?? null,
          tracking_number: mappedRemoval.tracking_number ?? null,
          carrier: mappedRemoval.carrier ?? null,
          shipment_date: mappedRemoval.shipment_date ?? null,
          order_date: mappedRemoval.order_date ?? null,
          order_type: mappedRemoval.order_type ?? null,
          requested_quantity: mappedRemoval.requested_quantity ?? null,
          shipped_quantity: mappedRemoval.shipped_quantity ?? null,
          disposed_quantity: mappedRemoval.disposed_quantity ?? null,
          cancelled_quantity: mappedRemoval.cancelled_quantity ?? null,
        });
      }
      archiveRows.push(baseArchive);
      if (!mappedRemoval) mapperNull++;
    }

    shipmentArchiveRowsAttempted += archiveRows.length;

    for (let i = 0; i < archiveRows.length; i += UPSERT_CHUNK_SIZE) {
      const chunk = archiveRows.slice(i, i + UPSERT_CHUNK_SIZE);
      if (chunk.length === 0) continue;
      const { error: upsertErr } = await supabaseServer.from("amazon_removal_shipments").upsert(chunk, {
        onConflict: REMOVAL_SHIPMENT_STAGING_CONFLICT,
        ignoreDuplicates: false,
      });
      if (upsertErr) {
        throw new Error(
          `[REMOVAL_SHIPMENT] upsert (staging-key) into amazon_removal_shipments failed: ${upsertErr.message}`,
        );
      }
    }

    shipmentArchiveRowsWritten += archiveRows.length;

    archived += archiveRows.length;
    await bumpSyncProgressMetadata(
      { uploadId, orgId, totalStagingRows, upserted: syncUpserted },
      archiveRows.length,
    );

    // Legacy step bypassed: loading `amazon_removal_shipments` by `.in("amazon_staging_id", ids)` for
    // per-batch enrichment of `amazon_removals` (PostgREST "Bad Request" on large batches). Tree +
    // allocations are rebuilt once after full sync via `rebuild_shipment_tree_from_removal_shipments`
    // and `rebuild_removal_item_allocations`.

    const enrichmentShipmentsConsidered = 0;
    const enrichmentRemovalsMatched = 0;
    const enrichmentRemovalsUpdated = 0;
    const enrichPayloadFieldCounts: Record<string, number> = {};
    for (const k of REMOVAL_ENRICH_VERIFY_FIELDS) enrichPayloadFieldCounts[k] = 0;

    const removalShipmentsUpsertRowCount = archiveRows.length;
    await deleteFromStaging(ids);

    verifyAStagingFetched += stagingRows.length;
    verifyAShipmentWritten += removalShipmentsUpsertRowCount;
    verifyAStagingDeleted += ids.length;
    verifyBEnrichConsidered += enrichmentShipmentsConsidered;
    verifyBEnrichMatched += enrichmentRemovalsMatched;
    verifyBEnrichUpdatedOk += enrichmentRemovalsUpdated;
    for (const fk of REMOVAL_ENRICH_VERIFY_FIELDS) {
      verifyBEnrichFieldInPayload[fk] += enrichPayloadFieldCounts[fk];
    }

    console.log(
      JSON.stringify({
        checkpoint: "REMOVAL_PIPELINE",
        stage: "AB_per_batch",
        upload_id: uploadId,
        organization_id: orgId,
        store_id: storeId,
        batch_index: removalShipmentBatchIndex,
        A_sync: {
          staging_rows_fetched: stagingRows.length,
          shipment_rows_written: removalShipmentsUpsertRowCount,
          shipment_archive_rows_attempted: shipmentArchiveRowsAttempted,
          shipment_archive_rows_written: shipmentArchiveRowsWritten,
          shipment_archive_rows_collapsed_by_raw_archive_logic: 0,
          staging_rows_deleted: ids.length,
        },
        B_enrichment: {
          legacy_bypassed: true,
          shipment_rows_considered: enrichmentShipmentsConsidered,
          removal_rows_matched: enrichmentRemovalsMatched,
          removal_rows_updated: enrichmentRemovalsUpdated,
          removal_updates_including_field: enrichPayloadFieldCounts,
        },
      }),
    );
  }

  const stagingCountReconciles =
    totalStagingRows <= 0 || archived === totalStagingRows;

  const pStoreIdForRpc = isUuidString(storeId) ? storeId : null;
  console.log(
    `[REMOVAL_SHIPMENT] shipment sync completed organization_id=${orgId} p_store_id=${pStoreIdForRpc ?? "null"}`,
  );

  const { data: treeCounts, error: treeErr } = await supabaseServer.rpc(
    "rebuild_shipment_tree_from_removal_shipments",
    { p_organization_id: orgId, p_store_id: pStoreIdForRpc },
  );
  if (treeErr) {
    throw new Error(
      `[REMOVAL_SHIPMENT] rebuild_shipment_tree_from_removal_shipments failed: ${treeErr.message}`,
    );
  }
  const treeRow = Array.isArray(treeCounts) ? treeCounts[0] : treeCounts;
  console.log(`[REMOVAL_SHIPMENT] shipment tree rebuilt`, JSON.stringify(treeRow ?? null));

  const { data: allocCount, error: allocErr } = await supabaseServer.rpc("rebuild_removal_item_allocations", {
    p_organization_id: orgId,
    p_store_id: pStoreIdForRpc,
  });
  if (allocErr) {
    throw new Error(`[REMOVAL_SHIPMENT] rebuild_removal_item_allocations failed: ${allocErr.message}`);
  }
  console.log(`[REMOVAL_SHIPMENT] removal allocations rebuilt count=${String(allocCount ?? "null")}`);

  console.log(
    JSON.stringify({
      checkpoint: "REMOVAL_PIPELINE",
      stage: "AB_upload_totals",
      upload_id: uploadId,
      organization_id: orgId,
      store_id: storeId,
      A_sync_totals: {
        staging_rows_fetched: verifyAStagingFetched,
        shipment_rows_written: verifyAShipmentWritten,
        shipment_archive_rows_attempted: shipmentArchiveRowsAttempted,
        shipment_archive_rows_written: shipmentArchiveRowsWritten,
        shipment_archive_rows_collapsed_by_raw_archive_logic: 0,
        staging_rows_deleted: verifyAStagingDeleted,
        initial_staging_row_count: totalStagingRows,
        staging_rows_processed: archived,
        staging_fetch_equals_delete: verifyAStagingFetched === verifyAStagingDeleted,
        shipment_written_vs_staging_processed_note:
          verifyAShipmentWritten < archived
            ? "fewer_shipment_upsert_rows_than_staging_lines_unexpected"
            : "ok",
      },
      B_enrichment_totals: {
        legacy_bypassed: true,
        shipment_rows_considered: verifyBEnrichConsidered,
        removal_rows_matched: verifyBEnrichMatched,
        removal_rows_updated: verifyBEnrichUpdatedOk,
        removal_updates_including_field: verifyBEnrichFieldInPayload,
      },
      integrity: {
        staging_rows_not_skipped_by_traversal: stagingCountReconciles,
        mapper_null_rows: mapperNull,
      },
    }),
  );

  console.log(
    JSON.stringify({
      phase: "REMOVAL_SHIPMENT_wave1_reconciliation",
      store_id: storeId,
      shipment_lines_archived: archived,
      removals_tracking_updates: removalsUpdated,
      mapper_null: mapperNull,
    }),
  );
  console.log(
    `[REMOVAL_SHIPMENT] Done: archived=${archived} removals_tracking_updates=${removalsUpdated} mapper_null=${mapperNull}`,
  );

  return { synced: archived, mapperNullCount: mapperNull };
}

/** Maps raw_report_uploads.report_type → canonical SyncKind. */
function resolveImportKind(reportType: string | null | undefined): SyncKind {
  const rt = String(reportType ?? "").trim();
  if (rt === "FBA_RETURNS" || rt === "fba_customer_returns")   return "FBA_RETURNS";
  if (rt === "REMOVAL_ORDER")                                   return "REMOVAL_ORDER";
  if (rt === "REMOVAL_SHIPMENT")                                return "REMOVAL_SHIPMENT";
  if (rt === "INVENTORY_LEDGER" || rt === "inventory_ledger")  return "INVENTORY_LEDGER";
  if (rt === "REIMBURSEMENTS" || rt === "reimbursements")      return "REIMBURSEMENTS";
  if (rt === "SETTLEMENT" || rt === "settlement_repository")   return "SETTLEMENT";
  if (rt === "SAFET_CLAIMS" || rt === "safe_t_claims")         return "SAFET_CLAIMS";
  if (rt === "TRANSACTIONS" || rt === "transaction_view")      return "TRANSACTIONS";
  if (rt === "REPORTS_REPOSITORY")                              return "REPORTS_REPOSITORY";
  if (rt === "ALL_ORDERS")                                      return "ALL_ORDERS";
  if (rt === "REPLACEMENTS")                                    return "REPLACEMENTS";
  if (rt === "FBA_GRADE_AND_RESELL")                            return "FBA_GRADE_AND_RESELL";
  if (rt === "MANAGE_FBA_INVENTORY")                            return "MANAGE_FBA_INVENTORY";
  if (rt === "FBA_INVENTORY")                                   return "FBA_INVENTORY";
  if (rt === "RESERVED_INVENTORY")                              return "RESERVED_INVENTORY";
  if (rt === "FEE_PREVIEW")                                     return "FEE_PREVIEW";
  if (rt === "MONTHLY_STORAGE_FEES")                            return "MONTHLY_STORAGE_FEES";
  return "UNKNOWN";
}

async function audit(
  orgId: string,
  action: string,
  entityId: string,
  detail?: Record<string, unknown>,
): Promise<void> {
  await supabaseServer.from("raw_report_import_audit").insert({
    organization_id: orgId,
    user_profile_id: null,
    action,
    entity_id: entityId,
    detail: detail ?? null,
  });
}

/** Write a "failed" status back to the upload row (best-effort, never throws). */
async function markFailed(uploadId: string, orgId: string, message: string): Promise<void> {
  try {
    const { data: prevRow } = await supabaseServer
      .from("raw_report_uploads")
      .select("metadata")
      .eq("id", uploadId)
      .maybeSingle();

    await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "failed",
        metadata: mergeUploadMetadata(
          (prevRow as { metadata?: unknown } | null)?.metadata,
          { error_message: message, failed_phase: "sync" },
        ),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId);
  } catch (inner) {
    console.error("[sync] markFailed write error:", inner);
  }
}

export async function POST(req: Request): Promise<Response> {
  let uploadIdForFail: string | null = null;
  let orgId = "";

  try {
    const body = (await req.json()) as Body;
    const uploadId = typeof body.upload_id === "string" ? body.upload_id.trim() : "";
    if (!isUuidString(uploadId)) {
      return NextResponse.json({ ok: false, error: "Invalid upload_id." }, { status: 400 });
    }
    uploadIdForFail = uploadId;

    const { data: row, error: fetchErr } = await supabaseServer
      .from("raw_report_uploads")
      .select("id, organization_id, metadata, status, report_type, column_mapping, file_name")
      .eq("id", uploadId)
      .maybeSingle();

    if (fetchErr || !row) {
      return NextResponse.json({ ok: false, error: "Upload session not found." }, { status: 404 });
    }

    orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
    if (!isUuidString(orgId)) {
      return NextResponse.json(
        { ok: false, error: "Invalid upload row (organization_id)." },
        { status: 500 },
      );
    }

    const status = String((row as { status?: unknown }).status ?? "");
    if (status !== "staged" && status !== "failed") {
      return NextResponse.json(
        {
          ok: false,
          error: `Phase 3 (Sync) requires status "staged" (or "failed" for retry). Current status is "${status}".${
            status === "mapped" || status === "ready"
              ? " Run Phase 2 (Process) first."
              : status === "needs_mapping"
                ? ' Use "Map Columns" first, then Process, then Sync.'
                : ""
          }`,
        },
        { status: 409 },
      );
    }

    const kind = resolveImportKind((row as { report_type?: string }).report_type);
    if (kind === "UNKNOWN") {
      return NextResponse.json(
        {
          ok: false,
          error:
            "Cannot sync: report type is not set. " +
            "Open the History table, set the correct report type from the dropdown, then re-run Process and Sync.",
        },
        { status: 422 },
      );
    }

    const hasDomainTable = DOMAIN_TABLE[kind] !== null;

    const columnMapping =
      (row as { column_mapping?: unknown }).column_mapping &&
      typeof (row as { column_mapping?: unknown }).column_mapping === "object" &&
      !Array.isArray((row as { column_mapping?: unknown }).column_mapping)
        ? ((row as { column_mapping?: unknown }).column_mapping as Record<string, string>)
        : null;

    const meta = (row as { metadata?: unknown }).metadata;

    const importStoreId = resolveImportStoreId(meta);
    if ((kind === "REMOVAL_ORDER" || kind === "REMOVAL_SHIPMENT") && !importStoreId) {
      return NextResponse.json(
        {
          ok: false,
          error:
            "Imports Target Store is required for removal reports. Choose a target store in the importer, save classification, then run Sync again.",
        },
        { status: 422 },
      );
    }

    // ── Optimistic lock — prevents concurrent clicks from double-syncing ───────
    const { data: locked, error: lockErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "processing",
        metadata: mergeUploadMetadata(meta, {
          process_progress: 0,
          sync_progress: 0,
          etl_phase: "sync",
          error_message: "",
        }),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId)
      .in("status", ["staged", "failed"])
      .select("id");

    if (lockErr) {
      return NextResponse.json({ ok: false, error: lockErr.message }, { status: 500 });
    }
    if (!locked || locked.length === 0) {
      return NextResponse.json(
        {
          ok: false,
          error: "Upload is not in a syncable state (another operation may be running).",
        },
        { status: 409 },
      );
    }

    await audit(orgId, "import.sync_started", uploadId, {
      fileName: (row as { file_name?: string }).file_name,
      kind,
      domainTable: DOMAIN_TABLE[kind] ?? "none",
    });

    // Same removal-order or removal-shipment file re-uploaded: drop older imports with identical
    // SHA-256 (storage + domain + history row). Scoped by report_type so order vs shipment files
    // do not cross-delete.
    if (kind === "REMOVAL_ORDER" || kind === "REMOVAL_SHIPMENT") {
      const rep = await removeOlderRemovalImportsWithSameFileContent(
        orgId,
        uploadId,
        meta,
        kind === "REMOVAL_SHIPMENT" ? "REMOVAL_SHIPMENT" : "REMOVAL_ORDER",
      );
      if (rep.ok && rep.removedUploadIds.length > 0) {
        console.log(
          `[sync][${kind}] Replaced ${rep.removedUploadIds.length} prior import(s) with the same file (SHA-256).`,
        );
        await audit(orgId, "import.removal_same_file_replaced_prior_uploads", uploadId, {
          removed_upload_ids: rep.removedUploadIds,
        });
      } else if (!rep.ok) {
        throw new Error(`Same-file cleanup failed: ${rep.error}`);
      }
    }

    const { count: stagingRowCount } = await supabaseServer
      .from(STAGING_TABLE)
      .select("*", { count: "exact", head: true })
      .eq("upload_id", uploadId)
      .eq("organization_id", orgId);

    const totalStagingRows = typeof stagingRowCount === "number" ? stagingRowCount : 0;
    const syncUpserted = { value: 0 };

    console.log(`[sync][${kind}] Starting sync — staging rows: ${totalStagingRows}, domain table: ${DOMAIN_TABLE[kind] ?? "none"}`);

    // ── Phase 3 core: read → map → upsert → delete (strictly sequenced) ───────
    //
    // STAGING PRESERVATION RULE:
    //   Staging rows are deleted only AFTER their corresponding domain batch is
    //   confirmed written.  If flushDomainBatch() throws at any point, the
    //   remaining staging rows are left untouched so the user can retry.
    //
    // REMOVAL_SHIPMENT: dedicated path — full raw rows → amazon_removal_shipments;
    // then DB RPCs rebuild shipment tree + removal_item_allocations (legacy amazon_removals enrichment bypassed).
    //
    // Errors propagate immediately — no swallowing, no silent fallbacks.
    let synced = 0;
    // Tracks staging rows where the mapper returned null (missing required anchor
    // field). These are removed from staging in the final cleanup but are never
    // written to the domain table — logged as a warning so data loss is visible.
    let mapperNullCount = 0;

    if (kind === "REMOVAL_SHIPMENT") {
      const r = await runRemovalShipmentSync({
        uploadId,
        orgId,
        storeId: importStoreId!,
        totalStagingRows,
        columnMapping,
        syncUpserted,
      });
      synced = r.synced;
      mapperNullCount = r.mapperNullCount;
    } else while (true) {
      // ── Read next chunk: always from the start — prior rows were deleted from staging. ──
      const { data: stagingRows, error: readErr } = await supabaseServer
        .from(STAGING_TABLE)
        .select("id, raw_row")
        .eq("upload_id", uploadId)
        .eq("organization_id", orgId)
        .range(0, STAGING_READ_BATCH - 1);

      if (readErr) throw new Error(`Staging read failed: ${readErr.message}`);
      if (!stagingRows || stagingRows.length === 0) break;

      const domainBatch: Record<string, unknown>[] = [];
      const batchStagingIds: string[] = [];

      for (const sr of stagingRows as { id: string; raw_row: Record<string, string> }[]) {
        if (hasDomainTable) {
          const rawRow = (sr.raw_row ?? {}) as Record<string, string>;
          const mappedRow = applyColumnMappingToRow(rawRow, columnMapping);

          let insertRow: Record<string, unknown> | null = null;

          if (kind === "FBA_RETURNS") {
            insertRow = mapRowToAmazonReturn(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "REMOVAL_ORDER") {
            insertRow = mapRowToAmazonRemoval(mappedRow, orgId, uploadId, importStoreId!) as Record<string, unknown> | null;
            if (insertRow) insertRow.source_staging_id = sr.id;
          } else if (kind === "INVENTORY_LEDGER") {
            insertRow = mapRowToAmazonInventoryLedger(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "REIMBURSEMENTS") {
            insertRow = mapRowToAmazonReimbursement(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "SETTLEMENT") {
            insertRow = mapRowToAmazonSettlement(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "SAFET_CLAIMS") {
            insertRow = mapRowToAmazonSafetClaim(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "TRANSACTIONS") {
            insertRow = mapRowToAmazonTransaction(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "REPORTS_REPOSITORY") {
            insertRow = mapRowToAmazonReportsRepository(mappedRow, orgId, uploadId) as Record<
              string,
              unknown
            > | null;
          } else if (
            kind === "ALL_ORDERS" ||
            kind === "REPLACEMENTS" ||
            kind === "FBA_GRADE_AND_RESELL" ||
            kind === "MANAGE_FBA_INVENTORY" ||
            kind === "FBA_INVENTORY" ||
            kind === "RESERVED_INVENTORY" ||
            kind === "FEE_PREVIEW" ||
            kind === "MONTHLY_STORAGE_FEES"
          ) {
            insertRow = mapRowToAmazonRawArchive(mappedRow, orgId, uploadId, importStoreId) as Record<string, unknown> | null;
          }

          if (insertRow) {
            domainBatch.push(insertRow);
            batchStagingIds.push(sr.id);
          } else {
            // Mapper returned null — required anchor field (order_id, lpn, etc.) is
            // missing. Count these so we can warn after the loop.
            mapperNullCount++;
          }

          // ── Flush domain batch at BATCH_SIZE ─────────────────────────────
          // On error flushDomainBatch() throws.  The staging IDs for THIS
          // batch are not yet in batchStagingIds_flushed so they are preserved.
          if (domainBatch.length >= BATCH_SIZE) {
            const inputBatch = domainBatch.splice(0, BATCH_SIZE);
            const stagingIdsForBatch = batchStagingIds.splice(0, inputBatch.length);
            const flushedCount = await flushDomainBatch(kind, inputBatch, {
              uploadId,
              orgId,
              totalStagingRows,
              upserted: syncUpserted,
            });
            synced += flushedCount;
            await deleteFromStaging(stagingIdsForBatch);
          }
        } else {
          // Recognized kind but no domain table — acknowledge as synced
          synced += 1;
          batchStagingIds.push(sr.id);
        }
      }

      // ── Flush remainder of this staging page ───────────────────────────────
      if (domainBatch.length > 0) {
        const inputBatch = domainBatch.splice(0);
        const stagingIdsForBatch = batchStagingIds.splice(0, inputBatch.length);
        const flushedCount = await flushDomainBatch(kind, inputBatch, {
          uploadId,
          orgId,
          totalStagingRows,
          upserted: syncUpserted,
        });
        synced += flushedCount;
        await deleteFromStaging(stagingIdsForBatch);
      }

      // Acknowledge no-domain rows
      if (batchStagingIds.length > 0) {
        await deleteFromStaging(batchStagingIds);
      }
    }

    // ── Row-count verification ────────────────────────────────────────────────
    // mapper-null rows are dropped; JS dedup further reduces rows within a batch.
    // Both are expected in different circumstances — surfaced here for visibility.
    if (mapperNullCount > 0) {
      if (kind === "REMOVAL_SHIPMENT") {
        console.warn(
          `[sync][REMOVAL_SHIPMENT] ${mapperNullCount} staging row(s) could not be mapped ` +
            `(missing order_id / anchor); raw rows were still archived to amazon_removal_shipments. ` +
            `Tracking was not applied to amazon_removals for those lines.`,
        );
      } else {
        console.warn(
          `[sync][${kind}] WARNING: ${mapperNullCount} staging row(s) were not written to ` +
            `${DOMAIN_TABLE[kind] ?? "domain table"} because the mapper could not find a required ` +
            `anchor field (order_id, lpn, etc.). ` +
            `These rows are removed from staging but have NO domain table entry.`,
        );
      }
    }
    if (kind !== "REMOVAL_SHIPMENT") {
      const jsDedupedAway = totalStagingRows - synced - mapperNullCount;
      if (jsDedupedAway > 0) {
        console.log(
          `[sync][${kind}] ${jsDedupedAway} staging row(s) were merged by within-batch deduplication ` +
            `(same conflict key in batch) — this is expected for Amazon reports that repeat rows.`,
        );
      }
      console.log(
        `[sync][${kind}] Row count summary: staging=${totalStagingRows} ` +
          `written=${synced} mapper_null=${mapperNullCount} deduped_in_batch=${Math.max(0, jsDedupedAway)}`,
      );
    } else {
      console.log(
        `[sync][REMOVAL_SHIPMENT] Row count summary: staging=${totalStagingRows} ` +
          `shipment_lines_archived=${synced} mapper_null=${mapperNullCount}`,
      );
    }

    // ── Final safety cleanup: delete any residual staging rows for this upload ─
    // Rows whose mapper returned null (e.g. missing required fields) are never
    // added to domainBatch/batchStagingIds, so they would survive the loop.
    // This single DELETE by upload_id catches all of them without touching
    // rows belonging to other concurrent uploads.
    {
      const { error: cleanupErr } = await supabaseServer
        .from(STAGING_TABLE)
        .delete()
        .eq("upload_id", uploadId)
        .eq("organization_id", orgId);
      if (cleanupErr) {
        console.warn("[sync] final staging cleanup warning:", cleanupErr.message);
        // Non-fatal: domain data is already written; stale staging rows will be
        // cleaned up on the next Process run for this upload.
      }
    }

    // ── Mark upload as synced ─────────────────────────────────────────────────
    const { data: prevRow } = await supabaseServer
      .from("raw_report_uploads")
      .select("metadata")
      .eq("id", uploadId)
      .maybeSingle();

    const syncCollapsedByDedupe =
      kind !== "REMOVAL_SHIPMENT"
        ? Math.max(0, totalStagingRows - synced - mapperNullCount)
        : 0;

    const wave1Extra =
      kind === "REMOVAL_ORDER" || kind === "REMOVAL_SHIPMENT"
        ? {
            wave1_import_store_id: importStoreId,
            wave1_sync_reconciliation: {
              kind,
              staging_row_count: totalStagingRows,
              domain_rows_written: synced,
              mapper_null: mapperNullCount,
              collapsed_by_business_dedupe: syncCollapsedByDedupe,
            },
          }
        : {};

    const { error: markErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "synced",
        metadata: mergeUploadMetadata(
          (prevRow as { metadata?: unknown } | null)?.metadata,
          {
            row_count: synced,
            staging_row_count: totalStagingRows,
            sync_row_count: synced,
            sync_mapper_null_count: mapperNullCount,
            sync_collapsed_by_dedupe: syncCollapsedByDedupe,
            process_progress: 100,
            sync_progress: 100,
            etl_phase: "sync",
            error_message: undefined,
            ...wave1Extra,
          },
        ),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId);

    if (markErr) throw new Error(`Sync succeeded but failed to save status: ${markErr.message}`);

    await audit(orgId, "import.sync_completed", uploadId, {
      rowsSynced: synced,
      kind,
      domainTable: DOMAIN_TABLE[kind] ?? "none",
    });

    return NextResponse.json({ ok: true, rowsSynced: synced, kind });

  } catch (e) {
    const message = e instanceof Error ? e.message : "Sync failed.";
    console.error("[sync] error:", message);

    // ── Write failed status back — staging rows are NOT touched ───────────────
    // Any staging rows not yet deleted remain intact so the user can retry
    // Phase 3 after fixing the underlying issue.
    if (uploadIdForFail && isUuidString(uploadIdForFail) && isUuidString(orgId)) {
      await markFailed(uploadIdForFail, orgId, message);
    }

    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

// =============================================================================
// ── Helpers ───────────────────────────────────────────────────────────────────
// =============================================================================

/**
 * Deletes staging rows by ID in chunks of 200.
 * Only called AFTER the corresponding domain batch has been confirmed written.
 */
async function deleteFromStaging(ids: string[]): Promise<void> {
  if (ids.length === 0) return;
  const chunk_size = 200;
  for (let i = 0; i < ids.length; i += chunk_size) {
    const chunk = ids.slice(i, i + chunk_size);
    const { error } = await supabaseServer
      .from(STAGING_TABLE)
      .delete()
      .in("id", chunk);
    if (error) throw new Error(`Staging cleanup failed: ${error.message}`);
  }
}

type SyncProgressOpts = {
  uploadId: string;
  orgId: string;
  totalStagingRows: number;
  /** Cumulative domain rows successfully upserted this sync (for progress bar). */
  upserted: { value: number };
};

async function bumpSyncProgressMetadata(opts: SyncProgressOpts, chunkRowCount: number): Promise<void> {
  if (opts.totalStagingRows <= 0 || chunkRowCount <= 0) return;
  opts.upserted.value += chunkRowCount;
  const { data: prevRow } = await supabaseServer
    .from("raw_report_uploads")
    .select("metadata")
    .eq("id", opts.uploadId)
    .maybeSingle();
  const pct = Math.min(
    99,
    Math.round((opts.upserted.value / Math.max(1, opts.totalStagingRows)) * 100),
  );
  await supabaseServer
    .from("raw_report_uploads")
    .update({
      metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
        sync_progress: pct,
        etl_phase: "sync",
      }),
      updated_at: new Date().toISOString(),
    })
    .eq("id", opts.uploadId)
    .eq("organization_id", opts.orgId);
}

/**
 * Packs and upserts a batch of mapped domain rows into the correct amazon_ table.
 *
 *  Step 1 — packPayloadForSupabase():
 *    Any key NOT in NATIVE_COLUMNS_MAP[kind] is redirected into the raw_data
 *    JSONB column.  This is the permanent guard against schema cache errors.
 *
 *  Step 2 — deduplicateByConflictKey():
 *    Removes same-batch duplicates before the Postgres upsert to avoid
 *    "ON CONFLICT DO UPDATE command cannot affect a row a second time".
 *
 *  Step 3 — supabase.upsert({ onConflict }) in chunks of UPSERT_CHUNK_SIZE:
 *    Writes progress to metadata.sync_progress after each chunk.
 *
 * @returns Number of rows actually written (after deduplication).
 */
async function flushDomainBatch(
  kind: SyncKind,
  rows: Record<string, unknown>[],
  syncProgress?: SyncProgressOpts,
): Promise<number> {
  if (rows.length === 0) return 0;

  const table = DOMAIN_TABLE[kind];
  if (!table) return rows.length; // no-op for UNKNOWN

  // ── Step 1: JSONB packing ──────────────────────────────────────────────────
  const nativeCols = NATIVE_COLUMNS_MAP[kind];
  const packed = nativeCols ? packPayloadForSupabase(rows, nativeCols) : rows;

  // ── Step 2: JS-level deduplication (normalised + quantity-summing for ledger)
  const deduped = deduplicateByConflictKey(kind, packed);
  console.log(`[${kind}] Original batch size: ${packed.length}, Cleaned batch size: ${deduped.length}`);

  // ── Step 3: chunked upsert / insert ────────────────────────────────────────
  const conflictKey = CONFLICT_KEY[kind];

  if (conflictKey) {
    for (let off = 0; off < deduped.length; off += UPSERT_CHUNK_SIZE) {
      const chunk = deduped.slice(off, off + UPSERT_CHUNK_SIZE);
      const { error } = await supabaseServer
        .from(table)
        .upsert(chunk, { onConflict: conflictKey, ignoreDuplicates: false });

      if (error) {
        throw new Error(
          `[${kind}] upsert into ${table} failed: ${error.message}` +
            ` (conflict key: ${conflictKey}, chunk size: ${chunk.length})`,
        );
      }
      if (syncProgress) await bumpSyncProgressMetadata(syncProgress, chunk.length);
    }
  } else {
    for (let off = 0; off < deduped.length; off += UPSERT_CHUNK_SIZE) {
      const chunk = deduped.slice(off, off + UPSERT_CHUNK_SIZE);
      const { error } = await supabaseServer.from(table).insert(chunk);
      if (error) {
        throw new Error(`[${kind}] insert into ${table} failed: ${error.message}`);
      }
      if (syncProgress) await bumpSyncProgressMetadata(syncProgress, chunk.length);
    }
  }

  return deduped.length;
}

/**
 * Removes duplicate rows within a batch using the same composite key that
 * Postgres would use for ON CONFLICT.
 *
 * INVENTORY_LEDGER special behaviour:
 *   • All key fields (fnsku, disposition, location, event_type) are normalised
 *     to trimmed-lowercase before comparison — prevents "Sellable" vs "sellable"
 *     from slipping through as two different keys.
 *   • When a duplicate is found, quantities are SUMMED (additive merge) rather
 *     than last-wins, which matches Amazon ledger semantics where the same
 *     FNSKU can appear multiple times in the same export with partial quantities.
 *
 * REPORTS_REPOSITORY: duplicate natural keys in the same batch merge by SUMMING
 * total_amount (matches pre-upsert dedupe spec).
 *
 * REMOVAL_ORDER: keyed by Wave 1 business line (store + logical line) — within-batch last-wins on same key.
 *
 * All other tables use last-occurrence-wins (standard upsert semantics).
 *
 * This prevents the Postgres error:
 *   "ON CONFLICT DO UPDATE command cannot affect row a second time"
 * which fires when the same conflict-key appears more than once in a single
 * INSERT statement — common with Amazon CSVs that contain duplicates.
 */
function deduplicateByConflictKey(
  kind: SyncKind,
  rows: Record<string, unknown>[],
): Record<string, unknown>[] {
  /** Normalise a scalar field value for safe key comparison. */
  const norm = (v: unknown): string =>
    String(v ?? "").trim().toLowerCase();

  const seen = new Map<string, Record<string, unknown>>();

  for (const row of rows) {
    let key: string;
    switch (kind) {
      case "FBA_RETURNS":
        key = `${norm(row.organization_id)}|${norm(row.lpn)}`;
        break;
      case "REMOVAL_ORDER":
        key = removalBusinessDedupKey(row);
        break;
      case "REMOVAL_SHIPMENT":
        key =
          row.source_staging_id != null && String(row.source_staging_id).trim() !== ""
            ? `sid|${String(row.organization_id)}|${String(row.upload_id)}|${String(row.source_staging_id)}`
            : removalLogicalLineDedupKey(row);
        break;
      case "INVENTORY_LEDGER":
        // Normalise every component of the 5-column unique constraint.
        // Case-insensitive comparison prevents "Sellable" / "SELLABLE" split.
        key = [
          norm(row.organization_id),
          norm(row.fnsku),
          norm(row.disposition),
          norm(row.location),
          norm(row.event_type),
        ].join("|");
        break;
      case "REIMBURSEMENTS":
        key = `${norm(row.organization_id)}|${norm(row.reimbursement_id)}|${norm(row.sku)}`;
        break;
      case "SETTLEMENT":
        key = `${norm(row.organization_id)}|${norm(row.upload_id)}|${norm(row.amazon_line_key)}`;
        break;
      case "SAFET_CLAIMS":
        key = `${norm(row.organization_id)}|${norm(row.safet_claim_id)}`;
        break;
      case "TRANSACTIONS":
        // Fixed key: use source_line_hash (content fingerprint).
        // Old key (org+order_id+tx_type+amount) collapsed distinct rows with the
        // same order+type+amount but different SKU/settlement/date.
        key = `${norm(row.organization_id)}|${String(row.source_line_hash ?? "")}`;
        break;
      case "REPORTS_REPOSITORY":
        key = [
          norm(row.organization_id),
          String(row.date_time ?? ""),
          norm(row.transaction_type),
          norm(row.order_id),
          norm(row.sku),
          norm(row.description),
        ].join("|");
        break;
      // Raw-archive types: all use source_line_hash — guaranteed unique per content
      case "ALL_ORDERS":
      case "REPLACEMENTS":
      case "FBA_GRADE_AND_RESELL":
      case "MANAGE_FBA_INVENTORY":
      case "FBA_INVENTORY":
      case "RESERVED_INVENTORY":
      case "FEE_PREVIEW":
      case "MONTHLY_STORAGE_FEES":
        key = `${norm(row.organization_id)}|${String(row.source_line_hash ?? "")}`;
        break;
      default:
        // UNKNOWN — give every row a unique key so nothing is silently dropped
        key = `${norm(row.organization_id)}|__unknown__|${Math.random()}`;
    }

    if (kind === "INVENTORY_LEDGER" && seen.has(key)) {
      // Additive quantity merge: sum quantities from all duplicate rows so no
      // ledger movement is silently discarded.
      const existing = seen.get(key)!;
      const existingQty =
        typeof existing.quantity === "number" ? existing.quantity : 0;
      const incomingQty =
        typeof row.quantity === "number" ? row.quantity : 0;
      seen.set(key, { ...row, quantity: existingQty + incomingQty });
    } else if (kind === "REPORTS_REPOSITORY" && seen.has(key)) {
      const existing = seen.get(key)!;
      const existingAmt =
        typeof existing.total_amount === "number" ? existing.total_amount : 0;
      const incomingAmt =
        typeof row.total_amount === "number" ? row.total_amount : 0;
      seen.set(key, { ...row, total_amount: existingAmt + incomingAmt });
    } else if (kind === "REMOVAL_ORDER" && seen.has(key)) {
      const existing = seen.get(key)!;
      // With source_staging_id keys, duplicates should not occur; keep last-wins for fallback.
      seen.set(key, { ...existing, ...row });
    } else {
      seen.set(key, row);
    }
  }

  return [...seen.values()];
}
