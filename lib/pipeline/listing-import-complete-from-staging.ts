/**
 * Listing-only: after rows land in `amazon_staging`, archive to `amazon_listing_report_rows_raw`
 * and merge into `catalog_products` in one server path (invoked from Process, not Sync/Generic).
 */

import { mapStagingMappedRowToListingRawInsert } from "../import-listing-physical-lines";
import { syncListingRawRowsToCatalogProducts } from "../import-listing-canonical-sync";
import { applyColumnMappingToRow, packPayloadForSupabase } from "../import-sync-mappers";
import { mergeUploadMetadata, type ImportRunMetrics } from "../raw-report-upload-metadata";
import { supabaseServer } from "../supabase-server";
import { isUuidString } from "../uuid";
import {
  CONFLICT_KEY,
  DOMAIN_TABLE,
  isListingAmazonSyncKind,
  resolveAmazonImportSyncKind,
  type AmazonSyncKind,
} from "./amazon-report-registry";
import { measureBatchUpsertMetrics, type BatchUpsertMetricDelta } from "./amazon-sync-batch-metrics";

const BATCH_SIZE = 500;
const UPSERT_CHUNK_SIZE = 500;
const STAGING_READ_BATCH = 1000;
const STAGING_TABLE = "amazon_staging";

const NATIVE_COLUMNS_LISTING_RAW = new Set([
  "organization_id",
  "store_id",
  "source_upload_id",
  "source_report_type",
  "row_number",
  "source_file_sha256",
  "source_physical_row_number",
  "seller_sku",
  "asin",
  "listing_id",
  "raw_payload",
  "source_line_hash",
  "parse_status",
  "parse_error",
]);

function resolveSourceFileSha256(meta: unknown, uploadId: string): string {
  const m = meta && typeof meta === "object" ? (meta as Record<string, unknown>) : {};
  const s = String(m.content_sha256 ?? "").trim().toLowerCase();
  if (s) return s;
  return `legacy-upload-${uploadId}`;
}

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

function attachPhysicalRowIdentity(
  row: Record<string, unknown> | null,
  stagingRowNumber: number,
  fileSha: string,
): void {
  if (!row) return;
  row.source_file_sha256 = fileSha;
  row.source_physical_row_number = stagingRowNumber;
}

function physicalLineDedupKey(row: Record<string, unknown>): string {
  const norm = (v: unknown): string => String(v ?? "").trim().toLowerCase();
  return `${norm(row.organization_id)}|${String(row.source_file_sha256 ?? "").trim().toLowerCase()}|${String(row.source_physical_row_number ?? "")}`;
}

function dedupeListingBatch(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  const seen = new Map<string, Record<string, unknown>>();
  for (const row of rows) {
    seen.set(physicalLineDedupKey(row), row);
  }
  return [...seen.values()];
}

async function deleteFromStaging(ids: string[], organizationId: string): Promise<void> {
  if (ids.length === 0) return;
  const chunkSize = 200;
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const { error } = await supabaseServer
      .from(STAGING_TABLE)
      .delete()
      .in("id", chunk)
      .eq("organization_id", organizationId);
    if (error) throw new Error(`Staging cleanup failed: ${error.message}`);
  }
}

type SyncProgressOpts = {
  uploadId: string;
  orgId: string;
  totalStagingRows: number;
  upserted: { value: number };
};

async function bumpListingSyncProgress(opts: SyncProgressOpts, chunkRowCount: number): Promise<void> {
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
  const prevMeta = (prevRow as { metadata?: unknown } | null)?.metadata;
  const prevIm =
    prevMeta && typeof prevMeta === "object" && prevMeta !== null && "import_metrics" in prevMeta
      ? { ...((prevMeta as { import_metrics?: Record<string, unknown> }).import_metrics ?? {}) }
      : {};

  await supabaseServer
    .from("raw_report_uploads")
    .update({
      metadata: mergeUploadMetadata(prevMeta, {
        sync_progress: pct,
        etl_phase: "sync",
        import_metrics: {
          ...prevIm,
          current_phase: "sync",
          rows_synced: opts.upserted.value,
          total_staging_rows: opts.totalStagingRows,
        },
      }),
      updated_at: new Date().toISOString(),
    })
    .eq("id", opts.uploadId)
    .eq("organization_id", opts.orgId);

  await supabaseServer.from("file_processing_status").upsert(
    {
      upload_id: opts.uploadId,
      organization_id: opts.orgId,
      status: "syncing",
      current_phase: "sync",
      current_phase_label: "Phase 2 — Raw archive → amazon_listing_report_rows_raw",
      upload_pct: 100,
      process_pct: 100,
      sync_pct: pct,
      phase1_upload_pct: 100,
      phase2_stage_pct: 100,
      phase3_raw_sync_pct: pct,
      phase4_generic_pct: 0,
      processed_rows: opts.upserted.value,
      total_rows: opts.totalStagingRows,
      staged_rows_written: opts.totalStagingRows,
      raw_rows_written: opts.upserted.value,
      import_metrics: {
        current_phase: "sync",
        rows_synced: opts.upserted.value,
        total_staging_rows: opts.totalStagingRows,
      },
    },
    { onConflict: "upload_id" },
  );
}

async function flushListingRawBatch(
  kind: AmazonSyncKind,
  rows: Record<string, unknown>[],
  syncProgress: SyncProgressOpts,
  syncMetricTotals: BatchUpsertMetricDelta,
): Promise<{ flushed: number; collapsedInBatch: number }> {
  if (rows.length === 0) return { flushed: 0, collapsedInBatch: 0 };
  const table = DOMAIN_TABLE[kind];
  if (!table) throw new Error(`[${kind}] listing flush: missing domain table`);
  const packed = packPayloadForSupabase(rows, NATIVE_COLUMNS_LISTING_RAW);
  const deduped = dedupeListingBatch(packed);
  const collapsedInBatch = Math.max(0, packed.length - deduped.length);

  if (deduped.length > 0) {
    const d = await measureBatchUpsertMetrics(
      supabaseServer,
      kind,
      table,
      syncProgress.orgId,
      syncProgress.uploadId,
      deduped,
    );
    syncMetricTotals.rows_synced_new += d.rows_synced_new;
    syncMetricTotals.rows_synced_updated += d.rows_synced_updated;
    syncMetricTotals.rows_synced_unchanged += d.rows_synced_unchanged;
    syncMetricTotals.rows_duplicate_against_existing += d.rows_duplicate_against_existing;
  }

  const conflictKey = CONFLICT_KEY[kind];
  if (!conflictKey) throw new Error(`[${kind}] listing flush: missing CONFLICT_KEY`);

  for (let off = 0; off < deduped.length; off += UPSERT_CHUNK_SIZE) {
    const chunk = deduped.slice(off, off + UPSERT_CHUNK_SIZE);
    const { error } = await supabaseServer
      .from(table)
      .upsert(chunk, { onConflict: conflictKey, ignoreDuplicates: false });
    if (error) {
      throw new Error(`[${kind}] upsert into ${table} failed: ${error.message} (chunk ${chunk.length})`);
    }
    await bumpListingSyncProgress(syncProgress, chunk.length);
  }

  return { flushed: deduped.length, collapsedInBatch };
}

export type ListingImportPhase2Summary = {
  fileRowsTotal: number;
  dataRowsPassed: number;
  rowsStaged: number;
  skippedEmpty: number;
};

/**
 * Reads `amazon_staging` for this upload, writes listing raw rows, merges catalog, clears staging.
 * Caller must have set `raw_report_uploads.status` to `processing` and completed Phase-2 parse * (staging populated), or legacy `staged` rows resumed via Process.
 */
export async function completeListingImportFromStaging(params: {
  uploadId: string;
  orgId: string;
  phase2?: ListingImportPhase2Summary;
}): Promise<void> {
  const { uploadId, orgId, phase2 } = params;

  const { data: row, error: fetchErr } = await supabaseServer
    .from("raw_report_uploads")
    .select("id, organization_id, metadata, report_type, column_mapping")
    .eq("id", uploadId)
    .maybeSingle();

  if (fetchErr || !row) {
    throw new Error("Upload session not found.");
  }

  const reportTypeRaw = String((row as { report_type?: string }).report_type ?? "").trim();
  const kind = resolveAmazonImportSyncKind(reportTypeRaw);
  if (!isListingAmazonSyncKind(kind)) {
    throw new Error(`completeListingImportFromStaging: not a listing kind (${kind})`);
  }

  const columnMapping =
    (row as { column_mapping?: unknown }).column_mapping &&
    typeof (row as { column_mapping?: unknown }).column_mapping === "object" &&
    !Array.isArray((row as { column_mapping?: unknown }).column_mapping)
      ? ((row as { column_mapping?: unknown }).column_mapping as Record<string, string>)
      : null;

  const meta = (row as { metadata?: unknown }).metadata;
  const sourceFileSha256 = resolveSourceFileSha256(meta, uploadId);
  const importStoreId = resolveImportStoreId(meta);

  const { count: stagingRowCount } = await supabaseServer
    .from(STAGING_TABLE)
    .select("*", { count: "exact", head: true })
    .eq("upload_id", uploadId)
    .eq("organization_id", orgId);

  const totalStagingRows = typeof stagingRowCount === "number" ? stagingRowCount : 0;
  if (totalStagingRows <= 0) {
    throw new Error("No staged rows found for this listing import — re-run Process from the mapped upload.");
  }

  const syncUpserted = { value: 0 };
  const syncMetricTotals: BatchUpsertMetricDelta = {
    rows_synced_new: 0,
    rows_synced_updated: 0,
    rows_synced_unchanged: 0,
    rows_duplicate_against_existing: 0,
  };

  await supabaseServer.from("file_processing_status").upsert(
    {
      upload_id: uploadId,
      organization_id: orgId,
      status: "syncing",
      current_phase: "sync",
      current_phase_label: "Phase 2 — Raw archive → amazon_listing_report_rows_raw",
      sync_pct: 0,
      upload_pct: 100,
      process_pct: 100,
      phase1_upload_pct: 100,
      phase2_stage_pct: 100,
      phase3_raw_sync_pct: 0,
      phase4_generic_pct: 0,
      total_rows: totalStagingRows,
      processed_rows: 0,
      staged_rows_written: totalStagingRows,
      phase3_status: "running",
      phase3_started_at: new Date().toISOString(),
    },
    { onConflict: "upload_id" },
  );

  let synced = 0;
  let syncDuplicateInBatchTotal = 0;

  const syncProgress: SyncProgressOpts = {
    uploadId,
    orgId,
    totalStagingRows,
    upserted: syncUpserted,
  };

  while (true) {
    const { data: stagingRows, error: readErr } = await supabaseServer
      .from(STAGING_TABLE)
      .select("id, row_number, raw_row, source_line_hash")
      .eq("upload_id", uploadId)
      .eq("organization_id", orgId)
      .range(0, STAGING_READ_BATCH - 1);

    if (readErr) throw new Error(`Staging read failed: ${readErr.message}`);
    if (!stagingRows || stagingRows.length === 0) break;

    const domainBatch: Record<string, unknown>[] = [];
    const batchStagingIds: string[] = [];

    for (const sr of stagingRows as {
      id: string;
      row_number: number;
      raw_row: Record<string, string>;
      source_line_hash?: string;
    }[]) {
      const rawRow = (sr.raw_row ?? {}) as Record<string, string>;
      const mappedRow = applyColumnMappingToRow(rawRow, columnMapping);
      const insertRow = mapStagingMappedRowToListingRawInsert({
        mappedRow,
        organizationId: orgId,
        storeId: importStoreId,
        sourceUploadId: uploadId,
        sourceReportType: reportTypeRaw,
        fileLineNumber: sr.row_number,
        stagingSourceLineHash: String(sr.source_line_hash ?? ""),
      });
      attachPhysicalRowIdentity(insertRow, sr.row_number, sourceFileSha256);
      domainBatch.push(insertRow);
      batchStagingIds.push(sr.id);

      if (domainBatch.length >= BATCH_SIZE) {
        const inputBatch = domainBatch.splice(0, BATCH_SIZE);
        const stagingIdsForBatch = batchStagingIds.splice(0, inputBatch.length);
        const flushResult = await flushListingRawBatch(kind, inputBatch, syncProgress, syncMetricTotals);
        synced += flushResult.flushed;
        syncDuplicateInBatchTotal += flushResult.collapsedInBatch;
        await deleteFromStaging(stagingIdsForBatch, orgId);
      }
    }

    if (domainBatch.length > 0) {
      const inputBatch = domainBatch.splice(0);
      const stagingIdsForBatch = batchStagingIds.splice(0, inputBatch.length);
      const flushResult = await flushListingRawBatch(kind, inputBatch, syncProgress, syncMetricTotals);
      synced += flushResult.flushed;
      syncDuplicateInBatchTotal += flushResult.collapsedInBatch;
      await deleteFromStaging(stagingIdsForBatch, orgId);
    }

    if (batchStagingIds.length > 0) {
      await deleteFromStaging(batchStagingIds, orgId);
    }
  }

  {
    const { error: cleanupErr } = await supabaseServer
      .from(STAGING_TABLE)
      .delete()
      .eq("upload_id", uploadId)
      .eq("organization_id", orgId);
    if (cleanupErr) {
      console.warn("[listing-complete] final staging cleanup:", cleanupErr.message);
    }
  }

  const syncCollapsedByDedupe = Math.max(0, totalStagingRows - synced);
  const importMetricsRaw: ImportRunMetrics = {
    physical_lines_seen: totalStagingRows,
    data_rows_seen: totalStagingRows,
    rows_staged: totalStagingRows,
    rows_synced_upserted: synced,
    rows_mapper_invalid: 0,
    rows_duplicate_in_file: syncDuplicateInBatchTotal,
    rows_net_collapsed_vs_staging: syncCollapsedByDedupe,
    rows_synced_new: syncMetricTotals.rows_synced_new,
    rows_synced_updated: syncMetricTotals.rows_synced_updated,
    rows_synced_unchanged: syncMetricTotals.rows_synced_unchanged,
    rows_duplicate_against_existing: syncMetricTotals.rows_duplicate_against_existing,
    current_phase: "generic",
  };

  const { data: prevAfterRaw } = await supabaseServer
    .from("raw_report_uploads")
    .select("metadata")
    .eq("id", uploadId)
    .maybeSingle();

  await supabaseServer
    .from("raw_report_uploads")
    .update({
      metadata: mergeUploadMetadata((prevAfterRaw as { metadata?: unknown } | null)?.metadata, {
        row_count: synced,
        staging_row_count: totalStagingRows,
        sync_row_count: synced,
        sync_mapper_null_count: 0,
        sync_collapsed_by_dedupe: syncCollapsedByDedupe,
        sync_duplicate_in_batch_rows: syncDuplicateInBatchTotal,
        import_metrics: importMetricsRaw,
        process_progress: 100,
        sync_progress: 100,
        catalog_listing_import_phase: "raw_archived",
        etl_phase: "generic",
        error_message: "",
        ...(phase2
          ? {
              physical_lines_seen: phase2.fileRowsTotal,
              data_rows_seen: phase2.dataRowsPassed,
              catalog_listing_file_rows_seen: phase2.fileRowsTotal,
              catalog_listing_data_rows_seen: phase2.dataRowsPassed,
              catalog_listing_raw_rows_stored: synced,
            }
          : {}),
      }),
      updated_at: new Date().toISOString(),
    })
    .eq("id", uploadId)
    .eq("organization_id", orgId);

  await supabaseServer.from("file_processing_status").upsert(
    {
      upload_id: uploadId,
      organization_id: orgId,
      status: "processing",
      current_phase: "generic",
      current_phase_label: "Phase 2 — Catalog merge → catalog_products",
      current_target_table: "catalog_products",
      upload_pct: 100,
      process_pct: 100,
      sync_pct: 100,
      phase1_upload_pct: 100,
      phase2_stage_pct: 100,
      phase3_raw_sync_pct: 100,
      phase4_generic_pct: 0,
      processed_rows: totalStagingRows,
      total_rows: totalStagingRows,
      raw_rows_written: synced,
      raw_rows_skipped_existing: syncMetricTotals.rows_duplicate_against_existing,
      staged_rows_written: totalStagingRows,
      phase3_status: "complete",
      phase3_completed_at: new Date().toISOString(),
      phase4_status: "running",
      phase4_started_at: new Date().toISOString(),
      error_message: null,
      import_metrics: importMetricsRaw,
    },
    { onConflict: "upload_id" },
  );

  const sourceFileSha = sourceFileSha256;
  const { count: rawCount } = await supabaseServer
    .from("amazon_listing_report_rows_raw")
    .select("*", { count: "exact", head: true })
    .eq("organization_id", orgId)
    .eq("source_file_sha256", sourceFileSha);
  const storedRawRows = typeof rawCount === "number" ? rawCount : 0;

  const m =
    meta && typeof meta === "object" && !Array.isArray(meta) ? (meta as Record<string, unknown>) : {};
  const fileRowsSeen = Math.max(
    storedRawRows,
    typeof m.catalog_listing_file_rows_seen === "number" ? m.catalog_listing_file_rows_seen : 0,
    phase2?.fileRowsTotal ?? 0,
  );

  const canonicalMetrics = await syncListingRawRowsToCatalogProducts({
    supabase: supabaseServer,
    organizationId: orgId,
    storeId: importStoreId,
    sourceUploadId: uploadId,
    sourceFileSha256: sourceFileSha,
    reportTypeRaw,
    fileRowsSeen,
    storedRawRows,
    progressScale: "full",
    onProgress: async (pct, pass2Done) => {
      await supabaseServer.from("file_processing_status").upsert(
        {
          upload_id: uploadId,
          organization_id: orgId,
          status: "processing",
          current_phase: "generic",
          current_phase_label: "Phase 2 — catalog_products",
          current_target_table: "catalog_products",
          phase4_generic_pct: Math.min(100, Math.max(0, Math.round(pct))),
          generic_rows_written: pass2Done,
          process_pct: pct,
        },
        { onConflict: "upload_id" },
      );
    },
  });

  const { data: prevRowFinal } = await supabaseServer
    .from("raw_report_uploads")
    .select("metadata")
    .eq("id", uploadId)
    .maybeSingle();

  await supabaseServer
    .from("raw_report_uploads")
    .update({
      status: "synced",
      import_pipeline_completed_at: new Date().toISOString(),
      metadata: mergeUploadMetadata((prevRowFinal as { metadata?: unknown } | null)?.metadata, {
        process_progress: 100,
        sync_progress: 100,
        catalog_listing_import_phase: "done",
        catalog_listing_canonical_rows_new: canonicalMetrics.canonical_rows_new,
        catalog_listing_canonical_rows_updated: canonicalMetrics.canonical_rows_updated,
        catalog_listing_canonical_rows_unchanged: canonicalMetrics.canonical_rows_unchanged,
        catalog_listing_canonical_rows_invalid_for_merge: canonicalMetrics.canonical_rows_invalid_for_merge,
        catalog_listing_canonical_rows_inserted: canonicalMetrics.canonical_rows_new,
        catalog_listing_canonical_rows_unchanged_or_merged: canonicalMetrics.canonical_rows_unchanged,
        ...(canonicalMetrics.identifier_map_sync_error
          ? { catalog_listing_identifier_map_sync_error: canonicalMetrics.identifier_map_sync_error }
          : { catalog_listing_identifier_map_sync_error: null }),
        import_metrics: { current_phase: "complete" },
        etl_phase: "complete",
        error_message: "",
      }),
      updated_at: new Date().toISOString(),
    })
    .eq("id", uploadId)
    .eq("organization_id", orgId);

  await supabaseServer.from("file_processing_status").upsert(
    {
      upload_id: uploadId,
      organization_id: orgId,
      status: "complete",
      current_phase: "complete",
      current_phase_label: "Complete",
      current_target_table: "catalog_products",
      upload_pct: 100,
      process_pct: 100,
      sync_pct: 100,
      phase1_upload_pct: 100,
      phase2_stage_pct: 100,
      phase3_raw_sync_pct: 100,
      phase4_generic_pct: 100,
      phase2_status: "complete",
      phase2_completed_at: new Date().toISOString(),
      phase3_status: "complete",
      phase3_completed_at: new Date().toISOString(),
      phase4_status: "complete",
      phase4_completed_at: new Date().toISOString(),
      generic_rows_written: storedRawRows,
      error_message: null,
    },
    { onConflict: "upload_id" },
  );
}
