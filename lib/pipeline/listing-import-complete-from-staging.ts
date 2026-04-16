/**
 * Listing Phase 4 only: merge `amazon_listing_report_rows_raw` → `catalog_products`
 * (+ product_identifier_map via canonical sync). Phase 3 raw landing runs in POST /api/settings/imports/sync.
 */

import { syncListingRawRowsToCatalogProducts } from "../import-listing-canonical-sync";
import { mergeUploadMetadata } from "../raw-report-upload-metadata";
import { supabaseServer } from "../supabase-server";
import { isUuidString } from "../uuid";
import {
  FPS_KEY_GENERIC,
  FPS_LABEL_COMPLETE,
  fpsLabelGeneric,
} from "./file-processing-status-contract";
import { isListingAmazonSyncKind, resolveAmazonImportSyncKind } from "./amazon-report-registry";

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

/**
 * POST /api/settings/imports/generic — listing catalog merge after `raw_synced`.
 */
export async function runListingCatalogGenericPhase(params: { uploadId: string; orgId: string }): Promise<void> {
  const { uploadId, orgId } = params;
  const genericTable = "catalog_products";
  const genericPhaseLabel = fpsLabelGeneric(genericTable);

  const { data: row, error: fetchErr } = await supabaseServer
    .from("raw_report_uploads")
    .select("id, organization_id, metadata, report_type")
    .eq("id", uploadId)
    .maybeSingle();

  if (fetchErr || !row) {
    throw new Error("Upload session not found.");
  }

  const reportTypeRaw = String((row as { report_type?: string }).report_type ?? "").trim();
  const kind = resolveAmazonImportSyncKind(reportTypeRaw);
  if (!isListingAmazonSyncKind(kind)) {
    throw new Error(`runListingCatalogGenericPhase: not a listing kind (${kind})`);
  }

  const meta = (row as { metadata?: unknown }).metadata;
  const sourceFileSha256 = resolveSourceFileSha256(meta, uploadId);
  const importStoreId = resolveImportStoreId(meta);

  const { count: rawCount } = await supabaseServer
    .from("amazon_listing_report_rows_raw")
    .select("*", { count: "exact", head: true })
    .eq("organization_id", orgId)
    .eq("source_file_sha256", sourceFileSha256);
  const storedRawRows = typeof rawCount === "number" ? rawCount : 0;

  const m =
    meta && typeof meta === "object" && !Array.isArray(meta) ? (meta as Record<string, unknown>) : {};
  const fileRowsSeen = Math.max(
    storedRawRows,
    typeof m.catalog_listing_file_rows_seen === "number" ? m.catalog_listing_file_rows_seen : 0,
  );

  await supabaseServer.from("file_processing_status").upsert(
    {
      upload_id: uploadId,
      organization_id: orgId,
      status: "processing",
      current_phase: "generic",
      phase_key: FPS_KEY_GENERIC,
      phase_label: genericPhaseLabel,
      current_phase_label: genericPhaseLabel,
      current_target_table: genericTable,
      generic_target_table: genericTable,
      upload_pct: 100,
      process_pct: 100,
      sync_pct: 100,
      phase1_upload_pct: 100,
      phase2_stage_pct: 100,
      phase3_raw_sync_pct: 100,
      phase4_generic_pct: 0,
      rows_eligible_for_generic: storedRawRows,
      phase4_status: "running",
      phase4_started_at: new Date().toISOString(),
    },
    { onConflict: "upload_id" },
  );

  const canonicalMetrics = await syncListingRawRowsToCatalogProducts({
    supabase: supabaseServer,
    organizationId: orgId,
    storeId: importStoreId,
    sourceUploadId: uploadId,
    sourceFileSha256: sourceFileSha256,
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
          phase_key: FPS_KEY_GENERIC,
          phase_label: genericPhaseLabel,
          current_phase_label: genericPhaseLabel,
          current_target_table: genericTable,
          generic_target_table: genericTable,
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
      phase_key: "complete",
      phase_label: FPS_LABEL_COMPLETE,
      current_phase_label: FPS_LABEL_COMPLETE,
      current_target_table: genericTable,
      generic_target_table: genericTable,
      next_action_key: null,
      next_action_label: null,
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
      canonical_rows_new: canonicalMetrics.canonical_rows_new,
      canonical_rows_updated: canonicalMetrics.canonical_rows_updated,
      canonical_rows_unchanged: canonicalMetrics.canonical_rows_unchanged,
      canonical_rows_invalid: canonicalMetrics.canonical_rows_invalid_for_merge,
      error_message: null,
    },
    { onConflict: "upload_id" },
  );
}
