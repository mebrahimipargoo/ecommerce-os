/**
 * Completes INVENTORY_LEDGER Phase 4: product_identifier_map enrich + upload/fps bookkeeping.
 * Shared by POST /api/settings/imports/generic and POST /api/settings/imports/sync (auto-run after Sync).
 */

import type { SupabaseClient } from "@supabase/supabase-js";

import { enrichIdentifierMapFromInventoryLedgerUpload } from "./inventory-ledger-identifier-enrich";
import { logAmazonImportEngineEvent } from "./pipeline/amazon-import-engine-log";
import type { AmazonImportEngineConfig } from "./pipeline/amazon-report-registry";
import { mergeUploadMetadata } from "./raw-report-upload-metadata";

export async function completeInventoryLedgerProductIdentifierMapPhase(opts: {
  supabase: SupabaseClient;
  organizationId: string;
  uploadId: string;
  storeId: string;
  reportTypeRaw: string;
  engine: AmazonImportEngineConfig;
}): Promise<{ enriched: Awaited<ReturnType<typeof enrichIdentifierMapFromInventoryLedgerUpload>>; mapUpserts: number }> {
  const { supabase, organizationId, uploadId, storeId, reportTypeRaw, engine } = opts;

  const enriched = await enrichIdentifierMapFromInventoryLedgerUpload({
    supabase,
    organizationId,
    uploadId,
    storeId,
  });

  const mapUpserts = enriched.ledger_bridge_rows_inserted + enriched.ledger_bridge_rows_enriched;

  const { data: prevInv } = await supabase
    .from("raw_report_uploads")
    .select("metadata")
    .eq("id", uploadId)
    .maybeSingle();

  const mergedInv = mergeUploadMetadata((prevInv as { metadata?: unknown } | null)?.metadata, {
    import_metrics: { current_phase: "complete" },
    etl_phase: "complete",
    error_message: "",
  }) as Record<string, unknown>;
  mergedInv.inventory_ledger_generic_map_upserts = mapUpserts;
  mergedInv.inventory_ledger_identifier_enrich = {
    ledger_bridge_rows_inserted: enriched.ledger_bridge_rows_inserted,
    ledger_bridge_rows_enriched: enriched.ledger_bridge_rows_enriched,
    ledger_rows_resolved_by_fnsku: enriched.ledger_rows_resolved_by_fnsku,
    ledger_rows_resolved_by_sku_asin: enriched.ledger_rows_resolved_by_sku_asin,
    ledger_rows_resolved_sku_only: enriched.ledger_rows_resolved_sku_only,
    ledger_rows_resolved_asin_only: enriched.ledger_rows_resolved_asin_only,
    ledger_rows_resolved_by_fallback:
      enriched.ledger_rows_resolved_by_sku_asin +
      enriched.ledger_rows_resolved_sku_only +
      enriched.ledger_rows_resolved_asin_only,
    unresolved_ambiguous: enriched.unresolved_ambiguous,
    unresolved_insert_failed: enriched.unresolved_insert_failed,
    unresolved_ledger_rows_remaining: enriched.unresolved_ledger_rows_remaining,
  };
  delete mergedInv.failed_phase;

  await supabase
    .from("raw_report_uploads")
    .update({
      status: "synced",
      import_pipeline_completed_at: new Date().toISOString(),
      metadata: mergedInv,
      updated_at: new Date().toISOString(),
    })
    .eq("id", uploadId)
    .eq("organization_id", organizationId);

  await supabase.from("file_processing_status").upsert(
    {
      upload_id: uploadId,
      organization_id: organizationId,
      status: "complete",
      current_phase: "complete",
      phase_key: "complete",
      phase_label: "Complete",
      current_phase_label: "Phase 4 complete — product_identifier_map enrich",
      current_target_table: "product_identifier_map",
      generic_target_table: engine.generic_target_table,
      upload_pct: 100,
      process_pct: 100,
      sync_pct: 100,
      phase1_upload_pct: 100,
      phase2_stage_pct: 100,
      phase3_raw_sync_pct: 100,
      phase4_generic_pct: 100,
      phase4_status: "complete",
      phase4_completed_at: new Date().toISOString(),
      generic_rows_written: mapUpserts,
      error_message: null,
    },
    { onConflict: "upload_id" },
  );

  logAmazonImportEngineEvent({
    report_type: reportTypeRaw,
    upload_id: uploadId,
    phase: "complete",
    target_table: "product_identifier_map",
    rows_processed: enriched.ledger_rows_scanned,
    generic_rows_written: mapUpserts,
  });

  return { enriched, mapUpserts };
}
