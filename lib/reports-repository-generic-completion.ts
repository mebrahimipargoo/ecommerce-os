/**
 * Phase 4 for REPORTS_REPOSITORY: non-blocking bookkeeping only.
 * Domain rows are already landed in Phase 3 (physical-line upsert + raw_data).
 */

import type { SupabaseClient } from "@supabase/supabase-js";

import { logAmazonImportEngineEvent } from "./pipeline/amazon-import-engine-log";
import type { AmazonImportEngineConfig } from "./pipeline/amazon-report-registry";
import { mergeUploadMetadata } from "./raw-report-upload-metadata";

export async function completeReportsRepositoryGenericPhase(opts: {
  supabase: SupabaseClient;
  organizationId: string;
  uploadId: string;
  engine: AmazonImportEngineConfig;
  reportTypeRaw: string;
}): Promise<{ domainRowCount: number }> {
  const { supabase, organizationId, uploadId, engine, reportTypeRaw } = opts;

  const { count, error: cntErr } = await supabase
    .from("amazon_reports_repository")
    .select("id", { count: "exact", head: true })
    .eq("organization_id", organizationId)
    .eq("upload_id", uploadId);
  if (cntErr) {
    console.warn(`[reports_repo_generic] count failed (non-fatal): ${cntErr.message}`);
  }
  const domainRowCount = typeof count === "number" ? count : 0;

  const { data: prev } = await supabase
    .from("raw_report_uploads")
    .select("metadata")
    .eq("id", uploadId)
    .maybeSingle();

  const merged = mergeUploadMetadata((prev as { metadata?: unknown } | null)?.metadata, {
    import_metrics: { current_phase: "complete" },
    etl_phase: "complete",
    error_message: "",
  }) as Record<string, unknown>;
  delete merged.failed_phase;

  await supabase
    .from("raw_report_uploads")
    .update({
      status: "synced",
      import_pipeline_completed_at: new Date().toISOString(),
      metadata: merged,
      updated_at: new Date().toISOString(),
    })
    .eq("id", uploadId)
    .eq("organization_id", organizationId);

  const { data: fpsPrior } = await supabase
    .from("file_processing_status")
    .select("total_rows, processed_rows")
    .eq("upload_id", uploadId)
    .maybeSingle();
  const prior = (fpsPrior ?? {}) as { total_rows?: unknown; processed_rows?: unknown };
  const totalRows =
    typeof prior.total_rows === "number" && prior.total_rows > 0 ? Math.floor(prior.total_rows) : domainRowCount;
  const processedRows =
    typeof prior.processed_rows === "number" && prior.processed_rows > 0
      ? Math.floor(prior.processed_rows)
      : domainRowCount;

  await supabase.from("file_processing_status").upsert(
    {
      upload_id: uploadId,
      organization_id: organizationId,
      total_rows: totalRows,
      processed_rows: processedRows,
      status: "complete",
      current_phase: "complete",
      phase_key: "complete",
      phase_label: "Complete",
      next_action_key: null,
      next_action_label: null,
      current_phase_label: "Phase 4 complete — reports repository (no-op enrich)",
      current_target_table: engine.generic_target_table ?? engine.sync_target_table,
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
      generic_rows_written: domainRowCount,
      error_message: null,
    },
    { onConflict: "upload_id" },
  );

  logAmazonImportEngineEvent({
    report_type: reportTypeRaw,
    upload_id: uploadId,
    phase: "complete",
    target_table: engine.generic_target_table ?? "amazon_reports_repository",
    rows_processed: domainRowCount,
  });

  return { domainRowCount };
}
