/** Single shared `file_processing_status` phase contract for all Amazon imports. */

export const FPS_LABEL_UPLOAD = "Upload raw file";
/** Completed process step (and label while staging is in progress). */
export const FPS_LABEL_PROCESS = "Process → amazon_staging";
export const FPS_LABEL_COMPLETE = "Complete";

/** `next_action_*` button labels — short, consistent across report types. */
export const FPS_NEXT_ACTION_LABEL_PROCESS = "Process";
export const FPS_NEXT_ACTION_LABEL_SYNC = "Sync";
export const FPS_NEXT_ACTION_LABEL_GENERIC = "Generic";

export const FPS_KEY_UPLOAD = "upload";
export const FPS_KEY_PROCESS = "process";
export const FPS_KEY_SYNC = "sync";
export const FPS_KEY_GENERIC = "generic";
export const FPS_KEY_COMPLETE = "complete";
export const FPS_KEY_FAILED = "failed";

export function fpsLabelSync(syncTargetTable: string | null): string {
  return `Sync → ${syncTargetTable ?? "domain"}`;
}

export function fpsLabelGeneric(genericTargetTable: string | null): string {
  return `Generic → ${genericTargetTable ?? "enrichment"}`;
}

export function fpsPctStage2(stagedRowsWritten: number, dataRowsTotal: number): number {
  const d = Math.max(1, dataRowsTotal);
  return Math.min(100, Math.round((stagedRowsWritten / d) * 100));
}

/** phase3_raw_sync_pct = (raw_rows_written + raw_rows_skipped_existing) / staged_rows_written */
export function fpsPctPhase3(
  rawRowsWritten: number,
  rawRowsSkippedExisting: number,
  stagedRowsWritten: number,
): number {
  const d = Math.max(1, stagedRowsWritten);
  return Math.min(100, Math.round(((rawRowsWritten + rawRowsSkippedExisting) / d) * 100));
}

/** phase4_generic_pct = generic_rows_written / rows_eligible_for_generic */
export function fpsPctPhase4(genericRowsWritten: number, rowsEligibleForGeneric: number): number {
  const d = Math.max(1, rowsEligibleForGeneric);
  return Math.min(100, Math.round((genericRowsWritten / d) * 100));
}

export function fpsNextAfterUpload(): "process" {
  return "process";
}

export function fpsNextAfterProcess(): "sync" {
  return "sync";
}

export function fpsNextAfterSync(supportsGeneric: boolean): "generic" | null {
  return supportsGeneric ? "generic" : null;
}

export function fpsNextAfterGeneric(): null {
  return null;
}
