/**
 * Operator-facing labels for `file_processing_status.current_phase` and
 * `raw_report_uploads.metadata.import_metrics.current_phase`.
 */
export function formatImportPhaseLabel(phase: string | null | undefined): string {
  const p = String(phase ?? "").trim().toLowerCase();
  switch (p) {
    case "upload":
    case "uploading":
      return "Uploading";
    case "staging":
      return "Staging";
    case "process":
    case "processing":
      return "Processing";
    case "staged":
      return "Staged";
    case "sync":
    case "syncing":
      return "Syncing";
    case "complete":
      return "Complete";
    case "failed":
      return "Failed";
    case "pending":
      return "Pending";
    case "raw_archive":
      return "Staging";
    case "canonical_sync":
      return "Processing";
    case "done":
      return "Complete";
    default:
      return p ? p.charAt(0).toUpperCase() + p.slice(1) : "—";
  }
}
