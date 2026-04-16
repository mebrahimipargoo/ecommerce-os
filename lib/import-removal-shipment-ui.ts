/**
 * REMOVAL_SHIPMENT-only: shared UI state, primary CTA, and progress copy.
 * Does not alter sync/dedupe logic — consumes the same DB/FPS fields as Import History.
 */

import type { ImportUiActionInput, ImportUiActionState } from "./import-ui-action-state";
import { resolveImportUiActionState } from "./import-ui-action-state";
import { resolveAmazonImportSyncKind } from "./pipeline/amazon-report-registry";
import { isListingReportType } from "./raw-report-types";

const RS = "REMOVAL_SHIPMENT";

function norm(s: unknown): string {
  return String(s ?? "")
    .trim()
    .toLowerCase();
}

function num(v: unknown, fallback = 0): number {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return fallback;
}

/**
 * Root cause fix: Universal Importer polled `report_type` before History/DB caught up as REMOVAL_SHIPMENT,
 * so `resolveImportUiActionState` saw UNKNOWN → needsP4 false → showGeneric never true on the top card.
 * History rows used the saved type and showed Generic. Coerce when the session already knows REMOVAL_SHIPMENT.
 */
export function coerceRemovalShipmentReportTypeForResolver(
  dbReportType: string,
  sessionDetectedOrEffectiveType: string | null | undefined,
): string {
  const db = String(dbReportType ?? "").trim();
  const hint = String(sessionDetectedOrEffectiveType ?? "").trim();
  if (db === RS) return RS;
  if (resolveAmazonImportSyncKind(db) === "REMOVAL_SHIPMENT") return RS;
  if ((db === "" || db === "UNKNOWN") && hint === RS) return RS;
  return db || hint || "UNKNOWN";
}

export function buildImportUiActionInputForRemovalShipment(
  input: ImportUiActionInput,
  sessionDetectedType: string | null | undefined,
): ImportUiActionInput {
  const rt = coerceRemovalShipmentReportTypeForResolver(input.reportType, sessionDetectedType);
  if (rt === RS) {
    return { ...input, reportType: RS };
  }
  const db = String(input.reportType ?? "").trim();
  const hint = String(sessionDetectedType ?? "").trim();
  if ((db === "" || db === "UNKNOWN") && hint && isListingReportType(hint)) {
    return { ...input, reportType: hint };
  }
  return input;
}

export function resolveRemovalShipmentUiState(
  input: ImportUiActionInput,
  sessionDetectedType: string | null | undefined,
): ImportUiActionState {
  return resolveImportUiActionState(buildImportUiActionInputForRemovalShipment(input, sessionDetectedType));
}

/** Single “next” action for REMOVAL_SHIPMENT — top card + History row must match. */
export type RemovalShipmentPrimaryCta =
  | "process"
  | "retry_process"
  | "sync"
  | "retry_sync"
  | "generic"
  | "generic_retry"
  | "complete"
  | null;

export function resolveRemovalShipmentPrimaryCta(
  st: ImportUiActionState,
  statusRaw: string,
  isLedger: boolean,
): RemovalShipmentPrimaryCta {
  if (isLedger || st.kind !== "REMOVAL_SHIPMENT") return null;
  const status = norm(statusRaw);

  if (st.genericInFlight) return null;

  if (st.showRetryProcess) return "retry_process";
  if (st.showRetrySync) return "retry_sync";
  if (st.showRetryGeneric) return "generic_retry";
  if (st.showGeneric) return "generic";
  if (st.showSync) return "sync";
  if (["mapped", "ready", "uploaded"].includes(status)) return "process";
  if (st.phase4Complete && (status === "synced" || status === "complete")) return "complete";
  return null;
}

/** Phase copy aligned with product spec (REMOVAL_SHIPMENT only). */
export const REMOVAL_SHIPMENT_UI_LABELS = {
  phase1Title: "Phase 1 — Upload",
  phase1Subtitle: "Upload raw file",
  phase2Title: "Phase 2 — Process",
  phase2Subtitle: "Parse and store physical rows in amazon_staging",
  phase3Title: "Phase 3 — Sync",
  phase3Subtitle: "Raw sync → amazon_removal_shipments",
  phase4Title: "Phase 4 — Generic",
  phase4Subtitle: "Shipment tree / expected_packages enrich",
} as const;

export type RemovalShipmentFpsRow = Record<string, unknown> | null | undefined;

/**
 * Real metrics from file_processing_status + raw_report_uploads.metadata (no invented progress).
 */
export function buildRemovalShipmentProgressModel(
  meta: Record<string, unknown> | null,
  fps: RemovalShipmentFpsRow,
): {
  uploadedBytes: number;
  totalBytes: number;
  uploadPct: number;
  dataRowsTotal: number;
  stagedRowsWritten: number;
  phase2Pct: number;
  rawRowsWritten: number;
  rawRowsSkippedExisting: number;
  syncDenominator: number;
  phase3Pct: number;
  genericRowsWritten: number;
  genericEligible: number;
  phase4Pct: number;
} {
  const m = meta ?? {};
  const f = fps && typeof fps === "object" ? fps : {};

  const uploadedBytes = num(m.uploaded_bytes ?? f.uploaded_bytes, 0);
  const totalBytes = num(m.total_bytes ?? m.file_size_bytes ?? f.total_bytes, 0);
  const upF = num(f.upload_pct, -1);
  const upM = num(m.upload_progress, -1);
  const uploadPct = Math.min(
    100,
    Math.max(
      0,
      upF >= 0 ? upF : upM >= 0 ? upM : totalBytes > 0 ? Math.round((uploadedBytes / totalBytes) * 100) : 0,
    ),
  );

  const dataRowsTotal = Math.max(
    0,
    num(f.data_rows_total, 0) ||
      num(f.file_rows_total, 0) ||
      num(m.data_rows_seen, 0) ||
      num(m.total_rows, 0),
  );
  const stagedRowsWritten = Math.max(
    0,
    num(f.staged_rows_written, 0) ||
      num(f.processed_rows, 0) ||
      num(m.staging_row_count, 0) ||
      num(m.row_count, 0),
  );
  const p2f = num(f.phase2_stage_pct, -1);
  const p2proc = num(f.process_pct, -1);
  const phase2Pct = Math.min(
    100,
    Math.max(
      0,
      p2f >= 0 ? p2f : p2proc >= 0 ? p2proc : dataRowsTotal > 0
        ? Math.round((stagedRowsWritten / dataRowsTotal) * 100)
        : 0,
    ),
  );

  const rawRowsWritten = Math.max(0, num(f.raw_rows_written, 0));
  const rawRowsSkippedExisting = Math.max(0, num(f.raw_rows_skipped_existing, 0));
  const syncDenominator = Math.max(
    1,
    num(f.staged_rows_written, 0) ||
      num(m.staging_row_count, 0) ||
      dataRowsTotal ||
      stagedRowsWritten ||
      1,
  );
  const p3f = num(f.phase3_raw_sync_pct, -1);
  const phase3Pct = Math.min(
    100,
    Math.max(
      0,
      p3f >= 0
        ? p3f
        : Math.round(((rawRowsWritten + rawRowsSkippedExisting) / syncDenominator) * 100),
    ),
  );

  const genericRowsWritten = Math.max(0, num(f.generic_rows_written, 0));
  const eligibleRaw = num(m.removal_shipment_lines_for_generic, 0);
  const genericEligible = Math.max(1, eligibleRaw || syncDenominator);
  const p4f = num(f.phase4_generic_pct, -1);
  const phase4Pct = Math.min(
    100,
    Math.max(0, p4f >= 0 ? p4f : Math.round((genericRowsWritten / genericEligible) * 100)),
  );

  return {
    uploadedBytes,
    totalBytes,
    uploadPct,
    dataRowsTotal,
    stagedRowsWritten,
    phase2Pct,
    rawRowsWritten,
    rawRowsSkippedExisting,
    syncDenominator,
    phase3Pct,
    genericRowsWritten,
    genericEligible,
    phase4Pct,
  };
}

export function formatRemovalShipmentPhase2CompleteMessage(stagedRows: number): string {
  return `Phase 2 complete — ${stagedRows.toLocaleString()} row(s) staged.`;
}

export function formatRemovalShipmentPhase3CompleteMessage(written: number, skipped: number): string {
  if (skipped > 0 && written === 0) {
    return `Phase 3 complete — 0 new shipment line(s) archived, ${skipped.toLocaleString()} skipped (already archived).`;
  }
  if (skipped > 0) {
    return `Phase 3 complete — ${written.toLocaleString()} new shipment line(s) archived, ${skipped.toLocaleString()} skipped (already archived).`;
  }
  return `Phase 3 complete — ${written.toLocaleString()} shipment line(s) archived.`;
}

export function formatRemovalShipmentPhase4PendingMessage(): string {
  return "Phase 4 pending — run Generic to enrich shipment tree / expected_packages.";
}

export function formatRemovalShipmentPhase4CompleteMessage(enriched: number): string {
  return `Phase 4 complete — ${enriched.toLocaleString()} row(s) enriched.`;
}

/**
 * Single badge line for Import History (REMOVAL_SHIPMENT). Returns null → use global status label.
 */
export function resolveRemovalShipmentPhaseBadgeLabel(
  st: ImportUiActionState,
  statusRaw: string,
  isLedger: boolean,
): string | null {
  if (isLedger || st.kind !== "REMOVAL_SHIPMENT") return null;
  const status = norm(statusRaw);
  const cta = resolveRemovalShipmentPrimaryCta(st, statusRaw, isLedger);

  if (cta === "complete") return "Complete";

  if (status === "failed") {
    if (st.showRetryProcess) return "Failed — retry Process";
    if (st.showRetrySync) return "Failed — retry Sync";
    if (st.showRetryGeneric) return "Failed — retry Generic";
    return "Failed";
  }

  if (cta === "retry_process") return "Retry Process";
  if (cta === "process") return "Phase 2: Process";
  if (cta === "retry_sync") return "Retry Sync";
  if (cta === "sync") return "Phase 3: Sync";
  if (cta === "generic_retry") return "Retry Generic (shipments)";
  if (cta === "generic") return "Phase 4: Generic (shipments)";

  return null;
}

/** Top-card status line under progress (REMOVAL_SHIPMENT only). */
export function buildRemovalShipmentTopCardResultMessage(
  st: ImportUiActionState,
  pm: ReturnType<typeof buildRemovalShipmentProgressModel> | null,
): string | null {
  if (st.kind !== "REMOVAL_SHIPMENT" || !pm) return null;

  if (st.phase4Complete) {
    return formatRemovalShipmentPhase4CompleteMessage(pm.genericRowsWritten);
  }

  if (st.phase3Complete && (st.showGeneric || st.showRetryGeneric)) {
    return formatRemovalShipmentPhase4PendingMessage();
  }

  if (st.phase3Complete) {
    return formatRemovalShipmentPhase3CompleteMessage(pm.rawRowsWritten, pm.rawRowsSkippedExisting);
  }

  if (st.phase2Complete) {
    return formatRemovalShipmentPhase2CompleteMessage(pm.stagedRowsWritten);
  }

  return null;
}
