/**
 * Single source of truth for import History + Universal Importer action gating.
 * Derives next actions from `file_processing_status` phase columns when present,
 * with safe fallbacks to `raw_report_uploads.status` / metadata (no legacy-only gating).
 */

import {
  AMAZON_REPORT_REGISTRY,
  requiresPhase4Generic,
  resolveAmazonImportSyncKind,
  type AmazonSyncKind,
} from "./pipeline/amazon-report-registry";
import { isListingReportType } from "./raw-report-types";

export type ImportFpsSnapshot = {
  phase2_status?: string | null;
  phase3_status?: string | null;
  phase4_status?: string | null;
  current_phase?: string | null;
  current_phase_label?: string | null;
  current_target_table?: string | null;
  /** `file_processing_status.status` */
  row_status?: string | null;
};

export type ImportUiActionInput = {
  reportType: string;
  status: string;
  metadata: Record<string, unknown> | null;
  fps: ImportFpsSnapshot | null | undefined;
  isLedgerSession?: boolean;
};

function norm(s: unknown): string {
  return String(s ?? "")
    .trim()
    .toLowerCase();
}

function phaseComplete(v: unknown): boolean {
  return norm(v) === "complete";
}

function metaObj(m: unknown): Record<string, unknown> | null {
  return m && typeof m === "object" && !Array.isArray(m) ? (m as Record<string, unknown>) : null;
}

export type ImportUiActionState = {
  kind: AmazonSyncKind;
  /** Normalized lifecycle key for badges / colors (may differ from DB when recovering stale `failed`). */
  badgeStatus: string;
  phase2Complete: boolean;
  phase3Complete: boolean;
  phase4Complete: boolean;
  worklistComplete: boolean;
  showSync: boolean;
  showGeneric: boolean;
  showWorklist: boolean;
  showRetryProcess: boolean;
  showRetrySync: boolean;
  showRetryGeneric: boolean;
  /** True while generic route holds the row in `processing` + `etl_phase: generic`. */
  genericInFlight: boolean;
};

export function resolveImportUiActionState(input: ImportUiActionInput): ImportUiActionState {
  const kind = resolveAmazonImportSyncKind(input.reportType);
  const status = norm(input.status);
  const meta = metaObj(input.metadata);
  const fps = input.fps ?? null;
  const isLedger = input.isLedgerSession === true;
  const listing = isListingReportType(input.reportType);

  const etlPhase = norm(meta?.etl_phase);
  const failedPhaseRaw = meta?.failed_phase != null ? String(meta.failed_phase).trim().toLowerCase() : "";
  const worklistCompleted = meta?.worklist_completed === true;

  const p2 = phaseComplete(fps?.phase2_status);
  const p3 = phaseComplete(fps?.phase3_status);
  const p4 = phaseComplete(fps?.phase4_status);

  const catalogDone =
    meta?.catalog_listing_import_phase === "done" || norm(meta?.catalog_listing_import_phase) === "done";

  const phase2Complete =
    p2 ||
    status === "staged" ||
    status === "raw_synced" ||
    status === "synced" ||
    status === "complete" ||
    (status === "processing" &&
      (etlPhase === "sync" || etlPhase === "generic" || etlPhase === "complete" || etlPhase === "worklist"));

  const phase3Complete =
    p3 ||
    status === "raw_synced" ||
    status === "synced" ||
    status === "complete" ||
    (status === "processing" && (etlPhase === "generic" || etlPhase === "complete" || etlPhase === "worklist")) ||
    (status === "failed" && failedPhaseRaw === "generic");

  const needsP4 = requiresPhase4Generic(kind);

  /**
   * Listing “Phase 4” is catalog merge inside the single Process step — done when FPS/metadata/row status agree.
   */
  const listingPhase4Done =
    isListingReportType(input.reportType) &&
    (p4 ||
      (catalogDone && (etlPhase === "complete" || status === "synced" || status === "complete")));

  /** Avoid treating legacy status alone as Phase 4 done (was hiding Generic while the bar showed "complete"). */
  const shipmentPhase4Done =
    kind === "REMOVAL_SHIPMENT" &&
    (p4 || (etlPhase === "complete" && (status === "synced" || status === "complete")));

  const phase4Complete = !needsP4 ? (listing ? listingPhase4Done : true) : shipmentPhase4Done;

  const registry = AMAZON_REPORT_REGISTRY[kind];
  const wantsWorklist = registry.generateWorklistAfterSync === true;

  const genericInFlight = status === "processing" && etlPhase === "generic";

  /**
   * Listing imports: derive Sync / Generic from phase columns (file_processing_status),
   * not legacy raw_report_uploads.status alone — avoids hiding Generic after a Phase 4
   * retry left status `failed` while phase3_status is still complete.
   */
  const removalShipment = kind === "REMOVAL_SHIPMENT";

  /** Listing finishes in Process only; removal shipment still uses Sync after staging. */
  const showSync =
    !isLedger &&
    (removalShipment
      ? !phase3Complete &&
        (status === "staged" || (status === "failed" && failedPhaseRaw === "sync"))
      : status === "staged" || (status === "failed" && failedPhaseRaw === "sync"));

  const showGeneric =
    !isLedger &&
    needsP4 &&
    !phase4Complete &&
    !genericInFlight &&
    (removalShipment ? phase3Complete : status === "raw_synced");

  const showWorklist =
    !isLedger &&
    wantsWorklist &&
    kind === "REMOVAL_ORDER" &&
    (status === "synced" || status === "complete") &&
    phase3Complete &&
    !worklistCompleted;

  const showRetryProcess =
    status === "failed" &&
    !isLedger &&
    (failedPhaseRaw === "process" ||
      (listing && (failedPhaseRaw === "sync" || failedPhaseRaw === "generic")));

  const showRetrySync = status === "failed" && failedPhaseRaw === "sync" && !phase3Complete && !listing;

  /** Phase 4 Generic retry after a failed generic attempt (listing + removal shipment). */
  const showRetryGeneric =
    status === "failed" &&
    failedPhaseRaw === "generic" &&
    needsP4 &&
    !phase4Complete;

  let badgeStatus = input.status;
  if (status === "failed" && needsP4 && phase4Complete) {
    badgeStatus = "synced";
  } else if (
    (listing || removalShipment) &&
    needsP4 &&
    status === "failed" &&
    failedPhaseRaw === "generic" &&
    phase3Complete &&
    !phase4Complete
  ) {
    badgeStatus = "raw_synced";
  } else if (status === "failed" && kind === "REMOVAL_ORDER" && wantsWorklist && worklistCompleted && phase3Complete) {
    badgeStatus = "complete";
  }

  if (wantsWorklist && worklistCompleted && (norm(badgeStatus) === "synced" || norm(badgeStatus) === "complete")) {
    badgeStatus = "complete";
  }

  return {
    kind,
    badgeStatus,
    phase2Complete,
    phase3Complete,
    phase4Complete,
    worklistComplete: worklistCompleted,
    showSync,
    showGeneric,
    showWorklist,
    showRetryProcess,
    showRetrySync,
    showRetryGeneric,
    genericInFlight,
  };
}

/** Universal Importer local phase enum — keep in sync with UniversalImporter.tsx */
export type UniversalImporterPhase =
  | "idle"
  | "uploading"
  | "mapped"
  | "needs_mapping"
  | "unsupported"
  | "processing"
  | "staged"
  | "syncing"
  | "synced"
  | "raw_synced"
  | "genericing"
  | "worklisting"
  | "worklisted"
  | "error";

/**
 * Reconcile top-card phase from DB when the user is not mid-flight on this session.
 * Returns null when local state should own the phase (e.g. uploading) or type is unknown.
 */
export function inferUniversalImporterPhase(input: ImportUiActionInput): UniversalImporterPhase | null {
  const status = norm(input.status);
  const meta = metaObj(input.metadata);
  const etlPhase = norm(meta?.etl_phase);
  const fps = input.fps ?? null;
  const cp = norm(fps?.current_phase);
  const fpsRowStatus = norm(fps?.row_status);
  const kind = resolveAmazonImportSyncKind(input.reportType);
  const wl = meta?.worklist_completed === true;
  const wantsWorklist = AMAZON_REPORT_REGISTRY[kind].generateWorklistAfterSync === true;
  const listing = isListingReportType(input.reportType);
  const failedPhaseRaw = meta?.failed_phase != null ? String(meta.failed_phase).trim().toLowerCase() : "";

  if (status === "pending" || status === "uploading") return null;
  if (status === "needs_mapping") return "needs_mapping";
  if (status === "mapped" || status === "ready" || status === "uploaded") return "mapped";
  if (status === "staged") return "staged";
  if (status === "raw_synced") return "raw_synced";

  if (status === "processing") {
    if (listing) return "processing";
    if (etlPhase === "generic") return "genericing";
    if (etlPhase === "worklist") return "worklisting";
    if (cp === "sync" || fpsRowStatus === "syncing") return "syncing";
    return "processing";
  }

  if (status === "synced" || status === "complete") {
    if (kind === "REMOVAL_ORDER" && wantsWorklist && wl) return "worklisted";
    return "synced";
  }

  if (status === "failed") {
    const st = resolveImportUiActionState(input);
    /** REMOVAL_SHIPMENT: recover UI if FPS/metadata show pipeline done but row stayed `failed`. */
    if (kind === "REMOVAL_SHIPMENT" && st.phase3Complete && st.phase4Complete) {
      return "synced";
    }
    if (
      kind === "REMOVAL_SHIPMENT" &&
      requiresPhase4Generic(kind) &&
      failedPhaseRaw === "generic" &&
      st.phase3Complete &&
      !st.phase4Complete
    ) {
      return "raw_synced";
    }
    if (norm(st.badgeStatus) === "failed") return null;
    return inferUniversalImporterPhase({ ...input, status: st.badgeStatus });
  }

  return null;
}
