/**
 * LISTING-only: shared phase labels, progress model, primary CTA, and status copy.
 * Top card and Import History must use the same resolvers (single source of truth).
 */

import { isListingAmazonSyncKind, type AmazonSyncKind } from "./pipeline/amazon-report-registry";
import type { ImportUiActionInput, ImportUiActionState } from "./import-ui-action-state";
import { resolveImportUiActionState } from "./import-ui-action-state";
import { isListingReportType } from "./raw-report-types";

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

/** Phase titles aligned with product spec (listing imports only). */
export const LISTING_IMPORT_UI_LABELS = {
  phase1Title: "Phase 1 — Upload raw file",
  phase1Subtitle: "bytes uploaded / file size",
  phase2Title: "Phase 2 — Parse and store physical rows in amazon_staging",
  phase2Subtitle: "staged_rows_written / data_rows_total",
  phase3Title: "Phase 3 — Raw sync → amazon_listing_report_rows_raw",
  phase3Subtitle: "(raw_rows_written + raw_rows_skipped_existing) / staged_rows_written",
  phase4Title: "Phase 4 — Catalog sync → catalog_products",
  phase4Subtitle: "catalog rows processed / rows eligible for catalog sync",
} as const;

export type ListingImportFpsRow = Record<string, unknown> | null | undefined;

/**
 * Real metrics from `file_processing_status` + `raw_report_uploads.metadata` (no invented progress).
 */
export type ListingImportProgressModel = {
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
  phase3Numerator: number;
  genericRowsWritten: number;
  catalogEligible: number;
  phase4Pct: number;
  phase4Numerator: number;
  catalogRowsNew: number;
  catalogRowsUpdated: number;
  catalogRowsUnchanged: number;
  listingRawArchived: number;
  listingRawSkipped: number;
  /** Phase 3 denominator per spec — may be 0 (then UI shows 0%). */
  phase3StagedDenominator: number;
  /** Phase 4 eligible rows — may be 0 (then UI shows 0%). */
  catalogEligibleRows: number;
  phase1PctListing: number;
  phase2PctListing: number;
  phase3PctListing: number;
  phase4PctListing: number;
};

export function buildListingImportProgressModel(
  meta: Record<string, unknown> | null,
  fps: ListingImportFpsRow,
): ListingImportProgressModel {
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
      num(m.catalog_listing_data_rows_seen, 0) ||
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
        : stagedRowsWritten > 0
          ? 100
          : 0,
    ),
  );

  const rawRowsWritten = Math.max(0, num(f.raw_rows_written, 0));
  const rawRowsSkippedExisting = Math.max(0, num(f.raw_rows_skipped_existing, 0));
  const phase3Numerator = rawRowsWritten + rawRowsSkippedExisting;
  const syncDenominator = Math.max(
    1,
    num(f.staged_rows_written, 0) ||
      num(m.staging_row_count, 0) ||
      dataRowsTotal ||
      stagedRowsWritten ||
      1,
  );
  const phase3StagedDenominator = Math.max(
    0,
    num(f.staged_rows_written, 0) || num(m.staging_row_count, 0),
  );
  const p3f = num(f.phase3_raw_sync_pct, -1);
  const phase3Pct = Math.min(
    100,
    Math.max(
      0,
      p3f >= 0 ? p3f : Math.round((phase3Numerator / syncDenominator) * 100),
    ),
  );

  const genericRowsWritten = Math.max(0, num(f.generic_rows_written, 0));
  const catalogEligible = Math.max(
    1,
    syncDenominator,
    num(m.catalog_listing_file_rows_seen, 0),
    num(f.total_rows, 0),
  );
  const catEligMeta = num(m.catalog_listing_file_rows_seen, -1);
  const catalogEligibleRows =
    catEligMeta >= 0 ? Math.max(0, catEligMeta) : phase3StagedDenominator;

  const im = m.import_metrics as {
    rows_synced_new?: unknown;
    rows_synced_updated?: unknown;
    rows_duplicate_against_existing?: unknown;
  } | undefined;
  const immNew = num(im?.rows_synced_new, -1);
  const immUpd = num(im?.rows_synced_updated, -1);
  const immSkip = num(im?.rows_duplicate_against_existing, -1);
  /** Prefer post-sync import_metrics split when present; else FPS counters. */
  const listingRawArchived = immNew >= 0 && immUpd >= 0 ? immNew + immUpd : rawRowsWritten;
  const listingRawSkipped = immSkip >= 0 ? immSkip : rawRowsSkippedExisting;

  const cn = num(m.catalog_listing_canonical_rows_new ?? m.catalog_listing_canonical_rows_inserted, -1);
  const cu = num(m.catalog_listing_canonical_rows_updated, -1);
  const cunch = num(
    m.catalog_listing_canonical_rows_unchanged ?? m.catalog_listing_canonical_rows_unchanged_or_merged,
    -1,
  );
  const catalogRowsNew = cn >= 0 ? cn : 0;
  const catalogRowsUpdated = cu >= 0 ? cu : 0;
  const catalogRowsUnchanged = cunch >= 0 ? cunch : 0;
  const catalogSum =
    cn >= 0 && cu >= 0 && cunch >= 0 ? catalogRowsNew + catalogRowsUpdated + catalogRowsUnchanged : -1;

  const phase4Numerator = catalogSum >= 0 ? catalogSum : genericRowsWritten;
  const p4f = num(f.phase4_generic_pct, -1);
  const phase4Pct = Math.min(
    100,
    Math.max(0, p4f >= 0 ? p4f : Math.round((phase4Numerator / catalogEligible) * 100)),
  );

  const phase1PctListing = totalBytes <= 0 ? 0 : uploadPct;
  const phase2PctListing = dataRowsTotal <= 0 ? 0 : phase2Pct;
  const phase3PctListing =
    phase3StagedDenominator <= 0
      ? 0
      : Math.min(100, Math.round((phase3Numerator / phase3StagedDenominator) * 100));
  const phase4PctListing =
    catalogEligibleRows <= 0
      ? 0
      : Math.min(100, Math.round((phase4Numerator / catalogEligibleRows) * 100));

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
    phase3Numerator,
    genericRowsWritten,
    catalogEligible,
    phase4Pct,
    phase4Numerator,
    catalogRowsNew,
    catalogRowsUpdated,
    catalogRowsUnchanged,
    listingRawArchived,
    listingRawSkipped,
    phase3StagedDenominator,
    catalogEligibleRows,
    phase1PctListing,
    phase2PctListing,
    phase3PctListing,
    phase4PctListing,
  };
}

export type ListingPrimaryCta =
  | "process"
  | "retry_process"
  | "sync"
  | "retry_sync"
  | "generic_catalog"
  | "retry_generic_catalog"
  | "complete"
  | null;

/** Optional client flags so History + top card stay aligned during in-flight requests. */
export type ListingImportUiClient = {
  localPhase?: string | null;
  isProcessing?: boolean;
  isSyncing?: boolean;
  isGenericing?: boolean;
  isUploading?: boolean;
};

export type ListingImportUiInput = ImportUiActionInput & { client?: ListingImportUiClient };

export type ListingImportCurrentPhase = 1 | 2 | 3 | 4 | "complete" | "failed";

export type ListingImportUiState = {
  currentPhase: ListingImportCurrentPhase;
  nextAction: ListingPrimaryCta;
  topCardButtonLabel: string;
  rowActionLabel: string;
  phaseBadgeText: string;
  /** Newline-separated lines for the top card summary. */
  phaseSummaryText: string;
  phase1Pct: number;
  phase2Pct: number;
  phase3Pct: number;
  phase4Pct: number;
  isComplete: boolean;
  isFailed: boolean;
  busyAction: null | "process" | "sync" | "generic";
  showProcessCta: boolean;
  showSyncCta: boolean;
  showGenericCta: boolean;
  progress: ListingImportProgressModel;
  /** One compact line for Import History “Rows” column. */
  rowMetricsLine: string;
};

function deriveListingNextAction(st: ImportUiActionState, statusRaw: string): ListingPrimaryCta {
  const status = norm(statusRaw);
  if (st.phase4Complete && (status === "synced" || status === "complete")) return "complete";
  if (st.showRetryProcess) return "retry_process";
  if (st.showRetrySync) return "retry_sync";
  if (st.showRetryGeneric) return "retry_generic_catalog";
  if (st.showGeneric) return "generic_catalog";
  if (st.showSync) return "sync";
  if (["mapped", "ready", "uploaded", "needs_mapping"].includes(status)) return "process";
  return null;
}

function applyLocalPhaseFallback(
  next: ListingPrimaryCta,
  statusRaw: string,
  client: ListingImportUiClient | undefined,
): ListingPrimaryCta {
  if (next != null || !client?.localPhase) return next;
  const lp = norm(client.localPhase);
  const status = norm(statusRaw);
  if (
    (lp === "mapped" || lp === "needs_mapping") &&
    ["mapped", "ready", "uploaded", "needs_mapping"].includes(status)
  ) {
    return "process";
  }
  if (lp === "staged" && status === "staged") return "sync";
  if (lp === "raw_synced" && (status === "raw_synced" || status === "synced")) return "generic_catalog";
  return next;
}

function deriveBusyAction(
  st: ImportUiActionState,
  statusRaw: string,
  meta: Record<string, unknown>,
  client: ListingImportUiClient | undefined,
): null | "process" | "sync" | "generic" {
  const status = norm(statusRaw);
  const etl = norm(meta.etl_phase);
  if (client?.isGenericing || st.genericInFlight) return "generic";
  if (client?.isSyncing || (status === "processing" && etl === "sync")) return "sync";
  if (
    client?.isProcessing ||
    (status === "processing" && etl !== "sync" && etl !== "generic" && etl !== "complete" && etl !== "worklist")
  ) {
    return "process";
  }
  return null;
}

function buildListingRowMetricsLine(
  pm: ListingImportProgressModel,
  st: ImportUiActionState,
  meta: Record<string, unknown>,
): string {
  const parts = [
    `staged ${pm.stagedRowsWritten.toLocaleString()}`,
    `raw new ${pm.listingRawArchived.toLocaleString()}`,
    `raw skipped ${pm.listingRawSkipped.toLocaleString()}`,
  ];
  if (st.phase3Complete) {
    parts.push(
      `catalog new ${pm.catalogRowsNew.toLocaleString()}`,
      `updated ${pm.catalogRowsUpdated.toLocaleString()}`,
      `unchanged ${pm.catalogRowsUnchanged.toLocaleString()}`,
    );
  }
  const im = meta.import_metrics as { rows_duplicate_against_existing?: unknown } | undefined;
  const dupD = num(im?.rows_duplicate_against_existing, -1);
  if (dupD >= 0) parts.push(`duplicates skipped ${dupD.toLocaleString()}`);
  return parts.join(" · ");
}

/**
 * Single resolver for LISTING: top upload card + Import History row (same state, no duplicated rules).
 */
export function resolveListingImportUiState(input: ListingImportUiInput): ListingImportUiState | null {
  if (input.isLedgerSession || !isListingReportType(input.reportType)) return null;
  const st = resolveImportUiActionState(input);
  if (!isListingAmazonSyncKind(st.kind as AmazonSyncKind)) return null;

  const meta = input.metadata && typeof input.metadata === "object" && !Array.isArray(input.metadata)
    ? (input.metadata as Record<string, unknown>)
    : {};
  const client = input.client;
  const pm = buildListingImportProgressModel(input.metadata, input.fps);
  const statusRaw = input.status;
  const status = norm(statusRaw);

  let next = deriveListingNextAction(st, statusRaw);
  next = applyLocalPhaseFallback(next, statusRaw, client);
  const busyAction = deriveBusyAction(st, statusRaw, meta, client);

  const isFailed = status === "failed";
  const isComplete =
    st.phase4Complete && (status === "synced" || status === "complete");

  let currentPhase: ListingImportCurrentPhase = 1;
  if (isFailed) currentPhase = "failed";
  else if (isComplete) currentPhase = "complete";
  else if (["pending", "uploading"].includes(status)) currentPhase = 1;
  else if (!st.phase2Complete) currentPhase = 2;
  else if (!st.phase3Complete) currentPhase = 3;
  else if (!st.phase4Complete) currentPhase = 4;
  else currentPhase = "complete";

  const showProcessCta =
    next === "process" ||
    next === "retry_process" ||
    busyAction === "process";
  const showSyncCta = next === "sync" || next === "retry_sync" || busyAction === "sync";
  const showGenericCta =
    next === "generic_catalog" ||
    next === "retry_generic_catalog" ||
    busyAction === "generic";

  let topCardButtonLabel = "";
  if (busyAction === "generic") topCardButtonLabel = "Generic (catalog)…";
  else if (busyAction === "sync") topCardButtonLabel = "Syncing…";
  else if (busyAction === "process") topCardButtonLabel = "Processing…";
  else if (next === "process") topCardButtonLabel = "Process Data";
  else if (next === "retry_process") topCardButtonLabel = "Retry Process";
  else if (next === "sync") topCardButtonLabel = "Sync";
  else if (next === "retry_sync") topCardButtonLabel = "Retry Sync";
  else if (next === "generic_catalog") topCardButtonLabel = "Generic (catalog)";
  else if (next === "retry_generic_catalog") topCardButtonLabel = "Retry Generic (catalog)";
  else if (next === "complete") topCardButtonLabel = "Complete";

  const rowActionLabel = topCardButtonLabel;

  let phaseBadgeText = "Listing import";
  if (isComplete) phaseBadgeText = "Complete";
  else if (isFailed) {
    if (st.showRetryProcess) phaseBadgeText = "Failed — retry Process";
    else if (st.showRetrySync) phaseBadgeText = "Failed — retry Sync";
    else if (st.showRetryGeneric) phaseBadgeText = "Failed — retry Generic (catalog)";
    else phaseBadgeText = "Failed";
  } else if (busyAction === "generic" || next === "generic_catalog" || next === "retry_generic_catalog") {
    phaseBadgeText =
      next === "retry_generic_catalog" || st.showRetryGeneric
        ? "Phase 4: Retry Generic (catalog)"
        : "Phase 4: Generic (catalog)";
  } else if (busyAction === "sync" || next === "sync" || next === "retry_sync") {
    phaseBadgeText = next === "retry_sync" ? "Phase 3: Retry Sync" : "Phase 3: Sync";
  } else if (busyAction === "process" || next === "process" || next === "retry_process") {
    phaseBadgeText = next === "retry_process" ? "Phase 2: Retry Process" : "Phase 2: Process";
  } else if (["pending", "uploading"].includes(status)) phaseBadgeText = "Phase 1: Upload";

  const phase2Line = formatListingPhase2CompleteMessage(pm.stagedRowsWritten);
  const phase3Line = `Phase 3 complete — raw archived ${pm.listingRawArchived.toLocaleString()}, skipped existing ${pm.listingRawSkipped.toLocaleString()}.`;
  const phase4PendingLine = "Phase 4 pending — run Generic (catalog) to sync catalog_products.";
  const phase4DoneLine = `Phase 4 complete — new ${pm.catalogRowsNew.toLocaleString()}, updated ${pm.catalogRowsUpdated.toLocaleString()}, unchanged ${pm.catalogRowsUnchanged.toLocaleString()}.`;

  let phaseSummaryText = "";
  if (isComplete) phaseSummaryText = phase4DoneLine;
  else if (isFailed) phaseSummaryText = phaseBadgeText;
  else if (st.phase4Complete) phaseSummaryText = phase4DoneLine;
  else if (st.phase3Complete && (st.showGeneric || st.showRetryGeneric)) phaseSummaryText = phase4PendingLine;
  else if (st.phase3Complete) phaseSummaryText = phase3Line;
  else if (st.phase2Complete) phaseSummaryText = phase2Line;

  const rowMetricsLine = buildListingRowMetricsLine(pm, st, meta);

  return {
    currentPhase,
    nextAction: next,
    topCardButtonLabel,
    rowActionLabel,
    phaseBadgeText,
    phaseSummaryText,
    phase1Pct: pm.phase1PctListing,
    phase2Pct: pm.phase2PctListing,
    phase3Pct: pm.phase3PctListing,
    phase4Pct: pm.phase4PctListing,
    isComplete,
    isFailed,
    busyAction,
    showProcessCta,
    showSyncCta,
    showGenericCta,
    progress: pm,
    rowMetricsLine,
  };
}

export function formatListingPhase2CompleteMessage(stagedRows: number): string {
  return `Phase 2 complete — ${stagedRows.toLocaleString()} row(s) staged.`;
}

export function formatListingPhase3CompleteMessage(
  rawArchived: number,
  rawSkippedExisting: number,
  stagingLinesProcessed: number,
): string {
  if (rawSkippedExisting > 0 && rawArchived === 0) {
    return `Phase 3 complete — 0 new raw row(s) archived, ${rawSkippedExisting.toLocaleString()} skipped (already archived).`;
  }
  if (rawSkippedExisting > 0) {
    return `Phase 3 complete — ${rawArchived.toLocaleString()} new raw row(s) archived, ${rawSkippedExisting.toLocaleString()} skipped (already archived).`;
  }
  if (stagingLinesProcessed > 0) {
    return `Phase 3 complete — ${rawArchived.toLocaleString()} raw row(s) archived (${stagingLinesProcessed.toLocaleString()} line(s) processed).`;
  }
  return `Phase 3 complete — ${rawArchived.toLocaleString()} raw row(s) archived.`;
}

export function formatListingPhase4PendingMessage(): string {
  return "Phase 4 pending — run Generic (catalog) to sync catalog_products.";
}

export function formatListingPhase4CompleteMessage(newRows: number, updated: number, unchanged: number): string {
  return `Phase 4 complete — ${newRows.toLocaleString()} new, ${updated.toLocaleString()} updated, ${unchanged.toLocaleString()} unchanged.`;
}

/** Badge / status chip for Import History (listing) — same resolver as top card. */
export function resolveListingStatusBadgeLabel(input: ListingImportUiInput): string | null {
  const ui = resolveListingImportUiState(input);
  return ui?.phaseBadgeText ?? null;
}

/** Top-card summary (listing) — newline-separated if extended later. */
export function buildListingTopCardResultMessage(input: ListingImportUiInput): string | null {
  const t = resolveListingImportUiState(input)?.phaseSummaryText?.trim();
  return t ? t : null;
}
