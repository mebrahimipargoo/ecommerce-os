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

/** Copy for listing pipeline (single Process on the server; UI shows logical sub-steps). */
export const LISTING_IMPORT_UI_LABELS = {
  phase1Title: "Phase 1 — Upload raw file",
  phase1Subtitle: "bytes uploaded / file size",
  phase2Title: "Phase 2 — Process listing import",
  phase2Subtitle: "raw archive + canonical catalog",
  phase3Title: "",
  phase3Subtitle: "",
  phase4Title: "",
  phase4Subtitle: "",
} as const;

export type ListingPipelineStepKey = "upload" | "mapping" | "staging" | "raw_archive" | "catalog";

export type ListingPipelineStepUi = {
  key: ListingPipelineStepKey;
  title: string;
  subtitle: string;
  /** 0–100 width for the progress bar */
  pct: number;
  /** Primary right-hand summary, e.g. "1.2 / 8.0 MB (15%)" or "420 / 1,200 rows" */
  rightLabel: string;
  /** Optional second line (e.g. target table during generic) */
  subLabel?: string;
  /** Visual state for the step row */
  tone: "upcoming" | "active" | "done" | "warning";
};

export function formatListingBytes(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return "—";
  if (n < 1024) return `${Math.round(n)} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

/**
 * Five-step listing pipeline view model — same source for top card + history column.
 * Progress is driven only by `metadata`, `file_processing_status`, and row `status` (no fake jumps).
 */
export function buildListingPipelineSteps(opts: {
  status: string;
  metadata: Record<string, unknown> | null | undefined;
  fps: Record<string, unknown> | null | undefined;
  /** Browser upload progress0–100 when DB row not updated yet */
  localUploadPct?: number;
  /** Selected file size during client upload */
  localFileSizeBytes?: number;
}): ListingPipelineStepUi[] {
  const st = norm(opts.status);
  const m = opts.metadata && typeof opts.metadata === "object" && !Array.isArray(opts.metadata)
    ? opts.metadata
    : {};
  const f = opts.fps && typeof opts.fps === "object" ? opts.fps : {};

  const totalBytes = Math.max(
    0,
    num(m.total_bytes ?? m.file_size_bytes ?? f.total_bytes ?? opts.localFileSizeBytes, 0),
  );
  const uploadedBytes = Math.max(0, num(m.uploaded_bytes ?? f.uploaded_bytes, 0));
  const upProgMeta = num(m.upload_progress, -1);
  const upProgFps = num(f.upload_pct, -1);
  const localUp = num(opts.localUploadPct, -1);
  let uploadPct = 0;
  if (st === "pending" || st === "uploading") {
    if (upProgFps >= 0) uploadPct = Math.min(100, Math.round(upProgFps));
    else if (upProgMeta >= 0) uploadPct = Math.min(100, Math.round(upProgMeta));
    else if (localUp >= 0) uploadPct = Math.min(100, Math.round(localUp));
    else if (totalBytes > 0 && uploadedBytes > 0) uploadPct = Math.min(100, Math.round((uploadedBytes / totalBytes) * 100));
  } else {
    uploadPct = 100;
  }
  const uploadDone = st !== "pending" && st !== "uploading";
  const uploadActive = !uploadDone;
  const uploadRight =
    totalBytes > 0
      ? `${formatListingBytes(uploadedBytes)} / ${formatListingBytes(totalBytes)} (${uploadPct}%)`
      : `${uploadPct}%`;

  const p2s = norm(f.phase2_status);
  const p3s = norm(f.phase3_status);
  const p4s = norm(f.phase4_status);
  const curPhase = norm(f.current_phase ?? m.etl_phase);
  const im = m.import_metrics as { current_phase?: string; rows_synced?: number; total_staging_rows?: number } | undefined;
  const imPhase = norm(im?.current_phase);

  const dataRowsTotal = Math.max(
    0,
    num(f.data_rows_total, 0) ||
      num(f.file_rows_total, 0) ||
      num(m.data_rows_seen, 0) ||
      num(m.catalog_listing_data_rows_seen, 0) ||
      num(m.total_rows, 0),
  );
  const stagedRows = Math.max(
    0,
    num(f.staged_rows_written, 0) ||
      num(f.processed_rows, 0) ||
      num(m.staging_row_count, 0),
  );
  const p2f = num(f.phase2_stage_pct, -1);
  const p2proc = num(f.process_pct, -1);
  let stagingPct = 0;
  if (p2f >= 0) stagingPct = Math.min(100, Math.round(p2f));
  else if (p2proc >= 0 && (curPhase === "staging" || imPhase === "staging")) stagingPct = Math.min(100, Math.round(p2proc));
  else if (dataRowsTotal > 0 && stagedRows > 0) stagingPct = Math.min(100, Math.round((stagedRows / dataRowsTotal) * 100));

  const totalStaging = Math.max(1, num(f.staged_rows_written, 0) || num(m.staging_row_count, 0) || dataRowsTotal || 1);
  const rowsSynced = num(im?.rows_synced, -1);
  const totalStgMeta = num(im?.total_staging_rows, -1);
  const rawWritten = Math.max(0, num(f.raw_rows_written, 0));
  const rawSkipped = Math.max(0, num(f.raw_rows_skipped_existing, 0));

  const lipEarly = norm(m.catalog_listing_import_phase);
  const stagingComplete =
    p2s === "complete" ||
    p3s === "running" ||
    p3s === "complete" ||
    curPhase === "sync" ||
    curPhase === "generic" ||
    curPhase === "raw_synced" ||
    curPhase === "complete" ||
    rawWritten > 0 ||
    lipEarly === "raw_archived" ||
    lipEarly === "done";
  const stagingActive =
    curPhase === "staging" ||
    imPhase === "staging" ||
    (st === "processing" && !stagingComplete && (p2s === "running" || p2s === ""));
  const p3f = num(f.phase3_raw_sync_pct, -1);
  const syncProgMeta = num(m.sync_progress, -1);
  const syncPctFps = num(f.sync_pct, -1);
  let rawPct = 0;
  if (p3f >= 0) rawPct = Math.min(100, Math.round(p3f));
  else if (syncPctFps >= 0 && curPhase === "sync") rawPct = Math.min(100, Math.round(syncPctFps));
  else if (syncProgMeta >= 0) rawPct = Math.min(100, Math.round(syncProgMeta));
  else if (totalStaging > 0 && rawWritten + rawSkipped > 0) {
    rawPct = Math.min(100, Math.round(((rawWritten + rawSkipped) / totalStaging) * 100));
  }
  const lip = lipEarly;
  const rawComplete =
    p3s === "complete" ||
    curPhase === "generic" ||
    curPhase === "complete" ||
    lip === "raw_archived" ||
    lip === "done";
  const rawActive =
    curPhase === "sync" ||
    imPhase === "sync" ||
    (st === "processing" &&
      (norm(m.etl_phase) === "sync" || (!rawComplete && stagingComplete && curPhase !== "staging")));

  const genPct = Math.min(100, Math.max(0, num(f.phase4_generic_pct, -1) >= 0 ? Math.round(num(f.phase4_generic_pct, 0)) : num(m.process_progress, 0)));
  const genRows = Math.max(0, num(f.generic_rows_written, 0));
  const catEligible = Math.max(1, num(m.catalog_listing_file_rows_seen, 0) || totalStaging);
  const targetTable =
    typeof f.current_target_table === "string" && f.current_target_table.trim() !== ""
      ? f.current_target_table.trim()
      : "catalog_products";
  const catalogComplete = lip === "done" || p4s === "complete" || st === "synced" || st === "complete";
  const catalogActive =
    curPhase === "generic" ||
    norm(m.etl_phase) === "generic" ||
    (st === "processing" && !catalogComplete && rawComplete);

  const catalogPct =
    num(f.phase4_generic_pct, -1) >= 0
      ? Math.min(100, Math.round(num(f.phase4_generic_pct, 0)))
      : catalogComplete
        ? 100
        : genRows > 0 && catEligible > 0
          ? Math.min(100, Math.round((genRows / catEligible) * 100))
          : genPct;

  const mappingNeedsAttention = st === "needs_mapping";
  const mappingUpcoming = st === "pending" || st === "uploading";

  const rawRightLabel = (() => {
    if (rawComplete) {
      const touched = rawWritten + rawSkipped;
      return touched > 0
        ? `${touched.toLocaleString()} rows · done`
        : "Done";
    }
    if (!rawActive) return "—";
    if (rowsSynced >= 0 && totalStgMeta >= 0) {
      return `${rowsSynced.toLocaleString()} / ${totalStgMeta.toLocaleString()} rows · ${rawPct}%`;
    }
    return `${rawPct}% · ${(rawWritten + rawSkipped).toLocaleString()} / ${totalStaging.toLocaleString()} lines`;
  })();

  const catalogRightLabel = (() => {
    if (catalogComplete) {
      const cn = num(m.catalog_listing_canonical_rows_new ?? m.catalog_listing_canonical_rows_inserted, 0);
      const cu = num(m.catalog_listing_canonical_rows_updated, 0);
      const cunch = num(
        m.catalog_listing_canonical_rows_unchanged ?? m.catalog_listing_canonical_rows_unchanged_or_merged,
        0,
      );
      if (cn + cu + cunch > 0) {
        return `new ${cn.toLocaleString()} · upd ${cu.toLocaleString()} · same ${cunch.toLocaleString()}`;
      }
      return "Done";
    }
    if (!catalogActive) return "—";
    return `${genRows.toLocaleString()} / ~${catEligible.toLocaleString()} rows · ${catalogPct}%`;
  })();

  const steps: ListingPipelineStepUi[] = [
    {
      key: "upload",
      title: "Upload",
      subtitle: "Copy file to storage",
      pct: uploadActive ? Math.max(3, uploadPct) : 100,
      rightLabel: uploadRight,
      tone: uploadActive ? "active" : "done",
    },
    {
      key: "mapping",
      title: "Map & classify",
      subtitle: "Report type + column mapping",
      pct: mappingNeedsAttention ? 100 : mappingUpcoming ? 8 : 100,
      rightLabel: mappingNeedsAttention
        ? "Review required"
        : mappingUpcoming
          ? "Waiting…"
          : "Ready",
      tone: mappingNeedsAttention ? "warning" : mappingUpcoming ? "upcoming" : "done",
    },
    {
      key: "staging",
      title: "Process — staging",
      subtitle: "amazon_staging",
      pct: stagingComplete ? 100 : stagingActive ? Math.max(4, stagingPct) : 0,
      rightLabel:
        dataRowsTotal > 0
          ? `${stagedRows.toLocaleString()} / ${dataRowsTotal.toLocaleString()} rows · ${stagingComplete ? 100 : stagingPct}%`
          : stagedRows > 0
            ? `${stagedRows.toLocaleString()} rows`
            : stagingComplete
              ? "Done"
              : "—",
      tone: stagingActive ? "active" : stagingComplete || stagedRows > 0 ? "done" : "upcoming",
    },
    {
      key: "raw_archive",
      title: "Sync — raw archive",
      subtitle: "amazon_listing_report_rows_raw",
      pct: rawComplete ? 100 : rawActive ? Math.max(4, rawPct) : 0,
      rightLabel: rawRightLabel,
      tone: rawActive ? "active" : rawComplete ? "done" : "upcoming",
    },
    {
      key: "catalog",
      title: "Generic — catalog",
      subtitle: "Merge into canonical snapshot",
      pct: catalogComplete ? 100 : catalogActive ? Math.max(4, catalogPct) : 0,
      rightLabel: catalogRightLabel,
      subLabel: catalogActive || catalogComplete ? `Target table: ${targetTable}` : undefined,
      tone: catalogActive ? "active" : catalogComplete ? "done" : "upcoming",
    },
  ];

  // Fix mapping step logic: "mappingDone" was wrong — re-evaluate tones
  steps[1].tone = mappingNeedsAttention ? "warning" : !uploadDone ? "upcoming" : "done";
  steps[1].pct = mappingNeedsAttention ? 100 : !uploadDone ? 5 : 100;
  steps[1].rightLabel = mappingNeedsAttention ? "Open Map Columns" : !uploadDone ? "After upload" : "Mapped";

  return steps;
}

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
  phase3StagedDenominator: number;
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

export type ListingImportCurrentPhase = 1 | 2 | "complete" | "failed";

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
  busyAction: null | "process";
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
  if (["mapped", "ready", "uploaded", "needs_mapping", "staged"].includes(status)) return "process";
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
  if (lp === "staged" && status === "staged") return "process";
  if (lp === "synced" && (status === "synced" || status === "complete")) return "complete";
  return next;
}

function deriveBusyAction(
  st: ImportUiActionState,
  statusRaw: string,
  client: ListingImportUiClient | undefined,
): null | "process" {
  const status = norm(statusRaw);
  if (client?.isProcessing || client?.isSyncing || client?.isGenericing) return "process";
  if (status === "processing") return "process";
  if (st.genericInFlight) return "process";
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
  if (st.phase4Complete) {
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
  const busyAction = deriveBusyAction(st, statusRaw, client);

  const isFailed = status === "failed";
  const isComplete =
    st.phase4Complete && (status === "synced" || status === "complete");

  let currentPhase: ListingImportCurrentPhase = 1;
  if (isFailed) currentPhase = "failed";
  else if (isComplete) currentPhase = "complete";
  else if (["pending", "uploading"].includes(status)) currentPhase = 1;
  else currentPhase = 2;

  const showProcessCta =
    next === "process" ||
    next === "retry_process" ||
    busyAction === "process";
  const showSyncCta = false;
  const showGenericCta = false;

  let topCardButtonLabel = "";
  if (busyAction === "process") topCardButtonLabel = "Processing…";
  else if (next === "process") topCardButtonLabel = "Process listing import";
  else if (next === "retry_process") topCardButtonLabel = "Retry Process";
  else if (next === "complete") topCardButtonLabel = "Complete";

  const rowActionLabel = topCardButtonLabel;

  let phaseBadgeText = "Listing import";
  if (isComplete) phaseBadgeText = "Complete";
  else if (isFailed) {
    phaseBadgeText = st.showRetryProcess ? "Failed — retry Process" : "Failed";
  } else if (busyAction === "process" || next === "process" || next === "retry_process") {
    phaseBadgeText = next === "retry_process" ? "Phase 2: Retry Process" : "Phase 2: Process listing import";
  } else if (["pending", "uploading"].includes(status)) phaseBadgeText = "Phase 1: Upload";

  const completeLine = `Complete — raw archive + catalog snapshot (new ${pm.catalogRowsNew.toLocaleString()}, updated ${pm.catalogRowsUpdated.toLocaleString()}, unchanged ${pm.catalogRowsUnchanged.toLocaleString()}).`;

  let phaseSummaryText = "";
  if (isComplete) phaseSummaryText = completeLine;
  else if (isFailed) phaseSummaryText = phaseBadgeText;
  else if (busyAction === "process") {
    phaseSummaryText = "Processing — raw rows to amazon_listing_report_rows_raw, then catalog_products.";
  } else if (next === "process" || next === "retry_process") {
    phaseSummaryText =
      "Run Process once: rows are archived to the listing raw table and merged into catalog_products.";
  }

  const rowMetricsLine = buildListingRowMetricsLine(pm, st, meta);

  const fpsRow =
    input.fps && typeof input.fps === "object" ? (input.fps as Record<string, unknown>) : null;
  const combinedPhase2Pct = Math.min(
    100,
    Math.max(pm.phase2PctListing, pm.phase3PctListing, pm.phase4PctListing, num(fpsRow?.process_pct, 0)),
  );

  return {
    currentPhase,
    nextAction: next,
    topCardButtonLabel,
    rowActionLabel,
    phaseBadgeText,
    phaseSummaryText,
    phase1Pct: pm.phase1PctListing,
    phase2Pct: combinedPhase2Pct,
    phase3Pct: 0,
    phase4Pct: 0,
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
  return `Staged ${stagedRows.toLocaleString()} row(s) — continuing with raw archive and catalog merge.`;
}

export function formatListingPhase3CompleteMessage(
  rawArchived: number,
  rawSkippedExisting: number,
  stagingLinesProcessed: number,
): string {
  if (rawSkippedExisting > 0 && rawArchived === 0) {
    return `Raw archive complete — 0 new row(s), ${rawSkippedExisting.toLocaleString()} skipped (already archived).`;
  }
  if (rawSkippedExisting > 0) {
    return `Raw archive complete — ${rawArchived.toLocaleString()} new row(s), ${rawSkippedExisting.toLocaleString()} skipped (already archived).`;
  }
  if (stagingLinesProcessed > 0) {
    return `Raw archive complete — ${rawArchived.toLocaleString()} row(s) (${stagingLinesProcessed.toLocaleString()} line(s) processed).`;
  }
  return `Raw archive complete — ${rawArchived.toLocaleString()} row(s).`;
}

export function formatListingPhase4PendingMessage(): string {
  return "";
}

export function formatListingPhase4CompleteMessage(newRows: number, updated: number, unchanged: number): string {
  return `Catalog complete — ${newRows.toLocaleString()} new, ${updated.toLocaleString()} updated, ${unchanged.toLocaleString()} unchanged.`;
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
