/**
 * Unified import pipeline model — single source of truth for ALL file types.
 *
 * Every import follows the same 4-phase pipeline:
 *   Phase 1: Upload (file → storage + AI classify)
 *   Phase 2: Process (CSV → amazon_staging)
 *   Phase 3: Sync (staging → domain table)
 *   Phase 4: Generic (optional post-sync enrichment)
 *
 * For types without Phase 4 (supports_generic=false), the step is "skipped".
 * For REMOVAL_ORDER, Generate Worklist is shown as an additional step.
 * For UNKNOWN types, the pipeline stops after Phase 1.
 *
 * Progress is driven from `file_processing_status` + `raw_report_uploads`
 * metadata — no invented values or special per-type branching.
 */

import {
  AMAZON_REPORT_REGISTRY,
  resolveAmazonImportSyncKind,
  type AmazonSyncKind,
} from "./amazon-report-registry";

// ── Types ─────────────────────────────────────────────────────────────────────

export type PipelineStepKey =
  | "upload"
  | "classify"
  | "process"
  | "sync"
  | "generic"
  | "worklist";

export type PipelineStepTone =
  | "pending"
  | "active"
  | "done"
  | "failed"
  | "skipped";

export type PipelineStep = {
  key: PipelineStepKey;
  label: string;
  subtitle: string;
  pct: number;
  tone: PipelineStepTone;
  rightLabel: string;
  subLabel?: string;
};

export type UnifiedPipelineModel = {
  kind: AmazonSyncKind;
  steps: PipelineStep[];
  overallPct: number;
  isComplete: boolean;
  isFailed: boolean;
  currentPhaseLabel: string;
  nextAction: "process" | "sync" | "generic" | "worklist" | "map_columns" | null;
  badgeStatus: string;
  badgeLabel: string;
  rowMetricsLine: string;
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function norm(s: unknown): string {
  return String(s ?? "").trim().toLowerCase();
}

function num(v: unknown, fallback = 0): number {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return fallback;
}

function fmtRows(n: number): string {
  return n.toLocaleString();
}

function fmtBytes(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return "—";
  if (n < 1024) return `${Math.round(n)} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

// ── Input types ───────────────────────────────────────────────────────────────

export type UnifiedPipelineInput = {
  reportType: string;
  status: string;
  metadata: Record<string, unknown> | null;
  fps: Record<string, unknown> | null;
  localUploadPct?: number;
  localFileSizeBytes?: number;
  /**
   * Only for in-flight actions where the row is still on the previous `status`
   * (e.g. Sync while DB still says `staged`). Do **not** set for Phase 2 Process —
   * that must come from DB + FPS like the History table.
   */
  ui?: {
    isSyncing?: boolean;
    isGenericing?: boolean;
    isWorklisting?: boolean;
  };
};

// ── Builder ───────────────────────────────────────────────────────────────────

export function buildUnifiedPipeline(input: UnifiedPipelineInput): UnifiedPipelineModel {
  const kind = resolveAmazonImportSyncKind(input.reportType);
  const reg = AMAZON_REPORT_REGISTRY[kind];
  const ui = input.ui ?? {};
  const st = norm(input.status);
  const m = input.metadata ?? {};
  const f = input.fps ?? {};

  const failedPhase = norm(m.failed_phase);
  const etlPhase = norm(m.etl_phase);
  const catalogPhase = norm(m.catalog_listing_import_phase);

  const p1s = norm(f.phase1_status);
  const p2s = norm(f.phase2_status);
  const p3s = norm(f.phase3_status);
  const p4s = norm(f.phase4_status);
  const curPhase = norm(f.current_phase ?? m.etl_phase);

  // ── Phase status derivation ─────────────────────────────────────────────

  const uploadDone =
    st !== "pending" && st !== "uploading";
  const classifyDone =
    uploadDone &&
    st !== "needs_mapping" &&
    st !== "unsupported";

  const phase2Complete =
    p2s === "complete" ||
    st === "staged" ||
    st === "raw_synced" ||
    st === "synced" ||
    st === "complete" ||
    (st === "processing" &&
      (curPhase === "sync" || curPhase === "generic" || curPhase === "complete")) ||
    (st === "failed" && (failedPhase === "sync" || failedPhase === "generic"));

  const phase3Complete =
    p3s === "complete" ||
    st === "raw_synced" ||
    st === "synced" ||
    st === "complete" ||
    (st === "processing" && (curPhase === "generic" || curPhase === "complete")) ||
    (st === "failed" && failedPhase === "generic") ||
    catalogPhase === "raw_archived" ||
    catalogPhase === "done";

  const needsP4 = reg.supports_generic;
  const phase4Complete =
    !needsP4 ||
    p4s === "complete" ||
    catalogPhase === "done" ||
    (needsP4 && etlPhase === "complete") ||
    (st === "complete") ||
    (st === "synced" && needsP4 && p4s === "complete");

  const wantsWorklist = reg.generateWorklistAfterSync === true;
  const worklistCompleted = m.worklist_completed === true;

  // ── Byte / row counters ─────────────────────────────────────────────────

  const totalBytes = Math.max(0, num(m.total_bytes ?? m.file_size_bytes ?? f.total_bytes ?? f.upload_bytes_total ?? input.localFileSizeBytes, 0));
  const rawUploadedBytes = Math.max(0, num(m.uploaded_bytes ?? f.uploaded_bytes ?? f.upload_bytes_written, 0));
  const uploadedBytes = uploadDone && rawUploadedBytes === 0 && totalBytes > 0 ? totalBytes : rawUploadedBytes;
  const upPctFps = num(f.upload_pct, -1);
  const upPctMeta = num(m.upload_progress, -1);
  const localUp = num(input.localUploadPct, -1);
  let uploadPct = 0;
  if (!uploadDone) {
    if (upPctFps >= 0) uploadPct = Math.min(100, Math.round(upPctFps));
    else if (upPctMeta >= 0) uploadPct = Math.min(100, Math.round(upPctMeta));
    else if (localUp >= 0) uploadPct = Math.min(100, Math.round(localUp));
    else if (totalBytes > 0 && uploadedBytes > 0) uploadPct = Math.min(100, Math.round((uploadedBytes / totalBytes) * 100));
  } else {
    uploadPct = 100;
  }

  const dataRowsTotal = Math.max(0,
    num(f.data_rows_total, 0) || num(f.file_rows_total, 0) ||
    num(m.data_rows_seen, 0) || num(m.catalog_listing_data_rows_seen, 0) ||
    num(m.total_rows, 0));
  const stagedRows = Math.max(0,
    num(f.staged_rows_written, 0) || num(f.processed_rows, 0) ||
    num(m.staging_row_count, 0) || num(m.row_count, 0));

  const p2pct = (() => {
    const v = num(f.phase2_stage_pct, -1);
    if (v >= 0) return Math.min(100, Math.round(v));
    const pp = num(f.process_pct, -1);
    if (pp >= 0) return Math.min(100, Math.round(pp));
    if (dataRowsTotal > 0 && stagedRows > 0)
      return Math.min(100, Math.round((stagedRows / dataRowsTotal) * 100));
    return phase2Complete ? 100 : 0;
  })();

  const rawWritten = Math.max(0, num(f.raw_rows_written, 0));
  const rawSkipped = Math.max(0, num(f.raw_rows_skipped_existing, 0));
  const syncDenom = Math.max(1, num(f.staged_rows_written, 0) || num(m.staging_row_count, 0) || dataRowsTotal || stagedRows || 1);
  const p3pct = (() => {
    const v = num(f.phase3_raw_sync_pct, -1);
    if (v >= 0) return Math.min(100, Math.round(v));
    const sp = num(f.sync_pct, -1);
    if (sp >= 0 && (curPhase === "sync" || etlPhase === "sync" || ui.isSyncing === true)) {
      return Math.min(100, Math.round(sp));
    }
    if (rawWritten + rawSkipped > 0)
      return Math.min(100, Math.round(((rawWritten + rawSkipped) / syncDenom) * 100));
    return phase3Complete ? 100 : 0;
  })();

  const genericWritten = Math.max(0, num(f.generic_rows_written, 0));
  const genericEligible = Math.max(1, num(f.rows_eligible_for_generic, 0) || num(m.removal_shipment_lines_for_generic, 0) || syncDenom);
  const canonNew = Math.max(0, num(f.canonical_rows_new ?? m.catalog_listing_canonical_rows_new ?? m.catalog_listing_canonical_rows_inserted, 0));
  const canonUpd = Math.max(0, num(f.canonical_rows_updated ?? m.catalog_listing_canonical_rows_updated, 0));
  const canonUnch = Math.max(0, num(f.canonical_rows_unchanged ?? m.catalog_listing_canonical_rows_unchanged ?? m.catalog_listing_canonical_rows_unchanged_or_merged, 0));
  const canonSum = canonNew + canonUpd + canonUnch;
  const p4gpctNum = num(f.phase4_generic_pct, -1);
  const p4pctRaw = (() => {
    if (!needsP4) return 0;
    if (phase4Complete) return 100;
    const fromCol = p4gpctNum >= 0 ? Math.min(100, Math.round(p4gpctNum)) : null;
    const numerator = canonSum > 0 ? canonSum : genericWritten;
    const fromRatio =
      numerator > 0 && genericEligible > 0
        ? Math.min(100, Math.round((numerator / genericEligible) * 100))
        : null;
    if (fromCol != null && fromRatio != null) return Math.max(fromCol, fromRatio);
    if (fromCol != null) return fromCol;
    if (fromRatio != null) return fromRatio;
    return 0;
  })();

  // ── Step: Upload ────────────────────────────────────────────────────────

  const uploadRight = totalBytes > 0
    ? `${fmtBytes(uploadedBytes)} / ${fmtBytes(totalBytes)} (${uploadPct}%)`
    : uploadDone ? "100%" : `${uploadPct}%`;

  const uploadStep: PipelineStep = {
    key: "upload",
    label: "Upload",
    subtitle: "Upload file to storage",
    pct: uploadDone ? 100 : Math.max(3, uploadPct),
    tone: st === "uploading" ? "active" : uploadDone ? "done" : "pending",
    rightLabel: uploadRight,
  };

  // ── Step: Classify ──────────────────────────────────────────────────────

  const classifyStep: PipelineStep = {
    key: "classify",
    label: "Map & classify",
    subtitle: "Auto-detect report type",
    pct: classifyDone ? 100 : st === "needs_mapping" ? 100 : uploadDone ? 50 : 0,
    tone: st === "needs_mapping"
      ? "failed"
      : !uploadDone
        ? "pending"
        : classifyDone
          ? "done"
          : "active",
    rightLabel: st === "needs_mapping"
      ? "Needs review"
      : !uploadDone
        ? "—"
        : classifyDone
          ? "Mapped"
          : "Classifying…",
  };

  // ── Step: Process ───────────────────────────────────────────────────────

  const processActive =
    ui.isSyncing !== true &&
    ui.isGenericing !== true &&
    st === "processing" &&
    !phase2Complete &&
    (p2s === "running" || p2s === "" || curPhase === "staging");
  const processRight = (() => {
    if (phase2Complete) {
      return dataRowsTotal > 0
        ? `${fmtRows(stagedRows)} / ${fmtRows(dataRowsTotal)} rows`
        : stagedRows > 0 ? `${fmtRows(stagedRows)} rows` : "done";
    }
    if (processActive) {
      return dataRowsTotal > 0
        ? `${fmtRows(stagedRows)} / ${fmtRows(dataRowsTotal)} rows · ${p2pct}%`
        : p2pct > 0 ? `${p2pct}%` : "…";
    }
    return "—";
  })();

  const processStep: PipelineStep = {
    key: "process",
    label: "Process — staging",
    subtitle: "Parse and stage rows",
    pct: phase2Complete ? 100 : processActive ? Math.max(3, p2pct) : 0,
    tone: st === "failed" && failedPhase === "process"
      ? "failed"
      : processActive
        ? "active"
        : phase2Complete
          ? "done"
          : kind === "UNKNOWN"
            ? "skipped"
            : "pending",
    rightLabel: processRight,
  };

  // ── Step: Sync ──────────────────────────────────────────────────────────

  const syncActive =
    ui.isSyncing === true ||
    (st === "processing" && (curPhase === "sync" || etlPhase === "sync" || catalogPhase === "raw_archive")) ||
    st === "syncing" ||
    (phase2Complete &&
      !phase3Complete &&
      st !== "processing" &&
      st !== "staged" &&
      st !== "failed" &&
      st !== "mapped" &&
      st !== "ready");
  const syncTarget = reg.sync_target_table ? reg.sync_target_table.replace(/^amazon_/, "").replaceAll("_", " ") : "domain";
  const syncRight = (() => {
    if (phase3Complete) {
      const total = rawWritten + rawSkipped;
      if (total > 0) return `${fmtRows(total)} rows · done`;
      return "done";
    }
    if (syncActive) {
      if (rawWritten + rawSkipped > 0)
        return `${fmtRows(rawWritten + rawSkipped)} / ${fmtRows(syncDenom)} · ${p3pct}%`;
      return p3pct > 0 ? `${p3pct}%` : "…";
    }
    return "—";
  })();

  const syncStep: PipelineStep = {
    key: "sync",
    label: "Sync",
    subtitle: `Write to ${syncTarget}`,
    pct: phase3Complete ? 100 : syncActive ? Math.max(3, p3pct) : 0,
    tone: st === "failed" && failedPhase === "sync"
      ? "failed"
      : syncActive
        ? "active"
        : phase3Complete
          ? "done"
          : kind === "UNKNOWN" || !reg.sync_target_table
            ? "skipped"
            : "pending",
    rightLabel: syncRight,
  };

  // ── Step: Generic ───────────────────────────────────────────────────────

  /** True while Generic is running or FPS shows partial progress (column and/or row ratio). */
  const genericRunning =
    (st === "processing" &&
      (curPhase === "generic" || etlPhase === "generic" || catalogPhase === "canonical_sync")) ||
    st === "genericing" ||
    ui.isGenericing === true ||
    p4s === "running" ||
    (needsP4 &&
      !phase4Complete &&
      p4gpctNum > 0 &&
      p4gpctNum < 100) ||
    (needsP4 &&
      !phase4Complete &&
      phase3Complete &&
      p4pctRaw > 0 &&
      p4pctRaw < 100 &&
      (st === "raw_synced" || st === "processing" || st === "genericing"));
  const p4pct =
    genericRunning && p4pctRaw === 0 ? 5 : p4pctRaw;
  const genericTarget = reg.generic_target_table ? reg.generic_target_table.replace(/^amazon_/, "").replaceAll("_", " ") : "enrichment";
  const genericRight = (() => {
    if (!needsP4) return "N/A";
    if (phase4Complete) {
      if (canonSum > 0) return `new ${fmtRows(canonNew)} · upd ${fmtRows(canonUpd)} · same ${fmtRows(canonUnch)}`;
      if (genericWritten > 0) return `${fmtRows(genericWritten)} rows`;
      return "done";
    }
    if (genericRunning) {
      if (genericWritten > 0)
        return `${fmtRows(genericWritten)} / ${fmtRows(genericEligible)} · ${p4pct}%`;
      return p4pct > 0 ? `${p4pct}%` : "…";
    }
    return "—";
  })();

  const genericStep: PipelineStep = {
    key: "generic",
    label: "Generic",
    subtitle: `Enrich ${genericTarget}`,
    pct: !needsP4 ? 0 : phase4Complete ? 100 : genericRunning ? Math.max(3, p4pct) : 0,
    tone: !needsP4
      ? "skipped"
      : st === "failed" && failedPhase === "generic"
        ? "failed"
        : genericRunning
          ? "active"
          : phase4Complete
            ? "done"
            : "pending",
    rightLabel: genericRight,
    subLabel: undefined,
  };

  // ── Step: Worklist (removal order only) ─────────────────────────────────

  const worklistActive =
    ui.isWorklisting === true ||
    st === "worklisting" ||
    (st === "processing" && etlPhase === "worklist");
  const worklistPct = num(m.worklist_progress, 0);
  const worklistStep: PipelineStep = {
    key: "worklist",
    label: "Generate Worklist",
    subtitle: "expected_packages",
    pct: worklistCompleted ? 100 : worklistActive ? Math.max(3, worklistPct) : 0,
    tone: !wantsWorklist
      ? "skipped"
      : worklistCompleted
        ? "done"
        : worklistActive
          ? "active"
          : "pending",
    rightLabel: !wantsWorklist
      ? "N/A"
      : worklistCompleted
        ? "done"
        : worklistActive
          ? worklistPct > 0 ? `${worklistPct}%` : "…"
          : "—",
  };

  // ── Assemble steps ──────────────────────────────────────────────────────

  const steps: PipelineStep[] = [uploadStep, classifyStep, processStep, syncStep];
  if (needsP4) steps.push(genericStep);
  if (wantsWorklist) steps.push(worklistStep);

  // ── Overall status ──────────────────────────────────────────────────────

  const allDone = steps.every((s) => s.tone === "done" || s.tone === "skipped");
  const anyFailed = steps.some((s) => s.tone === "failed") || st === "failed";
  const isComplete = allDone && !anyFailed;
  const isFailed = anyFailed && !isComplete;

  const activeSteps = steps.filter((s) => s.tone === "active" || s.tone === "failed");
  const lastDoneIdx = steps.reduce((acc, s, i) => (s.tone === "done" ? i : acc), -1);
  const nextPendingStep = steps.find((s, i) => i > lastDoneIdx && s.tone === "pending");
  const currentPhaseLabel = activeSteps.length > 0
    ? activeSteps[0].label
    : isComplete
      ? "Complete"
      : nextPendingStep
        ? `Next: ${nextPendingStep.label}`
        : lastDoneIdx >= 0
          ? steps[lastDoneIdx].label
          : "Pending";

  // ── Overall percentage (weighted average of applicable steps) ───────────

  const applicableSteps = steps.filter((s) => s.tone !== "skipped");
  const overallPct = applicableSteps.length > 0
    ? Math.round(applicableSteps.reduce((sum, s) => sum + s.pct, 0) / applicableSteps.length)
    : 0;

  // ── Next action ─────────────────────────────────────────────────────────

  let nextAction: UnifiedPipelineModel["nextAction"] = null;
  if (st === "needs_mapping") {
    nextAction = "map_columns";
  } else if (
    !phase2Complete &&
    classifyDone &&
    st !== "processing" &&
    st !== "syncing" &&
    st !== "genericing" &&
    st !== "failed" &&
    kind !== "UNKNOWN"
  ) {
    nextAction = "process";
  } else if (st === "failed" && failedPhase === "process") {
    nextAction = "process";
  } else if (
    (st === "staged" && ui.isSyncing !== true) ||
    (st === "failed" && failedPhase === "sync" && !phase3Complete) ||
    (phase2Complete && !phase3Complete && !syncActive && st !== "processing" && st !== "syncing")
  ) {
    nextAction = "sync";
  } else if (
    needsP4 &&
    !phase4Complete &&
    phase3Complete &&
    !genericRunning &&
    (st === "raw_synced" || (st === "failed" && failedPhase === "generic") ||
     (st !== "processing" && st !== "genericing" && st !== "syncing"))
  ) {
    nextAction = "generic";
  } else if (wantsWorklist && !worklistCompleted && phase3Complete && (st === "synced" || st === "complete")) {
    nextAction = "worklist";
  }

  // ── Badge ───────────────────────────────────────────────────────────────

  let badgeStatus = st;
  let badgeLabel = "";

  if (isComplete) {
    badgeStatus = "complete";
    badgeLabel = "Complete";
  } else if (st === "failed") {
    badgeStatus = "failed";
    if (failedPhase === "process") badgeLabel = "Failed — Process";
    else if (failedPhase === "sync") badgeLabel = "Failed — Sync";
    else if (failedPhase === "generic") badgeLabel = "Failed — Generic";
    else badgeLabel = "Failed";
  } else if (st === "uploading") {
    badgeLabel = "Uploading…";
  } else if (st === "needs_mapping") {
    badgeLabel = "Needs Mapping";
  } else if (st === "processing") {
    if (curPhase === "sync") badgeLabel = "Syncing…";
    else if (curPhase === "generic") badgeLabel = "Generic…";
    else badgeLabel = "Processing…";
  } else if (st === "staged") {
    badgeLabel = "Staged — run Sync";
  } else if (st === "raw_synced") {
    badgeLabel = "Synced — run Generic";
  } else if (st === "synced") {
    if (wantsWorklist && !worklistCompleted) badgeLabel = "Synced — run Worklist";
    else badgeLabel = "Complete";
  } else if (st === "mapped" || st === "ready" || st === "uploaded") {
    badgeLabel = "Ready — run Process";
  } else if (st === "pending") {
    badgeLabel = "Pending";
  } else {
    badgeLabel = st.charAt(0).toUpperCase() + st.slice(1);
  }

  // ── Row metrics ─────────────────────────────────────────────────────────

  const parts: string[] = [];
  if (stagedRows > 0) parts.push(`staged ${fmtRows(stagedRows)}`);
  if (rawWritten > 0) parts.push(`new ${fmtRows(rawWritten)}`);
  if (rawSkipped > 0) parts.push(`skipped ${fmtRows(rawSkipped)}`);
  if (phase4Complete && canonSum > 0) {
    parts.push(`new ${fmtRows(canonNew)} · upd ${fmtRows(canonUpd)} · same ${fmtRows(canonUnch)}`);
  } else if (genericWritten > 0) {
    parts.push(`generic ${fmtRows(genericWritten)}`);
  }
  const dupSkipped = Math.max(0, num(f.duplicate_rows_skipped ?? (m.import_metrics as Record<string, unknown> | undefined)?.rows_duplicate_against_existing, 0));
  if (dupSkipped > 0) parts.push(`dup ${fmtRows(dupSkipped)}`);

  const rowMetricsLine = parts.join(" · ") || "—";

  return {
    kind,
    steps,
    overallPct,
    isComplete,
    isFailed,
    currentPhaseLabel,
    nextAction,
    badgeStatus,
    badgeLabel,
    rowMetricsLine,
  };
}

// ── Badge color helper ────────────────────────────────────────────────────────

export function pipelineBadgeColor(status: string): string {
  const s = norm(status);
  if (s === "complete" || s === "synced") return "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300";
  if (s === "staged" || s === "raw_synced") return "bg-violet-500/15 text-violet-700 dark:text-violet-300";
  if (s === "mapped" || s === "ready" || s === "uploaded") return "bg-sky-500/15 text-sky-700 dark:text-sky-300";
  if (s === "failed") return "bg-destructive/15 text-destructive";
  if (s === "uploading" || s === "processing") return "bg-sky-500/15 text-sky-700 dark:text-sky-300";
  if (s === "needs_mapping") return "bg-amber-500/15 text-amber-700 dark:text-amber-300";
  if (s === "pending") return "bg-muted text-muted-foreground";
  return "bg-muted text-muted-foreground";
}

// ── Step bar color ────────────────────────────────────────────────────────────

export function stepBarColor(tone: PipelineStepTone): string {
  switch (tone) {
    case "active": return "bg-sky-500";
    case "done": return "bg-emerald-500";
    case "failed": return "bg-red-500";
    case "skipped": return "bg-muted-foreground/20";
    case "pending": return "bg-muted-foreground/30";
  }
}

export function stepBadgeColor(tone: PipelineStepTone): string {
  switch (tone) {
    case "active": return "bg-sky-600 text-white";
    case "done": return "bg-emerald-600 text-white";
    case "failed": return "bg-red-600 text-white";
    case "skipped": return "bg-muted text-muted-foreground";
    case "pending": return "bg-muted text-muted-foreground";
  }
}
