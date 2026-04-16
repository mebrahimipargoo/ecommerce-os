"use client";

import React, { useCallback, useEffect, useRef, useState } from "react";
import { Check, CheckCircle2, CheckSquare, FileSpreadsheet, Loader2, Lock, MapPin, RefreshCw, RotateCcw, Search, Square, Trash2 } from "lucide-react";
import { AMAZON_LEDGER_UPLOAD_SOURCE } from "../../../lib/raw-report-upload-metadata";
import type { RawReportUploadRow } from "../../../lib/raw-report-upload-row";
import { deleteRawReportUpload, listRawReportUploads, resetStuckUpload, updateRawReportType } from "./import-actions";
import { ColumnMappingModal } from "./ColumnMappingModal";
import { useUserRole } from "../../../components/UserRoleContext";
import type { RawReportType } from "../../../lib/raw-report-types";
import { REPORT_TYPE_SPECS } from "../../../lib/csv-import-mapping";
import { isListingReportType, RAW_REPORT_TYPE_ORDER } from "../../../lib/raw-report-types";
import { formatImportPhaseLabel } from "../../../lib/pipeline/import-phase-labels";
import type { ImportUiActionInput } from "../../../lib/import-ui-action-state";
import { resolveImportUiActionState } from "../../../lib/import-ui-action-state";
import {
  buildImportUiActionInputForRemovalShipment,
  resolveRemovalShipmentPhaseBadgeLabel,
  resolveRemovalShipmentPrimaryCta,
} from "../../../lib/import-removal-shipment-ui";
import { buildListingPipelineSteps, resolveListingImportUiState } from "../../../lib/listing-import-ui";
import { useDebugMode } from "../../../components/DebugModeContext";
import { DatabaseTag } from "../../../components/DatabaseTag";

function num(v: unknown, fallback = 0): number {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  return fallback;
}

/** Listing-only: same step model as the top importer card, compact for the history table. */
function ListingPipelineHistoryCell({
  status,
  metadata,
  fps,
}: {
  status: string;
  metadata: Record<string, unknown> | null;
  fps: RawReportUploadRow["file_processing_status"];
}) {
  const steps = buildListingPipelineSteps({
    status,
    metadata,
    fps: fps && typeof fps === "object" ? (fps as Record<string, unknown>) : null,
  });
  return (
    <div className="min-w-[200px] max-w-[280px] space-y-1">
      <ol className="space-y-1" aria-label="Listing pipeline steps">
        {steps.map((s) => {
          const barTint =
            s.tone === "active"
              ? "bg-sky-500"
              : s.tone === "done"
                ? "bg-emerald-500"
                : s.tone === "warning"
                  ? "bg-amber-500"
                  : "bg-muted-foreground/30";
          return (
                       <li key={s.key} className="space-y-0.5 text-[10px] leading-tight">
              <div className="flex items-baseline justify-between gap-1">
                <span className="min-w-0 truncate font-medium text-foreground" title={`${s.title} — ${s.subtitle}`}>
                  {s.title}
                </span>
                <span className="shrink-0 tabular-nums text-muted-foreground">{Math.round(s.pct)}%</span>
              </div>
              <div className="relative h-1 overflow-hidden rounded-full bg-muted">
                <div
                  className={`h-full rounded-full transition-all duration-500 ${barTint}`}
                  style={{ width: `${Math.max(s.tone === "upcoming" ? 0 : 2, Math.min(100, s.pct))}%` }}
                />
              </div>
              <span
                className="line-clamp-2 text-[9px] text-muted-foreground"
                title={[s.rightLabel, s.subLabel].filter(Boolean).join(" · ")}
              >
                {s.rightLabel}
                {s.subLabel ? (
                  <span className="text-violet-700 dark:text-violet-300"> · {s.subLabel}</span>
                ) : null}
              </span>
            </li>
          );
        })}
      </ol>
    </div>
  );
}

// Status badge label
function statusLabel(status: string, uploadProgress?: number, opts?: { listing?: boolean }): string {
  const listing = opts?.listing ?? false;
  switch (status) {
    case "mapped":
      return listing ? "Ready — Process Data" : "Phase 2: Process";
    case "staged":
      return listing ? "Ready — Process listing import" : "Phase 3: Sync";
    case "raw_synced":
      return "Phase 4: Generic";
    case "ready":
      return "Ready";
    case "uploaded":
      return "Uploaded";
    case "pending":
      if (uploadProgress != null && uploadProgress >= 100) return "Ready";
      return "Pending";
    case "uploading":
      return "Uploading…";
    case "processing":
      return "Processing…";
    case "synced":
      return listing ? "Complete (listing)" : "Complete";
    case "complete":
      return listing ? "Complete (listing)" : "Complete";
    case "failed":
      return "Failed";
    case "cancelled":
      return "Cancelled";
    case "needs_mapping":
      return "Needs Mapping";
    default:
      return status;
  }
}

function statusColorClass(status: string): string {
  if (status === "synced" || status === "complete")
    return "bg-emerald-500/15 text-emerald-800 dark:text-emerald-300";
  if (status === "staged" || status === "raw_synced")
    return "bg-violet-500/15 text-violet-800 dark:text-violet-300";
  if (status === "mapped" || status === "ready" || status === "uploaded")
    return "bg-sky-500/15 text-sky-800 dark:text-sky-300";
  if (status === "failed" || status === "cancelled")
    return "bg-destructive/15 text-destructive";
  if (status === "uploading" || status === "processing")
    return "bg-sky-500/15 text-sky-800 dark:text-sky-300";
  if (status === "needs_mapping")
    return "bg-amber-500/15 text-amber-800 dark:text-amber-300";
  return "bg-muted text-muted-foreground";
}

const LEGACY_REPORT_TYPE: Record<string, RawReportType> = {
  returns: "fba_customer_returns",
  inventory_adjustments: "inventory_ledger",
  removals: "safe_t_claims",
  other: "transaction_view",
};

function coerceReportType(v: string): RawReportType {
  if (RAW_REPORT_TYPE_ORDER.includes(v as RawReportType)) return v as RawReportType;
  return LEGACY_REPORT_TYPE[v] ?? "UNKNOWN";
}

type RawReportImportsPanelProps = {
  organizationId: string | null;
  /** Parent increments after upload / org change to trigger an immediate refetch (panel stays mounted). */
  refreshSignal?: number;
};

export function RawReportImportsPanel({ organizationId, refreshSignal = 0 }: RawReportImportsPanelProps) {
  const { debugMode } = useDebugMode();
  const { actorUserId } = useUserRole();
  const [rows, setRows] = useState<RawReportUploadRow[]>([]);
  const [loadErr, setLoadErr] = useState<string | null>(null);
  const [tableSearch, setTableSearch] = useState("");
  const [reportTypeSaveFlash, setReportTypeSaveFlash] = useState<Record<string, boolean>>({});

  // Per-row operation state
  const [processingIds, setProcessingIds] = useState<Set<string>>(() => new Set());
  const [syncingIds, setSyncingIds] = useState<Set<string>>(() => new Set());
  const [genericIds, setGenericIds] = useState<Set<string>>(() => new Set());
  const [worklistingIds, setWorklistingIds] = useState<Set<string>>(() => new Set());
  const [deletingIds, setDeletingIds] = useState<Set<string>>(() => new Set());
  const [resettingIds, setResettingIds] = useState<Set<string>>(() => new Set());

  // Batch select state
  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => new Set());
  const [batchDeleting, setBatchDeleting] = useState(false);

  const [mappingRow, setMappingRow] = useState<RawReportUploadRow | null>(null);

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const reportTypeOverrideRef = useRef<Map<string, string>>(new Map());
  /** Ignore stale list responses when org changes or overlapping polls complete out of order. */
  const listFetchGenerationRef = useRef(0);

  const refresh = useCallback(async () => {
    const gen = ++listFetchGenerationRef.current;
    const res = await listRawReportUploads({ organizationId, actorUserId });
    if (gen !== listFetchGenerationRef.current) return;
    if (res.ok) {
      const override = reportTypeOverrideRef.current;
      setRows(
        res.rows.map((row) => {
          const o = override.get(row.id);
          if (o != null && row.report_type !== o) return { ...row, report_type: o };
          if (o != null && row.report_type === o) override.delete(row.id);
          return row;
        }),
      );
      setLoadErr(null);
    } else {
      setLoadErr(res.error);
    }
  }, [organizationId, actorUserId]);

  useEffect(() => {
    void refresh();
  }, [refresh, refreshSignal]);

  useEffect(() => {
    pollRef.current = setInterval(() => {
      void refresh();
    }, 3500);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [refresh]);

  /** Faster refresh while Process or Sync is running so progress metadata updates visibly. */
  useEffect(() => {
    if (processingIds.size === 0 && syncingIds.size === 0 && genericIds.size === 0 && worklistingIds.size === 0) return;
    const t = setInterval(() => {
      void refresh();
    }, 700);
    return () => clearInterval(t);
  }, [processingIds.size, syncingIds.size, genericIds.size, worklistingIds.size, refresh]);

  // ── Single row delete ────────────────────────────────────────────────────
  const runDeleteUpload = async (r: RawReportUploadRow) => {
    if (!window.confirm(`Delete "${r.file_name}" and all associated data? This cannot be undone.`)) return;
    setDeletingIds((prev) => new Set([...prev, r.id]));
    setLoadErr(null);
    try {
      const res = await deleteRawReportUpload(r.id);
      if (!res.ok) { setLoadErr(res.error ?? "Delete failed."); return; }
      setSelectedIds((prev) => { const n = new Set(prev); n.delete(r.id); return n; });
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Delete failed.");
    } finally {
      setDeletingIds((prev) => { const n = new Set(prev); n.delete(r.id); return n; });
    }
  };

  // ── Batch delete ─────────────────────────────────────────────────────────
  const runBatchDelete = async () => {
    if (selectedIds.size === 0) return;
    if (!window.confirm(`Delete ${selectedIds.size} selected import(s) and all associated data? This cannot be undone.`)) return;
    setBatchDeleting(true);
    setLoadErr(null);
    const ids = [...selectedIds];
    try {
      for (const id of ids) {
        await deleteRawReportUpload(id);
      }
      setSelectedIds(new Set());
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Batch delete partially failed.");
    } finally {
      setBatchDeleting(false);
    }
  };

  // ── Phase 2: Process — unified route (staging for Amazon reports; listing branch for catalog exports) ──
  const runProcess = async (r: RawReportUploadRow) => {
    setProcessingIds((prev) => new Set([...prev, r.id]));
    setLoadErr(null);
    try {
      const res = await fetch("/api/settings/imports/process", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: r.id }),
      });
      const json = (await res.json()) as {
        ok?: boolean;
        error?: string;
        rowsStaged?: number;
        rowsProcessed?: number;
      };
      if (!res.ok || !json.ok) {
        setLoadErr(json.error ?? "Processing failed.");
        return;
      }
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Processing failed.");
    } finally {
      setProcessingIds((prev) => { const n = new Set(prev); n.delete(r.id); return n; });
    }
  };

  // ── Phase 4: Generic (catalog, removal shipment tree, etc.) ───────────────
  const runGeneric = async (r: RawReportUploadRow) => {
    setGenericIds((prev) => new Set([...prev, r.id]));
    setLoadErr(null);
    try {
      const res = await fetch("/api/settings/imports/generic", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: r.id }),
      });
      const json = (await res.json()) as { ok?: boolean; error?: string };
      if (!res.ok || !json.ok) {
        setLoadErr(json.error ?? "Generic phase failed.");
        await refresh();
        return;
      }
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Generic phase failed.");
      await refresh();
    } finally {
      setGenericIds((prev) => { const n = new Set(prev); n.delete(r.id); return n; });
    }
  };

  const runWorklist = async (r: RawReportUploadRow) => {
    setWorklistingIds((prev) => new Set([...prev, r.id]));
    setLoadErr(null);
    try {
      const res = await fetch("/api/settings/imports/generate-worklist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: r.id }),
      });
      const json = (await res.json()) as { ok?: boolean; error?: string };
      if (!res.ok || !json.ok) {
        setLoadErr(json.error ?? "Generate Worklist failed.");
        await refresh();
        return;
      }
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Generate Worklist failed.");
      await refresh();
    } finally {
      setWorklistingIds((prev) => { const n = new Set(prev); n.delete(r.id); return n; });
    }
  };

  // ── Phase 3: Sync (amazon_staging -> domain tables) ─────────────────────
  const runSync = async (r: RawReportUploadRow) => {
    setSyncingIds((prev) => new Set([...prev, r.id]));
    setLoadErr(null);
    try {
      const res = await fetch("/api/settings/imports/sync", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: r.id }),
      });
      const json = (await res.json()) as { ok?: boolean; error?: string; rowsSynced?: number; kind?: string };
      if (!res.ok || !json.ok) {
        setLoadErr(json.error ?? "Sync failed.");
        await refresh(); // refresh so the row shows "failed" status from the server
        return;
      }
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Sync failed.");
      await refresh(); // show any server-written "failed" status immediately
    } finally {
      setSyncingIds((prev) => { const n = new Set(prev); n.delete(r.id); return n; });
    }
  };

  // ── Reset stuck "processing" row ─────────────────────────────────────────
  const runResetStuck = async (r: RawReportUploadRow) => {
    setResettingIds((prev) => new Set([...prev, r.id]));
    setLoadErr(null);
    try {
      const res = await resetStuckUpload({ uploadId: r.id, actorUserId });
      if (!res.ok) {
        setLoadErr(res.error ?? "Reset failed.");
        return;
      }
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Reset failed.");
    } finally {
      setResettingIds((prev) => { const n = new Set(prev); n.delete(r.id); return n; });
    }
  };

  // ── Checkbox helpers ─────────────────────────────────────────────────────
  const toggleRow = (id: string) =>
    setSelectedIds((prev) => {
      const n = new Set(prev);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });

  const allSelected = rows.length > 0 && rows.every((r) => selectedIds.has(r.id));
  const toggleAll = () =>
    setSelectedIds(allSelected ? new Set() : new Set(rows.map((r) => r.id)));

  const anyBusy = (id: string) =>
    processingIds.has(id) ||
    syncingIds.has(id) ||
    genericIds.has(id) ||
    worklistingIds.has(id) ||
    deletingIds.has(id) ||
    resettingIds.has(id);

  return (
    <>
      {mappingRow && (
        <ColumnMappingModal
          row={mappingRow}
          onClose={() => setMappingRow(null)}
          onSaved={() => { setMappingRow(null); void refresh(); }}
        />
      )}

      {loadErr && (
        <div className="mb-4 rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {loadErr}
        </div>
      )}

      <div id="data-import-history" className="rounded-2xl border border-border bg-card shadow-sm scroll-mt-20">
        {/* ── History panel header ─────────────────────────────────────────────── */}
        <div className="flex items-center justify-between gap-3 border-b border-border px-5 py-4">
          {/* Left: title + search */}
          <div className="flex min-w-0 flex-1 items-center gap-3">
            <div className="flex shrink-0 items-center gap-2">
              <FileSpreadsheet className="h-4 w-4 text-muted-foreground" aria-hidden />
              <h2 className="text-sm font-semibold text-foreground">Import History</h2>
            </div>

            {/* Search input */}
            <div className="relative w-full max-w-xs">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" aria-hidden />
              <input
                type="search"
                placeholder="Search files…"
                value={tableSearch}
                onChange={(e) => setTableSearch(e.target.value)}
                className="h-8 w-full rounded-lg border border-border bg-background pl-8 pr-3 text-xs text-foreground outline-none placeholder:text-muted-foreground focus-visible:ring-2 focus-visible:ring-ring"
              />
            </div>
          </div>

          {/* Right: Delete selected */}
          {selectedIds.size > 0 && (
            <button
              type="button"
              disabled={batchDeleting}
              onClick={() => void runBatchDelete()}
              className="shrink-0 inline-flex items-center gap-1.5 rounded-lg border border-destructive/50 bg-destructive/10 px-3 py-1.5 text-xs font-semibold text-destructive transition hover:bg-destructive/20 disabled:opacity-50"
            >
              {batchDeleting ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
              ) : (
                <Trash2 className="h-3.5 w-3.5" aria-hidden />
              )}
              Delete {selectedIds.size} selected
            </button>
          )}
        </div>

        {/* Sub-description */}
        <div className="space-y-2 px-5 pt-2 pb-3 text-xs text-muted-foreground">
          <p>
            <strong className="text-foreground">Amazon listing exports:</strong> Phase 1 upload → one{" "}
            <strong>Process listing import</strong> step (raw rows to{" "}
            <code className="text-xs">amazon_listing_report_rows_raw</code>, then canonical{" "}
            <code className="text-xs">catalog_products</code>).
          </p>
          <p>
            <strong className="text-foreground">Other report types:</strong> after upload,{" "}
            <strong>Process</strong> stages rows, then <strong>Sync</strong> writes raw/domain tables. Removal shipment
            imports then run <strong>Generic</strong> (shipment tree). Removal orders use <strong>Generate Worklist</strong>{" "}
            when needed.
          </p>
          <p>
            Delete performs full cleanup across Storage, staging, and domain tables.
            {debugMode && (
              <span className="ml-2 font-mono text-[10px] text-muted-foreground/70">
                [<code>raw_report_uploads</code>]
              </span>
            )}
          </p>
        </div>

        {/* ── Scrollable table ─────────────────────────────────────────────────── */}
        <div className="relative max-h-[400px] overflow-x-auto overflow-y-auto rounded-b-2xl">
          <DatabaseTag table="raw_report_uploads" />
          <table className="w-full min-w-[1220px] border-collapse text-left text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/40 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                <th className="px-3 py-3">
                  {/* Select-all checkbox */}
                  <button
                    type="button"
                    onClick={toggleAll}
                    aria-label={allSelected ? "Deselect all" : "Select all"}
                    className="text-muted-foreground hover:text-foreground"
                  >
                    {allSelected
                      ? <CheckSquare className="h-4 w-4" aria-hidden />
                      : <Square className="h-4 w-4" aria-hidden />}
                  </button>
                </th>
                <th className="px-4 py-3">Import</th>
                <th className="px-4 py-3">Type</th>
                <th className="px-4 py-3">Uploaded by</th>
                <th className="px-4 py-3">Date</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3 text-right">Rows</th>
                <th className="px-3 py-3">Pipeline</th>
                <th className="px-4 py-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {(() => {
                const q = tableSearch.trim().toLowerCase();
                const filteredRows = q
                  ? rows.filter(
                      (r) =>
                        r.file_name.toLowerCase().includes(q) ||
                        (r.report_type ?? "").toLowerCase().includes(q),
                    )
                  : rows;
                if (filteredRows.length === 0) {
                  return (
                    <tr>
                      <td colSpan={9} className="px-4 py-8 text-center text-muted-foreground">
                        {rows.length === 0 ? "No imports yet." : `No results for "${tableSearch}".`}
                      </td>
                    </tr>
                  );
                }
                return filteredRows.map((r) => {
                const rt = coerceReportType(r.report_type);
                const isListing = isListingReportType(rt);
                const metaObj =
                  r.metadata && typeof r.metadata === "object" && !Array.isArray(r.metadata)
                    ? (r.metadata as Record<string, unknown>)
                    : null;
                const isLedgerSession = metaObj?.source === AMAZON_LEDGER_UPLOAD_SOURCE;
                const busy = anyBusy(r.id);

                const baseInput: ImportUiActionInput = {
                  reportType: String(r.report_type ?? ""),
                  status: r.status,
                  metadata: metaObj,
                  fps: r.file_processing_status,
                  isLedgerSession,
                };
                const uiInput = buildImportUiActionInputForRemovalShipment(
                  baseInput,
                  rt === "REMOVAL_SHIPMENT" ? "REMOVAL_SHIPMENT" : undefined,
                );
                const actionState = resolveImportUiActionState(uiInput);
                const badgeStatus = actionState.badgeStatus;
                const isRsRow = uiInput.reportType === "REMOVAL_SHIPMENT";
                const rsRowCta =
                  !isLedgerSession && isRsRow
                    ? resolveRemovalShipmentPrimaryCta(actionState, r.status, isLedgerSession)
                    : null;
                const rsBadge =
                  isRsRow && !isLedgerSession
                    ? resolveRemovalShipmentPhaseBadgeLabel(actionState, r.status, isLedgerSession)
                    : null;

                const listingUi =
                  !isLedgerSession && isListing && !isRsRow
                    ? resolveListingImportUiState({
                        ...uiInput,
                        client: {
                          isProcessing: processingIds.has(r.id),
                          isSyncing: syncingIds.has(r.id),
                          isGenericing: genericIds.has(r.id),
                        },
                      })
                    : null;

                const showMapColumns = r.status === "needs_mapping";
                const listingSyncGenericPlaceholders =
                  isListing &&
                  listingUi &&
                  !isLedgerSession &&
                  !["pending", "uploading"].includes(r.status);
                const showProcess =
                  !isLedgerSession &&
                  (isRsRow
                    ? rsRowCta === "process" || rsRowCta === "retry_process"
                    : isListing && listingUi
                      ? listingUi.showProcessCta
                      : r.status === "mapped" || r.status === "ready" || r.status === "uploaded");
                const showSync =
                  isRsRow && rsRowCta != null
                    ? rsRowCta === "sync" || rsRowCta === "retry_sync"
                    : isListing && listingUi
                      ? listingSyncGenericPlaceholders
                      : actionState.showSync;
                const showGeneric =
                  isRsRow && rsRowCta != null
                    ? rsRowCta === "generic" || rsRowCta === "generic_retry"
                    : isListing && listingUi
                      ? listingSyncGenericPlaceholders
                      : actionState.showGeneric;
                const showWorklist = actionState.showWorklist;
                const showRetryProcess = !isRsRow && !isListing && actionState.showRetryProcess;
                const showRetrySync = !isRsRow && !isListing && actionState.showRetrySync;
                const showRetryGeneric = !isRsRow && !isListing && actionState.showRetryGeneric;
                const showFailedError =
                  Boolean(r.errorMessage) &&
                  (String(badgeStatus).toLowerCase() === "failed" ||
                    ((isListing || isRsRow) &&
                      String(r.status).toLowerCase() === "failed" &&
                      !actionState.phase4Complete));

                return (
                  <tr
                    key={r.id}
                    data-upload-id={r.id}
                    className={[
                      "border-b border-border last:border-0",
                      selectedIds.has(r.id) ? "bg-muted/30" : "",
                    ].join(" ")}
                  >
                    {/* Checkbox */}
                    <td className="px-3 py-3">
                      <button
                        type="button"
                        onClick={() => toggleRow(r.id)}
                        aria-label={selectedIds.has(r.id) ? "Deselect row" : "Select row"}
                        disabled={busy}
                        className="text-muted-foreground hover:text-foreground disabled:opacity-40"
                      >
                        {selectedIds.has(r.id)
                          ? <CheckSquare className="h-4 w-4 text-primary" aria-hidden />
                          : <Square className="h-4 w-4" aria-hidden />}
                      </button>
                    </td>

                    {/* File name / ID */}
                    <td className="px-4 py-3">
                      <div className="flex max-w-[160px] flex-col gap-0.5" title={`${r.id}\n${r.file_name}`}>
                        <span className="truncate text-xs font-medium text-foreground">{r.file_name}</span>
                        <span className="font-mono text-[10px] text-muted-foreground">{r.id.slice(0, 8)}…</span>
                      </div>
                    </td>

                    {/* Report type dropdown */}
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-1.5">
                        <select
                          value={rt}
                          onChange={async (e) => {
                            const v = e.target.value as RawReportType;
                            const prevType = r.report_type;
                            reportTypeOverrideRef.current.set(r.id, v);
                            setRows((prev) =>
                              prev.map((row) => row.id === r.id ? { ...row, report_type: v } : row),
                            );
                            const res = await updateRawReportType({ uploadId: r.id, reportType: v, actorUserId });
                            if (res.ok) {
                              setReportTypeSaveFlash((m) => ({ ...m, [r.id]: true }));
                              window.setTimeout(() => {
                                setReportTypeSaveFlash((m) => { const n = { ...m }; delete n[r.id]; return n; });
                              }, 2200);
                            } else {
                              reportTypeOverrideRef.current.delete(r.id);
                              setRows((prev) =>
                                prev.map((row) => row.id === r.id ? { ...row, report_type: prevType } : row),
                              );
                              setLoadErr(res.error ?? "Update failed");
                            }
                          }}
                          className="h-8 max-w-[min(100%,22rem)] rounded-lg border border-border bg-background px-2 text-xs text-foreground"
                        >
                          {RAW_REPORT_TYPE_ORDER.map((v) => {
                            const s = REPORT_TYPE_SPECS[v];
                            const label =
                              s != null
                                ? `${s.shortLabel} (${s.description})`
                                : String(v);
                            return (
                              <option key={v} value={v}>
                                {label}
                              </option>
                            );
                          })}
                        </select>
                        {reportTypeSaveFlash[r.id] && (
                          <Check className="h-4 w-4 shrink-0 text-emerald-600 dark:text-emerald-400" aria-label="Saved" />
                        )}
                      </div>
                    </td>

                    {/* Uploader */}
                    <td className="px-4 py-3 font-mono text-xs text-muted-foreground">
                      {r.created_by_name ?? r.created_by ?? "—"}
                    </td>

                    {/* Date */}
                    <td className="whitespace-nowrap px-4 py-3 text-xs text-muted-foreground">
                      {new Date(r.created_at).toLocaleString()}
                    </td>

                    {/* Status badge / live progress */}
                    <td className="px-4 py-3">
                      {processingIds.has(r.id) ? (
                        // Live Phase 2 progress — shows X / Y rows (FPS + metadata; guard stale Phase-1 totals)
                        (() => {
                          const pct  = num(metaObj?.process_progress, 0);
                          const done = num(metaObj?.row_count,         0);
                          const rawTot  = num(metaObj?.total_rows,        0);
                          const im = metaObj?.import_metrics as { current_phase?: string } | undefined;
                          const phaseLbl =
                            im?.current_phase === "staging"
                              ? "Phase 2 — Stage to amazon_staging"
                              : formatImportPhaseLabel(im?.current_phase ?? "staging");
                          const tot =
                            rawTot > 0 && done > 0 && rawTot > done * 1.18
                              ? done
                              : rawTot;
                          return (
                            <div className="min-w-[120px]">
                              <div className="mb-1 flex flex-wrap items-center justify-between gap-x-2 gap-y-0.5 text-[10px] font-medium text-sky-600">
                                <span className="flex items-center gap-1">
                                  <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
                                  {phaseLbl}
                                </span>
                                <span className="tabular-nums text-muted-foreground">
                                  {done > 0 || tot > 0
                                    ? `rows ${done.toLocaleString()} / ${tot > 0 ? tot.toLocaleString() : "…"}`
                                    : pct > 0 ? `${pct}%` : "…"}
                                </span>
                              </div>
                              <div className="relative h-1.5 overflow-hidden rounded-full bg-muted">
                                <div
                                  className="absolute inset-0 animate-pulse rounded-full bg-sky-400/40"
                                />
                                <div
                                  className="h-full rounded-full bg-sky-500 transition-all duration-700"
                                  style={{ width: `${Math.max(5, pct)}%` }}
                                />
                              </div>
                            </div>
                          );
                        })()
                      ) : genericIds.has(r.id) ? (
                        (() => {
                          const gp = num(metaObj?.process_progress, 0);
                          return (
                            <div className="min-w-[120px]">
                              <div className="mb-1 flex flex-wrap items-center justify-between gap-x-2 gap-y-0.5 text-[10px] font-medium text-emerald-700 dark:text-emerald-400">
                                <span className="flex items-center gap-1">
                                  <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
                                  Phase 4 — Generic
                                </span>
                                <span className="tabular-nums text-muted-foreground">{gp > 0 ? `${gp}%` : "…"}</span>
                              </div>
                              <div className="relative h-1.5 overflow-hidden rounded-full bg-muted">
                                <div className="absolute inset-0 animate-pulse rounded-full bg-emerald-400/40" />
                                <div
                                  className="h-full rounded-full bg-emerald-500 transition-all duration-500"
                                  style={{ width: `${Math.max(5, Math.min(100, gp))}%` }}
                                />
                              </div>
                            </div>
                          );
                        })()
                      ) : syncingIds.has(r.id) ? (
                        (() => {
                          const syncPct = num(metaObj?.sync_progress, 0);
                          const im = metaObj?.import_metrics as {
                            current_phase?: string;
                            rows_synced?: number;
                            total_staging_rows?: number;
                          } | undefined;
                          const phaseLbl =
                            im?.current_phase === "sync"
                              ? "Phase 3 — Raw sync"
                              : formatImportPhaseLabel(im?.current_phase ?? "sync");
                          const rs = num(im?.rows_synced, -1);
                          const ts = num(im?.total_staging_rows, -1);
                          const rowHint =
                            rs >= 0 && ts >= 0
                              ? `rows ${rs.toLocaleString()} / ${ts.toLocaleString()}`
                              : `${syncPct}%`;
                          return (
                            <div className="min-w-[120px]">
                              <div className="mb-1 flex flex-wrap items-center justify-between gap-x-2 gap-y-0.5 text-[10px] font-medium text-violet-600">
                                <span className="flex items-center gap-1">
                                  <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
                                  {phaseLbl}
                                </span>
                                <span className="tabular-nums text-muted-foreground">{rowHint}</span>
                              </div>
                              <div className="relative h-1.5 overflow-hidden rounded-full bg-muted">
                                <div className="absolute inset-0 animate-pulse rounded-full bg-violet-400/40" />
                                <div
                                  className="h-full rounded-full bg-violet-500 transition-all duration-500"
                                  style={{ width: `${Math.max(5, Math.min(100, syncPct))}%` }}
                                />
                              </div>
                            </div>
                          );
                        })()
                      ) : (
                        <div className="flex flex-col gap-0.5">
                          <span
                            className={[
                              "inline-flex flex-wrap items-center gap-x-1.5 rounded-full px-2 py-0.5 text-xs font-medium",
                              statusColorClass(badgeStatus),
                            ].join(" ")}
                          >
                            <span>
                              {rsBadge ??
                                listingUi?.phaseBadgeText ??
                                statusLabel(badgeStatus, r.upload_progress, { listing: isListing })}
                            </span>
                            {(r.status === "uploading" || r.status === "processing") &&
                              r.upload_progress > 0 && r.upload_progress < 100 && (
                                <span className="font-normal tabular-nums text-muted-foreground">
                                  {r.upload_progress}%
                                </span>
                              )}
                          </span>
                          {showFailedError && (
                            <span className="max-w-[180px] truncate text-[10px] text-destructive" title={r.errorMessage!}>
                              {r.errorMessage}
                            </span>
                          )}
                        </div>
                      )}
                    </td>

                    {/* Row count — Phase 3+: sync/stage when present; else row_count / CSV total */}
                    <td className="px-4 py-3 text-right tabular-nums text-xs text-muted-foreground">
                      {(() => {
                        if (isListing) {
                          if (!listingUi) return "—";
                          return (
                            <span
                              title="Listing metrics (shared resolver with top card)."
                              className="block max-w-[min(100%,20rem)] cursor-help text-right text-[11px] leading-snug text-muted-foreground"
                            >
                              {listingUi.rowMetricsLine}
                            </span>
                          );
                        }
                        const stagedCount = num(
                          (metaObj as { staging_row_count?: unknown } | undefined)?.staging_row_count,
                          -1,
                        );
                        const syncCount = num(
                          (metaObj as { sync_row_count?: unknown } | undefined)?.sync_row_count,
                          -1,
                        );
                        const collapsed = num(
                          (metaObj as { sync_collapsed_by_dedupe?: unknown } | undefined)
                            ?.sync_collapsed_by_dedupe,
                          -1,
                        );
                        const processed = num(metaObj?.row_count, -1);
                        const total = num(metaObj?.total_rows, 0);
                        if (stagedCount >= 0 && syncCount >= 0) {
                          if (isRsRow) {
                            const im = metaObj?.import_metrics as
                              | { rows_duplicate_against_existing?: number }
                              | undefined;
                            const skippedDup =
                              num(im?.rows_duplicate_against_existing, -1) >= 0
                                ? num(im?.rows_duplicate_against_existing, 0)
                                : num(metaObj?.sync_collapsed_by_dedupe, 0);
                            const genMeta = num(metaObj?.removal_shipment_phase4_generic_rows_written, -1);
                            const fpsRow = r.file_processing_status;
                            const fpsObj =
                              fpsRow && typeof fpsRow === "object"
                                ? (fpsRow as {
                                    generic_rows_written?: unknown;
                                    raw_rows_written?: unknown;
                                    raw_rows_skipped_existing?: unknown;
                                  })
                                : null;
                            const rawNewF = fpsObj ? num(fpsObj.raw_rows_written, -1) : -1;
                            const rawSkipF = fpsObj ? num(fpsObj.raw_rows_skipped_existing, -1) : -1;
                            const genFps = fpsObj ? num(fpsObj.generic_rows_written, -1) : -1;
                            const genericDone = genMeta >= 0 ? genMeta : genFps;
                            const sub: string[] = [];
                            if (genericDone >= 0) sub.push(`generic ${genericDone.toLocaleString()}`);
                            if (skippedDup > 0) sub.push(`duplicates skipped ${skippedDup.toLocaleString()}`);
                            const main =
                              rawNewF >= 0 || rawSkipF >= 0
                                ? `staged ${stagedCount.toLocaleString()} · raw new ${(rawNewF >= 0 ? rawNewF : syncCount).toLocaleString()} · raw skipped ${(rawSkipF >= 0 ? rawSkipF : 0).toLocaleString()}`
                                : `raw ${syncCount.toLocaleString()} / staged ${stagedCount.toLocaleString()}`;
                            return (
                              <span
                                title="Removal shipment: FPS raw_rows_written / raw_rows_skipped_existing; staged = staging rows; duplicates skipped = lines already archived; generic = Phase 4 rows."
                                className="flex max-w-[min(100%,22rem)] cursor-help flex-wrap justify-end gap-x-1 gap-y-0.5 text-right leading-snug"
                              >
                                <span className="tabular-nums">{main}</span>
                                {sub.length > 0 && (
                                  <span className="block w-full text-[10px] text-muted-foreground">
                                    {sub.join(" · ")}
                                  </span>
                                )}
                              </span>
                            );
                          }
                          const dupInFile = num(metaObj?.sync_duplicate_in_batch_rows, -1);
                          const im = metaObj?.import_metrics as
                            | {
                                rows_synced_new?: number;
                                rows_synced_updated?: number;
                                rows_synced_unchanged?: number;
                                rows_duplicate_against_existing?: number;
                              }
                            | undefined;
                          const nNew = num(im?.rows_synced_new, -1);
                          const nUpd = num(im?.rows_synced_updated, -1);
                          const nUn = num(im?.rows_synced_unchanged, -1);
                          const nDupDb = num(im?.rows_duplicate_against_existing, -1);
                          const extra =
                            collapsed > 0
                              ? ` — ${collapsed.toLocaleString()} net merged vs staging (key collision)`
                              : "";
                          const dupNote =
                            dupInFile > 0 ? ` · ${dupInFile.toLocaleString()} dup key in batch` : "";
                          const metricNote =
                            nNew >= 0 && nUpd >= 0 && nUn >= 0 && nDupDb >= 0
                              ? ` · new ${nNew.toLocaleString()} · upd ${nUpd.toLocaleString()} · same ${nUn.toLocaleString()} · dup DB ${nDupDb.toLocaleString()}`
                              : "";
                          return (
                            <span
                              title={
                                "Phase 3: domain rows upserted / staging rows. " +
                                (collapsed > 0
                                  ? `Net ${collapsed} lines did not add a distinct row (duplicate key vs staging). `
                                  : "") +
                                (dupInFile > 0
                                  ? `${dupInFile} duplicate conflict key(s) collapsed inside a single upsert batch.`
                                  : "") +
                                (metricNote ? " Metrics: new / updated / unchanged / duplicate vs DB." : "")
                              }
                              className="flex max-w-[min(100%,22rem)] cursor-help flex-wrap justify-end gap-x-1 gap-y-0.5 text-right leading-snug"
                            >
                              <span className="tabular-nums">
                                rows {syncCount.toLocaleString()} / {stagedCount.toLocaleString()}
                              </span>
                              {(extra || dupNote || metricNote) && (
                                <span className="block w-full text-[10px] text-muted-foreground">
                                  {extra}
                                  {dupNote}
                                  {metricNote}
                                </span>
                              )}
                            </span>
                          );
                        }
                        if (processed < 0 && total > 0) return `— / ${total.toLocaleString()}`;
                        if (processed < 0) return "—";
                        if (total > 0)
                          return `${processed.toLocaleString()} / ${total.toLocaleString()}`;
                        return processed.toLocaleString();
                      })()}
                    </td>

                    {/* Listing pipeline — same resolver as top card */}
                    <td className="px-3 py-3 align-top">
                      {isListing && listingUi ? (
                        <ListingPipelineHistoryCell status={r.status} metadata={metaObj} fps={r.file_processing_status} />
                      ) : (
                        <span className="text-xs text-muted-foreground">—</span>
                      )}
                    </td>

                    {/* Actions */}
                    <td className="px-4 py-3 text-right">
                      <div className="inline-flex flex-wrap items-center justify-end gap-2">

                        {/* Map Columns — Phase 1 gap fill */}
                        {showMapColumns && (
                          <button
                            type="button"
                            onClick={() => setMappingRow(r)}
                            disabled={busy}
                            aria-label={`Map columns for ${r.file_name}`}
                            className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-amber-400/60 bg-amber-50 px-3 text-xs font-semibold text-amber-800 shadow-sm transition hover:bg-amber-100 disabled:opacity-50 dark:border-amber-600/40 dark:bg-amber-950/30 dark:text-amber-300"
                          >
                            <MapPin className="h-3.5 w-3.5" aria-hidden />
                            Map Columns
                          </button>
                        )}

                        {/* Process — Phase 2 */}
                        {showProcess && (
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() => void runProcess(r)}
                            aria-label={
                              isListing
                                ? `Process Data for ${r.file_name} (listing catalog)`
                                : `Process ${r.file_name} to staging`
                            }
                            className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-sky-400/60 bg-sky-50 px-3 text-xs font-semibold text-sky-800 shadow-sm transition hover:bg-sky-100 disabled:opacity-50 dark:border-sky-600/40 dark:bg-sky-950/30 dark:text-sky-300"
                          >
                            {processingIds.has(r.id) ? (
                              <>
                                <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                                Processing…
                              </>
                            ) : isRsRow && rsRowCta === "retry_process" ? (
                              "Retry Process"
                            ) : isListing && listingUi ? (
                              listingUi.rowActionLabel
                            ) : (
                              "Process"
                            )}
                          </button>
                        )}

                        {/* Sync — Phase 3 */}
                        {showSync && (
                          <button
                            type="button"
                            disabled={busy || Boolean(isListing && listingUi)}
                            title={
                              isListing && listingUi
                                ? "Raw archive runs inside Process for listing imports."
                                : undefined
                            }
                            onClick={() => {
                              if (isListing && listingUi) return;
                              void runSync(r);
                            }}
                            aria-label={`Sync ${r.file_name} to domain tables`}
                            className={[
                              "inline-flex h-8 items-center gap-1.5 rounded-lg border px-3 text-xs font-semibold shadow-sm transition disabled:opacity-50",
                              isListing && listingUi
                                ? "cursor-not-allowed border-emerald-500/40 bg-emerald-500/10 text-emerald-900 dark:text-emerald-100"
                                : "border-violet-400/60 bg-violet-50 text-violet-800 hover:bg-violet-100 dark:border-violet-600/40 dark:bg-violet-950/30 dark:text-violet-300",
                            ].join(" ")}
                          >
                            {isListing && listingUi ? (
                              <Lock className="h-3.5 w-3.5 shrink-0 opacity-80" aria-hidden />
                            ) : syncingIds.has(r.id) ? (
                              <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                            ) : (
                              <CheckCircle2 className="h-3.5 w-3.5 shrink-0 opacity-90" aria-hidden />
                            )}
                            {syncingIds.has(r.id) && !(isListing && listingUi) ? (
                              <>Syncing…</>
                            ) : isRsRow && rsRowCta === "retry_sync" ? (
                              "Retry Sync"
                            ) : isListing && listingUi ? (
                              "Sync (in Process)"
                            ) : (
                              "Sync"
                            )}
                          </button>
                        )}

                        {showGeneric && (
                          <button
                            type="button"
                            disabled={busy || Boolean(isListing && listingUi)}
                            title={
                              isListing && listingUi
                                ? "Catalog merge runs inside Process for listing imports."
                                : undefined
                            }
                            onClick={() => {
                              if (isListing && listingUi) return;
                              void runGeneric(r);
                            }}
                            aria-label={`Run generic phase for ${r.file_name}`}
                            className={[
                              "inline-flex h-8 items-center gap-1.5 rounded-lg border px-3 text-xs font-semibold shadow-sm transition disabled:opacity-50",
                              isListing && listingUi
                                ? "cursor-not-allowed border-violet-500/40 bg-violet-500/10 text-violet-950 dark:text-violet-100"
                                : "border-emerald-400/60 bg-emerald-50 text-emerald-900 hover:bg-emerald-100 dark:border-emerald-600/40 dark:bg-emerald-950/30 dark:text-emerald-200",
                            ].join(" ")}
                          >
                            {isListing && listingUi ? (
                              <Lock className="h-3.5 w-3.5 shrink-0 opacity-80" aria-hidden />
                            ) : genericIds.has(r.id) ? (
                              <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                            ) : null}
                            {genericIds.has(r.id) && !(isListing && listingUi) ? (
                              <>Generic…</>
                            ) : isRsRow ? (
                              rsRowCta === "generic_retry" ? "Retry Generic (shipments)" : "Generic (shipments)"
                            ) : isListing && listingUi ? (
                              "Generic (in Process)"
                            ) : (
                              "Generic (shipments)"
                            )}
                          </button>
                        )}

                        {showWorklist && (
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() => void runWorklist(r)}
                            aria-label={`Generate worklist for ${r.file_name}`}
                            className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-amber-400/60 bg-amber-50 px-3 text-xs font-semibold text-amber-900 shadow-sm transition hover:bg-amber-100 disabled:opacity-50 dark:border-amber-600/40 dark:bg-amber-950/30 dark:text-amber-200"
                          >
                            {worklistingIds.has(r.id) ? (
                              <>
                                <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                                Worklist…
                              </>
                            ) : (
                              "Generate Worklist"
                            )}
                          </button>
                        )}

                        {/* In-progress server-side indicator */}
                        {r.status === "processing" && !processingIds.has(r.id) && !syncingIds.has(r.id) && !genericIds.has(r.id) && (
                          <div className="flex flex-col items-end gap-1">
                            {r.errorMessage ? (
                              <span className="max-w-[200px] truncate text-[10px] font-medium text-destructive" title={r.errorMessage}>
                                ✕ {r.errorMessage}
                              </span>
                            ) : (
                              <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
                                <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                                Working…
                              </span>
                            )}
                            <button
                              type="button"
                              disabled={busy}
                              onClick={() => void runResetStuck(r)}
                              aria-label={`Reset stuck import ${r.file_name}`}
                              className="inline-flex h-7 items-center gap-1 rounded-lg border border-amber-400/60 bg-amber-50 px-2 text-[11px] font-semibold text-amber-800 shadow-sm transition hover:bg-amber-100 disabled:opacity-50 dark:border-amber-600/40 dark:bg-amber-950/30 dark:text-amber-300"
                            >
                              {resettingIds.has(r.id) ? (
                                <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
                              ) : (
                                <RefreshCw className="h-3 w-3" aria-hidden />
                              )}
                              Reset Stuck
                            </button>
                          </div>
                        )}

                        {/* Retry Process — Phase 2 failed */}
                        {showRetryProcess && (
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() => void runProcess(r)}
                            aria-label={`Retry processing ${r.file_name}`}
                            className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-amber-400/60 bg-amber-50 px-3 text-xs font-semibold text-amber-800 shadow-sm transition hover:bg-amber-100 disabled:opacity-50 dark:border-amber-600/40 dark:bg-amber-950/30 dark:text-amber-300"
                          >
                            {processingIds.has(r.id) ? (
                              <>
                                <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                                Retrying…
                              </>
                            ) : (
                              <>
                                <RotateCcw className="h-3.5 w-3.5" aria-hidden />
                                Retry Process
                              </>
                            )}
                          </button>
                        )}

                        {/* Retry Sync — Phase 3 failed */}
                        {showRetrySync && (
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() => void runSync(r)}
                            aria-label={`Retry syncing ${r.file_name}`}
                            className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-violet-400/60 bg-violet-50 px-3 text-xs font-semibold text-violet-800 shadow-sm transition hover:bg-violet-100 disabled:opacity-50 dark:border-violet-600/40 dark:bg-violet-950/30 dark:text-violet-300"
                          >
                            {syncingIds.has(r.id) ? (
                              <>
                                <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                                Retrying…
                              </>
                            ) : (
                              <>
                                <RotateCcw className="h-3.5 w-3.5" aria-hidden />
                                Retry Sync
                              </>
                            )}
                          </button>
                        )}

                        {showRetryGeneric && (
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() => void runGeneric(r)}
                            aria-label={`Retry generic phase for ${r.file_name}`}
                            className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-emerald-400/60 bg-emerald-50 px-3 text-xs font-semibold text-emerald-900 shadow-sm transition hover:bg-emerald-100 disabled:opacity-50 dark:border-emerald-600/40 dark:bg-emerald-950/30 dark:text-emerald-200"
                          >
                            {genericIds.has(r.id) ? (
                              <>
                                <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                                Retrying…
                              </>
                            ) : (
                              <>
                                <RotateCcw className="h-3.5 w-3.5" aria-hidden />
                                {isListing ? "Retry Generic (catalog)" : "Retry Generic"}
                              </>
                            )}
                          </button>
                        )}

                        {/* Delete — always visible */}
                        <button
                          type="button"
                          disabled={busy || batchDeleting}
                          onClick={() => void runDeleteUpload(r)}
                          aria-label={`Delete ${r.file_name}`}
                          className="inline-flex h-8 items-center gap-1 rounded-lg border border-destructive/40 bg-background px-2.5 text-xs font-medium text-destructive shadow-sm transition hover:bg-destructive/10 disabled:opacity-50"
                        >
                          {deletingIds.has(r.id) ? (
                            <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                          ) : (
                            <Trash2 className="h-3.5 w-3.5" aria-hidden />
                          )}
                          Delete
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              });
              })()}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
