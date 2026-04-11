"use client";

import React, { useCallback, useEffect, useRef, useState } from "react";
import { Check, CheckSquare, FileSpreadsheet, Loader2, MapPin, RefreshCw, RotateCcw, Search, Square, Trash2 } from "lucide-react";
import { AMAZON_LEDGER_UPLOAD_SOURCE } from "../../../lib/raw-report-upload-metadata";
import type { RawReportUploadRow } from "../../../lib/raw-report-upload-row";
import { deleteRawReportUpload, listRawReportUploads, resetStuckUpload, updateRawReportType } from "./import-actions";
import { ColumnMappingModal } from "./ColumnMappingModal";
import { useUserRole } from "../../../components/UserRoleContext";
import type { RawReportType } from "../../../lib/raw-report-types";
import { REPORT_TYPE_SPECS } from "../../../lib/csv-import-mapping";
import { isListingReportType, RAW_REPORT_TYPE_ORDER } from "../../../lib/raw-report-types";
import { useDebugMode } from "../../../components/DebugModeContext";
import { DatabaseTag } from "../../../components/DatabaseTag";

function num(v: unknown, fallback = 0): number {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  return fallback;
}

// Status badge label
function statusLabel(status: string, uploadProgress?: number, opts?: { listing?: boolean }): string {
  const listing = opts?.listing ?? false;
  switch (status) {
    case "mapped":
      return listing ? "Ready — Process Data" : "Phase 2: Process";
    case "staged":
      return "Phase 3: Sync";
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
      return listing ? "Complete (listing)" : "Synced";
    case "complete":
      return "Complete";
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
  if (status === "staged")
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

  /** Faster refresh while Process is running so listing progress updates are visible. */
  useEffect(() => {
    if (processingIds.size === 0) return;
    const t = setInterval(() => {
      void refresh();
    }, 700);
    return () => clearInterval(t);
  }, [processingIds.size, refresh]);

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

  // ── Phase 2: Process — staging (most types) OR direct catalog import (listing exports) ──
  const runProcess = async (r: RawReportUploadRow) => {
    setProcessingIds((prev) => new Set([...prev, r.id]));
    setLoadErr(null);
    try {
      const rt = String(r.report_type ?? "").trim();
      const listing = isListingReportType(rt);
      const res = await fetch(
        listing ? "/api/settings/imports/process" : "/api/settings/imports/stage",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ upload_id: r.id }),
        },
      );
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
    processingIds.has(id) || syncingIds.has(id) || deletingIds.has(id) || resettingIds.has(id);

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

      <div className="rounded-2xl border border-border bg-card shadow-sm">
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
            <strong className="text-foreground">Amazon listing exports:</strong> Phase 1 upload, then{" "}
            <strong>Process Data</strong> (parse raw rows + sync catalog). There is no separate Sync step and no
            Generate step.
          </p>
          <p>
            <strong className="text-foreground">Other report types:</strong> after upload,{" "}
            <strong>Process</strong> stages rows, then <strong>Sync</strong> writes domain tables (removal flows may
            add Generate Worklist).
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
          <table className="w-full min-w-[1080px] border-collapse text-left text-sm">
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
                      <td colSpan={8} className="px-4 py-8 text-center text-muted-foreground">
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

                // Determine what failed_phase metadata says so we show the right Retry button
                const failedPhase =
                  r.status === "failed"
                    ? ((metaObj?.failed_phase as "process" | "sync" | undefined) ?? "process")
                    : undefined;

                const showMapColumns = r.status === "needs_mapping";
                const showProcess =
                  !isLedgerSession &&
                  (r.status === "mapped" || r.status === "ready" || r.status === "uploaded");
                const showSync = r.status === "staged";
                // Retry buttons — shown when a row is in "failed" state
                const showRetryProcess =
                  r.status === "failed" && !isLedgerSession && failedPhase === "process";
                const showRetrySync =
                  r.status === "failed" && failedPhase === "sync";

                return (
                  <tr
                    key={r.id}
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
                        // Live Phase 2 progress — shows X / Y rows
                        (() => {
                          const pct  = num(metaObj?.process_progress, 0);
                          const done = num(metaObj?.row_count,         0);
                          const tot  = num(metaObj?.total_rows,        0);
                          return (
                            <div className="min-w-[120px]">
                              <div className="mb-1 flex items-center justify-between gap-2 text-[10px] font-medium text-sky-600">
                                <span className="flex items-center gap-1">
                                  <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
                                  Processing
                                </span>
                                <span className="tabular-nums text-muted-foreground">
                                  {done > 0 || tot > 0
                                    ? `${done.toLocaleString()}${tot > 0 ? ` / ${tot.toLocaleString()}` : ""}`
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
                      ) : syncingIds.has(r.id) ? (
                        // Live Phase 3 progress
                        <div className="min-w-[120px]">
                          <div className="mb-1 flex items-center gap-1 text-[10px] font-medium text-violet-600">
                            <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
                            Syncing…
                          </div>
                          <div className="relative h-1.5 overflow-hidden rounded-full bg-muted">
                            <div className="absolute inset-0 animate-pulse rounded-full bg-violet-400/40" />
                            <div className="h-1.5 w-1/2 rounded-full bg-violet-500" />
                          </div>
                        </div>
                      ) : (
                        <div className="flex flex-col gap-0.5">
                          <span
                            className={[
                              "inline-flex flex-wrap items-center gap-x-1.5 rounded-full px-2 py-0.5 text-xs font-medium",
                              statusColorClass(r.status),
                            ].join(" ")}
                          >
                            <span>{statusLabel(r.status, r.upload_progress, { listing: isListing })}</span>
                            {(r.status === "uploading" || r.status === "processing") &&
                              r.upload_progress > 0 && r.upload_progress < 100 && (
                                <span className="font-normal tabular-nums text-muted-foreground">
                                  {r.upload_progress}%
                                </span>
                              )}
                          </span>
                          {r.status === "failed" && r.errorMessage && (
                            <span className="max-w-[180px] truncate text-[10px] text-destructive" title={r.errorMessage}>
                              {r.errorMessage}
                            </span>
                          )}
                        </div>
                      )}
                    </td>

                    {/* Row count — Phase 3+: sync/stage when present; else row_count / CSV total */}
                    <td className="px-4 py-3 text-right tabular-nums text-xs text-muted-foreground">
                      {(() => {
                        const listingRaw = num(metaObj?.catalog_listing_raw_rows_stored, -1);
                        const listingData = num(metaObj?.catalog_listing_data_rows_seen, -1);
                        const listingNew = num(
                          metaObj?.catalog_listing_canonical_rows_new ?? metaObj?.catalog_listing_canonical_rows_inserted,
                          -1,
                        );
                        const listingUnchanged = num(
                          metaObj?.catalog_listing_canonical_rows_unchanged ??
                            metaObj?.catalog_listing_canonical_rows_unchanged_or_merged,
                          -1,
                        );
                        if (
                          isListing &&
                          listingRaw >= 0 &&
                          listingData >= 0 &&
                          (r.status === "synced" || r.status === "processing" || r.status === "complete")
                        ) {
                          const syncNote =
                            r.status === "synced" && listingNew >= 0 && listingUnchanged >= 0
                              ? ` · new ${listingNew.toLocaleString()} · unchanged ${listingUnchanged.toLocaleString()}`
                              : "";
                          return (
                            <span
                              title="Listing import: raw rows stored vs non-empty data lines; canonical counts after Process Data."
                              className="cursor-help"
                            >
                              {`${listingRaw.toLocaleString()} raw / ${listingData.toLocaleString()} data${syncNote}`}
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
                          const extra =
                            collapsed > 0
                              ? ` — ${collapsed.toLocaleString()} staging line(s) merged (same unique key)`
                              : "";
                          return (
                            <span
                              title={
                                "Phase 3: unique rows upserted / rows in staging. " +
                                (collapsed > 0
                                  ? `${collapsed} CSV lines shared a key with another line and did not add a new DB row.`
                                  : "")
                              }
                              className="cursor-help"
                            >
                              {`${syncCount.toLocaleString()} / ${stagedCount.toLocaleString()}${extra}`}
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
                            ) : isListing ? (
                              "Process Data"
                            ) : (
                              "Process"
                            )}
                          </button>
                        )}

                        {/* Sync — Phase 3 */}
                        {showSync && (
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() => void runSync(r)}
                            aria-label={`Sync ${r.file_name} to domain tables`}
                            className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-violet-400/60 bg-violet-50 px-3 text-xs font-semibold text-violet-800 shadow-sm transition hover:bg-violet-100 disabled:opacity-50 dark:border-violet-600/40 dark:bg-violet-950/30 dark:text-violet-300"
                          >
                            {syncingIds.has(r.id) ? (
                              <>
                                <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                                Syncing…
                              </>
                            ) : (
                              "Sync"
                            )}
                          </button>
                        )}

                        {/* In-progress server-side indicator */}
                        {r.status === "processing" && !processingIds.has(r.id) && !syncingIds.has(r.id) && (
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
