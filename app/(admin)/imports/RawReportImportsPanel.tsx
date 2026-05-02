"use client";

import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  Check,
  CheckSquare,
  FileSpreadsheet,
  Loader2,
  MapPin,
  RotateCcw,
  Search,
  Square,
  Trash2,
} from "lucide-react";
import { AMAZON_LEDGER_UPLOAD_SOURCE } from "../../../lib/raw-report-upload-metadata";
import type { RawReportUploadRow } from "../../../lib/raw-report-upload-row";
import {
  deleteRawReportUpload,
  listRawReportUploads,
  resetStuckUpload,
  resetStuckProductIdentityUpload,
  updateRawReportType,
} from "./import-actions";
import { ColumnMappingModal } from "./ColumnMappingModal";
import { useUserRole } from "../../../components/UserRoleContext";
import type { RawReportType } from "../../../lib/raw-report-types";
import { REPORT_TYPE_SPECS } from "../../../lib/csv-import-mapping";
import { RAW_REPORT_TYPE_ORDER } from "../../../lib/raw-report-types";
import { DatabaseTag } from "../../../components/DatabaseTag";
import {
  buildUnifiedPipeline,
  pipelineBadgeColor,
  stepBarColor,
  stepBadgeColor,
  type PipelineStep,
  type UnifiedPipelineModel,
} from "../../../lib/pipeline/unified-import-pipeline";

// ── Helpers ───────────────────────────────────────────────────────────────────

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

async function readImportApiJson<T extends { ok?: boolean; error?: string; details?: string }>(
  res: Response,
): Promise<T> {
  const text = await res.text();
  if (!text.trim()) {
    return {
      ok: false,
      error: `Import API returned an empty ${res.status} response.`,
      details: "",
    } as T;
  }
  try {
    return JSON.parse(text) as T;
  } catch {
    return {
      ok: false,
      error: `Import API returned non-JSON ${res.status} response.`,
      details: text.slice(0, 2000),
    } as T;
  }
}

// ── Compact pipeline cell ─────────────────────────────────────────────────────

function PipelineCell({ pipeline }: { pipeline: UnifiedPipelineModel }) {
  const visibleSteps = pipeline.steps.filter((s) => s.tone !== "skipped");
  return (
    <div className="w-full min-w-0 max-w-[280px] space-y-1 overflow-hidden">
      <ol className="w-full min-w-0 space-y-1" aria-label="Import pipeline">
        {visibleSteps.map((s) => (
          <PipelineStepRow key={s.key} step={s} />
        ))}
      </ol>
    </div>
  );
}

function PipelineStepRow({ step }: { step: PipelineStep }) {
  const bar = stepBarColor(step.tone);
  const fill = Math.min(100, Math.max(0, step.pct));
  const isActive = step.tone === "active";
  return (
    <li className="w-full min-w-0 space-y-0.5 text-[10px] leading-tight overflow-hidden">
      <div className="flex items-baseline justify-between gap-1 min-w-0">
        <span className="min-w-0 truncate font-medium text-foreground">
          {step.label}
        </span>
        <span className="shrink-0 tabular-nums text-muted-foreground">
          {step.tone === "done"
            ? "\u2713"
            : step.tone === "failed"
              ? "\u2715"
              : step.pct > 0
                ? `${Math.round(step.pct)}%`
                : ""}
        </span>
      </div>
      <div className="relative h-1.5 w-full min-w-0 overflow-hidden rounded-full bg-muted/90">
        {isActive && (
          <div className="absolute inset-0 animate-pulse rounded-full bg-sky-400/30" />
        )}
        <div
          className={`h-full rounded-full transition-[width] duration-500 ease-out ${bar}`}
          style={{ width: `${fill}%` }}
        />
      </div>
      {step.rightLabel && step.rightLabel !== "\u2014" && step.rightLabel !== "N/A" && (
        <span className="block truncate text-[9px] text-muted-foreground">
          {step.rightLabel}
        </span>
      )}
    </li>
  );
}

// ── Main panel ────────────────────────────────────────────────────────────────

type RawReportImportsPanelProps = {
  organizationId: string | null;
  refreshSignal?: number;
};

export function RawReportImportsPanel({
  organizationId,
  refreshSignal = 0,
}: RawReportImportsPanelProps) {
  const { actorUserId } = useUserRole();
  const [rows, setRows] = useState<RawReportUploadRow[]>([]);
  const [loadErr, setLoadErr] = useState<string | null>(null);
  const [tableSearch, setTableSearch] = useState("");
  const [reportTypeSaveFlash, setReportTypeSaveFlash] = useState<Record<string, boolean>>({});

  const [busyIds, setBusyIds] = useState<Record<string, string>>({});
  const [deletingIds, setDeletingIds] = useState<Set<string>>(() => new Set());

  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => new Set());
  const [batchDeleting, setBatchDeleting] = useState(false);

  const [mappingRow, setMappingRow] = useState<RawReportUploadRow | null>(null);

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const reportTypeOverrideRef = useRef<Map<string, string>>(new Map());
  const listFetchGenerationRef = useRef(0);

  const markBusy = (id: string, phase: string) =>
    setBusyIds((prev) => ({ ...prev, [id]: phase }));
  const clearBusy = (id: string) =>
    setBusyIds((prev) => {
      const next = { ...prev };
      delete next[id];
      return next;
    });

  // ── Data fetching ─────────────────────────────────────────────────────────

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
    // 10s instead of 5s — the active pipeline card in UniversalImporter
    // already polls by upload_id every 1.5s. The history panel only needs
    // to catch newly-uploaded files from other sessions or status changes
    // that the active card poller doesn't see.
    pollRef.current = setInterval(() => void refresh(), 10000);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [refresh]);

  useEffect(() => {
    const hasBusy = Object.keys(busyIds).length > 0;
    if (!hasBusy) return;
    const t = setInterval(() => void refresh(), 1200);
    return () => clearInterval(t);
  }, [busyIds, refresh]);

  // ── Operations ──────────────────────────────────────────────────────────────

  const runDeleteUpload = async (r: RawReportUploadRow) => {
    if (
      !window.confirm(
        `Delete "${r.file_name}" and all associated data? This cannot be undone.`,
      )
    )
      return;
    setDeletingIds((prev) => new Set([...prev, r.id]));
    setRows((prev) => prev.map((row) => (row.id === r.id ? { ...row, status: "deleting" } : row)));
    setLoadErr(null);
    try {
      const res = await deleteRawReportUpload(r.id);
      if (!res.ok) {
        setLoadErr(res.error ?? "Delete failed.");
        return;
      }
      setRows((prev) => prev.filter((row) => row.id !== r.id));
      setSelectedIds((prev) => {
        const n = new Set(prev);
        n.delete(r.id);
        return n;
      });
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Delete failed.");
      await refresh();
    } finally {
      setDeletingIds((prev) => {
        const n = new Set(prev);
        n.delete(r.id);
        return n;
      });
    }
  };

  const runBatchDelete = async () => {
    if (selectedIds.size === 0) return;
    if (
      !window.confirm(
        `Delete ${selectedIds.size} selected import(s) and all associated data? This cannot be undone.`,
      )
    )
      return;
    setBatchDeleting(true);
    setLoadErr(null);
    const ids = [...selectedIds];
    try {
      for (const id of ids) {
        setRows((prev) => prev.filter((row) => row.id !== id));
        await deleteRawReportUpload(id);
      }
      setSelectedIds(new Set());
    } catch (e) {
      setLoadErr(
        e instanceof Error ? e.message : "Batch delete partially failed.",
      );
    } finally {
      setBatchDeleting(false);
      await refresh();
    }
  };

  const runProcess = async (r: RawReportUploadRow) => {
    markBusy(r.id, "processing");
    setLoadErr(null);
    try {
      const res = await fetch("/api/settings/imports/process", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: r.id }),
      });
      const json = await readImportApiJson<{ ok?: boolean; error?: string; details?: string }>(res);
      if (!res.ok || !json.ok) {
        setLoadErr(json.details || json.error || "Processing failed.");
      }
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Processing failed.");
    } finally {
      clearBusy(r.id);
    }
  };

  const runSync = async (r: RawReportUploadRow) => {
    markBusy(r.id, "syncing");
    setLoadErr(null);
    try {
      const res = await fetch("/api/settings/imports/sync", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: r.id }),
      });
      const json = await readImportApiJson<{ ok?: boolean; error?: string; details?: string }>(res);
      if (!res.ok || !json.ok) {
        setLoadErr(json.details || json.error || "Sync failed.");
      }
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Sync failed.");
    } finally {
      clearBusy(r.id);
    }
  };

  const runGeneric = async (r: RawReportUploadRow) => {
    markBusy(r.id, "generic");
    setLoadErr(null);
    try {
      const res = await fetch("/api/settings/imports/generic", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: r.id }),
      });
      const json = await readImportApiJson<{ ok?: boolean; error?: string; details?: string }>(res);
      if (!res.ok || !json.ok) {
        setLoadErr(json.details || json.error || "Generic phase failed.");
      }
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Generic phase failed.");
    } finally {
      clearBusy(r.id);
    }
  };

  const runWorklist = async (r: RawReportUploadRow) => {
    markBusy(r.id, "worklist");
    setLoadErr(null);
    try {
      const res = await fetch("/api/settings/imports/generate-worklist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: r.id }),
      });
      const json = await readImportApiJson<{ ok?: boolean; error?: string; details?: string }>(res);
      if (!res.ok || !json.ok) {
        setLoadErr(json.details || json.error || "Generate Worklist failed.");
      }
      await refresh();
    } catch (e) {
      setLoadErr(
        e instanceof Error ? e.message : "Generate Worklist failed.",
      );
    } finally {
      clearBusy(r.id);
    }
  };

  const runResetStuck = async (r: RawReportUploadRow) => {
    markBusy(r.id, "resetting");
    setLoadErr(null);
    try {
      const res = await resetStuckUpload({ uploadId: r.id, actorUserId });
      if (!res.ok) setLoadErr(res.error ?? "Reset failed.");
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Reset failed.");
    } finally {
      clearBusy(r.id);
    }
  };

  const runResetStuckProductIdentity = async (r: RawReportUploadRow) => {
    markBusy(r.id, "resetting-pi");
    setLoadErr(null);
    try {
      const res = await resetStuckProductIdentityUpload({ uploadId: r.id, actorUserId });
      if (!res.ok) {
        setLoadErr(res.error ?? "Reset failed.");
      } else if (res.stagingRowsDeleted != null) {
        setLoadErr(null);
      }
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Reset failed.");
    } finally {
      clearBusy(r.id);
    }
  };

  // ── Selection helpers ───────────────────────────────────────────────────────

  const toggleRow = (id: string) =>
    setSelectedIds((prev) => {
      const n = new Set(prev);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });

  const allSelected =
    rows.length > 0 && rows.every((r) => selectedIds.has(r.id));
  const toggleAll = () =>
    setSelectedIds(allSelected ? new Set() : new Set(rows.map((r) => r.id)));

  // ── Filter rows ─────────────────────────────────────────────────────────────

  const q = tableSearch.trim().toLowerCase();
  const filteredRows = q
    ? rows.filter(
        (r) =>
          r.file_name.toLowerCase().includes(q) ||
          (r.report_type ?? "").toLowerCase().includes(q),
      )
    : rows;

  return (
    <>
      {mappingRow && (
        <ColumnMappingModal
          row={mappingRow}
          onClose={() => setMappingRow(null)}
          onSaved={() => {
            setMappingRow(null);
            void refresh();
          }}
        />
      )}

      {loadErr && (
        <div className="mb-4 rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {loadErr}
        </div>
      )}

      <div
        id="data-import-history"
        className="rounded-2xl border border-border bg-card shadow-sm scroll-mt-20"
      >
        {/* Header */}
        <div className="flex items-center justify-between gap-3 border-b border-border px-5 py-4">
          <div className="flex min-w-0 flex-1 items-center gap-3">
            <div className="flex shrink-0 items-center gap-2">
              <FileSpreadsheet
                className="h-4 w-4 text-muted-foreground"
                aria-hidden
              />
              <h2 className="text-sm font-semibold text-foreground">
                Import History
              </h2>
            </div>
            <div className="relative w-full max-w-xs">
              <Search
                className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground"
                aria-hidden
              />
              <input
                type="search"
                placeholder="Search files…"
                value={tableSearch}
                onChange={(e) => setTableSearch(e.target.value)}
                className="h-8 w-full rounded-lg border border-border bg-background pl-8 pr-3 text-xs text-foreground outline-none placeholder:text-muted-foreground focus-visible:ring-2 focus-visible:ring-ring"
              />
            </div>
          </div>

          {selectedIds.size > 0 && (
            <button
              type="button"
              disabled={batchDeleting}
              onClick={() => void runBatchDelete()}
              className="shrink-0 inline-flex items-center gap-1.5 rounded-lg border border-destructive/50 bg-destructive/10 px-3 py-1.5 text-xs font-semibold text-destructive transition hover:bg-destructive/20 disabled:opacity-50"
            >
              {batchDeleting ? (
                <Loader2
                  className="h-3.5 w-3.5 animate-spin"
                  aria-hidden
                />
              ) : (
                <Trash2 className="h-3.5 w-3.5" aria-hidden />
              )}
              Delete {selectedIds.size} selected
            </button>
          )}
        </div>

        {/* Info text */}
        <div className="px-5 pt-2 pb-3 text-[11px] text-muted-foreground">
          <p>
            Every file follows: <strong className="text-foreground">Upload</strong> → <strong className="text-foreground">Process</strong> → <strong className="text-foreground">Sync</strong> → <strong className="text-foreground">Generic</strong> (when applicable).
          </p>
        </div>

        {/* Table */}
        <div className="relative max-h-[500px] overflow-x-auto overflow-y-auto rounded-b-2xl">
          <DatabaseTag table="raw_report_uploads" />
          <table className="w-full min-w-0 table-fixed border-collapse text-left text-sm">
            <colgroup>
              <col className="w-10" />
              <col className="w-[13%]" />
              <col className="w-[12%]" />
              <col className="w-[9%]" />
              <col className="w-[9%]" />
              <col className="w-[9%]" />
              <col className="w-[7%]" />
              <col className="w-[26%]" />
              <col className="w-[15%]" />
            </colgroup>
            <thead>
              <tr className="border-b border-border bg-muted/40 text-[10px] font-semibold uppercase tracking-wide text-muted-foreground">
                <th className="px-2 py-2.5">
                  <button
                    type="button"
                    onClick={toggleAll}
                    aria-label={
                      allSelected ? "Deselect all" : "Select all"
                    }
                    className="text-muted-foreground hover:text-foreground"
                  >
                    {allSelected ? (
                      <CheckSquare className="h-4 w-4" aria-hidden />
                    ) : (
                      <Square className="h-4 w-4" aria-hidden />
                    )}
                  </button>
                </th>
                <th className="px-2 py-2.5">Import</th>
                <th className="px-2 py-2.5">Type</th>
                <th className="px-2 py-2.5">Uploaded by</th>
                <th className="px-2 py-2.5">Date</th>
                <th className="px-2 py-2.5">Status</th>
                <th className="px-2 py-2.5 text-right">Rows</th>
                <th className="px-2 py-2.5">Pipeline</th>
                <th className="px-2 py-2.5 text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {filteredRows.length === 0 ? (
                <tr>
                  <td
                    colSpan={9}
                    className="px-4 py-8 text-center text-muted-foreground"
                  >
                    {rows.length === 0
                      ? "No imports yet."
                      : `No results for "${tableSearch}".`}
                  </td>
                </tr>
              ) : (
                filteredRows.map((r) => (
                  <HistoryRow
                    key={r.id}
                    row={r}
                    busy={!!busyIds[r.id]}
                    busyPhase={busyIds[r.id] ?? null}
                    isDeleting={deletingIds.has(r.id)}
                    isSelected={selectedIds.has(r.id)}
                    batchDeleting={batchDeleting}
                    onToggle={() => toggleRow(r.id)}
                    onProcess={() => void runProcess(r)}
                    onSync={() => void runSync(r)}
                    onGeneric={() => void runGeneric(r)}
                    onWorklist={() => void runWorklist(r)}
                    onDelete={() => void runDeleteUpload(r)}
                    onResetStuck={() => void runResetStuck(r)}
                    onResetStuckProductIdentity={() => void runResetStuckProductIdentity(r)}
                    onMapColumns={() => setMappingRow(r)}
                    onReportTypeChange={async (v: RawReportType) => {
                      const prevType = r.report_type;
                      reportTypeOverrideRef.current.set(r.id, v);
                      setRows((prev) =>
                        prev.map((row) =>
                          row.id === r.id
                            ? { ...row, report_type: v }
                            : row,
                        ),
                      );
                      const res = await updateRawReportType({
                        uploadId: r.id,
                        reportType: v,
                        actorUserId,
                      });
                      if (res.ok) {
                        setReportTypeSaveFlash((m) => ({
                          ...m,
                          [r.id]: true,
                        }));
                        window.setTimeout(() => {
                          setReportTypeSaveFlash((m) => {
                            const n = { ...m };
                            delete n[r.id];
                            return n;
                          });
                        }, 2200);
                      } else {
                        reportTypeOverrideRef.current.delete(r.id);
                        setRows((prev) =>
                          prev.map((row) =>
                            row.id === r.id
                              ? { ...row, report_type: prevType }
                              : row,
                          ),
                        );
                        setLoadErr(res.error ?? "Update failed");
                      }
                    }}
                    reportTypeSaved={!!reportTypeSaveFlash[r.id]}
                  />
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

// ── Individual row component (memoized for perf) ──────────────────────────────

type HistoryRowProps = {
  row: RawReportUploadRow;
  busy: boolean;
  busyPhase: string | null;
  isDeleting: boolean;
  isSelected: boolean;
  batchDeleting: boolean;
  onToggle: () => void;
  onProcess: () => void;
  onSync: () => void;
  onGeneric: () => void;
  onWorklist: () => void;
  onDelete: () => void;
  onResetStuck: () => void;
  onResetStuckProductIdentity: () => void;
  onMapColumns: () => void;
  onReportTypeChange: (v: RawReportType) => void;
  reportTypeSaved: boolean;
};

const HistoryRow = React.memo(function HistoryRow({
  row: r,
  busy,
  busyPhase,
  isDeleting,
  isSelected,
  batchDeleting,
  onToggle,
  onProcess,
  onSync,
  onGeneric,
  onWorklist,
  onDelete,
  onResetStuck,
  onResetStuckProductIdentity,
  onMapColumns,
  onReportTypeChange,
  reportTypeSaved
}: HistoryRowProps) {
  const rt = coerceReportType(r.report_type);
  const metaObj =
    r.metadata && typeof r.metadata === "object" && !Array.isArray(r.metadata)
      ? (r.metadata as Record<string, unknown>)
      : null;
  const isLedgerSession = metaObj?.source === AMAZON_LEDGER_UPLOAD_SOURCE;

  const pipeline = buildUnifiedPipeline({
    reportType: String(r.report_type ?? ""),
    status: r.status,
    metadata: metaObj,
    fps: r.file_processing_status as Record<string, unknown> | null,
    ui:
      busyPhase === "syncing"
        ? { isSyncing: true }
        : busyPhase === "generic"
          ? { isGenericing: true }
          : busyPhase === "worklist"
            ? { isWorklisting: true }
            : undefined,
  });

  const anyBusy = busy || isDeleting;

  const showMapColumns = r.status === "needs_mapping";
  const showProcess =
    !isLedgerSession &&
    pipeline.nextAction === "process" &&
    !busy;
  const showSync =
    !isLedgerSession &&
    pipeline.nextAction === "sync" &&
    !busy;
  const showGeneric =
    !isLedgerSession &&
    pipeline.nextAction === "generic" &&
    !busy;
  const showWorklist =
    !isLedgerSession &&
    pipeline.nextAction === "worklist" &&
    !busy;
  const showResetStuck =
    r.status === "processing" && !busy && r.report_type !== "PRODUCT_IDENTITY";
  const showResetStuckProductIdentity =
    r.report_type === "PRODUCT_IDENTITY" &&
    (r.status === "processing" || r.status === "staged" || r.status === "failed") &&
    !busy;

  return (
    <tr
      data-upload-id={r.id}
      className={[
        "border-b border-border last:border-0 transition-opacity",
        isSelected ? "bg-muted/30" : "",
        isDeleting ? "opacity-40" : "",
      ].join(" ")}
    >
      {/* Checkbox */}
      <td className="px-2 py-2.5 align-top">
        <button
          type="button"
          onClick={onToggle}
          aria-label={isSelected ? "Deselect row" : "Select row"}
          disabled={anyBusy}
          className="text-muted-foreground hover:text-foreground disabled:opacity-40"
        >
          {isSelected ? (
            <CheckSquare
              className="h-4 w-4 text-primary"
              aria-hidden
            />
          ) : (
            <Square className="h-4 w-4" aria-hidden />
          )}
        </button>
      </td>

      {/* File name */}
      <td className="px-2 py-2.5 align-top">
        <div
          className="flex min-w-0 flex-col gap-0.5 break-words"
          title={`${r.id}\n${r.file_name}`}
        >
          <span className="text-[11px] font-medium leading-snug text-foreground">
            {r.file_name}
          </span>
          <span className="font-mono text-[9px] text-muted-foreground">
            {r.id.slice(0, 8)}…
          </span>
        </div>
      </td>

      {/* Report type */}
      <td className="px-2 py-2.5 align-top">
        <div className="flex min-w-0 items-start gap-1">
          <select
            value={rt}
            onChange={(e) =>
              onReportTypeChange(e.target.value as RawReportType)
            }
            className="h-auto min-h-8 w-full min-w-0 max-w-full rounded-lg border border-border bg-background py-1 pl-1.5 pr-1 text-[10px] leading-tight text-foreground"
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
          {reportTypeSaved && (
            <Check
              className="h-4 w-4 shrink-0 text-emerald-600 dark:text-emerald-400"
              aria-label="Saved"
            />
          )}
        </div>
      </td>

      {/* Uploader */}
      <td className="px-2 py-2.5 align-top font-mono text-[10px] leading-snug text-muted-foreground">
        <span className="break-all">
          {r.created_by_name ?? r.created_by ?? "—"}
        </span>
      </td>

      {/* Date */}
      <td className="px-2 py-2.5 align-top text-[10px] leading-snug text-muted-foreground">
        <span className="break-words">
          {new Date(r.created_at).toLocaleString()}
        </span>
      </td>

      {/* Status badge */}
      <td className="px-2 py-2.5 align-top">
        <div className="flex min-w-0 flex-col gap-0.5">
          {busyPhase ? (
            <span className="flex items-center gap-1 rounded-md bg-sky-500/15 px-1.5 py-0.5 text-[10px] font-medium text-sky-700 dark:text-sky-300">
              <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
              {busyPhase === "processing"
                ? "Processing…"
                : busyPhase === "syncing"
                  ? "Syncing…"
                  : busyPhase === "generic"
                    ? "Generic…"
                    : busyPhase === "worklist"
                      ? "Worklist…"
                      : "Working…"}
            </span>
          ) : (
            <span
              className={[
                "inline-flex min-w-0 items-center rounded-md px-1.5 py-0.5 text-[10px] font-medium leading-snug",
                pipelineBadgeColor(pipeline.badgeStatus),
              ].join(" ")}
            >
              <span className="min-w-0 break-words">
                {pipeline.badgeLabel}
              </span>
            </span>
          )}
          {pipeline.isFailed && r.errorMessage && !busyPhase && (
            <span
              className="max-w-[180px] truncate text-[10px] text-destructive"
              title={r.errorMessage}
            >
              {r.errorMessage}
            </span>
          )}
        </div>
      </td>

      {/* Row count */}
      <td className="px-2 py-2.5 text-right align-top tabular-nums text-[10px] leading-snug text-muted-foreground">
        <span
          title={pipeline.rowMetricsLine}
          className="block min-w-0 cursor-help break-words text-right"
        >
          {pipeline.rowMetricsLine}
        </span>
      </td>

      {/* Pipeline */}
      <td className="px-2 py-2.5 align-top">
        <PipelineCell pipeline={pipeline} />
      </td>

      {/* Actions */}
      <td className="px-2 py-2.5 text-right align-top">
        <div className="inline-flex flex-wrap items-center justify-end gap-1.5">
          {showMapColumns && (
            <ActionButton
              onClick={onMapColumns}
              disabled={anyBusy}
              color="amber"
              icon={<MapPin className="h-3.5 w-3.5" aria-hidden />}
            >
              Map Columns
            </ActionButton>
          )}

          {showProcess && (
            <ActionButton
              onClick={onProcess}
              disabled={anyBusy}
              color="sky"
            >
              {pipeline.isFailed ? "Retry Process" : "Process"}
            </ActionButton>
          )}

          {busyPhase === "processing" && (
            <ActionButton disabled color="sky" icon={<Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />}>
              Processing…
            </ActionButton>
          )}

          {showSync && (
            <ActionButton
              onClick={onSync}
              disabled={anyBusy}
              color="violet"
            >
              {pipeline.isFailed ? "Retry Sync" : "Sync"}
            </ActionButton>
          )}

          {busyPhase === "syncing" && (
            <ActionButton disabled color="violet" icon={<Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />}>
              Syncing…
            </ActionButton>
          )}

          {showGeneric && (
            <ActionButton
              onClick={onGeneric}
              disabled={anyBusy}
              color="emerald"
            >
              {pipeline.isFailed ? "Retry Generic" : "Generic"}
            </ActionButton>
          )}

          {busyPhase === "generic" && (
            <ActionButton disabled color="emerald" icon={<Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />}>
              Generic…
            </ActionButton>
          )}

          {showWorklist && (
            <ActionButton
              onClick={onWorklist}
              disabled={anyBusy}
              color="amber"
            >
              Worklist
            </ActionButton>
          )}

          {busyPhase === "worklist" && (
            <ActionButton disabled color="amber" icon={<Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />}>
              Worklist…
            </ActionButton>
          )}

          {showResetStuck && (
            <ActionButton
              onClick={onResetStuck}
              disabled={anyBusy}
              color="amber"
              icon={<RotateCcw className="h-3 w-3" aria-hidden />}
              small
            >
              Reset
            </ActionButton>
          )}

          {showResetStuckProductIdentity && (
            <ActionButton
              onClick={onResetStuckProductIdentity}
              disabled={anyBusy}
              color="amber"
              icon={<RotateCcw className="h-3 w-3" aria-hidden />}
              small
            >
              Reset&nbsp;PI
            </ActionButton>
          )}

          <button
            type="button"
            disabled={anyBusy || batchDeleting}
            onClick={onDelete}
            aria-label={`Delete ${r.file_name}`}
            className="inline-flex h-7 items-center gap-1 rounded-lg border border-destructive/40 bg-background px-2 text-[11px] font-medium text-destructive shadow-sm transition hover:bg-destructive/10 disabled:opacity-50"
          >
            {isDeleting ? (
              <Loader2
                className="h-3.5 w-3.5 animate-spin"
                aria-hidden
              />
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

// ── Shared action button ──────────────────────────────────────────────────────

const COLOR_MAP: Record<string, string> = {
  sky: "border-sky-400/60 bg-sky-50 text-sky-800 hover:bg-sky-100 dark:border-sky-600/40 dark:bg-sky-950/30 dark:text-sky-300",
  violet: "border-violet-400/60 bg-violet-50 text-violet-800 hover:bg-violet-100 dark:border-violet-600/40 dark:bg-violet-950/30 dark:text-violet-300",
  emerald: "border-emerald-400/60 bg-emerald-50 text-emerald-900 hover:bg-emerald-100 dark:border-emerald-600/40 dark:bg-emerald-950/30 dark:text-emerald-200",
  amber: "border-amber-400/60 bg-amber-50 text-amber-800 hover:bg-amber-100 dark:border-amber-600/40 dark:bg-amber-950/30 dark:text-amber-300",
};

function ActionButton({
  children,
  onClick,
  disabled,
  color,
  icon,
  small,
}: {
  children: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  color: string;
  icon?: React.ReactNode;
  small?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={[
        "inline-flex items-center gap-1 rounded-lg border font-semibold shadow-sm transition disabled:opacity-50",
        small ? "h-6 px-2 text-[10px]" : "h-7 px-2.5 text-[11px]",
        COLOR_MAP[color] ?? COLOR_MAP.sky,
      ].join(" ")}
    >
      {icon}
      {children}
    </button>
  );
}
