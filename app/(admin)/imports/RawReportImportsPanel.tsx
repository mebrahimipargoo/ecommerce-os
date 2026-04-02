"use client";

import React, { useCallback, useEffect, useRef, useState } from "react";
import { Check, FileSpreadsheet, Loader2, Trash2 } from "lucide-react";
import {
  deleteRawReportUpload,
  listRawReportUploads,
  updateRawReportType,
  type RawReportUploadRow,
} from "./import-actions";
import { useUserRole } from "../../../components/UserRoleContext";
import type { RawReportType } from "../../../lib/raw-report-types";
import { REPORT_TYPE_SPECS } from "../../../lib/csv-import-mapping";
import { RAW_REPORT_TYPE_ORDER } from "../../../lib/raw-report-types";
import { DatabaseTag } from "../../../components/DatabaseTag";

function statusLabel(status: string, uploadProgress?: number): string {
  switch (status) {
    case "pending":
      if (uploadProgress != null && uploadProgress >= 100) return "Ready";
      return "Pending";
    case "uploading":
      return "Uploading";
    case "processing":
      return "Processing";
    case "complete":
      return "Complete";
    case "failed":
      return "Failed";
    case "cancelled":
      return "Cancelled";
    default:
      return status;
  }
}

const LEGACY_REPORT_TYPE: Record<string, RawReportType> = {
  returns: "fba_customer_returns",
  inventory_adjustments: "inventory_ledger",
  removals: "safe_t_claims",
  other: "transaction_view",
};

function coerceReportType(v: string): RawReportType {
  if (RAW_REPORT_TYPE_ORDER.includes(v as RawReportType)) return v as RawReportType;
  return LEGACY_REPORT_TYPE[v] ?? "fba_customer_returns";
}

type RawReportImportsPanelProps = {
  /** Matches ledger “Target company”; list is filtered by `raw_report_uploads.company_id`. */
  companyId: string | null;
};

/** History + actions for `raw_report_uploads`. Scoped to the selected company from Amazon Inventory Ledger. */
export function RawReportImportsPanel({ companyId }: RawReportImportsPanelProps) {
  const { actorUserId } = useUserRole();
  const [rows, setRows] = useState<RawReportUploadRow[]>([]);
  const [loadErr, setLoadErr] = useState<string | null>(null);
  const [reportTypeSaveFlash, setReportTypeSaveFlash] = useState<Record<string, boolean>>({});
  const [processingIds, setProcessingIds] = useState<Set<string>>(() => new Set());
  const [deletingIds, setDeletingIds] = useState<Set<string>>(() => new Set());

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const reportTypeOverrideRef = useRef<Map<string, string>>(new Map());

  const refresh = useCallback(async () => {
    const res = await listRawReportUploads({ companyId, actorUserId });
    if (res.ok) {
      const override = reportTypeOverrideRef.current;
      setRows(
        res.rows.map((row) => {
          const o = override.get(row.id);
          if (o != null && row.report_type !== o) {
            return { ...row, report_type: o };
          }
          if (o != null && row.report_type === o) {
            override.delete(row.id);
          }
          return row;
        }),
      );
      setLoadErr(null);
    } else {
      setLoadErr(res.error);
    }
  }, [companyId, actorUserId]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    pollRef.current = setInterval(() => {
      void refresh();
    }, 2000);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [refresh]);

  const runDeleteUpload = async (r: RawReportUploadRow) => {
    if (
      !window.confirm(
        `Delete this import record and storage for “${r.file_name}”? This cannot be undone.`,
      )
    ) {
      return;
    }
    setDeletingIds((prev) => new Set([...prev, r.id]));
    setLoadErr(null);
    try {
      const res = await deleteRawReportUpload(r.id);
      if (!res.ok) {
        setLoadErr(res.error ?? "Delete failed.");
        return;
      }
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Delete failed.");
    } finally {
      setDeletingIds((prev) => {
        const next = new Set(prev);
        next.delete(r.id);
        return next;
      });
    }
  };

  const runProcessPipeline = async (r: RawReportUploadRow) => {
    setProcessingIds((prev) => new Set([...prev, r.id]));
    setLoadErr(null);
    try {
      const res = await fetch("/api/settings/imports/process", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: r.id }),
      });
      const json = (await res.json()) as { ok?: boolean; error?: string; rowsProcessed?: number };
      if (!res.ok || !json.ok) {
        setLoadErr(json.error ?? "Processing failed.");
        return;
      }
      await refresh();
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Processing failed.");
    } finally {
      setProcessingIds((prev) => {
        const next = new Set(prev);
        next.delete(r.id);
        return next;
      });
    }
  };

  return (
    <>
      {loadErr && (
        <div className="mb-4 rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {loadErr}
        </div>
      )}

      <div className="mt-12 border-t border-border pt-10">
        <div className="mb-3 flex items-center gap-2">
          <FileSpreadsheet className="h-4 w-4 text-muted-foreground" aria-hidden />
          <h2 className="text-sm font-semibold text-foreground">History</h2>
        </div>
        <p className="mb-4 max-w-2xl text-xs text-muted-foreground">
          Raw report sessions from <code className="rounded bg-muted px-1 text-[11px]">raw_report_uploads</code>.
          Use <strong>Amazon Inventory Ledger</strong> above for CSV → staging; other pipelines may appear here when
          used.
        </p>
        <div className="relative overflow-x-auto rounded-xl border border-border bg-card shadow-sm">
          <DatabaseTag table="raw_report_uploads" />
          <table className="w-full min-w-[960px] border-collapse text-left text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/40 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                <th className="px-4 py-3">File name</th>
                <th className="px-4 py-3">Type</th>
                <th className="px-4 py-3">Uploaded by</th>
                <th className="px-4 py-3">Date</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3 text-right">Row count</th>
                <th className="px-4 py-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">
                    No imports yet.
                  </td>
                </tr>
              )}
              {rows.map((r) => {
                const rt = coerceReportType(r.report_type);
                return (
                  <tr key={r.id} className="border-b border-border last:border-0">
                    <td className="max-w-[220px] truncate px-4 py-3 font-medium text-foreground" title={r.file_name}>
                      {r.file_name}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-1.5">
                        <select
                          value={rt}
                          onChange={async (e) => {
                            const v = e.target.value as RawReportType;
                            const prevType = r.report_type;
                            reportTypeOverrideRef.current.set(r.id, v);
                            setRows((prevRows) =>
                              prevRows.map((row) => (row.id === r.id ? { ...row, report_type: v } : row)),
                            );
                            const res = await updateRawReportType({
                              uploadId: r.id,
                              reportType: v,
                              actorUserId,
                            });
                            if (res.ok) {
                              setReportTypeSaveFlash((m) => ({ ...m, [r.id]: true }));
                              window.setTimeout(() => {
                                setReportTypeSaveFlash((m) => {
                                  const next = { ...m };
                                  delete next[r.id];
                                  return next;
                                });
                              }, 2200);
                            } else {
                              reportTypeOverrideRef.current.delete(r.id);
                              setRows((prevRows) =>
                                prevRows.map((row) =>
                                  row.id === r.id ? { ...row, report_type: prevType } : row,
                                ),
                              );
                              setLoadErr(res.error ?? "Update failed");
                            }
                          }}
                          className="h-8 max-w-[min(100%,24rem)] rounded-lg border border-border bg-background px-2 text-xs text-foreground"
                        >
                          {RAW_REPORT_TYPE_ORDER.map((v) => {
                            const s = REPORT_TYPE_SPECS[v];
                            return (
                              <option key={v} value={v}>
                                {s.shortLabel} ({s.description})
                              </option>
                            );
                          })}
                        </select>
                        {reportTypeSaveFlash[r.id] && (
                          <Check
                            className="h-4 w-4 shrink-0 text-emerald-600 dark:text-emerald-400"
                            aria-label="Saved"
                          />
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-3 font-mono text-xs text-muted-foreground">
                      {r.created_by_name ?? r.created_by ?? "—"}
                    </td>
                    <td className="whitespace-nowrap px-4 py-3 text-muted-foreground">
                      {new Date(r.created_at).toLocaleString()}
                    </td>
                    <td className="px-4 py-3">
                      <span
                        className={[
                          "inline-flex flex-wrap items-center gap-x-1.5 rounded-full px-2 py-0.5 text-xs font-medium",
                          r.status === "complete"
                            ? "bg-emerald-500/15 text-emerald-800 dark:text-emerald-300"
                            : r.status === "failed"
                              ? "bg-destructive/15 text-destructive"
                              : r.status === "uploading" || r.status === "processing"
                                ? "bg-sky-500/15 text-sky-800 dark:text-sky-300"
                                : "bg-muted text-muted-foreground",
                        ].join(" ")}
                      >
                        <span>{statusLabel(r.status, r.upload_progress)}</span>
                        {(r.status === "pending" ||
                          r.status === "uploading" ||
                          r.status === "processing") &&
                          r.upload_progress > 0 &&
                          r.upload_progress < 100 && (
                            <span className="font-normal tabular-nums text-muted-foreground">
                              {r.upload_progress}%
                            </span>
                          )}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right tabular-nums text-muted-foreground">
                      {r.row_count != null ? r.row_count.toLocaleString() : "—"}
                    </td>
                    <td className="px-4 py-3 text-right">
                      <div className="inline-flex flex-wrap items-center justify-end gap-2">
                        {r.status === "pending" && r.upload_progress >= 100 ? (
                          <button
                            type="button"
                            disabled={processingIds.has(r.id)}
                            onClick={() => void runProcessPipeline(r)}
                            aria-label={processingIds.has(r.id) ? "Processing import" : `Process ${r.file_name}`}
                            className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-border bg-background px-3 text-xs font-medium text-foreground shadow-sm transition hover:bg-accent disabled:cursor-not-allowed disabled:opacity-60"
                          >
                            {processingIds.has(r.id) ? (
                              <>
                                <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                                <span>Processing…</span>
                              </>
                            ) : (
                              "Process"
                            )}
                          </button>
                        ) : r.status === "processing" ? (
                          <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
                            <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                            Processing…
                          </span>
                        ) : (
                          <span className="text-xs text-muted-foreground">—</span>
                        )}
                        {r.status !== "uploading" && (
                          <button
                            type="button"
                            disabled={deletingIds.has(r.id) || processingIds.has(r.id)}
                            onClick={() => void runDeleteUpload(r)}
                            aria-label={`Delete ${r.file_name}`}
                            className="inline-flex h-8 items-center gap-1 rounded-lg border border-destructive/40 bg-background px-2.5 text-xs font-medium text-destructive shadow-sm transition hover:bg-destructive/10 disabled:cursor-not-allowed disabled:opacity-60"
                          >
                            {deletingIds.has(r.id) ? (
                              <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                            ) : (
                              <Trash2 className="h-3.5 w-3.5" aria-hidden />
                            )}
                            Delete
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
