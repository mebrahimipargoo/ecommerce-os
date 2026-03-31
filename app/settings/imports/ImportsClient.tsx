"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { flushSync } from "react-dom";
import { Check, Database, FileSpreadsheet, Loader2, Upload } from "lucide-react";
import { detectReportType } from "../../../lib/detect-amazon-report-type";
import {
  createRawReportUploadSession,
  failRawReportUpload,
  finalizeRawReportUpload,
  listRawReportUploads,
  rawUploadExistsWithMd5Hash,
  updateRawReportType,
  type RawReportUploadRow,
} from "./import-actions";
import { useUserRole } from "../../../components/UserRoleContext";
import type { RawReportType } from "../../../lib/raw-report-types";
import { REPORT_TYPE_SPECS } from "../../../lib/csv-import-mapping";
import { RAW_REPORT_TYPE_ORDER } from "../../../lib/raw-report-types";
import { FALLBACK_ORGANIZATION_ID } from "../../../lib/organization";
import { DatabaseTag } from "../../../components/DatabaseTag";
import {
  computeFileMd5Hex,
  countDataRowsForFile,
  getImportFileExtension,
  isAllowedImportExtension,
  peekImportFileHeaders,
} from "../../../lib/import-file-analysis";

const CHUNK_SIZE = 4 * 1024 * 1024;

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

type PendingUploadMeta = {
  md5Hex: string;
  rowCount: number;
  ext: string;
};

export function ImportsClient() {
  const { actorUserId, actorName } = useUserRole();
  const [reportType, setReportType] = useState<RawReportType>("fba_customer_returns");
  const [rows, setRows] = useState<RawReportUploadRow[]>([]);
  const [loadErr, setLoadErr] = useState<string | null>(null);
  const [dragOver, setDragOver] = useState(false);
  const [busy, setBusy] = useState(false);
  const [isCreatingSession, setIsCreatingSession] = useState(false);
  const [localProgress, setLocalProgress] = useState<number | null>(null);
  const [localPhase, setLocalPhase] = useState<"idle" | "upload" | "finalize">("idle");

  const [pendingFile, setPendingFile] = useState<File | null>(null);
  const [pendingMeta, setPendingMeta] = useState<PendingUploadMeta | null>(null);
  const [identityModalOpen, setIdentityModalOpen] = useState(false);
  const [manualReportType, setManualReportType] = useState<RawReportType>("fba_customer_returns");
  const [uploadToast, setUploadToast] = useState<string | null>(null);

  const [reportTypeSaveFlash, setReportTypeSaveFlash] = useState<Record<string, boolean>>({});
  const [processingIds, setProcessingIds] = useState<Set<string>>(() => new Set());

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const runUploadLockRef = useRef(false);
  const chooseFileLockRef = useRef(false);
  /** Until the server row matches, prefer this value over stale poll reads. */
  const reportTypeOverrideRef = useRef<Map<string, string>>(new Map());

  const refresh = useCallback(async () => {
    const res = await listRawReportUploads();
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
  }, []);

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

  useEffect(() => {
    if (!busy) return;
    const fast = setInterval(() => {
      void refresh();
    }, 750);
    return () => clearInterval(fast);
  }, [busy, refresh]);

  const latestActive = useMemo(() => {
    const uploading = rows.find(
      (r) =>
        r.status === "pending" ||
        r.status === "uploading" ||
        r.status === "processing",
    );
    return uploading ?? null;
  }, [rows]);

  const combinedUploadPct =
    localProgress != null && localPhase !== "idle"
      ? localProgress
      : latestActive?.upload_progress ?? 0;
  const combinedProcessPct =
    localPhase === "finalize"
      ? Math.max(latestActive?.process_progress ?? 0, 95)
      : latestActive?.process_progress ?? 0;

  const dropzoneDisabled = busy || isCreatingSession || identityModalOpen;

  const runUpload = async (
    file: File,
    mapping: Record<string, string> | null,
    reportTypeForSession: RawReportType | undefined,
    meta: PendingUploadMeta,
  ) => {
    if (runUploadLockRef.current) return;
    runUploadLockRef.current = true;
    const rt = reportTypeForSession ?? reportType;
    setBusy(true);
    setLocalPhase("upload");
    setLocalProgress(0);
    void refresh();
    try {
      const totalParts = Math.max(1, Math.ceil(file.size / CHUNK_SIZE));
      const session = await createRawReportUploadSession({
        fileName: file.name,
        totalBytes: file.size,
        reportType: rt,
        md5Hash: meta.md5Hex,
        fileExtension: meta.ext,
        fileSizeBytes: file.size,
        uploadChunksCount: totalParts,
        columnMapping: mapping && Object.keys(mapping).length > 0 ? mapping : null,
        actorUserId,
      });
      if (!session.ok) {
        console.error("[imports] createRawReportUploadSession failed:", session.error);
        setLoadErr(session.error);
        setIsCreatingSession(false);
        return;
      }
      setIsCreatingSession(false);
      console.info("[imports] session row created", session.id, "mapping persisted on insert");

      const mapPayload =
        mapping && Object.keys(mapping).length > 0 ? mapping : null;
      const optimistic: RawReportUploadRow = {
        id: session.id,
        organization_id: FALLBACK_ORGANIZATION_ID,
        file_name: file.name,
        report_type: rt,
        storage_prefix: session.storagePrefix,
        status: "pending",
        upload_progress: 0,
        process_progress: 0,
        uploaded_bytes: 0,
        total_bytes: file.size,
        row_count: null,
        column_mapping: mapPayload,
        errorMessage: null,
        metadata: {
          total_bytes: file.size,
          storage_prefix: session.storagePrefix,
          upload_progress: 0,
          uploaded_bytes: 0,
          process_progress: 0,
          md5_hash: meta.md5Hex,
          file_extension: meta.ext,
          file_size_bytes: file.size,
          upload_chunks_count: totalParts,
        },
        created_by: actorUserId ?? null,
        created_by_name: actorUserId ? actorName : null,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
      flushSync(() => {
        setRows((prev) => {
          const rest = prev.filter((r) => r.id !== session.id);
          return [optimistic, ...rest];
        });
      });

      await refresh();

      const extForChunk = meta.ext.replace(/^\./, "");
      for (let i = 0; i < totalParts; i++) {
        const slice = file.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
        const fd = new FormData();
        fd.append("file", slice, `part-${i}`);
        fd.append("upload_id", session.id);
        fd.append("part_index", String(i));
        fd.append("total_parts", String(totalParts));
        fd.append("total_bytes", String(file.size));
        fd.append("file_extension", extForChunk);
        if (actorUserId) fd.append("actor_user_id", actorUserId);

        console.info(`[imports:chunk] POST part ${i + 1}/${totalParts}`, session.id, slice.size, "bytes");
        const res = await fetch("/api/settings/imports/chunk", { method: "POST", body: fd });
        const json = (await res.json()) as { ok?: boolean; error?: string };
        console.info(`[imports:chunk] response`, res.status, json);
        if (!res.ok || !json.ok) {
          console.error("[imports:chunk] failed", i, json);
          await failRawReportUpload({
            uploadId: session.id,
            message: json.error ?? `Chunk ${i} failed`,
            actorUserId,
          });
          setLoadErr(json.error ?? "Upload failed");
          return;
        }
        setLocalProgress(Math.round(((i + 1) / totalParts) * 100));
        await refresh();
      }

      setLocalPhase("finalize");
      const fin = await finalizeRawReportUpload({
        uploadId: session.id,
        rowCount: meta.rowCount,
        columnMapping: mapping,
        actorUserId,
      });
      if (!fin.ok) {
        setLoadErr(fin.error ?? "Finalize failed");
        return;
      }
      setLocalProgress(100);
      await refresh();
    } finally {
      setBusy(false);
      setLocalPhase("idle");
      setLocalProgress(null);
      runUploadLockRef.current = false;
    }
  };

  const onChooseFile = async (file: File | null) => {
    if (!file || dropzoneDisabled || chooseFileLockRef.current) return;

    const ext = getImportFileExtension(file.name);
    if (!isAllowedImportExtension(ext)) {
      setLoadErr("Please choose a .csv, .txt, or .xlsx file.");
      return;
    }

    chooseFileLockRef.current = true;
    setIsCreatingSession(true);
    setLoadErr(null);

    let deferClearCreatingSession = false;
    try {
      const md5Hex = await computeFileMd5Hex(file);

      const dupRes = await rawUploadExistsWithMd5Hash(md5Hex);
      if (!dupRes.ok) {
        setLoadErr(dupRes.error);
        return;
      }
      if (dupRes.exists) {
        window.alert("This exact file content was already uploaded. Duplicate detected.");
        const proceed = window.confirm("Upload this file anyway?");
        if (!proceed) {
          return;
        }
      }

      const rowCount = await countDataRowsForFile(file, ext);
      const headers = await peekImportFileHeaders(file, ext);
      if (headers.length === 0) {
        setLoadErr("Could not read a header row from this file.");
        return;
      }

      const meta: PendingUploadMeta = { md5Hex, rowCount, ext };
      const firstRowString = headers.join(",");
      const detected = detectReportType(file.name, firstRowString);

      if (detected != null) {
        const label = REPORT_TYPE_SPECS[detected].shortLabel;
        setUploadToast(`Auto-detected as ${label}`);
        window.setTimeout(() => setUploadToast(null), 4500);
        setReportType(detected);
        await runUpload(file, null, detected, meta);
        return;
      }

      setPendingFile(file);
      setPendingMeta(meta);
      setManualReportType(reportType);
      setIdentityModalOpen(true);
      deferClearCreatingSession = true;
      setIsCreatingSession(false);
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : "Could not prepare file.");
    } finally {
      chooseFileLockRef.current = false;
      if (!deferClearCreatingSession) {
        setIsCreatingSession(false);
      }
    }
  };

  const confirmManualReportType = async () => {
    if (!pendingFile || !pendingMeta) return;
    setIdentityModalOpen(false);
    const file = pendingFile;
    const meta = pendingMeta;
    const rt = manualReportType;
    setPendingFile(null);
    setPendingMeta(null);
    await runUpload(file, null, rt, meta);
  };

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragOver(false);
    const f = e.dataTransfer.files?.[0];
    void onChooseFile(f ?? null);
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
    <div className="mx-auto w-full max-w-6xl px-4 py-6 md:px-6">
      <div className="mb-8 flex flex-col gap-2">
        <div className="flex items-center gap-2 text-muted-foreground">
          <Database className="h-5 w-5 shrink-0" aria-hidden />
          <span className="text-xs font-semibold uppercase tracking-widest">Data Management</span>
        </div>
        <h1 className="text-2xl font-semibold tracking-tight text-foreground">Imports</h1>
        <p className="max-w-2xl text-sm text-muted-foreground">
          Upload large Amazon exports using chunked transfers (4MB parts). CSV, TXT, and Excel supported.
        </p>
      </div>

      {uploadToast && (
        <div
          role="status"
          aria-live="polite"
          className="fixed bottom-6 right-6 z-[320] flex max-w-md items-center gap-2 rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm font-medium text-emerald-800 shadow-lg dark:border-emerald-700/50 dark:bg-emerald-950/90 dark:text-emerald-200"
        >
          <Check className="h-4 w-4 shrink-0 text-emerald-600 dark:text-emerald-400" aria-hidden />
          {uploadToast}
        </div>
      )}

      {loadErr && (
        <div className="mb-4 rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {loadErr}
        </div>
      )}

      <div className="mb-6 flex flex-wrap items-center gap-3">
        <label className="text-xs font-medium text-muted-foreground">Default type for new uploads</label>
        <select
          value={reportType}
          onChange={(e) => setReportType(e.target.value as RawReportType)}
          disabled={busy}
          className="h-9 max-w-full min-w-[min(100%,22rem)] rounded-lg border border-border bg-card px-3 text-sm text-foreground shadow-sm outline-none ring-offset-background focus-visible:ring-2 focus-visible:ring-ring"
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
      </div>

      <div
        role="button"
        tabIndex={0}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            document.getElementById("imports-file-input")?.click();
          }
        }}
        onDragEnter={(e) => {
          e.preventDefault();
          setDragOver(true);
        }}
        onDragOver={(e) => {
          e.preventDefault();
          setDragOver(true);
        }}
        onDragLeave={(e) => {
          e.preventDefault();
          if (!e.currentTarget.contains(e.relatedTarget as Node)) setDragOver(false);
        }}
        onDrop={onDrop}
        className={[
          "relative flex h-44 w-full max-w-3xl flex-col items-center justify-center rounded-xl border-2 border-dashed",
          "border-border bg-muted/20 px-4 text-center transition-shadow",
          dragOver ? "ring-2 ring-primary/50 ring-offset-2 ring-offset-background" : "ring-0 ring-offset-0",
          dropzoneDisabled ? "pointer-events-none opacity-70" : "cursor-pointer hover:bg-muted/35",
        ].join(" ")}
        onClick={() => !dropzoneDisabled && document.getElementById("imports-file-input")?.click()}
      >
        <input
          id="imports-file-input"
          type="file"
          accept=".csv,.txt,.xlsx,text/csv,text/plain,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
          className="sr-only"
          disabled={dropzoneDisabled}
          onChange={(e) => void onChooseFile(e.target.files?.[0] ?? null)}
        />
        {busy || isCreatingSession ? (
          <Loader2 className="mb-2 h-8 w-8 animate-spin text-primary" aria-hidden />
        ) : (
          <Upload className="mb-2 h-8 w-8 text-muted-foreground" aria-hidden />
        )}
        <p className="text-sm font-medium text-foreground">Drag & drop a file here</p>
        <p className="mt-1 text-xs text-muted-foreground">
          or click to browse · .csv, .txt, .xlsx · chunks of 4MB · very large files supported
        </p>
      </div>

      <div className="mt-8 max-w-3xl space-y-2">
        <div className="flex items-center justify-between text-xs font-medium text-muted-foreground">
          <span>Upload progress</span>
          <span>{combinedUploadPct}%</span>
        </div>
        <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
          <div
            className="h-full rounded-full bg-primary transition-[width] duration-300 ease-out"
            style={{ width: `${Math.min(100, combinedUploadPct)}%` }}
          />
        </div>
        <div className="flex items-center justify-between text-xs font-medium text-muted-foreground">
          <span>Processing status</span>
          <span>{combinedProcessPct}%</span>
        </div>
        <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
          <div
            className="h-full rounded-full bg-sky-600/80 transition-[width] duration-300 ease-out dark:bg-sky-500/80"
            style={{ width: `${Math.min(100, combinedProcessPct)}%` }}
          />
        </div>
      </div>

      <div className="mt-12">
        <div className="mb-3 flex items-center gap-2">
          <FileSpreadsheet className="h-4 w-4 text-muted-foreground" aria-hidden />
          <h2 className="text-sm font-semibold text-foreground">History</h2>
        </div>
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
                              prevRows.map((row) =>
                                row.id === r.id ? { ...row, report_type: v } : row,
                              ),
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
                      {r.created_by ?? "—"}
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
                      {r.status === "pending" && r.upload_progress >= 100 ? (
                        <button
                          type="button"
                          disabled={busy || processingIds.has(r.id)}
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
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {identityModalOpen && pendingFile && (
        <div
          className="fixed inset-0 z-[300] flex items-center justify-center bg-foreground/40 p-4 backdrop-blur-sm"
          role="dialog"
          aria-modal="true"
          aria-labelledby="identity-title"
        >
          <div className="w-full max-w-lg rounded-xl border border-border bg-card p-6 shadow-xl">
            <h3 id="identity-title" className="text-lg font-semibold text-foreground">
              Select report type
            </h3>
            <p className="mt-2 text-sm text-muted-foreground">
              We could not identify this file. Please select the report type:
            </p>
            <label className="mt-4 block text-xs font-medium text-muted-foreground">
              Report type
              <select
                value={manualReportType}
                onChange={(e) => setManualReportType(e.target.value as RawReportType)}
                className="mt-1 h-10 w-full rounded-lg border border-border bg-background px-3 text-sm"
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
            </label>
            <div className="mt-6 flex justify-end gap-2">
              <button
                type="button"
                className="h-9 rounded-lg border border-border px-4 text-sm font-medium text-foreground transition hover:bg-accent"
                onClick={() => {
                  setIdentityModalOpen(false);
                  setPendingFile(null);
                  setPendingMeta(null);
                }}
              >
                Cancel
              </button>
              <button
                type="button"
                className="h-9 rounded-lg bg-primary px-4 text-sm font-medium text-primary-foreground transition hover:bg-primary/90"
                onClick={() => void confirmManualReportType()}
              >
                Continue upload
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
