"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { flushSync } from "react-dom";
import { Database, FileSpreadsheet, Loader2, Upload } from "lucide-react";
import {
  createRawReportUploadSession,
  failRawReportUpload,
  finalizeRawReportUpload,
  listRawReportUploads,
  updateRawReportType,
  type RawReportUploadRow,
} from "./import-actions";
import { useUserRole } from "../../../components/UserRoleContext";
import type { RawReportType } from "../../../lib/raw-report-types";
import { getImportMappingDefaults, mergeImportMappingDefault } from "./import-mapping-defaults-actions";
import {
  needsMappingPreviewForType,
  parseCsvHeaderLine,
  REPORT_TYPE_SPECS,
  suggestMappingForType,
} from "../../../lib/csv-import-mapping";
import { RAW_REPORT_TYPE_ORDER } from "../../../lib/raw-report-types";
import { FALLBACK_ORGANIZATION_ID } from "../../../lib/organization";
import { DatabaseTag } from "../../../components/DatabaseTag";

const CHUNK_SIZE = 4 * 1024 * 1024;

function peekCsvHeaders(file: File, maxBytes = 65536): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const blob = file.slice(0, Math.min(maxBytes, file.size));
    const reader = new FileReader();
    reader.onload = () => {
      const raw = typeof reader.result === "string" ? reader.result : "";
      const text = raw.replace(/^\uFEFF/, "");
      const lineEnd = text.indexOf("\n");
      const first = (lineEnd === -1 ? text : text.slice(0, lineEnd)).replace(/\r$/, "");
      resolve(parseCsvHeaderLine(first));
    };
    reader.onerror = () => reject(reader.error ?? new Error("read failed"));
    reader.readAsText(blob);
  });
}

function statusLabel(status: string): string {
  switch (status) {
    case "pending":
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

export function ImportsClient() {
  const { actorUserId } = useUserRole();
  const [reportType, setReportType] = useState<RawReportType>("fba_customer_returns");
  const [rows, setRows] = useState<RawReportUploadRow[]>([]);
  const [loadErr, setLoadErr] = useState<string | null>(null);
  const [dragOver, setDragOver] = useState(false);
  const [busy, setBusy] = useState(false);
  const [localProgress, setLocalProgress] = useState<number | null>(null);
  const [localPhase, setLocalPhase] = useState<"idle" | "upload" | "finalize">("idle");

  const [mappingOpen, setMappingOpen] = useState(false);
  const [pendingFile, setPendingFile] = useState<File | null>(null);
  const [headerPreview, setHeaderPreview] = useState<string[]>([]);
  const [suggestedMap, setSuggestedMap] = useState<Record<string, string>>({});
  const [mappingForType, setMappingForType] = useState<RawReportType>("fba_customer_returns");

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const refresh = useCallback(async () => {
    const res = await listRawReportUploads();
    if (res.ok) {
      setRows(res.rows);
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

  const runUpload = async (
    file: File,
    mapping: Record<string, string> | null,
    reportTypeForSession?: RawReportType,
  ) => {
    const rt = reportTypeForSession ?? reportType;
    setBusy(true);
    setLocalPhase("upload");
    setLocalProgress(0);
    try {
      const session = await createRawReportUploadSession({
        fileName: file.name,
        totalBytes: file.size,
        reportType: rt,
        columnMapping: mapping && Object.keys(mapping).length > 0 ? mapping : null,
        actorUserId,
      });
      if (!session.ok) {
        console.error("[imports] createRawReportUploadSession failed:", session.error);
        setLoadErr(session.error);
        return;
      }
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
        error_log: null,
        uploaded_by: actorUserId ?? null,
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

      const totalParts = Math.max(1, Math.ceil(file.size / CHUNK_SIZE));
      for (let i = 0; i < totalParts; i++) {
        const slice = file.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
        const fd = new FormData();
        fd.append("file", slice, `part-${i}`);
        fd.append("upload_id", session.id);
        fd.append("part_index", String(i));
        fd.append("total_parts", String(totalParts));
        fd.append("total_bytes", String(file.size));
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
      const rowEst = Math.max(0, Math.floor(file.size / 120));
      const fin = await finalizeRawReportUpload({
        uploadId: session.id,
        rowEstimate: rowEst,
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
    }
  };

  const onChooseFile = async (file: File | null) => {
    if (!file || busy) return;
    if (!file.name.toLowerCase().endsWith(".csv")) {
      setLoadErr("Please choose a .csv file.");
      return;
    }
    setLoadErr(null);
    const headers = await peekCsvHeaders(file);
    if (headers.length === 0) {
      setLoadErr("Could not read a header row from this file.");
      return;
    }
    setPendingFile(file);
    setHeaderPreview(headers);
    setMappingForType(reportType);

    if (needsMappingPreviewForType(headers, reportType)) {
      const defaults = await getImportMappingDefaults();
      const suggested = suggestMappingForType(headers, reportType);
      const saved = defaults[reportType];
      setSuggestedMap({ ...suggested, ...(saved ?? {}) });
      setMappingOpen(true);
      return;
    }

    await runUpload(file, null);
  };

  const confirmMapping = async () => {
    if (!pendingFile) return;
    const spec = REPORT_TYPE_SPECS[mappingForType];
    const map =
      suggestedMap[spec.canonicalKey]?.length
        ? suggestedMap
        : suggestMappingForType(headerPreview, mappingForType);
    const merged = await mergeImportMappingDefault(mappingForType, map);
    if (!merged.ok) {
      console.warn("[imports] mergeImportMappingDefault:", merged.error);
    }
    setMappingOpen(false);
    await runUpload(pendingFile, map, mappingForType);
    setPendingFile(null);
  };

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragOver(false);
    const f = e.dataTransfer.files?.[0];
    void onChooseFile(f ?? null);
  };

  const mappingSpec = REPORT_TYPE_SPECS[mappingForType];

  return (
    <div className="mx-auto w-full max-w-6xl px-4 py-6 md:px-6">
      <div className="mb-8 flex flex-col gap-2">
        <div className="flex items-center gap-2 text-muted-foreground">
          <Database className="h-5 w-5 shrink-0" aria-hidden />
          <span className="text-xs font-semibold uppercase tracking-widest">Data Management</span>
        </div>
        <h1 className="text-2xl font-semibold tracking-tight text-foreground">Imports</h1>
        <p className="max-w-2xl text-sm text-muted-foreground">
          Upload large Amazon CSV exports using chunked transfers (4MB parts).
        </p>
      </div>

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
          busy ? "pointer-events-none opacity-70" : "cursor-pointer hover:bg-muted/35",
        ].join(" ")}
        onClick={() => !busy && document.getElementById("imports-file-input")?.click()}
      >
        <input
          id="imports-file-input"
          type="file"
          accept=".csv,text/csv"
          className="sr-only"
          disabled={busy}
          onChange={(e) => void onChooseFile(e.target.files?.[0] ?? null)}
        />
        {busy ? (
          <Loader2 className="mb-2 h-8 w-8 animate-spin text-primary" aria-hidden />
        ) : (
          <Upload className="mb-2 h-8 w-8 text-muted-foreground" aria-hidden />
        )}
        <p className="text-sm font-medium text-foreground">Drag & drop CSV here</p>
        <p className="mt-1 text-xs text-muted-foreground">or click to browse · chunks of 4MB · very large files supported</p>
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
          <table className="w-full min-w-[880px] border-collapse text-left text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/40 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                <th className="px-4 py-3">File name</th>
                <th className="px-4 py-3">Type</th>
                <th className="px-4 py-3">Uploaded by</th>
                <th className="px-4 py-3">Date</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3 text-right">Row count</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
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
                      <select
                        value={rt}
                        onChange={async (e) => {
                          const v = e.target.value as RawReportType;
                          const res = await updateRawReportType({
                            uploadId: r.id,
                            reportType: v,
                            actorUserId,
                          });
                          if (res.ok) void refresh();
                          else setLoadErr(res.error ?? "Update failed");
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
                    </td>
                    <td className="px-4 py-3 text-muted-foreground">
                      {r.uploaded_by_name ?? (r.uploaded_by ? r.uploaded_by.slice(0, 8) + "…" : "—")}
                    </td>
                    <td className="whitespace-nowrap px-4 py-3 text-muted-foreground">
                      {new Date(r.created_at).toLocaleString()}
                    </td>
                    <td className="px-4 py-3">
                      <span
                        className={[
                          "inline-flex rounded-full px-2 py-0.5 text-xs font-medium",
                          r.status === "complete"
                            ? "bg-emerald-500/15 text-emerald-800 dark:text-emerald-300"
                            : r.status === "failed"
                              ? "bg-destructive/15 text-destructive"
                              : r.status === "uploading" || r.status === "processing"
                                ? "bg-sky-500/15 text-sky-800 dark:text-sky-300"
                                : "bg-muted text-muted-foreground",
                        ].join(" ")}
                      >
                        {statusLabel(r.status)}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right tabular-nums text-muted-foreground">
                      {r.row_count != null ? r.row_count.toLocaleString() : "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {mappingOpen && pendingFile && (
        <div
          className="fixed inset-0 z-[300] flex items-center justify-center bg-foreground/40 p-4 backdrop-blur-sm"
          role="dialog"
          aria-modal="true"
          aria-labelledby="mapping-title"
        >
          <div className="w-full max-w-lg rounded-xl border border-border bg-card p-6 shadow-xl">
            <h3 id="mapping-title" className="text-lg font-semibold text-foreground">
              Mapping preview
            </h3>
            <p className="mt-2 text-sm text-muted-foreground">
              For <span className="font-medium text-foreground">{mappingSpec.shortLabel}</span>, we expect a
              primary column like{" "}
              <code className="rounded bg-muted px-1 font-mono text-xs">{mappingSpec.canonicalKey}</code> (
              {mappingSpec.description}). No matching header was found automatically.
            </p>
            <div className="mt-4 max-h-40 overflow-auto rounded-lg border border-border bg-muted/30 p-3 font-mono text-xs text-foreground">
              {headerPreview.join(" · ") || "(no headers parsed)"}
            </div>
            <label className="mt-4 block text-xs font-medium text-muted-foreground">
              Map to <code className="font-mono text-[11px]">{mappingSpec.canonicalKey}</code>
              <select
                value={suggestedMap[mappingSpec.canonicalKey] ?? ""}
                onChange={(e) =>
                  setSuggestedMap((m) => ({ ...m, [mappingSpec.canonicalKey]: e.target.value }))
                }
                className="mt-1 h-10 w-full rounded-lg border border-border bg-background px-3 text-sm"
              >
                <option value="">Select column…</option>
                {headerPreview.map((h) => (
                  <option key={h} value={h}>
                    {h}
                  </option>
                ))}
              </select>
            </label>
            <div className="mt-6 flex justify-end gap-2">
              <button
                type="button"
                className="h-9 rounded-lg border border-border px-4 text-sm font-medium text-foreground transition hover:bg-accent"
                onClick={() => {
                  setMappingOpen(false);
                  setPendingFile(null);
                }}
              >
                Cancel
              </button>
              <button
                type="button"
                className="h-9 rounded-lg bg-primary px-4 text-sm font-medium text-primary-foreground transition hover:bg-primary/90 disabled:opacity-50"
                disabled={!suggestedMap[mappingSpec.canonicalKey]}
                onClick={() => void confirmMapping()}
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
