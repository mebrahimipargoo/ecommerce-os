"use client";

import React, { useCallback, useRef, useState } from "react";
import { FileText, Loader2, UploadCloud, X } from "lucide-react";
import { isAdminRole, useUserRole } from "../../../components/UserRoleContext";
import { REPORT_TYPE_SPECS } from "../../../lib/csv-import-mapping";
import { parseCsvToMatrix } from "../../../lib/csv-parse-basic";
import { RAW_REPORT_TYPE_ORDER, type RawReportType } from "../../../lib/raw-report-types";
import {
  createRawReportUploadSession,
  finalizeRawReportUpload,
  updateUploadSessionClassification,
} from "./import-actions";

const CHUNK_SIZE = 4 * 1024 * 1024; // 4 MB

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

/** Full-file SHA-256 (64 hex). Used so re-uploading the same removal CSV can replace prior imports. */
async function sha256HexFullFile(file: File): Promise<string> {
  const buf = await file.arrayBuffer();
  const hashBuf = await crypto.subtle.digest("SHA-256", buf);
  return Array.from(new Uint8Array(hashBuf))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/** Legacy 32-hex fingerprint for `md5_hash` column validation (first 32 chars of full SHA-256). */
function first32HexOfSha256(fullSha256: string): string {
  return fullSha256.slice(0, 32).toLowerCase();
}

function getFileExtension(name: string): string {
  return name.split(".").pop()?.toLowerCase() ?? "csv";
}

/** Read header row from the start of the file (first 1 MiB) without loading the whole file. */
async function readCsvHeadersFromFile(f: File): Promise<string[]> {
  const n = Math.min(f.size, 1024 * 1024);
  const text = await f.slice(0, n).text();
  const ext = getFileExtension(f.name);
  const matrix = parseCsvToMatrix(text.trim(), ext === "txt" ? "\t" : undefined);
  if (matrix.length === 0) return [];
  return matrix[0].map((h) => h.trim()).filter((cell) => cell.length > 0);
}

type Phase = "idle" | "uploading" | "processing" | "done" | "needs_mapping" | "error";

type RawReportUploaderProps = {
  /** Called when upload + processing completes so the history panel can refresh. */
  onUploadComplete?: () => void;
};

export function RawReportUploader({ onUploadComplete }: RawReportUploaderProps) {
  const { role, actorUserId } = useUserRole();
  const [reportType, setReportType] = useState<RawReportType>(RAW_REPORT_TYPE_ORDER[0] ?? "FBA_RETURNS");
  const [file, setFile] = useState<File | null>(null);
  const [phase, setPhase] = useState<Phase>("idle");
  const [uploadPct, setUploadPct] = useState(0);
  const [processPct, setProcessPct] = useState(0);
  const [isDragging, setIsDragging] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const abortRef = useRef(false);

  const reset = useCallback(() => {
    setFile(null);
    setPhase("idle");
    setUploadPct(0);
    setProcessPct(0);
    setErr(null);
    abortRef.current = false;
  }, []);

  function acceptFile(f: File) {
    const ext = getFileExtension(f.name);
    if (!["csv", "txt", "xlsx", "xls"].includes(ext)) {
      setErr("Only .csv, .txt, .xlsx, and .xls files are supported.");
      return;
    }
    setFile(f);
    setErr(null);
    setPhase("idle");
    setUploadPct(0);
    setProcessPct(0);
  }

  function handleDragOver(e: React.DragEvent) {
    e.preventDefault();
    setIsDragging(true);
  }
  function handleDragLeave() {
    setIsDragging(false);
  }
  function handleDrop(e: React.DragEvent) {
    e.preventDefault();
    setIsDragging(false);
    const f = e.dataTransfer.files?.[0];
    if (f) acceptFile(f);
  }

  async function startUpload() {
    if (!file || phase === "uploading" || phase === "processing") return;
    abortRef.current = false;
    setPhase("uploading");
    setErr(null);
    setUploadPct(0);
    setProcessPct(0);

    try {
      const ext = getFileExtension(file.name);
      const totalBytes = file.size;
      const totalParts = Math.max(1, Math.ceil(totalBytes / CHUNK_SIZE));
      const contentSha256 = await sha256HexFullFile(file);
      const md5Hash = first32HexOfSha256(contentSha256);

      // ── RECORD-FIRST: Insert the DB row immediately with status="uploading".
      // The file appears in History the moment this call resolves — before any
      // chunk is sent and before classification. If the upload fails later, the
      // row stays with status="failed" so the user can delete it from History.
      const session = await createRawReportUploadSession({
        fileName: file.name,
        totalBytes,
        reportType: "UNKNOWN",   // updated below after classify-headers returns
        md5Hash,
        contentSha256,
        fileExtension: ext,
        fileSizeBytes: totalBytes,
        uploadChunksCount: totalParts,
        initialStatus: "uploading",
        actorUserId,
      });
      if (!session.ok) throw new Error(session.error);
      const uploadId = session.id;

      // Notify parent immediately so the History panel remounts / re-fetches.
      // This gives the user instant visual feedback that the upload has started.
      onUploadComplete?.();

      // ── Read CSV headers (first 1 MiB slice — does not load the whole file)
      const headers = await readCsvHeadersFromFile(file);
      if (headers.length === 0) {
        throw new Error("Could not read CSV headers. Check the file is valid CSV.");
      }

      // ── Classify headers (rule-based → GPT fallback)
      const clsRes = await fetch("/api/settings/imports/classify-headers", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ headers, actor_user_id: actorUserId }),
      });
      const clsJson = (await clsRes.json()) as {
        ok?: boolean;
        report_type?: string;
        column_mapping?: Record<string, string>;
        needs_mapping?: boolean;
        error?: string;
      };
      if (!clsRes.ok || !clsJson.ok) {
        throw new Error(clsJson.error ?? "Could not classify CSV headers.");
      }
      const resolvedReportType = clsJson.report_type as RawReportType;
      const resolvedColumnMapping = clsJson.column_mapping ?? {};
      const needsManualMapping = clsJson.needs_mapping ?? false;
      setReportType(resolvedReportType);

      // ── Patch the existing session row with classification results.
      // This also saves csv_headers into metadata for the mapping modal dropdowns.
      await updateUploadSessionClassification({
        uploadId,
        reportType: resolvedReportType,
        columnMapping: resolvedColumnMapping,
        csvHeaders: headers,
        actorUserId,
      });

      // ── Upload file chunks sequentially
      for (let i = 0; i < totalParts; i++) {
        if (abortRef.current) throw new Error("Upload cancelled.");
        const start = i * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, totalBytes);
        const chunk = file.slice(start, end);

        const form = new FormData();
        form.append("upload_id", uploadId);
        form.append("part_index", String(i));
        form.append("total_parts", String(totalParts));
        form.append("total_bytes", String(totalBytes));
        form.append("file_extension", ext);
        if (actorUserId) form.append("actor_user_id", actorUserId);
        form.append("file", chunk);

        const res = await fetch("/api/settings/imports/chunk", { method: "POST", body: form });
        const json = (await res.json()) as { ok?: boolean; error?: string };
        if (!res.ok || !json.ok) throw new Error(json.error ?? "Chunk upload failed.");

        setUploadPct(Math.round(((i + 1) / totalParts) * 100));
      }

      // ── Finalize: transition to "ready" (or "needs_mapping" if mapping is incomplete)
      await finalizeRawReportUpload({
        uploadId,
        actorUserId,
        targetStatus: needsManualMapping ? "needs_mapping" : "ready",
      });
      setUploadPct(100);

      if (needsManualMapping) {
        setPhase("needs_mapping");
        onUploadComplete?.();
        return;
      }

      // ── Auto-process when mapping resolved cleanly (Inventory Ledger / exact headers)
      setPhase("processing");
      setProcessPct(5);

      const processRes = await fetch("/api/settings/imports/process", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: uploadId }),
      });
      const processJson = (await processRes.json()) as { ok?: boolean; error?: string; rowsProcessed?: number };

      if (!processRes.ok || !processJson.ok) {
        setProcessPct(0);
        throw new Error(processJson.error ?? "Processing failed.");
      }

      setProcessPct(100);
      setPhase("done");
      onUploadComplete?.();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Upload failed.");
      setPhase("error");
    }
  }

  if (!isAdminRole(role)) return null;

  return (
    <div className="rounded-xl border border-border bg-card shadow-sm">
      {/* Header */}
      <div className="border-b border-border px-5 py-3">
        <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
          Raw Report Uploads
        </p>
      </div>

      <div className="space-y-5 px-5 py-5">
        {/* Type selector */}
        <div>
          <label className="mb-1.5 block text-xs font-medium text-foreground">
            Default type for new uploads
          </label>
          <select
            value={reportType}
            onChange={(e) => setReportType(e.target.value as RawReportType)}
            disabled={phase === "uploading" || phase === "processing"}
            className="h-9 max-w-[min(100%,28rem)] rounded-lg border border-border bg-background px-2 text-sm text-foreground shadow-sm outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50"
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

        {/* Drop zone */}
        {(phase === "idle" || phase === "error") && (
          <div
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
            onClick={() => fileInputRef.current?.click()}
            className={[
              "flex cursor-pointer flex-col items-center justify-center gap-3 rounded-xl border-2 border-dashed px-6 py-10 text-center transition-colors",
              isDragging
                ? "border-sky-400 bg-sky-50/60 dark:border-sky-600 dark:bg-sky-950/20"
                : file
                  ? "border-emerald-300 bg-emerald-50/40 dark:border-emerald-700 dark:bg-emerald-950/20"
                  : "border-border bg-muted/20 hover:border-primary/40 hover:bg-muted/40",
            ].join(" ")}
          >
            <UploadCloud
              className={`h-8 w-8 ${file ? "text-emerald-500" : "text-muted-foreground"}`}
            />
            {file ? (
              <>
                <div className="flex items-center gap-2">
                  <FileText className="h-4 w-4 text-emerald-600" />
                  <span className="text-sm font-semibold text-emerald-700 dark:text-emerald-300">
                    {file.name}
                  </span>
                </div>
                <span className="text-xs text-muted-foreground">{formatBytes(file.size)}</span>
                <span className="text-xs text-muted-foreground">Click or drop a different file to replace</span>
              </>
            ) : (
              <>
                <p className="text-sm font-medium text-foreground">Drag &amp; drop a file here</p>
                <p className="text-xs text-muted-foreground">
                  or click to browse · .csv, .txt, .xlsx · 40MB parts · very large files supported
                </p>
                <p className="max-w-md text-[11px] leading-snug text-muted-foreground">
                  Removal orders: uploading the same file again (identical bytes) removes the previous import’s
                  data at Sync time — you do not need a separate “replace” toggle.
                </p>
              </>
            )}
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv,.txt,.xlsx,.xls,text/csv,text/plain,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
              className="hidden"
              onChange={(e) => {
                const f = e.target.files?.[0];
                e.target.value = "";
                if (f) acceptFile(f);
              }}
            />
          </div>
        )}

        {/* In-progress file bar */}
        {file && (phase === "uploading" || phase === "processing" || phase === "done") && (
          <div className="flex items-center gap-3 rounded-xl border border-border bg-muted/30 px-4 py-2.5">
            <FileText className="h-4 w-4 shrink-0 text-muted-foreground" />
            <p className="min-w-0 flex-1 truncate text-sm font-medium text-foreground">{file.name}</p>
            <span className="text-xs text-muted-foreground">{formatBytes(file.size)}</span>
          </div>
        )}

        {/* Progress bars */}
        <div className="max-w-3xl space-y-2">
          <div className="flex items-center justify-between text-xs font-medium text-foreground/70">
            <span>Upload progress</span>
            <span>{uploadPct}%</span>
          </div>
          <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
            <div
              className="h-full rounded-full bg-blue-600 transition-[width] duration-300 ease-out dark:bg-blue-500"
              style={{ width: `${Math.min(100, uploadPct)}%` }}
            />
          </div>
          <div className="flex items-center justify-between text-xs font-medium text-foreground/70">
            <span>Processing status</span>
            <span>{processPct}%</span>
          </div>
          <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
            <div
              className="h-full rounded-full bg-blue-500/90 transition-[width] duration-300 ease-out dark:bg-blue-400/90"
              style={{ width: `${Math.min(100, processPct)}%` }}
            />
          </div>
        </div>

        {/* Error */}
        {err && (
          <div className="flex items-start gap-2 rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
            <X className="mt-0.5 h-4 w-4 shrink-0" />
            {err}
          </div>
        )}

        {/* Action buttons */}
        <div className="flex flex-wrap items-center gap-3">
          {(phase === "idle" || phase === "error") && file && (
            <button
              type="button"
              onClick={() => void startUpload()}
              className="inline-flex items-center gap-2 rounded-lg bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground shadow transition hover:bg-primary/90"
            >
              <UploadCloud className="h-4 w-4" />
              Upload &amp; Process
            </button>
          )}
          {(phase === "uploading" || phase === "processing") && (
            <div className="inline-flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin text-sky-500" />
              {phase === "uploading" ? "Uploading…" : "Processing…"}
            </div>
          )}
          {(phase === "done" || phase === "needs_mapping" || phase === "error" || (phase === "idle" && file)) && (
            <button
              type="button"
              onClick={reset}
              className="inline-flex items-center gap-2 rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium text-foreground transition hover:bg-accent"
            >
              {phase === "done" || phase === "needs_mapping" ? "Upload another" : "Reset"}
            </button>
          )}
        </div>

        {phase === "done" && (
          <p className="text-sm font-medium text-emerald-600 dark:text-emerald-400">
            ✓ Upload complete — history updated below.
          </p>
        )}

        {phase === "needs_mapping" && (
          <p className="text-sm font-medium text-amber-600 dark:text-amber-400">
            ⚠ Column mapping required — find this file in History below and click &quot;Map Columns&quot; before syncing.
          </p>
        )}
      </div>
    </div>
  );
}
