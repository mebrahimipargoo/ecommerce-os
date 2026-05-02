"use client";

import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import axios, { AxiosError } from "axios";
import { CheckCircle2, FileText, Loader2, UploadCloud, XCircle } from "lucide-react";

import { listStoresForImports } from "../../(admin)/imports/companies-actions";
import { useUserRole } from "../../../components/UserRoleContext";

type ReportType = "removal_shipment_detail" | "inventory_ledger" | "reimbursements";

type Phase = "idle" | "uploading" | "processing" | "success" | "error";

type StoreOption = {
  id: string;
  organization_id: string;
  display_name: string;
};

const REPORT_TYPE_OPTIONS: { value: ReportType; label: string }[] = [
  { value: "removal_shipment_detail", label: "Removal Shipment Detail" },
  { value: "inventory_ledger", label: "FBA Inventory Ledger" },
  { value: "reimbursements", label: "FBA Reimbursements" },
];

/** Same-origin proxy (buffers full body in Next). */
const ETL_PROXY_UPLOAD = "/api/etl/upload-raw";

const FILE_IMPORT_SESSION_SCOPE = "ecommerce_os_file_import_scope_v1";
const FILE_IMPORT_SESSION_DRAFT = "ecommerce_os_file_import_draft_v1";

/**
 * Optional: set `NEXT_PUBLIC_ETL_API_ORIGIN=http://127.0.0.1:8000` in `.env.local`
 * so large files upload straight to FastAPI (one hop, no Next body buffer).
 * FastAPI CORS is already open for ETL routes.
 */
function directEtlUploadUrl(): string | null {
  const raw = (process.env.NEXT_PUBLIC_ETL_API_ORIGIN || "").trim().replace(/\/$/, "");
  if (!raw) return null;
  return `${raw}/etl/upload-raw`;
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

function formatResponseDetail(data: { detail?: unknown; message?: unknown } | undefined): string | undefined {
  const d = data?.detail;
  if (typeof d === "string") return d;
  if (Array.isArray(d)) {
    const parts = d.map((item) => {
      if (item && typeof item === "object" && "msg" in item) {
        return String((item as { msg?: string }).msg ?? JSON.stringify(item));
      }
      return typeof item === "string" ? item : JSON.stringify(item);
    });
    return parts.join("; ").slice(0, 2000);
  }
  if (d != null && typeof d === "object") return JSON.stringify(d).slice(0, 2000);
  const m = data?.message;
  if (typeof m === "string") return m;
  return undefined;
}

function axiosMessage(error: unknown, fallback: string): string {
  const err = error as AxiosError<{ detail?: unknown; message?: unknown }>;
  return formatResponseDetail(err.response?.data) ?? err.message ?? fallback;
}

export default function RawFileImportPage() {
  const { actorUserId, organizationId, organizationName, profileLoading } = useUserRole();

  const [stores, setStores] = useState<StoreOption[]>([]);
  const [storeId, setStoreId] = useState("");
  const [storesLoading, setStoresLoading] = useState(false);
  const [storesError, setStoresError] = useState<string | null>(null);

  const [reportType, setReportType] = useState<ReportType>("removal_shipment_detail");
  const [file, setFile] = useState<File | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [phase, setPhase] = useState<Phase>("idle");
  const [uploadPct, setUploadPct] = useState(0);
  const [uploadBytes, setUploadBytes] = useState<{ loaded: number; total: number } | null>(null);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [storesReady, setStoresReady] = useState(false);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const storeScopeKeyRef = useRef<string>("");

  const uploadTargetLabel = useMemo(() => (directEtlUploadUrl() ? "Python ETL (direct)" : "Next.js proxy → Python"), []);

  const reset = useCallback(() => {
    setFile(null);
    setPhase("idle");
    setUploadPct(0);
    setUploadBytes(null);
    setErrorMsg(null);
    try {
      sessionStorage.removeItem(FILE_IMPORT_SESSION_DRAFT);
    } catch {
      /* ignore */
    }
  }, []);

  useLayoutEffect(() => {
    if (typeof window === "undefined" || !actorUserId || !organizationId) return;
    const scopeKey = `${actorUserId}::${organizationId}`;
    try {
      const stored = sessionStorage.getItem(FILE_IMPORT_SESSION_SCOPE);
      if (stored === scopeKey && storeScopeKeyRef.current === "") {
        storeScopeKeyRef.current = scopeKey;
      }
    } catch {
      /* ignore */
    }
  }, [actorUserId, organizationId]);

  useEffect(() => {
    if (!organizationId || typeof window === "undefined" || !storesReady) return;
    try {
      sessionStorage.setItem(
        FILE_IMPORT_SESSION_DRAFT,
        JSON.stringify({ organizationId, storeId, reportType }),
      );
    } catch {
      /* ignore */
    }
  }, [organizationId, storeId, reportType, storesReady]);

  useEffect(() => {
    let cancelled = false;

    if (profileLoading || !actorUserId || !organizationId) {
      setStoresLoading(false);
      setStoresReady(false);
      return () => {
        cancelled = true;
      };
    }

    const scopeKey = `${actorUserId}::${organizationId}`;
    const scopeChanged = storeScopeKeyRef.current !== scopeKey;
    if (scopeChanged) {
      storeScopeKeyRef.current = scopeKey;
      setStores([]);
      setStoreId("");
      setStoresError(null);
      reset();
    }

    setStoresReady(false);
    setStoresLoading(true);
    listStoresForImports(actorUserId, organizationId)
      .then((result) => {
        if (cancelled) return;
        if (!result.ok) {
          setStoresError(result.error);
          setStores([]);
          setStoreId("");
          return;
        }
        setStores(result.rows);
        if (scopeChanged) {
          setStoreId(result.rows[0]?.id ?? "");
        }
        try {
          sessionStorage.setItem(FILE_IMPORT_SESSION_SCOPE, scopeKey);
        } catch {
          /* ignore */
        }
        if (result.rows.length > 0) {
          try {
            const raw = sessionStorage.getItem(FILE_IMPORT_SESSION_DRAFT);
            if (!raw) return;
            const draft = JSON.parse(raw) as {
              organizationId?: string;
              storeId?: string;
              reportType?: ReportType;
            };
            if (draft.organizationId !== organizationId) return;
            if (draft.storeId && result.rows.some((r) => r.id === draft.storeId)) {
              setStoreId(draft.storeId);
            }
            if (
              draft.reportType &&
              REPORT_TYPE_OPTIONS.some((o) => o.value === draft.reportType)
            ) {
              setReportType(draft.reportType);
            }
          } catch {
            /* ignore */
          }
        }
      })
      .finally(() => {
        if (!cancelled) {
          setStoresLoading(false);
          setStoresReady(true);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [actorUserId, organizationId, profileLoading, reset]);

  function acceptFile(f: File) {
    const ext = f.name.split(".").pop()?.toLowerCase() ?? "";
    if (!["csv", "txt"].includes(ext)) {
      setErrorMsg("Only .csv and .txt files are supported.");
      return;
    }
    setFile(f);
    setErrorMsg(null);
    setPhase("idle");
    setUploadPct(0);
    setUploadBytes(null);
  }

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback(() => {
    setIsDragging(false);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    const f = e.dataTransfer.files?.[0];
    if (f) acceptFile(f);
  }, []);

  const canUpload =
    Boolean(file) &&
    Boolean(organizationId) &&
    Boolean(storeId) &&
    phase !== "uploading" &&
    phase !== "processing" &&
    !storesLoading &&
    !profileLoading;

  async function handleUpload() {
    if (!canUpload || !file || !organizationId || !storeId) return;

    const formData = new FormData();
    formData.append("file", file);
    formData.append("report_type", reportType);
    formData.append("organization_id", organizationId);
    formData.append("store_id", storeId);

    const url = directEtlUploadUrl() ?? ETL_PROXY_UPLOAD;

    setPhase("uploading");
    setUploadPct(0);
    setUploadBytes({ loaded: 0, total: file.size || 0 });
    setErrorMsg(null);

    try {
      await axios.post(url, formData, {
        timeout: 0,
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
        // Do NOT set Content-Type — axios sets multipart + boundary automatically.
        onUploadProgress(event) {
          const total = event.total || file.size;
          if (!total) return;
          setUploadBytes({ loaded: event.loaded, total });
          setUploadPct(Math.min(99, Math.round((event.loaded / total) * 100)));
          if (event.loaded >= total) setPhase("processing");
        },
      });
      setUploadPct(100);
      setUploadBytes((prev) =>
        prev && prev.total > 0 ? { loaded: prev.total, total: prev.total } : { loaded: file.size, total: file.size },
      );
      setPhase("success");
    } catch (err) {
      setErrorMsg(axiosMessage(err, "Upload failed."));
      setPhase("error");
    }
  }

  const isSubmitting = phase === "uploading" || phase === "processing";
  const showDropZone = phase === "idle" || phase === "error";

  return (
    <div className="min-h-screen bg-background px-4 py-10 sm:px-6 lg:px-8">
      <div className="mx-auto max-w-2xl">
        <div className="overflow-hidden rounded-2xl border border-border bg-card shadow-lg">
          <div className="border-b border-border bg-muted/30 px-6 py-5">
            <div className="flex items-center gap-3">
              <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-primary/10">
                <UploadCloud className="h-5 w-5 text-primary" />
              </div>
              <div>
                <h1 className="text-base font-semibold text-foreground">Amazon Raw File Importer</h1>
                <p className="text-xs text-muted-foreground">
                  Upload path: <span className="font-medium text-foreground">{uploadTargetLabel}</span>
                  {directEtlUploadUrl() ? null : (
                    <span> — for very large CSVs, set </span>
                  )}
                  {!directEtlUploadUrl() ? (
                    <code className="rounded bg-muted px-1 text-[10px]">NEXT_PUBLIC_ETL_API_ORIGIN</code>
                  ) : null}
                  {!directEtlUploadUrl() ? <span> in `.env.local` to upload straight to Python.</span> : null}
                </p>
              </div>
            </div>
          </div>

          <div className="space-y-6 px-6 py-6">
            <div className="rounded-lg border border-border bg-muted/20 px-3 py-2 text-xs text-muted-foreground">
              <p>
                <span className="font-medium text-foreground">Company:</span>{" "}
                {profileLoading ? "…" : organizationName || "—"}
              </p>
              {storesLoading ? (
                <p className="mt-1 flex items-center gap-2">
                  <Loader2 className="h-3 w-3 animate-spin" /> Loading stores…
                </p>
              ) : storesError ? (
                <p className="mt-1 text-destructive">{storesError}</p>
              ) : stores.length === 0 ? (
                <p className="mt-1 text-amber-700 dark:text-amber-300">No stores for this company.</p>
              ) : (
                <label className="mt-2 block">
                  <span className="font-medium text-foreground">Store</span>
                  <select
                    value={storeId}
                    disabled={isSubmitting}
                    onChange={(e) => {
                      setStoreId(e.target.value);
                      reset();
                    }}
                    className="mt-1 h-9 w-full rounded-md border border-border bg-background px-2 text-sm"
                  >
                    {stores.map((s) => (
                      <option key={s.id} value={s.id}>
                        {s.display_name}
                      </option>
                    ))}
                  </select>
                </label>
              )}
            </div>

            <div>
              <label htmlFor="report-type" className="mb-1.5 block text-sm font-medium text-foreground">
                Select Report Type
              </label>
              <select
                id="report-type"
                value={reportType}
                onChange={(e) => setReportType(e.target.value as ReportType)}
                disabled={isSubmitting}
                className="h-10 w-full rounded-lg border border-border bg-background px-3 text-sm text-foreground shadow-sm outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
              >
                {REPORT_TYPE_OPTIONS.map(({ value, label }) => (
                  <option key={value} value={value}>
                    {label}
                  </option>
                ))}
              </select>
            </div>

            {showDropZone && (
              <div
                role="button"
                tabIndex={0}
                aria-label="File drop zone — click or drag a file here"
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                onDrop={handleDrop}
                onClick={() => fileInputRef.current?.click()}
                onKeyDown={(e) => e.key === "Enter" && fileInputRef.current?.click()}
                className={[
                  "flex cursor-pointer flex-col items-center justify-center gap-3 rounded-xl border-2 border-dashed px-6 py-12 text-center transition-colors",
                  isDragging
                    ? "border-sky-400 bg-sky-50/60 dark:border-sky-600 dark:bg-sky-950/20"
                    : file
                      ? "border-emerald-400 bg-emerald-50/40 dark:border-emerald-700 dark:bg-emerald-950/20"
                      : "border-border bg-muted/20 hover:border-primary/50 hover:bg-muted/40",
                ].join(" ")}
              >
                <UploadCloud
                  className={`h-10 w-10 ${file ? "text-emerald-500" : isDragging ? "text-sky-500" : "text-muted-foreground"}`}
                />

                {file ? (
                  <>
                    <div className="flex items-center gap-2">
                      <FileText className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
                      <span className="text-sm font-semibold text-emerald-700 dark:text-emerald-300">
                        {file.name}
                      </span>
                    </div>
                    <span className="text-xs text-muted-foreground">{formatBytes(file.size)}</span>
                    <span className="text-xs text-muted-foreground">
                      Click or drop a different file to replace
                    </span>
                  </>
                ) : (
                  <>
                    <p className="text-sm font-medium text-foreground">
                      {isDragging ? "Drop your file here" : "Drag & drop a file here"}
                    </p>
                    <p className="text-xs text-muted-foreground">
                      or click to browse &nbsp;·&nbsp; .csv and .txt files only
                    </p>
                  </>
                )}

                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv,.txt,text/csv,text/plain"
                  className="hidden"
                  onChange={(e) => {
                    const f = e.target.files?.[0];
                    e.target.value = "";
                    if (f) acceptFile(f);
                  }}
                />
              </div>
            )}

            {file && isSubmitting && (
              <div className="flex items-center gap-3 rounded-xl border border-border bg-muted/30 px-4 py-2.5">
                <FileText className="h-4 w-4 shrink-0 text-muted-foreground" />
                <p className="min-w-0 flex-1 truncate text-sm font-medium text-foreground">{file.name}</p>
                <span className="shrink-0 text-xs text-muted-foreground">{formatBytes(file.size)}</span>
              </div>
            )}

            {isSubmitting && (
              <div className="space-y-2">
                <div className="flex items-center justify-between text-xs font-medium text-foreground/70">
                  <span>Upload progress</span>
                  <span>{uploadPct}%</span>
                </div>
                <div className="h-2.5 w-full overflow-hidden rounded-full bg-muted">
                  <div
                    className="h-full rounded-full bg-blue-600 transition-[width] duration-300 ease-out dark:bg-blue-500"
                    style={{ width: `${Math.min(100, uploadPct)}%` }}
                  />
                </div>
                {uploadBytes && uploadBytes.total > 0 ? (
                  <p className="text-[11px] tabular-nums text-muted-foreground">
                    {formatBytes(uploadBytes.loaded)} / {formatBytes(uploadBytes.total)} sent
                  </p>
                ) : null}
                <div className="flex items-center gap-2 pt-1">
                  {phase === "processing" ? (
                    <>
                      <Loader2 className="h-3.5 w-3.5 animate-spin text-blue-500" />
                      <p className="text-xs text-muted-foreground">
                        Bytes delivered. Server is hashing, storing, and registering the upload — please wait.
                      </p>
                    </>
                  ) : (
                    <p className="text-xs text-muted-foreground">Uploading — keep this tab open.</p>
                  )}
                </div>
              </div>
            )}

            {phase === "success" && (
              <div className="flex items-start gap-3 rounded-xl border border-emerald-300 bg-emerald-50 px-4 py-4 dark:border-emerald-700 dark:bg-emerald-950/30">
                <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0 text-emerald-600 dark:text-emerald-400" />
                <div>
                  <p className="text-sm font-semibold text-emerald-800 dark:text-emerald-300">
                    File uploaded successfully
                  </p>
                  <p className="mt-0.5 text-xs text-emerald-700 dark:text-emerald-400">
                    Row is in Imports as <strong>mapped</strong>. Use Settings → Imports to Process (stage) then Sync
                    when ready.
                  </p>
                </div>
              </div>
            )}

            {phase === "error" && errorMsg && (
              <div className="flex items-start gap-3 rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-4">
                <XCircle className="mt-0.5 h-5 w-5 shrink-0 text-destructive" />
                <div>
                  <p className="text-sm font-semibold text-destructive">Upload failed</p>
                  <p className="mt-0.5 text-xs text-destructive/80">{errorMsg}</p>
                </div>
              </div>
            )}

            <div className="flex flex-wrap items-center gap-3">
              {(phase === "idle" || phase === "error") && (
                <button
                  type="button"
                  onClick={() => void handleUpload()}
                  disabled={!canUpload}
                  className="inline-flex items-center gap-2 rounded-lg bg-primary px-5 py-2.5 text-sm font-semibold text-primary-foreground shadow transition hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  <UploadCloud className="h-4 w-4" />
                  Upload to Server
                </button>
              )}

              {isSubmitting && (
                <div className="inline-flex items-center gap-2 text-sm text-muted-foreground">
                  <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
                  {phase === "uploading" ? "Uploading…" : "Processing…"}
                </div>
              )}

              {(phase === "success" || phase === "error" || (phase === "idle" && file)) && (
                <button
                  type="button"
                  onClick={reset}
                  className="inline-flex items-center gap-2 rounded-lg border border-border bg-background px-4 py-2.5 text-sm font-medium text-foreground transition hover:bg-accent"
                >
                  {phase === "success" ? "Upload Another File" : "Reset"}
                </button>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
