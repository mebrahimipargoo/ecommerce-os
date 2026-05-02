"use client";

import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import axios, { AxiosError } from "axios";
import Link from "next/link";
import {
  AlertCircle,
  BrainCircuit,
  CheckCircle2,
  Database,
  FileText,
  Loader2,
  RefreshCw,
  Trash2,
  UploadCloud,
  XCircle,
} from "lucide-react";
import { listStoresForImports } from "../../(admin)/imports/companies-actions";
import { useUserRole } from "../../../components/UserRoleContext";

const API_BASE = "/api/etl";

/** Survives remount when navigating away and back (same tab). */
const AMAZON_ETL_SESSION_SCOPE = "ecommerce_os_amazon_etl_scope_v1";
const AMAZON_ETL_SESSION_DRAFT = "ecommerce_os_amazon_etl_draft_v1";

type UploadState = "IDLE" | "DETECTING" | "READY" | "UPLOADING" | "SUCCESS" | "ERROR";

type StoreOption = {
  id: string;
  organization_id: string;
  display_name: string;
};

type ReportType =
  | "inventory_ledger"
  | "reimbursements"
  | "removals"
  | "amazon_all_orders"
  | "unknown";

type AmazonEtlDraftV1 = {
  organizationId: string;
  storeId: string;
  selectedType: ReportType;
  successUploadId: string | null;
  message: string | null;
  uploadState: UploadState;
};

type PipelineRow = {
  upload_id?: string;
  status?: string;
  upload_pct?: number;
  process_pct?: number;
  sync_pct?: number;
  phase2_stage_pct?: number;
  phase3_raw_sync_pct?: number;
  current_phase?: string;
  current_phase_label?: string | null;
  staged_rows_written?: number;
  processed_rows?: number;
  data_rows_total?: number;
  error_message?: string | null;
};

type HistoryRow = {
  id: string;
  file_name: string;
  report_type: string;
  metadata?: {
    row_count?: number;
    processed_rows?: number;
    ui_report_slug?: string;
    import_store_id?: string;
    process_progress?: number;
    sync_progress?: number;
    import_metrics?: { current_phase?: string; failure_reason?: string };
  } | null;
  pipeline?: PipelineRow | null;
  created_at: string;
  status: string;
};

type UploadHistoryResponse = {
  status?: string;
  history?: HistoryRow[] | null;
};

type DetectResponse = {
  detected_type?: string;
  confidence?: number;
  method?: string;
};

const REPORT_OPTIONS: { value: ReportType; label: string }[] = [
  { value: "inventory_ledger", label: "FBA Inventory Ledger" },
  { value: "reimbursements", label: "FBA Reimbursements" },
  { value: "removals", label: "Removal Orders / Shipments" },
  { value: "amazon_all_orders", label: "Amazon All Orders" },
  { value: "unknown", label: "Unknown / Other" },
];

const STATE_STYLES: Record<UploadState, string> = {
  IDLE: "bg-muted text-muted-foreground",
  DETECTING: "bg-violet-100 text-violet-700 dark:bg-violet-950/50 dark:text-violet-300",
  READY: "bg-sky-100 text-sky-700 dark:bg-sky-950/50 dark:text-sky-300",
  UPLOADING: "bg-amber-100 text-amber-700 dark:bg-amber-950/50 dark:text-amber-300",
  SUCCESS: "bg-emerald-100 text-emerald-700 dark:bg-emerald-950/50 dark:text-emerald-300",
  ERROR: "bg-red-100 text-red-700 dark:bg-red-950/50 dark:text-red-300",
};

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDate(value: string): string {
  try {
    return new Intl.DateTimeFormat(undefined, {
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    }).format(new Date(value));
  } catch {
    return value;
  }
}

function normalizeReportType(value: string | undefined | null): ReportType {
  const raw = (value ?? "").trim().toLowerCase();
  if (REPORT_OPTIONS.some((o) => o.value === raw)) return raw as ReportType;
  if (raw === "removal_shipment" || raw === "removal_shipment_detail" || raw === "removal_order") {
    return "removals";
  }
  if (raw === "all_orders" || raw === "all-orders") return "amazon_all_orders";
  return "unknown";
}

function prettyType(row: HistoryRow): string {
  const slug = normalizeReportType(row.metadata?.ui_report_slug ?? row.report_type);
  return REPORT_OPTIONS.find((o) => o.value === slug)?.label ?? row.report_type;
}

function statusClass(status: string): string {
  const s = status.toLowerCase();
  if (["complete", "completed", "success", "synced", "mapped", "done"].includes(s)) {
    return "bg-emerald-100 text-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-300";
  }
  if (["processing", "pending", "uploading", "ready", "uploaded"].includes(s)) {
    return "bg-sky-100 text-sky-700 dark:bg-sky-950/40 dark:text-sky-300";
  }
  if (["error", "failed"].includes(s)) {
    return "bg-red-100 text-red-700 dark:bg-red-950/40 dark:text-red-300";
  }
  return "bg-muted text-muted-foreground";
}

function normalizeHistory(data: UploadHistoryResponse | HistoryRow[] | undefined): HistoryRow[] {
  if (!data) return [];
  if (Array.isArray(data)) return data;
  return Array.isArray(data.history) ? data.history : [];
}

async function extractHeaders(file: File): Promise<string[]> {
  const sample = await file.slice(0, 2048).text();
  const firstLine = sample.split(/\r?\n/)[0] ?? "";
  const delimiter = firstLine.includes("\t") ? "\t" : ",";
  return firstLine
    .split(delimiter)
    .map((h) => h.trim().replace(/^["']|["']$/g, "").trim())
    .filter(Boolean);
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
  return (
    formatResponseDetail(err.response?.data) ?? err.message ?? fallback
  );
}

const IMPORT_PROCESS_URL = "/api/settings/imports/process";
const IMPORT_SYNC_URL = "/api/settings/imports/sync";

const STAGEABLE_STATUSES = new Set(["mapped", "ready", "uploaded", "pending", "failed"]);

function canRunProcess(row: HistoryRow): boolean {
  return STAGEABLE_STATUSES.has(String(row.status ?? "").toLowerCase());
}

function canRunSync(row: HistoryRow): boolean {
  const s = String(row.status ?? "").toLowerCase();
  if (s === "staged") return true;
  if (s === "failed" && (row.pipeline?.staged_rows_written ?? 0) > 0) return true;
  return false;
}

function resolveStagePercent(row: HistoryRow): number | null {
  const fps = row.pipeline;
  if (fps && typeof fps.phase2_stage_pct === "number" && Number.isFinite(fps.phase2_stage_pct)) {
    return Math.max(0, Math.min(100, Math.round(fps.phase2_stage_pct)));
  }
  const m = row.metadata?.process_progress;
  if (typeof m === "number" && Number.isFinite(m)) return Math.max(0, Math.min(100, Math.round(m)));
  return null;
}

function resolveSyncPercent(row: HistoryRow): number | null {
  const fps = row.pipeline;
  if (fps && typeof fps.phase3_raw_sync_pct === "number" && Number.isFinite(fps.phase3_raw_sync_pct)) {
    return Math.max(0, Math.min(100, Math.round(fps.phase3_raw_sync_pct)));
  }
  if (fps && typeof fps.sync_pct === "number" && Number.isFinite(fps.sync_pct)) {
    return Math.max(0, Math.min(100, Math.round(fps.sync_pct)));
  }
  const m = row.metadata?.sync_progress;
  if (typeof m === "number" && Number.isFinite(m)) return Math.max(0, Math.min(100, Math.round(m)));
  return null;
}

export default function AmazonEtlPage() {
  const {
    actorUserId,
    organizationId,
    organizationName,
    profileLoading,
  } = useUserRole();

  const [stores, setStores] = useState<StoreOption[]>([]);
  const [storeId, setStoreId] = useState("");
  const [storesLoading, setStoresLoading] = useState(false);
  const [storesError, setStoresError] = useState<string | null>(null);

  const [uploadState, setUploadState] = useState<UploadState>("IDLE");
  const [file, setFile] = useState<File | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [detectedType, setDetectedType] = useState<ReportType | null>(null);
  const [selectedType, setSelectedType] = useState<ReportType>("unknown");
  const [detectMeta, setDetectMeta] = useState<{ confidence?: number; method?: string } | null>(null);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadBytes, setUploadBytes] = useState<{ loaded: number; total: number } | null>(null);
  const [successUploadId, setSuccessUploadId] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [pipelineAction, setPipelineAction] = useState<{
    uploadId: string;
    kind: "process" | "sync";
  } | null>(null);

  const [history, setHistory] = useState<HistoryRow[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [storesReady, setStoresReady] = useState(false);

  const inputRef = useRef<HTMLInputElement>(null);
  const storeScopeKeyRef = useRef<string>("");

  const latestSuccessRow = useMemo(
    () => (successUploadId ? history.find((h) => h.id === successUploadId) : undefined),
    [history, successUploadId],
  );
  const quickCanSync = latestSuccessRow ? canRunSync(latestSuccessRow) : false;

  const selectedStore = useMemo(
    () => stores.find((store) => store.id === storeId) ?? null,
    [stores, storeId],
  );
  const busy =
    uploadState === "DETECTING" || uploadState === "UPLOADING" || pipelineAction != null;
  const canSelectFile = Boolean(organizationId) && Boolean(storeId) && !busy;
  const canConfirmUpload =
    uploadState === "READY" &&
    Boolean(file) &&
    Boolean(organizationId) &&
    Boolean(storeId) &&
    selectedType !== "unknown";

  const resetUpload = useCallback(() => {
    setUploadState("IDLE");
    setFile(null);
    setDetectedType(null);
    setSelectedType("unknown");
    setDetectMeta(null);
    setUploadProgress(0);
    setUploadBytes(null);
    setMessage(null);
    setSuccessUploadId(null);
    setPipelineAction(null);
    try {
      sessionStorage.removeItem(AMAZON_ETL_SESSION_DRAFT);
    } catch {
      /* ignore */
    }
  }, []);

  const loadHistory = useCallback(async (orgId: string) => {
    setHistoryLoading(true);
    try {
      const response = await axios.get<UploadHistoryResponse | HistoryRow[]>(
        `${API_BASE}/upload-history/${orgId}`,
      );
      setHistory(normalizeHistory(response.data));
    } catch (error) {
      setHistoryError(axiosMessage(error, "Could not load upload history."));
      setHistory([]);
    } finally {
      setHistoryLoading(false);
    }
  }, []);

  const runProcessForUpload = useCallback(
    async (uploadId: string) => {
      if (!organizationId) return;
      setPipelineAction({ uploadId, kind: "process" });
      setHistoryError(null);
      const interval = window.setInterval(() => {
        void loadHistory(organizationId);
      }, 2000);
      try {
        const res = await axios.post(IMPORT_PROCESS_URL, { upload_id: uploadId }, { timeout: 0 });
        const data = res.data as { ok?: boolean; error?: string; recoverable?: boolean };
        if (data && data.ok === false) {
          const hint = data.recoverable === true ? " Click Process again to resume staging." : "";
          setHistoryError(String(data.error ?? "Process failed.") + hint);
        }
      } catch (err) {
        setHistoryError(axiosMessage(err, "Process failed."));
      } finally {
        clearInterval(interval);
        await loadHistory(organizationId);
        setPipelineAction(null);
      }
    },
    [organizationId, loadHistory],
  );

  const runSyncForUpload = useCallback(
    async (uploadId: string) => {
      if (!organizationId) return;
      setPipelineAction({ uploadId, kind: "sync" });
      setHistoryError(null);
      const interval = window.setInterval(() => {
        void loadHistory(organizationId);
      }, 2000);
      try {
        const res = await axios.post(IMPORT_SYNC_URL, { upload_id: uploadId }, { timeout: 0 });
        const data = res.data as { ok?: boolean; error?: string };
        if (data && data.ok === false) {
          setHistoryError(String(data.error ?? "Sync failed."));
        }
      } catch (err) {
        setHistoryError(axiosMessage(err, "Sync failed."));
      } finally {
        clearInterval(interval);
        await loadHistory(organizationId);
        setPipelineAction(null);
      }
    },
    [organizationId, loadHistory],
  );

  const shouldPollHistory = useMemo(
    () => history.some((r) => String(r.status ?? "").toLowerCase() === "processing"),
    [history],
  );

  useEffect(() => {
    if (!organizationId || !shouldPollHistory) return;
    const t = window.setInterval(() => {
      void loadHistory(organizationId);
    }, 2000);
    return () => clearInterval(t);
  }, [organizationId, shouldPollHistory, loadHistory]);

  useLayoutEffect(() => {
    if (typeof window === "undefined" || !actorUserId || !organizationId) return;
    const scopeKey = `${actorUserId}::${organizationId}`;
    try {
      const stored = sessionStorage.getItem(AMAZON_ETL_SESSION_SCOPE);
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
      const draft: AmazonEtlDraftV1 = {
        organizationId,
        storeId,
        selectedType,
        successUploadId,
        message,
        uploadState,
      };
      sessionStorage.setItem(AMAZON_ETL_SESSION_DRAFT, JSON.stringify(draft));
    } catch {
      /* ignore */
    }
  }, [organizationId, storeId, selectedType, successUploadId, message, uploadState, storesReady]);

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
      resetUpload();
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
          sessionStorage.setItem(AMAZON_ETL_SESSION_SCOPE, scopeKey);
        } catch {
          /* ignore */
        }

        if (result.rows.length > 0) {
          try {
            const raw = sessionStorage.getItem(AMAZON_ETL_SESSION_DRAFT);
            if (!raw) return;
            const draft = JSON.parse(raw) as Partial<AmazonEtlDraftV1>;
            if (draft.organizationId !== organizationId) return;
            if (draft.storeId && result.rows.some((r) => r.id === draft.storeId)) {
              setStoreId(draft.storeId);
            }
            const st = draft.selectedType;
            if (st && REPORT_OPTIONS.some((o) => o.value === st)) {
              setSelectedType(st);
            }
            if (typeof draft.successUploadId === "string" && draft.successUploadId.trim()) {
              setSuccessUploadId(draft.successUploadId.trim());
            }
            if (typeof draft.message === "string") {
              setMessage(draft.message);
            }
            if (draft.uploadState === "SUCCESS" || draft.uploadState === "ERROR") {
              setUploadState(draft.uploadState);
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
  }, [actorUserId, organizationId, profileLoading, resetUpload]);

  useEffect(() => {
    if (profileLoading || !organizationId) return;
    void loadHistory(organizationId);
  }, [organizationId, profileLoading, loadHistory]);

  async function handleFile(fileToDetect: File) {
    const ext = fileToDetect.name.split(".").pop()?.toLowerCase() ?? "";
    if (!["csv", "txt"].includes(ext)) {
      setUploadState("ERROR");
      setMessage("Please select a .csv or .txt file.");
      return;
    }
    if (!organizationId || !storeId) {
      setUploadState("ERROR");
      setMessage("Select a company and store before choosing a file.");
      return;
    }

    setFile(fileToDetect);
    setUploadProgress(0);
    setMessage(null);
    setDetectedType(null);
    setDetectMeta(null);
    setUploadState("DETECTING");

    try {
      const headers = await extractHeaders(fileToDetect);
      if (headers.length === 0) {
        throw new Error("No header row was found in this file.");
      }
      const response = await axios.post<DetectResponse>(
        `${API_BASE}/detect-headers`,
        {
          headers,
          organization_id: organizationId,
        },
        { timeout: 120000, headers: { "Content-Type": "application/json" } },
      );
      const detected = normalizeReportType(response.data.detected_type);
      setDetectedType(detected);
      setSelectedType(detected);
      setDetectMeta({
        confidence: response.data.confidence,
        method: response.data.method,
      });
      setUploadState("READY");
    } catch (error) {
      setDetectedType("unknown");
      setSelectedType("unknown");
      setMessage(axiosMessage(error, "Detection failed. Choose a report type manually."));
      setUploadState("READY");
    }
  }

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    if (canSelectFile) setIsDragging(true);
  }, [canSelectFile]);

  const onDragLeave = useCallback(() => setIsDragging(false), []);

  const onDrop = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    setIsDragging(false);
    const dropped = event.dataTransfer.files?.[0];
    if (dropped && canSelectFile) void handleFile(dropped);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canSelectFile, organizationId, storeId]);

  async function confirmAndUpload() {
    if (!file || !organizationId || !storeId || selectedType === "unknown") return;

    const formData = new FormData();
    formData.append("file", file);
    formData.append("report_type", selectedType);
    formData.append("organization_id", organizationId);
    formData.append("store_id", storeId);

    setUploadState("UPLOADING");
    setUploadProgress(0);
    setUploadBytes({ loaded: 0, total: file.size || 0 });
    setMessage(null);

    try {
      const response = await axios.post<{
        message?: string;
        upload_id?: string;
        next_step?: string;
      }>(`${API_BASE}/upload-raw`, formData, {
        timeout: 0,
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
        onUploadProgress(event) {
          const total = event.total || file.size;
          if (!total) return;
          setUploadBytes({ loaded: event.loaded, total });
          setUploadProgress(Math.min(99, Math.round((event.loaded / total) * 100)));
        },
      });
      setUploadProgress(100);
      setUploadBytes((prev) =>
        prev && prev.total > 0 ? { loaded: prev.total, total: prev.total } : { loaded: file.size, total: file.size },
      );
      setUploadState("SUCCESS");
      const uid =
        typeof response.data?.upload_id === "string" && response.data.upload_id.trim()
          ? response.data.upload_id.trim()
          : null;
      setSuccessUploadId(uid);
      setMessage(
        typeof response.data?.message === "string" && response.data.message.trim()
          ? response.data.message.trim()
          : "File saved to storage. Open Imports → Process (staging), then Sync (Amazon tables).",
      );
      await loadHistory(organizationId);
    } catch (error) {
      setUploadState("ERROR");
      setMessage(axiosMessage(error, "Upload failed."));
    }
  }

  async function deleteUpload(row: HistoryRow) {
    const ok = window.confirm(`Delete "${row.file_name}" from upload history?`);
    if (!ok || !organizationId) return;
    setDeletingId(row.id);
    try {
      await axios.delete(`${API_BASE}/upload/${row.id}`);
      await loadHistory(organizationId);
    } catch (error) {
      setHistoryError(axiosMessage(error, "Could not delete upload."));
    } finally {
      setDeletingId(null);
    }
  }

  return (
    <div className="min-h-screen bg-background px-4 py-8 sm:px-6 lg:px-8">
      <div className="mx-auto max-w-6xl space-y-8">
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-widest text-violet-500">
              Data Management
            </p>
            <h1 className="mt-2 text-3xl font-bold tracking-tight text-foreground">
              Amazon ETL Importer
            </h1>
            <p className="mt-2 max-w-2xl text-sm text-muted-foreground">
              Upload (storage + import row) → <strong className="font-medium text-foreground">Stage</strong> writes{" "}
              <code className="rounded bg-muted px-1 text-xs">amazon_staging</code> →{" "}
              <strong className="font-medium text-foreground">Sync</strong> lands rows in Amazon domain tables. Progress
              bars use real byte counts for uploads and server counters while Stage/Sync run.
            </p>
          </div>
          <div className="rounded-2xl border border-border bg-card px-4 py-3 text-sm shadow-sm">
            <p className="text-xs text-muted-foreground">Selected company</p>
            <p className="mt-1 max-w-xs truncate font-semibold text-foreground">
              {profileLoading ? "Loading..." : organizationName || "No company selected"}
            </p>
          </div>
        </div>

        <section className="grid gap-6 lg:grid-cols-[minmax(0,1.15fr)_minmax(18rem,0.85fr)]">
          <div className="overflow-hidden rounded-2xl border border-border bg-card shadow-sm">
            <div className="flex flex-col gap-3 border-b border-border bg-muted/30 px-6 py-5 sm:flex-row sm:items-center sm:justify-between">
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-violet-500/10">
                  <UploadCloud className="h-5 w-5 text-violet-500" />
                </div>
                <div>
                  <h2 className="text-base font-semibold text-foreground">Upload report</h2>
                  <p className="text-xs text-muted-foreground">CSV/TXT files only</p>
                </div>
              </div>
              <span className={`rounded-full px-3 py-1 text-xs font-semibold ${STATE_STYLES[uploadState]}`}>
                {uploadState}
              </span>
            </div>

            <div className="space-y-5 px-6 py-6">
              <div>
                <label htmlFor="etl-store" className="mb-1.5 block text-sm font-medium text-foreground">
                  Store
                </label>
                {storesLoading ? (
                  <div className="flex h-10 items-center gap-2 text-sm text-muted-foreground">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Loading stores for selected company...
                  </div>
                ) : storesError ? (
                  <div className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
                    {storesError}
                  </div>
                ) : stores.length === 0 ? (
                  <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-800 dark:bg-amber-950/20 dark:text-amber-300">
                    No active stores found for this company.
                  </div>
                ) : (
                  <select
                    id="etl-store"
                    value={storeId}
                    disabled={busy}
                    onChange={(event) => {
                      setStoreId(event.target.value);
                      resetUpload();
                    }}
                    className="h-10 w-full rounded-lg border border-border bg-background px-3 text-sm text-foreground shadow-sm outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {stores.map((store) => (
                      <option key={store.id} value={store.id}>
                        {store.display_name}
                      </option>
                    ))}
                  </select>
                )}
              </div>

              {(uploadState === "IDLE" || uploadState === "ERROR") && (
                <div>
                  <label className="mb-1.5 block text-sm font-medium text-foreground">
                    Report file
                  </label>
                  <button
                    type="button"
                    disabled={!canSelectFile}
                    onClick={() => inputRef.current?.click()}
                    onDragOver={onDragOver}
                    onDragLeave={onDragLeave}
                    onDrop={onDrop}
                    className={[
                      "flex w-full flex-col items-center justify-center gap-3 rounded-2xl border-2 border-dashed px-6 py-12 text-center transition",
                      isDragging
                        ? "border-violet-400 bg-violet-50 dark:border-violet-600 dark:bg-violet-950/20"
                        : "border-border bg-muted/20 hover:border-violet-400/70 hover:bg-muted/40",
                      !canSelectFile ? "cursor-not-allowed opacity-50" : "cursor-pointer",
                    ].join(" ")}
                  >
                    <UploadCloud className="h-10 w-10 text-violet-500" />
                    <div>
                      <p className="text-sm font-semibold text-foreground">
                        Drag and drop a report file
                      </p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        or click to browse. The file is not uploaded until you confirm.
                      </p>
                    </div>
                  </button>
                  <input
                    ref={inputRef}
                    type="file"
                    accept=".csv,.txt,text/csv,text/plain"
                    className="hidden"
                    onChange={(event) => {
                      const selected = event.target.files?.[0];
                      event.target.value = "";
                      if (selected) void handleFile(selected);
                    }}
                  />
                </div>
              )}

              {uploadState === "DETECTING" && file && (
                <div className="rounded-2xl border border-violet-200 bg-violet-50 p-5 dark:border-violet-800 dark:bg-violet-950/20">
                  <div className="flex items-center gap-3">
                    <BrainCircuit className="h-6 w-6 animate-pulse text-violet-500" />
                    <div className="min-w-0 flex-1">
                      <p className="font-semibold text-violet-900 dark:text-violet-200">
                        Detecting report type
                      </p>
                      <p className="mt-1 truncate text-xs text-violet-700/80 dark:text-violet-300">
                        Reading headers from {file.name}
                      </p>
                    </div>
                    <Loader2 className="h-5 w-5 animate-spin text-violet-500" />
                  </div>
                </div>
              )}

              {uploadState === "READY" && file && (
                <div className="space-y-5 rounded-2xl border border-border bg-muted/20 p-5">
                  <div className="flex items-center gap-3">
                    <FileText className="h-5 w-5 shrink-0 text-violet-500" />
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-sm font-semibold text-foreground">{file.name}</p>
                      <p className="text-xs text-muted-foreground">{formatBytes(file.size)}</p>
                    </div>
                    <span className="rounded-full bg-violet-100 px-2.5 py-1 text-xs font-semibold text-violet-700 dark:bg-violet-950/50 dark:text-violet-300">
                      {detectMeta?.method === "rules" ? "Rule detected" : "AI detected"}
                    </span>
                  </div>

                  {message && (
                    <div className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800 dark:border-amber-800 dark:bg-amber-950/20 dark:text-amber-300">
                      <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                      {message}
                    </div>
                  )}

                  <div>
                    <label htmlFor="report-type" className="mb-1.5 block text-sm font-medium text-foreground">
                      Detected report type
                    </label>
                    <select
                      id="report-type"
                      value={selectedType}
                      onChange={(event) => setSelectedType(event.target.value as ReportType)}
                      className="h-10 w-full rounded-lg border border-border bg-background px-3 text-sm text-foreground shadow-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    >
                      {REPORT_OPTIONS.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.value}
                        </option>
                      ))}
                    </select>
                    <p className="mt-1.5 text-xs text-muted-foreground">
                      Detected as <span className="font-semibold">{detectedType ?? "unknown"}</span>. Override this before upload if it is wrong.
                    </p>
                  </div>
                </div>
              )}

              {uploadState === "UPLOADING" && file && (
                <div className="space-y-3 rounded-2xl border border-border bg-muted/20 p-5">
                  <div className="flex items-center gap-3">
                    <FileText className="h-5 w-5 shrink-0 text-violet-500" />
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-sm font-semibold text-foreground">{file.name}</p>
                      <p className="text-xs text-muted-foreground">
                        {uploadProgress < 100
                          ? "Sending bytes to the app server (browser → Next.js)…"
                          : "Saving to storage and registering upload in Supabase…"}
                      </p>
                      {uploadBytes && uploadBytes.total > 0 ? (
                        <p className="mt-1 text-xs tabular-nums text-muted-foreground">
                          {formatBytes(uploadBytes.loaded)} / {formatBytes(uploadBytes.total)} transferred
                        </p>
                      ) : null}
                    </div>
                    <span className="text-sm font-semibold text-foreground">{uploadProgress}%</span>
                  </div>
                  <div className="h-2.5 overflow-hidden rounded-full bg-muted">
                    <div
                      className="h-full rounded-full bg-violet-600 transition-[width] duration-300"
                      style={{ width: `${uploadProgress}%` }}
                    />
                  </div>
                </div>
              )}

              {uploadState === "SUCCESS" && (
                <div className="flex items-start gap-3 rounded-2xl border border-emerald-300 bg-emerald-50 p-4 dark:border-emerald-800 dark:bg-emerald-950/20">
                  <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0 text-emerald-600" />
                  <div className="min-w-0 flex-1">
                    <p className="font-semibold text-emerald-900 dark:text-emerald-200">Upload complete</p>
                    <p className="mt-1 text-sm text-emerald-800 dark:text-emerald-300">{message}</p>
                    {successUploadId ? (
                      <p className="mt-2 font-mono text-xs text-emerald-800/80 dark:text-emerald-300/80">
                        Upload id: {successUploadId}
                      </p>
                    ) : null}
                    <div className="mt-3 flex flex-wrap items-center gap-2">
                      {successUploadId ? (
                        <>
                          <button
                            type="button"
                            disabled={!organizationId || pipelineAction?.uploadId === successUploadId}
                            onClick={() => void runProcessForUpload(successUploadId)}
                            className="inline-flex items-center gap-2 rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-violet-700 disabled:cursor-not-allowed disabled:opacity-50"
                          >
                            {pipelineAction?.uploadId === successUploadId &&
                            pipelineAction.kind === "process" ? (
                              <Loader2 className="h-4 w-4 animate-spin" />
                            ) : null}
                            2 — Stage rows (Process)
                          </button>
                          <button
                            type="button"
                            disabled={
                              !organizationId ||
                              pipelineAction?.uploadId === successUploadId ||
                              !quickCanSync
                            }
                            title={
                              quickCanSync
                                ? "Land staged rows into Amazon domain tables"
                                : "Available after status is “staged” (run Stage first, then refresh if needed)."
                            }
                            onClick={() => void runSyncForUpload(successUploadId)}
                            className="inline-flex items-center gap-2 rounded-lg border border-emerald-600/50 bg-background px-4 py-2 text-sm font-semibold text-emerald-800 transition hover:bg-emerald-50 disabled:cursor-not-allowed disabled:opacity-50 dark:text-emerald-200 dark:hover:bg-emerald-950/40"
                          >
                            {pipelineAction?.uploadId === successUploadId &&
                            pipelineAction.kind === "sync" ? (
                              <Loader2 className="h-4 w-4 animate-spin" />
                            ) : null}
                            3 — Sync to Amazon tables
                          </button>
                        </>
                      ) : null}
                      <Link
                        href="/imports"
                        className="inline-flex items-center rounded-lg border border-border px-3 py-2 text-sm font-medium text-foreground transition hover:bg-accent"
                      >
                        Open Imports
                      </Link>
                    </div>
                    <p className="mt-2 text-xs text-emerald-800 dark:text-emerald-300">
                      Run <strong className="font-semibold">Stage</strong> first (CSV → <code className="rounded bg-muted px-1">amazon_staging</code>
                      ), then <strong className="font-semibold">Sync</strong> (staging → domain tables). Use{" "}
                      <strong>Sync</strong> only when history shows status <strong>staged</strong>.
                    </p>
                  </div>
                </div>
              )}

              {uploadState === "ERROR" && message && (
                <div className="flex items-start gap-3 rounded-2xl border border-destructive/40 bg-destructive/10 p-4">
                  <XCircle className="mt-0.5 h-5 w-5 shrink-0 text-destructive" />
                  <div>
                    <p className="font-semibold text-destructive">Upload failed</p>
                    <p className="mt-1 text-sm text-destructive/80">{message}</p>
                  </div>
                </div>
              )}

              <div className="flex flex-wrap items-center gap-3">
                {uploadState === "READY" && (
                  <button
                    type="button"
                    disabled={!canConfirmUpload}
                    onClick={() => void confirmAndUpload()}
                    className="inline-flex items-center gap-2 rounded-lg bg-violet-600 px-5 py-2.5 text-sm font-semibold text-white shadow-sm transition hover:bg-violet-700 disabled:cursor-not-allowed disabled:opacity-40"
                  >
                    <UploadCloud className="h-4 w-4" />
                    Confirm &amp; Upload
                  </button>
                )}
                {busy && (
                  <div className="inline-flex items-center gap-2 text-sm text-muted-foreground">
                    <Loader2 className="h-4 w-4 animate-spin text-violet-500" />
                    {uploadState === "DETECTING" ? "Detecting..." : "Uploading..."}
                  </div>
                )}
                {(uploadState === "READY" || uploadState === "SUCCESS" || uploadState === "ERROR") && (
                  <button
                    type="button"
                    onClick={resetUpload}
                    className="inline-flex items-center gap-2 rounded-lg border border-border bg-background px-4 py-2.5 text-sm font-medium text-foreground transition hover:bg-accent"
                  >
                    {uploadState === "SUCCESS" ? "Upload another" : "Start over"}
                  </button>
                )}
              </div>
            </div>
          </div>

          <aside className="rounded-2xl border border-border bg-card p-5 shadow-sm">
            <div className="flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-sky-500/10">
                <Database className="h-5 w-5 text-sky-500" />
              </div>
              <div>
                <h2 className="text-base font-semibold text-foreground">Import scope</h2>
                <p className="text-xs text-muted-foreground">Current company and store</p>
              </div>
            </div>
            <dl className="mt-5 space-y-4 text-sm">
              <div>
                <dt className="text-xs uppercase tracking-wide text-muted-foreground">Company</dt>
                <dd className="mt-1 break-words font-medium text-foreground">{organizationName || "None selected"}</dd>
              </div>
              <div>
                <dt className="text-xs uppercase tracking-wide text-muted-foreground">Store</dt>
                <dd className="mt-1 break-words font-medium text-foreground">
                  {selectedStore?.display_name ?? "No store selected"}
                </dd>
              </div>
              <div>
                <dt className="text-xs uppercase tracking-wide text-muted-foreground">Supported reports</dt>
                <dd className="mt-2 flex flex-wrap gap-2">
                  {REPORT_OPTIONS.filter((o) => o.value !== "unknown").map((o) => (
                    <span key={o.value} className="rounded-full bg-muted px-2.5 py-1 text-xs text-muted-foreground">
                      {o.value}
                    </span>
                  ))}
                </dd>
              </div>
            </dl>
          </aside>
        </section>

        <section className="overflow-hidden rounded-2xl border border-border bg-card shadow-sm">
          <div className="flex flex-col gap-3 border-b border-border bg-muted/30 px-6 py-5 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h2 className="text-base font-semibold text-foreground">Upload history</h2>
              <p className="text-xs text-muted-foreground">Files imported for the selected company</p>
            </div>
            <button
              type="button"
              disabled={historyLoading || !organizationId}
              onClick={() => {
                if (!organizationId) return;
                setHistoryError(null);
                void loadHistory(organizationId);
              }}
              className="inline-flex items-center gap-2 rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium text-foreground transition hover:bg-accent disabled:opacity-40"
            >
              <RefreshCw className={`h-4 w-4 ${historyLoading ? "animate-spin" : ""}`} />
              Refresh
            </button>
          </div>

          {historyLoading ? (
            <div className="flex items-center justify-center gap-2 px-6 py-12 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Loading history...
            </div>
          ) : historyError ? (
            <div className="flex items-center gap-2 px-6 py-8 text-sm text-destructive">
              <XCircle className="h-4 w-4" />
              {historyError}
            </div>
          ) : history.length === 0 ? (
            <div className="px-6 py-12 text-center">
              <Database className="mx-auto h-8 w-8 text-muted-foreground/60" />
              <p className="mt-3 text-sm font-medium text-foreground">No uploads yet</p>
              <p className="mt-1 text-xs text-muted-foreground">
                Imports for this company will appear here.
              </p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                    <th className="px-4 py-3">File Name</th>
                    <th className="px-4 py-3">Type</th>
                    <th className="px-4 py-3 text-right">Rows</th>
                    <th className="px-4 py-3">Date</th>
                    <th className="px-4 py-3">Status</th>
                    <th className="min-w-[11rem] px-4 py-3">Pipeline</th>
                    <th className="px-4 py-3 text-right">Action</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {history.map((row) => (
                    <tr key={row.id} className="transition hover:bg-muted/30">
                      <td className="max-w-[18rem] truncate px-4 py-3 font-medium text-foreground" title={row.file_name}>
                        {row.file_name}
                      </td>
                      <td className="px-4 py-3 text-muted-foreground">{prettyType(row)}</td>
                      <td className="px-4 py-3 text-right tabular-nums text-muted-foreground">
                        {row.metadata?.row_count != null ? row.metadata.row_count.toLocaleString() : "—"}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 text-muted-foreground">{formatDate(row.created_at)}</td>
                      <td className="px-4 py-3">
                        <span className={`inline-flex rounded-full px-2.5 py-0.5 text-xs font-semibold capitalize ${statusClass(row.status)}`}>
                          {row.status}
                        </span>
                      </td>
                      <td className="min-w-[11rem] px-4 py-3 align-top">
                        <div className="space-y-2">
                          {String(row.status).toLowerCase() === "processing" ? (
                            <div className="space-y-1">
                              {(() => {
                                const sp = resolveSyncPercent(row);
                                const pp = resolveStagePercent(row);
                                const pct =
                                  sp != null && sp > 0 ? sp : pp != null && pp > 0 ? pp : sp ?? pp ?? null;
                                return pct != null ? (
                                  <div className="h-1.5 overflow-hidden rounded-full bg-muted">
                                    <div
                                      className="h-full rounded-full bg-violet-500 transition-[width]"
                                      style={{ width: `${pct}%` }}
                                    />
                                  </div>
                                ) : (
                                  <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                                    <Loader2 className="h-3.5 w-3.5 animate-spin text-violet-500" />
                                    Working…
                                  </div>
                                );
                              })()}
                              {row.pipeline?.current_phase_label ? (
                                <p className="line-clamp-2 text-[11px] leading-snug text-muted-foreground">
                                  {row.pipeline.current_phase_label}
                                </p>
                              ) : null}
                            </div>
                          ) : null}
                          <div className="flex flex-wrap gap-1.5">
                            {canRunProcess(row) ? (
                              <button
                                type="button"
                                disabled={
                                  !organizationId ||
                                  pipelineAction?.uploadId === row.id ||
                                  deletingId === row.id
                                }
                                onClick={() => void runProcessForUpload(row.id)}
                                className="inline-flex items-center gap-1 rounded-md bg-violet-600 px-2.5 py-1 text-xs font-semibold text-white shadow-sm transition hover:bg-violet-700 disabled:cursor-not-allowed disabled:opacity-40"
                              >
                                {pipelineAction?.uploadId === row.id && pipelineAction.kind === "process" ? (
                                  <Loader2 className="h-3 w-3 animate-spin" />
                                ) : null}
                                Stage
                              </button>
                            ) : null}
                            {canRunSync(row) ? (
                              <button
                                type="button"
                                disabled={
                                  !organizationId ||
                                  pipelineAction?.uploadId === row.id ||
                                  deletingId === row.id
                                }
                                onClick={() => void runSyncForUpload(row.id)}
                                className="inline-flex items-center gap-1 rounded-md border border-emerald-600/60 bg-background px-2.5 py-1 text-xs font-semibold text-emerald-800 transition hover:bg-emerald-50 disabled:cursor-not-allowed disabled:opacity-40 dark:text-emerald-200 dark:hover:bg-emerald-950/30"
                              >
                                {pipelineAction?.uploadId === row.id && pipelineAction.kind === "sync" ? (
                                  <Loader2 className="h-3 w-3 animate-spin" />
                                ) : null}
                                Sync
                              </button>
                            ) : null}
                            {!canRunProcess(row) && !canRunSync(row) && String(row.status).toLowerCase() !== "processing" ? (
                              <span className="text-xs text-muted-foreground">—</span>
                            ) : null}
                          </div>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-right">
                        <button
                          type="button"
                          disabled={deletingId === row.id}
                          onClick={() => void deleteUpload(row)}
                          className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-border text-muted-foreground transition hover:border-destructive/50 hover:bg-destructive/10 hover:text-destructive disabled:opacity-40"
                          title="Delete upload"
                        >
                          {deletingId === row.id ? (
                            <Loader2 className="h-4 w-4 animate-spin" />
                          ) : (
                            <Trash2 className="h-4 w-4" />
                          )}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
