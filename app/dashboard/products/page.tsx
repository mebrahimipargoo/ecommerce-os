"use client";

import React, { useCallback, useEffect, useRef, useState } from "react";
import axios, { AxiosError } from "axios";
import Link from "next/link";
import { useUserRole } from "../../../components/UserRoleContext";
import { useRbacPermissions } from "../../../hooks/useRbacPermissions";
import { getPimIntegrationsSummary, type PimIntegrationsSummary } from "./pim-actions";
import {
  Ban,
  Building2,
  CheckCircle2,
  Cloud,
  FileSpreadsheet,
  Globe2,
  LayoutGrid,
  Link2,
  Loader2,
  Package,
  Plug,
  RefreshCw,
  Sparkles,
  Table2,
  UploadCloud,
  X,
} from "lucide-react";
import { PimCatalogHub } from "./pim/PimCatalogHub";

function etlApiBase(): string {
  const raw = (process.env.NEXT_PUBLIC_ETL_API_ORIGIN || "").trim().replace(/\/$/, "");
  return raw || "http://127.0.0.1:8000";
}

type TabId = "catalog" | "import" | "integrations";

type ImportPhase = "idle" | "selected" | "uploading" | "success" | "error";

type SeedProductsMetrics = {
  rows_processed?: number;
  vendors_created: number;
  categories_created?: number;
  products_created: number;
  products_updated?: number;
  identifiers_created?: number;
  identifiers_updated?: number;
  prices_inserted?: number;
  products_enriched_by_amazon: number;
  skipped_no_identity?: number;
  skipped_ambiguous?: number;
  errors?: string[];
  /** Legacy ETL response (pre store-scoped PIM metrics). */
  skus_mapped?: number;
  skipped_garbage?: number;
};

type SeedProductsResponse = {
  status: string;
  message: string;
  metrics: SeedProductsMetrics;
};

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

function CatalogSheetsSettingsLink() {
  const { canSeeSettings } = useRbacPermissions();
  if (!canSeeSettings) {
    return (
      <span className="inline-flex items-center gap-1.5 rounded-lg border border-dashed border-border px-3 py-1.5 text-xs font-medium text-muted-foreground">
        <Plug className="h-3.5 w-3.5 shrink-0" aria-hidden />
        Catalog and Google Sheets — contact an admin for Settings access
      </span>
    );
  }
  return (
    <Link
      href="/settings#catalog_imports"
      className="inline-flex items-center gap-1.5 rounded-lg border border-border px-3 py-1.5 text-xs font-semibold text-foreground/90 transition hover:bg-muted/60"
    >
      <Plug className="h-3.5 w-3.5" aria-hidden />
      Catalog and Google Sheets (Settings)
    </Link>
  );
}

const TABS: { id: TabId; label: string; icon: React.ElementType }[] = [
  { id: "catalog", label: "Catalog Hub", icon: LayoutGrid },
  { id: "import", label: "Quick CSV & files", icon: Sparkles },
  { id: "integrations", label: "Integrations & API", icon: Plug },
];

export default function ProductInformationManagementPage() {
  const [activeTab, setActiveTab] = useState<TabId>("catalog");
  const { organizationId, organizationName, profileLoading } = useUserRole();

  return (
    <div className="relative min-h-screen overflow-hidden bg-background px-4 py-10 sm:px-6 lg:px-8">
      <div
        className="pointer-events-none absolute inset-0 opacity-40 dark:opacity-25"
        aria-hidden
      >
        <div className="absolute -left-32 top-0 h-96 w-96 rounded-full bg-primary/20 blur-3xl" />
        <div className="absolute bottom-0 right-0 h-80 w-80 rounded-full bg-sky-400/15 blur-3xl dark:bg-sky-500/10" />
      </div>

      <div className="relative mx-auto max-w-6xl">
        <header className="mb-8 rounded-2xl border border-border/60 bg-card/70 p-6 shadow-xl backdrop-blur-md dark:bg-card/50 sm:p-8">
          <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-widest text-primary">PIM</p>
              <h1 className="mt-1 text-2xl font-semibold tracking-tight text-foreground sm:text-3xl">
                Product Information Management
              </h1>
              <p className="mt-2 max-w-xl text-sm text-muted-foreground">
                Live catalog from your workspace data, a fast CSV path through the ETL service, and integration status.
                For full staged file imports (manual report upload flow), use{" "}
                <strong className="font-medium text-foreground">Imports</strong> under Data Management (
                <Link href="/dashboard/file-import" className="font-medium text-primary underline-offset-2 hover:underline">
                  /dashboard/file-import
                </Link>
                ).
              </p>
            </div>
          </div>

          <nav
            className="mt-8 flex flex-wrap gap-2 rounded-xl border border-border/50 bg-muted/20 p-1.5 backdrop-blur-sm"
            aria-label="PIM sections"
          >
            {TABS.map(({ id, label, icon: Icon }) => {
              const selected = activeTab === id;
              return (
                <button
                  key={id}
                  type="button"
                  onClick={() => setActiveTab(id)}
                  className={[
                    "inline-flex flex-1 items-center justify-center gap-2 rounded-lg px-3 py-2.5 text-sm font-medium transition-all min-w-[140px]",
                    selected
                      ? "bg-card text-foreground shadow-md ring-1 ring-border/80"
                      : "text-muted-foreground hover:bg-background/60 hover:text-foreground",
                  ].join(" ")}
                >
                  <Icon className="h-4 w-4 shrink-0" aria-hidden />
                  {label}
                </button>
              );
            })}
          </nav>
        </header>

        {activeTab === "catalog" && <PimCatalogHub organizationId={organizationId} />}
        {activeTab === "import" && (
          <AiCsvImportPanel
            organizationId={organizationId}
            organizationName={organizationName}
            profileLoading={profileLoading}
          />
        )}
        {activeTab === "integrations" && <IntegrationsPanel organizationId={organizationId} />}
      </div>
    </div>
  );
}

type AiCsvImportPanelProps = {
  organizationId: string | null;
  organizationName: string;
  profileLoading: boolean;
};

function AiCsvImportPanel({ organizationId, organizationName, profileLoading }: AiCsvImportPanelProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [file, setFile] = useState<File | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [phase, setPhase] = useState<ImportPhase>("idle");
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [metrics, setMetrics] = useState<SeedProductsMetrics | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [etlDefaultStoreId, setEtlDefaultStoreId] = useState<string | null>(null);
  const [etlStoreSummaryLoading, setEtlStoreSummaryLoading] = useState(false);

  useEffect(() => {
    const oid = organizationId?.trim();
    if (!oid) {
      setEtlDefaultStoreId(null);
      return;
    }
    let cancelled = false;
    setEtlStoreSummaryLoading(true);
    void getPimIntegrationsSummary(oid)
      .then((r) => {
        if (cancelled) return;
        if (!r.ok) {
          setEtlDefaultStoreId(null);
          return;
        }
        setEtlDefaultStoreId(r.data.defaultStoreId?.trim() || null);
      })
      .finally(() => {
        if (!cancelled) setEtlStoreSummaryLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [organizationId]);

  const acceptFile = useCallback((f: File) => {
    const ext = f.name.split(".").pop()?.toLowerCase() ?? "";
    if (ext !== "csv") {
      setErrorMsg("Only .csv files are supported.");
      setFile(null);
      setPhase("idle");
      return;
    }
    setFile(f);
    setErrorMsg(null);
    setMetrics(null);
    setSuccessMessage(null);
    setPhase("selected");
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback(() => {
    setIsDragging(false);
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setIsDragging(false);
      const f = e.dataTransfer.files?.[0];
      if (f) acceptFile(f);
    },
    [acceptFile],
  );

  const resetImport = useCallback(() => {
    setFile(null);
    setPhase("idle");
    setErrorMsg(null);
    setMetrics(null);
    setSuccessMessage(null);
    if (fileInputRef.current) fileInputRef.current.value = "";
  }, []);

  async function startIntelligentSync() {
    if (!file || phase === "uploading" || profileLoading || !organizationId) return;
    const sid = etlDefaultStoreId?.trim();
    if (!sid) {
      setErrorMsg("Set a default store in Settings → General before CSV seed (ETL requires store_id).");
      setPhase("error");
      return;
    }

    const formData = new FormData();
    formData.append("file", file);
    formData.append("organization_id", organizationId);
    formData.append("store_id", sid);

    setPhase("uploading");
    setErrorMsg(null);
    setMetrics(null);
    setSuccessMessage(null);

    try {
      const base = etlApiBase();
      const response = await axios.post<SeedProductsResponse>(`${base}/etl/seed-products`, formData, {
        timeout: 0,
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
      });
      const m = response.data?.metrics;
      if (m) {
        setMetrics(m);
        setSuccessMessage(response.data.message ?? "Sync completed.");
        setPhase("success");
      } else {
        setErrorMsg("Unexpected response: missing metrics.");
        setPhase("error");
      }
    } catch (err) {
      setErrorMsg(axiosMessage(err, "Intelligent sync failed."));
      setPhase("error");
    }
  }

  const isBusy = phase === "uploading";
  const showDropZone = phase === "idle" || phase === "selected" || phase === "error";
  const canSync = Boolean(organizationId) && !profileLoading && Boolean(etlDefaultStoreId?.trim());

  return (
    <section
      className="rounded-2xl border border-border/60 bg-card/70 p-6 shadow-xl backdrop-blur-md dark:bg-card/50 sm:p-8"
      aria-labelledby="import-heading"
    >
      <h2 id="import-heading" className="text-lg font-semibold text-foreground">
        Quick CSV & file routing
      </h2>
      <p className="mt-1 text-sm text-muted-foreground">
        This tab runs the <strong>fast CSV seed</strong> endpoint only (<code className="rounded bg-muted px-1 text-[11px]">.csv</code>
        ). For Excel, multiple sheets, text/PDF flows, and the full Amazon-managed pipeline (read → stage → clean →
        tables → sync with reliable progress), use the dedicated importers — same behavior as the rest of the platform.
      </p>

      <div className="mt-4 flex flex-col gap-2 rounded-xl border border-sky-200/80 bg-sky-50/50 px-4 py-3 text-sm dark:border-sky-800/50 dark:bg-sky-950/25">
        <p className="font-medium text-sky-950 dark:text-sky-100">Managed imports (recommended for large or multi-format files)</p>
        <div className="flex flex-wrap gap-2">
          <Link
            href="/dashboard/file-import"
            className="inline-flex items-center gap-1.5 rounded-lg border border-sky-300 bg-background px-3 py-1.5 text-xs font-semibold text-sky-900 shadow-sm transition hover:bg-sky-100 dark:border-sky-700 dark:text-sky-100 dark:hover:bg-sky-900/40"
          >
            <FileSpreadsheet className="h-3.5 w-3.5" aria-hidden />
            Imports
          </Link>
          <CatalogSheetsSettingsLink />
        </div>
      </div>

      <div className="mt-4 rounded-lg border border-border bg-muted/20 px-3 py-2 text-xs text-muted-foreground">
        <p>
          <span className="font-medium text-foreground">Workspace:</span>{" "}
          {profileLoading ? "…" : organizationName || "—"}
        </p>
        {!profileLoading && !organizationId ? (
          <p className="mt-1 text-amber-800 dark:text-amber-200">
            No organization in scope. Choose a company from the header (super admins may switch workspace).
          </p>
        ) : null}
        {!profileLoading && organizationId && !etlStoreSummaryLoading && !etlDefaultStoreId?.trim() ? (
          <p className="mt-1 text-amber-800 dark:text-amber-200">
            CSV seed needs a default store. Set it under Settings → General (organization default store).
          </p>
        ) : null}
        {etlStoreSummaryLoading && organizationId ? (
          <p className="mt-1 flex items-center gap-1.5 text-muted-foreground">
            <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
            Loading default store for ETL…
          </p>
        ) : null}
      </div>

      <div className="mt-6 space-y-6">
        {showDropZone && (
          <div
            role="button"
            tabIndex={0}
            aria-label="Drop zone for product CSV"
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
            onClick={() => !isBusy && fileInputRef.current?.click()}
            onKeyDown={(e) => {
              if (!isBusy && (e.key === "Enter" || e.key === " ")) {
                e.preventDefault();
                fileInputRef.current?.click();
              }
            }}
            className={[
              "flex cursor-pointer flex-col items-center justify-center gap-3 rounded-xl border-2 border-dashed px-6 py-14 text-center transition-colors",
              isBusy ? "pointer-events-none opacity-60" : "",
              isDragging
                ? "border-sky-400 bg-sky-50/50 dark:border-sky-500 dark:bg-sky-950/30"
                : file && phase !== "idle"
                  ? "border-emerald-400/80 bg-emerald-50/30 dark:border-emerald-600 dark:bg-emerald-950/20"
                  : "border-border bg-muted/20 hover:border-primary/40 hover:bg-muted/35",
            ].join(" ")}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              className="hidden"
              disabled={isBusy}
              onChange={(e) => {
                const f = e.target.files?.[0];
                if (f) acceptFile(f);
              }}
            />
            <div className="flex h-14 w-14 items-center justify-center rounded-2xl border border-border/60 bg-background/80 shadow-inner">
              <UploadCloud className={`h-7 w-7 ${file ? "text-emerald-500" : "text-primary"}`} />
            </div>
            {file && phase !== "idle" ? (
              <>
                <div className="flex items-center gap-2">
                  <FileSpreadsheet className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
                  <span className="text-sm font-semibold text-emerald-800 dark:text-emerald-200">{file.name}</span>
                </div>
                <p className="max-w-md text-xs text-muted-foreground">
                  {profileLoading ? (
                    "Resolving workspace…"
                  ) : organizationId ? (
                    <>
                      Target organization{" "}
                      <code className="rounded bg-muted px-1 font-mono text-[10px] text-foreground">{organizationId}</code>
                    </>
                  ) : (
                    <span className="text-amber-800 dark:text-amber-200">
                      Select a workspace organization before syncing.
                    </span>
                  )}
                </p>
                <button
                  type="button"
                  disabled={isBusy || !canSync}
                  onClick={(e) => {
                    e.stopPropagation();
                    void startIntelligentSync();
                  }}
                  className="mt-2 inline-flex h-10 items-center gap-2 rounded-lg bg-primary px-5 text-sm font-semibold text-primary-foreground shadow-sm transition hover:opacity-95 disabled:cursor-not-allowed disabled:opacity-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                >
                  {isBusy ? (
                    <>
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Processing…
                    </>
                  ) : (
                    <>
                      <Sparkles className="h-4 w-4" />
                      Start Intelligent Sync
                    </>
                  )}
                </button>
              </>
            ) : (
              <>
                <p className="text-sm font-medium text-foreground">Drag and drop your .csv here</p>
                <p className="text-xs text-muted-foreground">or click to browse — CSV only</p>
              </>
            )}
          </div>
        )}

        {errorMsg && (
          <div
            className="rounded-lg border border-destructive/30 bg-destructive/10 px-4 py-3 text-sm text-destructive"
            role="alert"
          >
            {errorMsg}
          </div>
        )}

        {phase === "success" && metrics && (
          <div className="space-y-4">
            <div className="flex items-center gap-2 text-sm text-emerald-600 dark:text-emerald-400">
              <CheckCircle2 className="h-4 w-4 shrink-0" />
              <span>{successMessage}</span>
            </div>
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
              {typeof metrics.rows_processed === "number" && (
                <MetricCard icon={Table2} label="Rows processed" value={metrics.rows_processed} tone="slate" />
              )}
              <MetricCard icon={Building2} label="Vendors created" value={metrics.vendors_created} tone="sky" />
              {typeof metrics.categories_created === "number" && (
                <MetricCard icon={LayoutGrid} label="Categories created" value={metrics.categories_created} tone="sky" />
              )}
              <MetricCard icon={Package} label="Products created" value={metrics.products_created} tone="violet" />
              {typeof metrics.products_updated === "number" && (
                <MetricCard icon={Package} label="Products updated" value={metrics.products_updated} tone="violet" />
              )}
              <MetricCard
                icon={Globe2}
                label="Enriched by Amazon"
                value={metrics.products_enriched_by_amazon}
                tone="emerald"
              />
              <MetricCard
                icon={Link2}
                label="Identifiers created"
                value={metrics.identifiers_created ?? metrics.skus_mapped ?? 0}
                tone="amber"
              />
              {typeof metrics.identifiers_updated === "number" && (
                <MetricCard icon={Link2} label="Identifiers updated" value={metrics.identifiers_updated} tone="amber" />
              )}
              {typeof metrics.prices_inserted === "number" && (
                <MetricCard icon={FileSpreadsheet} label="Prices inserted" value={metrics.prices_inserted} tone="emerald" />
              )}
              {typeof metrics.skipped_no_identity === "number" && (
                <MetricCard icon={Ban} label="Skipped (no identity)" value={metrics.skipped_no_identity} tone="slate" />
              )}
              {typeof metrics.skipped_ambiguous === "number" && (
                <MetricCard icon={Ban} label="Skipped (ambiguous)" value={metrics.skipped_ambiguous} tone="slate" />
              )}
              {typeof metrics.skipped_garbage === "number" && (
                <MetricCard icon={Ban} label="Rows skipped (legacy)" value={metrics.skipped_garbage} tone="slate" />
              )}
            </div>
            {Array.isArray(metrics.errors) && metrics.errors.length > 0 ? (
              <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-950 dark:text-amber-100">
                <p className="font-medium">Import messages ({metrics.errors.length})</p>
                <ul className="mt-2 max-h-40 list-inside list-disc space-y-1 overflow-y-auto">
                  {metrics.errors.slice(0, 25).map((e, i) => (
                    <li key={i}>{e}</li>
                  ))}
                </ul>
              </div>
            ) : null}
            <button
              type="button"
              onClick={resetImport}
              className="text-sm font-medium text-primary underline-offset-4 hover:underline"
            >
              Upload another file
            </button>
          </div>
        )}

        {phase === "uploading" && (
          <div className="flex flex-col items-center justify-center gap-4 rounded-xl border border-primary/20 bg-primary/5 py-16">
            <Loader2 className="h-12 w-12 animate-spin text-primary" aria-label="Loading" />
            <p className="text-sm font-medium text-foreground">Mapping columns and syncing catalog…</p>
            <p className="text-xs text-muted-foreground">This may take a moment for large files.</p>
          </div>
        )}
      </div>
    </section>
  );
}

function MetricCard({
  icon: Icon,
  label,
  value,
  tone,
}: {
  icon: React.ElementType;
  label: string;
  value: number;
  tone: "sky" | "violet" | "emerald" | "amber" | "slate";
}) {
  const toneRing: Record<typeof tone, string> = {
    sky: "ring-sky-500/20 bg-sky-500/5",
    violet: "ring-violet-500/20 bg-violet-500/5",
    emerald: "ring-emerald-500/20 bg-emerald-500/5",
    amber: "ring-amber-500/20 bg-amber-500/5",
    slate: "ring-slate-500/20 bg-slate-500/5",
  };
  const toneIcon: Record<typeof tone, string> = {
    sky: "text-sky-600 dark:text-sky-400",
    violet: "text-violet-600 dark:text-violet-400",
    emerald: "text-emerald-600 dark:text-emerald-400",
    amber: "text-amber-600 dark:text-amber-400",
    slate: "text-slate-600 dark:text-slate-400",
  };

  return (
    <div className={`rounded-xl border border-border/60 p-5 shadow-sm ring-1 backdrop-blur-sm ${toneRing[tone]}`}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">{label}</p>
          <p className="mt-2 text-3xl font-semibold tabular-nums tracking-tight text-foreground">{value}</p>
        </div>
        <div className={`rounded-lg border border-border/40 bg-background/80 p-2 ${toneIcon[tone]}`}>
          <Icon className="h-5 w-5" aria-hidden />
        </div>
      </div>
    </div>
  );
}

type IntegrationsPanelProps = {
  organizationId: string | null;
};

function IntegrationsPanel({ organizationId }: IntegrationsPanelProps) {
  return <IntegrationsHub organizationId={organizationId} />;
}

function IntegrationStatusCard({
  title,
  description,
  icon: Icon,
  connected,
  configureHref,
  canConfigureSettings,
}: {
  title: string;
  description: string;
  icon: React.ElementType;
  connected: boolean;
  configureHref: string;
  canConfigureSettings: boolean;
}) {
  return (
    <div className="rounded-xl border border-border/60 bg-muted/10 p-4 shadow-sm">
      <div className="flex items-start gap-3">
        <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg border border-border/50 bg-card">
          <Icon className="h-5 w-5 text-primary" aria-hidden />
        </div>
        <div className="min-w-0 flex-1">
          <p className="text-sm font-semibold text-foreground">{title}</p>
          <p className="mt-1 text-xs text-muted-foreground">{description}</p>
          <p
            className={
              connected
                ? "mt-2 text-sm font-medium text-emerald-600 dark:text-emerald-400"
                : "mt-2 text-sm font-medium text-amber-700 dark:text-amber-300"
            }
          >
            {connected ? "Connected" : "Not configured"}
          </p>
          {canConfigureSettings ? (
            <Link
              href={configureHref}
              className="mt-2 inline-block text-xs font-medium text-primary underline-offset-2 hover:underline"
            >
              Open in Settings
            </Link>
          ) : (
            <p className="mt-2 text-xs text-muted-foreground">Contact an admin to configure.</p>
          )}
        </div>
      </div>
    </div>
  );
}

function IntegrationsHub({ organizationId }: { organizationId: string | null }) {
  const { canSeeSettings } = useRbacPermissions();
  const [summary, setSummary] = useState<PimIntegrationsSummary | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loadingSummary, setLoadingSummary] = useState(false);
  const [syncPhase, setSyncPhase] = useState<"idle" | "syncing" | "done" | "error">("idle");
  const [syncMessage, setSyncMessage] = useState<string | null>(null);
  const [syncError, setSyncError] = useState<string | null>(null);

  const loadSummary = useCallback(async () => {
    if (!organizationId?.trim()) {
      setSummary(null);
      setLoadError(null);
      setLoadingSummary(false);
      return;
    }
    setLoadingSummary(true);
    setLoadError(null);
    try {
      const res = await getPimIntegrationsSummary(organizationId.trim());
      if (!res.ok) {
        setLoadError(res.error);
        setSummary(null);
        return;
      }
      setSummary(res.data);
    } catch (e) {
      setLoadError(e instanceof Error ? e.message : "Failed to load integrations.");
      setSummary(null);
    } finally {
      setLoadingSummary(false);
    }
  }, [organizationId]);

  useEffect(() => {
    void loadSummary();
  }, [loadSummary]);

  async function syncGoogleSheets() {
    const oid = organizationId?.trim();
    if (!oid) return;
    setSyncPhase("syncing");
    setSyncError(null);
    setSyncMessage(null);
    try {
      const sid = summary?.defaultStoreId?.trim();
      if (!sid) {
        setSyncError("Set a default store in Settings → General (or pick a store for imports) before Google Sheets sync.");
        setSyncPhase("error");
        return;
      }
      const fd = new FormData();
      fd.append("organization_id", oid);
      fd.append("store_id", sid);
      const base = etlApiBase();
      const res = await axios.post<{ status?: string; message?: string }>(`${base}/etl/sync-google-sheets`, fd, {
        timeout: 120_000,
      });
      setSyncMessage(res.data?.message ?? res.data?.status ?? "Sync finished.");
      setSyncPhase("done");
      void loadSummary();
      window.dispatchEvent(new Event("pim-catalog-refresh"));
    } catch (err) {
      setSyncError(axiosMessage(err, "Google Sheets sync failed."));
      setSyncPhase("error");
    }
  }

  const sheetId = summary?.googleSheetId?.trim() ?? "";
  const googleReady = Boolean(summary?.connectionStatus.googleSheets);
  const amazonReady = Boolean(summary?.connectionStatus.amazonSpApi);
  const openAiReady = Boolean(summary?.connectionStatus.openai);

  return (
    <div className="space-y-6">
      {!organizationId?.trim() ? (
        <p className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-sm text-amber-900 dark:text-amber-100">
          Select a workspace organization in the header.
        </p>
      ) : null}

      {loadError ? (
        <div
          className="flex flex-col gap-3 rounded-xl border border-destructive/30 bg-destructive/10 px-4 py-4 sm:flex-row sm:items-center sm:justify-between"
          role="alert"
        >
          <p className="text-sm text-destructive">{loadError}</p>
          <button
            type="button"
            disabled={loadingSummary || !organizationId?.trim()}
            onClick={() => void loadSummary()}
            className="inline-flex shrink-0 items-center gap-2 rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium text-foreground transition hover:bg-accent disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${loadingSummary ? "animate-spin" : ""}`} />
            Retry
          </button>
        </div>
      ) : null}

      <section
        className="rounded-2xl border border-border/60 bg-card/70 p-6 shadow-xl backdrop-blur-md dark:bg-card/50 sm:p-8"
        aria-labelledby="integrations-status-heading"
      >
        <h2 id="integrations-status-heading" className="text-lg font-semibold text-foreground">
          Connection status
        </h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Read-only summary from workspace keys and marketplaces. No secrets are entered on this page — configure
          everything in{" "}
          {canSeeSettings ? (
            <Link href="/settings" className="font-medium text-primary underline-offset-2 hover:underline">
              Settings
            </Link>
          ) : (
            <span className="font-medium text-muted-foreground">Settings (contact an admin for access)</span>
          )}
          .
        </p>
        {loadingSummary && !summary ? (
          <div className="mt-6 flex justify-center py-10">
            <Loader2 className="h-8 w-8 animate-spin text-primary" aria-label="Loading" />
          </div>
        ) : (
          <div className="mt-6 grid gap-4 sm:grid-cols-3">
            <IntegrationStatusCard
              title="Amazon SP-API"
              description="Selling Partner / marketplace credentials for enrichment and catalog."
              icon={Globe2}
              connected={amazonReady}
              configureHref="/settings#marketplaces"
              canConfigureSettings={canSeeSettings}
            />
            <IntegrationStatusCard
              title="OpenAI / GPT"
              description="LLM keys used by Sheets sync and intelligent mapping when configured."
              icon={Sparkles}
              connected={openAiReady}
              configureHref="/settings#ai_quotas"
              canConfigureSettings={canSeeSettings}
            />
            <IntegrationStatusCard
              title="Google Sheets"
              description="Service account JSON and spreadsheet ID for catalog sync."
              icon={Table2}
              connected={googleReady}
              configureHref="/settings#catalog_imports"
              canConfigureSettings={canSeeSettings}
            />
          </div>
        )}
      </section>

      <section
        className="rounded-2xl border border-border/60 bg-card/70 p-6 shadow-xl backdrop-blur-md dark:bg-card/50 sm:p-8"
        aria-labelledby="integrations-google-heading"
      >
        <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div className="flex items-start gap-3">
            <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl border border-border/60 bg-gradient-to-br from-violet-500/15 to-sky-500/15">
              <Table2 className="h-6 w-6 text-primary" aria-hidden />
            </div>
            <div>
              <h2 id="integrations-google-heading" className="text-lg font-semibold text-foreground">
                Google Sheets sync
              </h2>
              <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
                Run a full multi-tab sync against your Python backend. Spreadsheet ID and service account are managed in{" "}
                {canSeeSettings ? (
                  <Link href="/settings#catalog_imports" className="font-medium text-primary underline-offset-2 hover:underline">
                    Settings → Catalog & Google Sheets
                  </Link>
                ) : (
                  <span className="font-medium text-muted-foreground">Settings → Catalog & Google Sheets (contact an admin)</span>
                )}
                .
              </p>
            </div>
          </div>
        </div>

        <div className="mt-6 space-y-2">
          <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">google_sheet_id</p>
          {loadingSummary && !summary ? (
            <div className="flex justify-center py-6">
              <Loader2 className="h-6 w-6 animate-spin text-primary" aria-label="Loading" />
            </div>
          ) : (
            <div className="rounded-xl border border-border bg-muted/20 p-4 font-mono text-sm break-all text-foreground">
              {sheetId || "— (not set in workspace_settings.module_configs.catalog — add it in Settings → Catalog & Google Sheets)"}
            </div>
          )}
          <p className="text-xs text-muted-foreground">
            Service account status:{" "}
            <span className={googleReady ? "font-medium text-emerald-600 dark:text-emerald-400" : "font-medium text-amber-700 dark:text-amber-300"}>
              {googleReady ? "JSON key present for this organization" : "Missing google_sheets_api key — paste JSON in Settings"}
            </span>
          </p>
        </div>

        <button
          type="button"
          disabled={!organizationId?.trim() || syncPhase === "syncing" || loadingSummary}
          onClick={() => void syncGoogleSheets()}
          className="mt-6 flex w-full min-h-[3.5rem] items-center justify-center gap-3 rounded-2xl bg-gradient-to-r from-violet-600 to-sky-600 px-6 py-4 text-lg font-bold tracking-tight text-white shadow-lg shadow-violet-500/25 transition hover:opacity-95 disabled:cursor-not-allowed disabled:opacity-45"
        >
          {syncPhase === "syncing" ? (
            <>
              <Loader2 className="h-6 w-6 shrink-0 animate-spin" />
              Syncing with Google Sheets…
            </>
          ) : (
            <>
              <Cloud className="h-6 w-6 shrink-0" />
              Sync Now with Google Sheets
            </>
          )}
        </button>
        <p className="mt-2 text-center text-[11px] text-muted-foreground">
          POST <code className="rounded bg-muted px-1">{etlApiBase()}/etl/sync-google-sheets</code> with{" "}
          <code className="rounded bg-muted px-1">organization_id</code>
        </p>
        {syncMessage ? (
          <p className="mt-4 text-center text-sm font-medium text-emerald-600 dark:text-emerald-400">{syncMessage}</p>
        ) : null}
        {syncError ? (
          <p className="mt-4 text-center text-sm text-destructive" role="alert">
            {syncError}
          </p>
        ) : null}
      </section>
    </div>
  );
}
