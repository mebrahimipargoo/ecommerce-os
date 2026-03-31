"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";
import { FileDown, Loader2, ShieldAlert } from "lucide-react";
import {
  listClaimReportHistory,
  type ClaimReportHistoryRow,
  type ClaimReportHistoryStatusLabel,
} from "./claim-report-history-actions";
import { refreshClaimReportSignedUrl } from "./claim-submission-actions";

function formatDateTime(iso: string): string {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(iso));
}

function toYyyyMmDd(d: Date): string {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function startUtcIsoFromYyyyMmDd(s: string): string {
  const t = s.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(t)) return new Date(0).toISOString();
  return `${t}T00:00:00.000Z`;
}

function endUtcIsoFromYyyyMmDd(s: string): string {
  const t = s.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(t)) return new Date().toISOString();
  return `${t}T23:59:59.999Z`;
}

const STATUS_BADGE: Record<ClaimReportHistoryStatusLabel, string> = {
  Generated: "border-sky-700/60 bg-sky-950/50 text-sky-200",
  Submitted: "border-amber-700/60 bg-amber-950/50 text-amber-200",
  Denied: "border-rose-700/60 bg-rose-950/50 text-rose-200",
  "Generating...": "border-violet-700/60 bg-violet-950/50 text-violet-200",
  Failed: "border-slate-600/60 bg-slate-900/50 text-slate-300",
};

function isHttpUrl(path: string): boolean {
  return /^https?:\/\//i.test(path.trim());
}

export function ClaimReportHistoryClient({
  organizationId,
  initialRows,
  initialError,
}: {
  organizationId: string;
  initialRows: ClaimReportHistoryRow[];
  initialError: string | null;
}) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [rows, setRows] = useState<ClaimReportHistoryRow[]>(initialRows);
  const [loadError, setLoadError] = useState<string | null>(initialError);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [polling, setPolling] = useState(false);

  const now = useMemo(() => new Date(), []);
  const defaultTo = useMemo(() => toYyyyMmDd(now), [now]);
  const defaultFrom = useMemo(() => {
    const d = new Date(now);
    d.setUTCDate(d.getUTCDate() - 90);
    return toYyyyMmDd(d);
  }, [now]);

  const urlFrom = searchParams.get("from");
  const urlTo = searchParams.get("to");
  const urlType = searchParams.get("type");

  const [dateFrom, setDateFrom] = useState(urlFrom && /^\d{4}-\d{2}-\d{2}$/.test(urlFrom) ? urlFrom : defaultFrom);
  const [dateTo, setDateTo] = useState(urlTo && /^\d{4}-\d{2}-\d{2}$/.test(urlTo) ? urlTo : defaultTo);
  const [claimType, setClaimType] = useState(urlType ?? "all");

  const persistQuery = useCallback(
    (next: { from: string; to: string; type: string }) => {
      const p = new URLSearchParams();
      p.set("from", next.from);
      p.set("to", next.to);
      if (next.type && next.type !== "all") p.set("type", next.type);
      router.replace(`/claim-engine/report-history?${p.toString()}`, { scroll: false });
    },
    [router],
  );

  const refetch = useCallback(async () => {
    setPolling(true);
    const res = await listClaimReportHistory({
      organizationId,
      dateFrom: startUtcIsoFromYyyyMmDd(dateFrom),
      dateTo: endUtcIsoFromYyyyMmDd(dateTo),
    });
    setPolling(false);
    if (res.ok) {
      setRows(res.data);
      setLoadError(null);
    } else {
      setLoadError(res.error ?? "Refresh failed.");
    }
  }, [organizationId, dateFrom, dateTo]);

  useEffect(() => {
    void refetch();
    // Initial load + URL defaults — interval below keeps rows in sync when report_url is filled in.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const id = window.setInterval(() => {
      void refetch();
    }, 12_000);
    return () => window.clearInterval(id);
  }, [refetch]);

  const claimTypeOptions = useMemo(() => {
    const set = new Set<string>();
    for (const r of rows) {
      if (r.claim_type?.trim()) set.add(r.claim_type.trim());
    }
    return [...set].sort((a, b) => a.localeCompare(b));
  }, [rows]);

  const filteredRows = useMemo(() => {
    if (!claimType || claimType === "all") return rows;
    return rows.filter((r) => (r.claim_type ?? "").trim() === claimType);
  }, [rows, claimType]);

  async function handleDownload(row: ClaimReportHistoryRow) {
    const path = row.report_url?.trim();
    if (!path) return;
    setBusyId(row.id);
    try {
      if (isHttpUrl(path)) {
        window.open(path, "_blank", "noopener,noreferrer");
        return;
      }
      const r = await refreshClaimReportSignedUrl(path);
      const url = r.ok ? r.url : null;
      if (!url) {
        window.alert(r.error ?? "Could not create a download link.");
        return;
      }
      window.open(url, "_blank", "noopener,noreferrer");
    } finally {
      setBusyId(null);
    }
  }

  function applyDateFilters() {
    persistQuery({ from: dateFrom, to: dateTo, type: claimType });
    void refetch();
  }

  return (
    <>
      <header className="flex h-16 flex-col gap-2 border-b border-slate-200 bg-white/80 px-4 backdrop-blur-xl dark:border-slate-800 dark:bg-slate-950/70 sm:flex-row sm:items-center sm:justify-between sm:gap-4 md:px-6">
        <div className="flex flex-col">
          <div className="flex items-center gap-2">
            <ShieldAlert className="h-4 w-4 text-sky-500" />
            <h1 className="text-base font-semibold tracking-tight text-slate-900 dark:text-slate-50 sm:text-sm">
              Claim report history
            </h1>
          </div>
          <p className="text-xs text-muted-foreground">
            PDFs stored on claim submissions — refreshes every 12s while this page is open.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Link
            href="/claim-engine"
            className="text-xs font-medium text-sky-600 hover:text-sky-500 dark:text-sky-400"
          >
            ← Claim Engine
          </Link>
        </div>
      </header>

      <main className="flex-1 overflow-y-auto overflow-x-hidden bg-slate-50 dark:bg-gradient-to-br dark:from-slate-950 dark:via-slate-950 dark:to-slate-900">
        <div className="mx-auto flex w-full max-w-[100vw] flex-col gap-6 px-4 py-6 sm:px-4 lg:px-8">
          {loadError ? (
            <div className="rounded-2xl border border-rose-700/60 bg-rose-950/40 px-4 py-3 text-xs text-rose-100">
              {loadError}
            </div>
          ) : null}

          <section className="flex flex-col gap-3 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-800 dark:bg-slate-950/80">
            <div className="flex flex-wrap items-end gap-3">
              <label className="flex flex-col gap-1 text-[11px] font-medium text-slate-600 dark:text-slate-400">
                From
                <input
                  type="date"
                  value={dateFrom}
                  onChange={(e) => setDateFrom(e.target.value)}
                  className="rounded-lg border border-slate-200 bg-white px-2 py-1.5 text-sm text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                />
              </label>
              <label className="flex flex-col gap-1 text-[11px] font-medium text-slate-600 dark:text-slate-400">
                To
                <input
                  type="date"
                  value={dateTo}
                  onChange={(e) => setDateTo(e.target.value)}
                  className="rounded-lg border border-slate-200 bg-white px-2 py-1.5 text-sm text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                />
              </label>
              <label className="flex min-w-[10rem] flex-col gap-1 text-[11px] font-medium text-slate-600 dark:text-slate-400">
                Claim type
                <select
                  value={claimType}
                  onChange={(e) => {
                    const v = e.target.value;
                    setClaimType(v);
                    persistQuery({ from: dateFrom, to: dateTo, type: v });
                  }}
                  className="rounded-lg border border-slate-200 bg-white px-2 py-1.5 text-sm text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                >
                  <option value="all">All types</option>
                  {claimTypeOptions.map((t) => (
                    <option key={t} value={t}>
                      {t}
                    </option>
                  ))}
                </select>
              </label>
              <button
                type="button"
                onClick={() => applyDateFilters()}
                className="inline-flex items-center justify-center rounded-xl bg-sky-600 px-3 py-2 text-xs font-semibold text-white hover:bg-sky-500"
              >
                Apply range
              </button>
              {polling ? (
                <span className="flex items-center gap-1 text-[11px] text-slate-500">
                  <Loader2 className="h-3 w-3 animate-spin" /> Syncing…
                </span>
              ) : null}
            </div>
          </section>

          <section className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-950/80">
            <div className="overflow-x-auto">
              <table className="w-full min-w-[720px] text-left text-sm">
                <thead>
                  <tr className="border-b border-slate-200 bg-slate-50/80 dark:border-slate-800 dark:bg-slate-900/50">
                    <th className="px-4 py-3 text-[11px] font-medium uppercase tracking-wide text-slate-500">
                      Report name
                    </th>
                    <th className="px-4 py-3 text-[11px] font-medium uppercase tracking-wide text-slate-500">
                      Type
                    </th>
                    <th className="px-4 py-3 text-[11px] font-medium uppercase tracking-wide text-slate-500">
                      Generated by
                    </th>
                    <th className="px-4 py-3 text-[11px] font-medium uppercase tracking-wide text-slate-500">
                      Date
                    </th>
                    <th className="px-4 py-3 text-[11px] font-medium uppercase tracking-wide text-slate-500">
                      Status
                    </th>
                    <th className="px-4 py-3 text-right text-[11px] font-medium uppercase tracking-wide text-slate-500">
                      Action
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="px-4 py-12 text-center text-sm leading-relaxed text-slate-600 dark:text-slate-400">
                        {rows.length === 0 ? (
                          <>
                            No claim reports generated yet. Once the AI Agent finishes a task, the PDF will appear
                            here.
                          </>
                        ) : (
                          <>No reports for this claim type in the selected range.</>
                        )}
                      </td>
                    </tr>
                  ) : (
                    filteredRows.map((row) => {
                      const canDownload = Boolean(row.report_url?.trim());
                      return (
                        <tr
                          key={row.id}
                          className="border-b border-slate-100 last:border-0 dark:border-slate-800/80"
                        >
                          <td className="px-4 py-3 font-medium text-slate-900 dark:text-slate-100">
                            {row.report_name}
                          </td>
                          <td className="px-4 py-3 text-slate-700 dark:text-slate-300">{row.claim_type ?? "—"}</td>
                          <td className="px-4 py-3 text-slate-700 dark:text-slate-300">
                            {row.generated_by ?? "—"}
                          </td>
                          <td className="whitespace-nowrap px-4 py-3 text-slate-600 dark:text-slate-400">
                            {formatDateTime(row.created_at)}
                          </td>
                          <td className="px-4 py-3">
                            <span
                              className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-semibold ${STATUS_BADGE[row.status_label]}`}
                            >
                              {row.status_label}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-right">
                            <button
                              type="button"
                              disabled={!canDownload || busyId === row.id}
                              onClick={() => void handleDownload(row)}
                              className="inline-flex items-center gap-1 rounded-lg border border-slate-200 bg-white px-2.5 py-1.5 text-xs font-semibold text-slate-800 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:hover:bg-slate-800"
                              title={canDownload ? "Download / open PDF" : "PDF not ready yet"}
                            >
                              {busyId === row.id ? (
                                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                              ) : (
                                <FileDown className="h-3.5 w-3.5" />
                              )}
                              Download
                            </button>
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </section>
        </div>
      </main>
    </>
  );
}
