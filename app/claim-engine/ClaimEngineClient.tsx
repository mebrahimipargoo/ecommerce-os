"use client";

import Link from "next/link";
import {
  AlertTriangle,
  Clock,
  FileText,
  ShieldAlert,
  TrendingUp,
} from "lucide-react";
import type { PalletRecord, PackageRecord, ReturnRecord } from "../returns/actions";
import { ItemIdentifiersCell, StatusBadge } from "../returns/_components";

type ClaimRow = {
  id: string;
  amount: number;
  status: "pending" | "recovered" | "suspicious";
  claim_type: string | null;
  marketplace_provider: string | null;
  created_at: string;
  amazon_order_id: string | null;
};

function formatCurrency(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatDate(iso: string): string {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(iso));
}

function providerLabel(raw: string | null): string {
  if (!raw) return "—";
  const map: Record<string, string> = {
    amazon_sp_api: "Amazon",
    walmart_api: "Walmart",
    ebay_api: "eBay",
  };
  return map[raw] ?? raw;
}

const STATUS_STYLES: Record<string, string> = {
  pending:
    "border-amber-700/60 bg-amber-950/50 text-amber-300",
  recovered:
    "border-emerald-700/60 bg-emerald-950/50 text-emerald-300",
  suspicious:
    "border-rose-700/60 bg-rose-950/50 text-rose-300",
};

export function ClaimEngineClient({
  claims,
  claimPipelineItems,
  pallets,
  packages,
  claimsError,
  pipelineError,
}: {
  claims: ClaimRow[];
  claimPipelineItems: ReturnRecord[];
  pallets: PalletRecord[];
  packages: PackageRecord[];
  claimsError: string | null;
  pipelineError: string | null;
}) {
  const totalRecovered = claims
    .filter((c) => c.status === "recovered")
    .reduce((sum, c) => sum + (Number(c.amount) || 0), 0);
  const pendingCount = claims.filter((c) => c.status === "pending").length;
  const suspiciousCount = claims.filter((c) => c.status === "suspicious").length;

  const pkgMap = new Map(packages.map((p) => [p.id, p]));
  const pltMap = new Map(pallets.map((p) => [p.id, p]));

  return (
    <>
      <header className="flex h-16 flex-col gap-2 border-b border-slate-200 bg-white/80 px-4 backdrop-blur-xl dark:border-slate-800 dark:bg-slate-950/70 sm:flex-row sm:items-center sm:justify-between sm:gap-4 md:px-6">
        <div className="flex flex-col">
          <div className="flex items-center gap-2">
            <ShieldAlert className="h-4 w-4 text-sky-500" />
            <h1 className="text-base font-semibold tracking-tight text-slate-900 dark:text-slate-50 sm:text-sm">Claim Engine</h1>
          </div>
          <p className="text-xs text-muted-foreground">
            Marketplace claim sync, pipeline items with ASIN / FNSKU / SKU, and recovery metrics.
          </p>
        </div>
        <Link
          href="/"
          className="text-xs font-medium text-sky-600 hover:text-sky-500 dark:text-sky-400"
        >
          ← Dashboard
        </Link>
      </header>

      <main className="flex-1 overflow-y-auto overflow-x-hidden bg-slate-50 dark:bg-gradient-to-br dark:from-slate-950 dark:via-slate-950 dark:to-slate-900">
        <div className="mx-auto flex w-full max-w-[100vw] flex-col gap-6 px-4 py-6 sm:px-4 lg:px-8">
          {(claimsError || pipelineError) && (
            <div className="rounded-2xl border border-rose-700/60 bg-rose-950/40 px-4 py-3 text-xs text-rose-100">
              <span className="font-semibold">Data warning:</span>{" "}
              {[claimsError, pipelineError].filter(Boolean).join(" · ")}
            </div>
          )}

          <section className="grid grid-cols-1 gap-4 sm:grid-cols-3">
            <div className="relative overflow-hidden rounded-2xl border bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80 from-sky-500/10 via-sky-500/5 to-sky-500/0 border-sky-500/30">
              <div className="relative flex items-start justify-between gap-3">
                <div className="space-y-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-400">Total Recovered</p>
                  <p className="text-2xl font-semibold tracking-tight text-slate-50">{formatCurrency(totalRecovered)}</p>
                </div>
                <TrendingUp className="h-5 w-5 text-sky-400" />
              </div>
              <p className="relative mt-3 text-[11px] text-slate-400">Sum of recovered claim amounts (synced)</p>
            </div>
            <div className="relative overflow-hidden rounded-2xl border bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80 from-amber-500/10 via-amber-500/5 to-amber-500/0 border-amber-500/30">
              <div className="relative flex items-start justify-between gap-3">
                <div className="space-y-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-400">Pending Claims</p>
                  <p className="text-2xl font-semibold tracking-tight text-slate-50">{pendingCount}</p>
                </div>
                <Clock className="h-5 w-5 text-amber-400" />
              </div>
              <p className="relative mt-3 text-[11px] text-slate-400">Awaiting action from marketplace sync</p>
            </div>
            <div className="relative overflow-hidden rounded-2xl border bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80 from-rose-500/10 via-rose-500/5 to-rose-500/0 border-rose-500/30">
              <div className="relative flex items-start justify-between gap-3">
                <div className="space-y-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-400">Suspicious</p>
                  <p className="text-2xl font-semibold tracking-tight text-slate-50">{suspiciousCount}</p>
                </div>
                <AlertTriangle className="h-5 w-5 text-rose-400" />
              </div>
              <p className="relative mt-3 text-[11px] text-slate-400">Flagged by adapter rules</p>
            </div>
          </section>

          <section className="w-full overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-950/70">
            <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 px-4 py-3 dark:border-slate-800">
              <div>
                <p className="text-xs font-semibold tracking-tight text-slate-900 dark:text-slate-50">Return items — claim pipeline</p>
                <p className="text-[11px] text-muted-foreground">
                  Ready for claim or pending evidence (ASIN / FNSKU / SKU for filing).
                </p>
              </div>
            </div>
            {claimPipelineItems.length === 0 ? (
              <div className="px-6 py-10 text-center text-sm text-muted-foreground">
                No items in the claim pipeline. Mark conditions and photos in Returns Processing to surface rows here.
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full min-w-[720px] text-sm">
                  <thead>
                    <tr className="border-b border-slate-200 bg-slate-50 dark:border-slate-800 dark:bg-slate-900/40">
                      <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Item</th>
                      <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Identifiers</th>
                      <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Pallet #</th>
                      <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Package #</th>
                      <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Status</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60">
                    {claimPipelineItems.map((r) => {
                      const pk = r.pallet_id ? pltMap.get(r.pallet_id) : null;
                      const pg = r.package_id ? pkgMap.get(r.package_id) : null;
                      return (
                        <tr key={r.id} className="transition hover:bg-accent/40">
                          <td className="px-4 py-3 align-top">
                            <p className="max-w-[200px] font-medium text-slate-900 dark:text-slate-100">{r.item_name || "—"}</p>
                            <p className="mt-0.5 text-[10px] text-muted-foreground">{formatDate(r.created_at)}</p>
                          </td>
                          <td className="px-4 py-3 align-top">
                            <ItemIdentifiersCell
                              compact
                              showItemHeading={false}
                              alwaysShowCodeRows
                              itemName=""
                              asin={r.asin}
                              fnsku={r.fnsku}
                              sku={r.sku}
                              storePlatform={r.stores?.platform}
                            />
                          </td>
                          <td className="px-4 py-3 align-top font-mono text-xs text-muted-foreground">{pk?.pallet_number ?? "—"}</td>
                          <td className="px-4 py-3 align-top font-mono text-xs text-muted-foreground">{pg?.package_number ?? "—"}</td>
                          <td className="px-4 py-3 align-top">
                            <StatusBadge status={r.status} />
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          <section className="w-full overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-950/70">
            <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3 dark:border-slate-800">
              <div>
                <p className="text-xs font-semibold tracking-tight text-slate-900 dark:text-slate-50">Synced marketplace claims</p>
                <p className="text-[11px] text-muted-foreground">Latest rows from connected adapters (Sync Claims in Settings).</p>
              </div>
              <FileText className="h-4 w-4 text-muted-foreground" />
            </div>

            {claims.length === 0 ? (
              <div className="flex flex-col items-center justify-center gap-3 px-6 py-12 text-center">
                <div className="flex h-12 w-12 items-center justify-center rounded-full bg-slate-100 ring-1 ring-slate-200 dark:bg-slate-900 dark:ring-slate-800">
                  <FileText className="h-5 w-5 text-muted-foreground" />
                </div>
                <p className="text-sm font-medium text-slate-700 dark:text-slate-300">No claims yet</p>
                <p className="max-w-xs text-xs text-slate-500">
                  Connect a marketplace adapter and run &ldquo;Sync Claims&rdquo; to pull in claim rows.
                </p>
                <Link
                  href="/settings"
                  className="mt-1 inline-flex items-center gap-1.5 rounded-full border border-slate-200 bg-slate-100 px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:border-slate-600 dark:hover:text-white"
                >
                  Adapter settings
                </Link>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full min-w-[600px] text-sm">
                  <thead>
                    <tr className="border-b border-slate-200 bg-slate-50 dark:border-slate-800 dark:bg-slate-900/40">
                      <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Sales Channel</th>
                      <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Claim Type</th>
                      <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Order / Ref</th>
                      <th className="px-4 py-2.5 text-right text-[11px] font-medium uppercase tracking-wide text-slate-500">Amount</th>
                      <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Status</th>
                      <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Date</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60">
                    {claims.map((claim) => (
                      <tr key={claim.id} className="transition hover:bg-accent hover:text-accent-foreground/40">
                        <td className="px-4 py-3">
                          {claim.marketplace_provider ? (
                            <span
                              className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px] font-semibold ${
                                claim.marketplace_provider === "amazon_sp_api"
                                  ? "border-amber-300 bg-amber-50 text-amber-700 dark:border-amber-700/50 dark:bg-amber-950/30 dark:text-amber-300"
                                  : claim.marketplace_provider === "walmart_api"
                                    ? "border-sky-300 bg-sky-50 text-sky-700 dark:border-sky-700/50 dark:bg-sky-950/30 dark:text-sky-300"
                                    : "border-rose-300 bg-rose-50 text-rose-700 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-300"
                              }`}
                            >
                              <span className="h-1.5 w-1.5 rounded-full bg-current" />
                              {providerLabel(claim.marketplace_provider)}
                            </span>
                          ) : (
                            <span className="text-xs text-muted-foreground">—</span>
                          )}
                        </td>
                        <td className="px-4 py-3 text-xs text-slate-600 dark:text-slate-300">
                          {claim.claim_type ?? "—"}
                        </td>
                        <td className="px-4 py-3 text-xs font-mono text-muted-foreground">
                          {claim.amazon_order_id ?? "—"}
                        </td>
                        <td className="px-4 py-3 text-right text-xs font-semibold text-slate-900 dark:text-slate-50">
                          {formatCurrency(Number(claim.amount) || 0)}
                        </td>
                        <td className="px-4 py-3">
                          <span
                            className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] font-medium ${STATUS_STYLES[claim.status] ?? STATUS_STYLES.pending}`}
                          >
                            <span className="h-1.5 w-1.5 rounded-full bg-current" />
                            {claim.status.charAt(0).toUpperCase() + claim.status.slice(1)}
                          </span>
                        </td>
                        <td className="px-4 py-3 text-xs text-muted-foreground">
                          {formatDate(claim.created_at)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        </div>
      </main>
    </>
  );
}
