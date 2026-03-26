import Link from "next/link";
import {
  Search,
  TrendingUp,
  Clock,
  AlertTriangle,
  FileText,
} from "lucide-react";
import { supabaseServer } from "../lib/supabase-server";
import { getReturnsAnalyticsData } from "./returns/actions";
import { DashboardAnalytics } from "../components/DashboardAnalytics";

const DEFAULT_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000001";

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


export default async function Page() {
  let claims: ClaimRow[] = [];
  let fetchError: string | null = null;
  const analyticsRes = await getReturnsAnalyticsData();
  const analyticsData = analyticsRes.ok ? analyticsRes.data ?? null : null;

  try {
    const { data, error } = await supabaseServer
      .from("claims")
      .select(
        "id, amount, status, claim_type, marketplace_provider, created_at, amazon_order_id"
      )
      .eq("organization_id", DEFAULT_ORGANIZATION_ID)
      .order("created_at", { ascending: false })
      .limit(50);

    if (error) throw new Error(error.message);
    claims = (data ?? []) as ClaimRow[];
  } catch (err) {
    fetchError = err instanceof Error ? err.message : "Failed to load claims.";
  }

  // --- Real KPI calculations ---
  const totalRecovered = claims
    .filter((c) => c.status === "recovered")
    .reduce((sum, c) => sum + (Number(c.amount) || 0), 0);

  const pendingCount = claims.filter((c) => c.status === "pending").length;
  const suspiciousCount = claims.filter((c) => c.status === "suspicious").length;
  const totalCount = claims.length;

  const recentClaims = claims.slice(0, 10);

  const kpis = [
    {
      label: "Total Recovered",
      value: formatCurrency(totalRecovered),
      change: "Recovered claims",
      trend: "Sum of recovered claim amounts",
      icon: TrendingUp,
      accentClass: "from-sky-500/10 via-sky-500/5 to-sky-500/0 border-sky-500/30",
      pillClass: "bg-sky-500/10 text-sky-400 border-sky-500/30",
    },
    {
      label: "Pending Claims",
      value: String(pendingCount),
      change: "Awaiting action",
      trend: "Claims currently in pending status",
      icon: Clock,
      accentClass: "from-amber-500/10 via-amber-500/5 to-amber-500/0 border-amber-500/30",
      pillClass: "bg-amber-500/10 text-amber-400 border-amber-500/30",
    },
    {
      label: "Suspicious Returns",
      value: String(suspiciousCount),
      change: "Flagged for review",
      trend: "Claims marked as suspicious",
      icon: AlertTriangle,
      accentClass: "from-rose-500/10 via-rose-500/5 to-rose-500/0 border-rose-500/30",
      pillClass: "bg-rose-500/10 text-rose-400 border-rose-500/30",
    },
  ];

  return (
    <>
        {/* Header */}
        <header className="flex h-16 flex-col gap-2 border-b border-slate-200 bg-white/80 px-4 backdrop-blur-xl dark:border-slate-800 dark:bg-slate-950/70 sm:flex-row sm:items-center sm:justify-between sm:gap-4 md:px-6">
          <div className="flex flex-col">
            <h1 className="text-base font-semibold tracking-tight text-slate-900 dark:text-slate-50 sm:text-sm">Dashboard</h1>
            <p className="text-xs text-muted-foreground">Unified control center for returns, claims, and adapters.</p>
          </div>
          <div className="flex flex-wrap items-center gap-2 sm:gap-4">
            <div className="relative hidden w-72 items-center md:flex">
              <Search className="pointer-events-none absolute left-3 h-4 w-4 text-muted-foreground" />
              <input
                type="search"
                placeholder="Search orders, RMAs, claims…"
                className="h-9 w-full rounded-lg border border-border bg-muted pl-9 pr-3 text-xs text-foreground placeholder:text-muted-foreground shadow-sm outline-none ring-0 transition focus:border-primary focus:ring-1 focus:ring-ring"
              />
            </div>
          </div>
        </header>

        {/* Main Content */}
        <main className="flex-1 overflow-y-auto overflow-x-hidden bg-slate-50 dark:bg-gradient-to-br dark:from-slate-950 dark:via-slate-950 dark:to-slate-900">
          <div className="mx-auto flex w-full max-w-[100vw] flex-col gap-6 px-4 py-6 sm:px-4 lg:px-8">

            {/* KPI Cards */}
            <section className="grid grid-cols-1 gap-4 sm:grid-cols-2 sm:gap-6 lg:grid-cols-3">
              {kpis.map((kpi) => (
                <div
                  key={kpi.label}
                  className={`relative overflow-hidden rounded-2xl border bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80 transition hover:-translate-y-0.5 hover:shadow-[0_18px_45px_rgba(15,23,42,0.85)] ${kpi.accentClass}`}
                >
                  <div className="pointer-events-none absolute inset-0 bg-gradient-to-br from-slate-100/2 via-slate-100/0 to-slate-100/0" />
                  <div className="relative flex items-start justify-between gap-3">
                    <div className="space-y-2">
                      <p className="text-xs font-medium uppercase tracking-wide text-slate-400">{kpi.label}</p>
                      <p className="text-2xl font-semibold tracking-tight text-slate-50">{kpi.value}</p>
                    </div>
                    <span className={`inline-flex items-center gap-1 rounded-full border px-2 py-1 text-[10px] font-medium ${kpi.pillClass}`}>
                      <span className="h-1.5 w-1.5 rounded-full bg-current" />
                      {kpi.change}
                    </span>
                  </div>
                  <p className="relative mt-3 text-[11px] text-slate-400">{kpi.trend}</p>
                </div>
              ))}
            </section>

            <DashboardAnalytics data={analyticsData} />

            {/* Fetch error banner */}
            {fetchError && (
              <div className="rounded-2xl border border-rose-700/60 bg-rose-950/40 px-4 py-3 text-xs text-rose-100">
                <span className="font-semibold">Data warning:</span> {fetchError}
              </div>
            )}

            {/* Secondary Content */}
            <section className="flex flex-col gap-4 lg:grid lg:grid-cols-3">
              {/* Left: Returns Pipeline */}
              <div className="min-h-[300px] w-full overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-950/70 lg:col-span-2">
                <div className="flex flex-col gap-2 border-b border-slate-200 px-4 py-3 dark:border-slate-800 sm:flex-row sm:items-center sm:justify-between">
                  <div>
                    <p className="text-xs font-semibold tracking-tight text-slate-900 dark:text-slate-50">Returns Pipeline</p>
                    <p className="text-[11px] text-muted-foreground">Live view across marketplaces and carriers.</p>
                  </div>
                  <span className="w-fit rounded-full bg-slate-100 px-2.5 py-1.5 text-[11px] font-medium text-slate-600 ring-1 ring-slate-200 dark:bg-slate-900 dark:text-slate-200 dark:ring-slate-700">
                    All time
                  </span>
                </div>
                <div className="grid w-full min-w-0 grid-cols-1 gap-4 px-4 py-4 sm:grid-cols-2 md:grid-cols-4">
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">Total Claims</p>
                    <p className="text-lg font-semibold text-slate-900 dark:text-slate-50">{totalCount}</p>
                    <p className="text-[11px] text-muted-foreground">All statuses</p>
                  </div>
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">Pending</p>
                    <p className="text-lg font-semibold text-amber-600 dark:text-amber-400">{pendingCount}</p>
                    <p className="text-[11px] text-amber-600/70 dark:text-amber-500/70">Awaiting action</p>
                  </div>
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">Recovered</p>
                    <p className="text-lg font-semibold text-emerald-600 dark:text-emerald-400">
                      {claims.filter((c) => c.status === "recovered").length}
                    </p>
                    <p className="text-[11px] text-emerald-600/70 dark:text-emerald-500/70">{formatCurrency(totalRecovered)} total</p>
                  </div>
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">Suspicious</p>
                    <p className="text-lg font-semibold text-rose-600 dark:text-rose-400">{suspiciousCount}</p>
                    <p className="text-[11px] text-rose-600/70 dark:text-rose-500/70">Pattern anomalies</p>
                  </div>
                </div>
              </div>

              {/* Right: SLA Overview */}
              <div className="min-h-[300px] w-full overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-950/70">
                <div className="border-b border-slate-200 px-4 py-3 dark:border-slate-800">
                  <p className="text-xs font-semibold tracking-tight text-slate-900 dark:text-slate-50">SLA Overview</p>
                  <p className="text-[11px] text-muted-foreground">Where your attention is required today.</p>
                </div>
                <div className="space-y-3 px-4 py-4">
                  <div className="flex items-center justify-between rounded-lg bg-slate-100 px-3 py-2 dark:bg-slate-900/80">
                    <div>
                      <p className="text-xs font-medium text-slate-900 dark:text-slate-50">Marketplace responses</p>
                      <p className="text-[11px] text-muted-foreground">
                        {pendingCount} ticket{pendingCount !== 1 ? "s" : ""} require response.
                      </p>
                    </div>
                    <span className="shrink-0 text-xs font-semibold text-amber-600 dark:text-amber-400">4h</span>
                  </div>
                  <div className="flex flex-col gap-2 rounded-lg bg-slate-100/70 px-3 py-2 dark:bg-slate-900/60 sm:flex-row sm:items-center sm:justify-between">
                    <div>
                      <p className="text-xs font-medium text-slate-900 dark:text-slate-50">Carrier evidence</p>
                      <p className="text-[11px] text-muted-foreground">9 shipments awaiting proof-of-delivery uploads.</p>
                    </div>
                    <span className="shrink-0 text-xs font-semibold text-sky-600 dark:text-sky-400">Today</span>
                  </div>
                  <div className="flex flex-col gap-2 rounded-lg bg-slate-100/70 px-3 py-2 dark:bg-slate-900/60 sm:flex-row sm:items-center sm:justify-between">
                    <div>
                      <p className="text-xs font-medium text-slate-900 dark:text-slate-50">Suspicious returns</p>
                      <p className="text-[11px] text-muted-foreground">
                        {suspiciousCount} high-value return{suspiciousCount !== 1 ? "s" : ""} flagged for review.
                      </p>
                    </div>
                    <span className="shrink-0 text-xs font-semibold text-rose-600 dark:text-rose-400">Review</span>
                  </div>
                </div>
              </div>
            </section>

            {/* Recent Claims Table */}
            <section className="w-full overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-950/70">
              <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3 dark:border-slate-800">
                <div>
                  <p className="text-xs font-semibold tracking-tight text-slate-900 dark:text-slate-50">Recent Claims</p>
                  <p className="text-[11px] text-muted-foreground">Latest {recentClaims.length} claims synced from connected marketplaces.</p>
                </div>
                <FileText className="h-4 w-4 text-muted-foreground" />
              </div>

              {recentClaims.length === 0 ? (
                <div className="flex flex-col items-center justify-center gap-3 px-6 py-12 text-center">
                  <div className="flex h-12 w-12 items-center justify-center rounded-full bg-slate-100 ring-1 ring-slate-200 dark:bg-slate-900 dark:ring-slate-800">
                    <FileText className="h-5 w-5 text-muted-foreground" />
                  </div>
                  <p className="text-sm font-medium text-slate-700 dark:text-slate-300">No claims yet</p>
                  <p className="max-w-xs text-xs text-slate-500">
                    Connect a marketplace adapter and click &ldquo;Sync Claims&rdquo; to pull in your first claims.
                  </p>
                  <Link
                    href="/settings"
                    className="mt-1 inline-flex items-center gap-1.5 rounded-full border border-slate-200 bg-slate-100 px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:border-slate-600 dark:hover:text-white"
                  >
                    Connect a Store
                  </Link>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full min-w-[600px] text-sm">
                    <thead>
                      <tr className="border-b border-slate-200 bg-slate-50 dark:border-slate-800 dark:bg-slate-900/40">
                        <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Provider</th>
                        <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Claim Type</th>
                        <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Order / Ref</th>
                        <th className="px-4 py-2.5 text-right text-[11px] font-medium uppercase tracking-wide text-slate-500">Amount</th>
                        <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Status</th>
                        <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Date</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60">
                      {recentClaims.map((claim) => (
                        <tr key={claim.id} className="transition hover:bg-accent hover:text-accent-foreground/40">
                          <td className="px-4 py-3 text-xs font-medium text-slate-700 dark:text-slate-200">
                            {providerLabel(claim.marketplace_provider)}
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
