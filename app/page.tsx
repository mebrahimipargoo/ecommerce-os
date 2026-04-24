import Link from "next/link";
import { Search, Package2, Boxes, RotateCcw, Send, DollarSign } from "lucide-react";
import { getDashboardSnapshot } from "./returns/actions";

function formatUsdSafe(n: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number.isFinite(n) ? n : 0);
}

export default async function Page() {
  const snapRes = await getDashboardSnapshot();
  const snap = snapRes.ok ? snapRes.data : null;
  const fetchError = snapRes.ok ? null : snapRes.error ?? "Failed to load dashboard.";

  return (
    <>
      <header className="flex h-16 flex-col gap-2 border-b border-slate-200 bg-white/80 px-4 backdrop-blur-xl dark:border-slate-800 dark:bg-slate-950/70 sm:flex-row sm:items-center sm:justify-between sm:gap-4 md:px-6">
        <div className="flex flex-col">
          <h1 className="text-base font-semibold tracking-tight text-slate-900 dark:text-slate-50 sm:text-sm">Dashboard</h1>
          <p className="text-xs text-muted-foreground">At-a-glance volume for returns, pallets, and packages.</p>
        </div>
        <div className="flex flex-wrap items-center gap-2 sm:gap-4">
          <div className="relative hidden w-72 items-center md:flex">
            <Search className="pointer-events-none absolute left-3 h-4 w-4 text-muted-foreground" />
            <input
              type="search"
              placeholder="Search orders, RMAs, claims…"
              className="h-9 w-full rounded-lg border border-border bg-muted pl-9 pr-3 text-xs text-foreground placeholder:text-muted-foreground shadow-sm outline-none ring-0 transition focus:border-sky-500 focus:ring-1 focus:ring-sky-500/40"
            />
          </div>
        </div>
      </header>

      <main className="flex-1 overflow-y-auto overflow-x-hidden bg-slate-50 dark:bg-gradient-to-br dark:from-slate-950 dark:via-slate-950 dark:to-slate-900">
        <div className="mx-auto flex w-full max-w-[100vw] flex-col gap-6 px-4 py-6 sm:px-4 lg:px-8">
          {fetchError && (
            <div className="rounded-2xl border border-rose-700/60 bg-rose-950/40 px-4 py-3 text-xs text-rose-100">
              <span className="font-semibold">Data warning:</span> {fetchError}
            </div>
          )}

          <section className="grid grid-cols-1 gap-4 sm:grid-cols-3">
            <div className="relative overflow-hidden rounded-2xl border border-slate-200 bg-white px-4 py-5 shadow-sm ring-1 ring-slate-200/80 dark:border-slate-800 dark:bg-slate-950/70 dark:ring-slate-800">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">Returns today</p>
                  <p className="mt-1 text-3xl font-semibold tabular-nums text-slate-900 dark:text-slate-50">
                    {snap?.returnsToday ?? "—"}
                  </p>
                  <p className="mt-2 text-[11px] text-muted-foreground">New return items recorded since midnight UTC.</p>
                </div>
                <span className="inline-flex h-10 w-10 items-center justify-center rounded-xl bg-sky-500/10 text-sky-600 dark:text-sky-400">
                  <RotateCcw className="h-5 w-5" />
                </span>
              </div>
            </div>

            <div className="relative overflow-hidden rounded-2xl border border-slate-200 bg-white px-4 py-5 shadow-sm ring-1 ring-slate-200/80 dark:border-slate-800 dark:bg-slate-950/70 dark:ring-slate-800">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">Pallets</p>
                  <p className="mt-1 text-3xl font-semibold tabular-nums text-slate-900 dark:text-slate-50">
                    {snap?.palletCount ?? "—"}
                  </p>
                  <p className="mt-2 text-[11px] text-muted-foreground">Active pallets in your organization.</p>
                </div>
                <span className="inline-flex h-10 w-10 items-center justify-center rounded-xl bg-violet-500/10 text-violet-600 dark:text-violet-400">
                  <Boxes className="h-5 w-5" />
                </span>
              </div>
            </div>

            <div className="relative overflow-hidden rounded-2xl border border-slate-200 bg-white px-4 py-5 shadow-sm ring-1 ring-slate-200/80 dark:border-slate-800 dark:bg-slate-950/70 dark:ring-slate-800">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">Packages</p>
                  <p className="mt-1 text-3xl font-semibold tabular-nums text-slate-900 dark:text-slate-50">
                    {snap?.packageCount ?? "—"}
                  </p>
                  <p className="mt-2 text-[11px] text-muted-foreground">Active packages in your organization.</p>
                </div>
                <span className="inline-flex h-10 w-10 items-center justify-center rounded-xl bg-emerald-500/10 text-emerald-600 dark:text-emerald-400">
                  <Package2 className="h-5 w-5" />
                </span>
              </div>
            </div>

            <div className="relative overflow-hidden rounded-2xl border border-slate-200 bg-white px-4 py-5 shadow-sm ring-1 ring-slate-200/80 dark:border-slate-800 dark:bg-slate-950/70 dark:ring-slate-800">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">Claims ready to send</p>
                  <p className="mt-1 text-3xl font-semibold tabular-nums text-slate-900 dark:text-slate-50">
                    {snap?.claimsReadyToSend ?? "—"}
                  </p>
                  <p className="mt-2 text-[11px] text-muted-foreground">
                    Submission queue — <code className="rounded bg-muted px-1 font-mono text-[10px]">ready_to_send</code> for Agent polling.
                  </p>
                </div>
                <span className="inline-flex h-10 w-10 items-center justify-center rounded-xl bg-sky-500/10 text-sky-600 dark:text-sky-400">
                  <Send className="h-5 w-5" />
                </span>
              </div>
            </div>

            <div className="relative overflow-hidden rounded-2xl border border-slate-200 bg-white px-4 py-5 shadow-sm ring-1 ring-slate-200/80 dark:border-slate-800 dark:bg-slate-950/70 dark:ring-slate-800">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">Returns est. value</p>
                  <p className="mt-1 text-2xl font-semibold tabular-nums text-slate-900 dark:text-slate-50 sm:text-3xl">
                    {snap ? formatUsdSafe(snap.returnsEstimatedValueUsd) : "—"}
                  </p>
                  <p className="mt-2 text-[11px] text-muted-foreground">
                    Sum of <code className="rounded bg-muted px-1 font-mono text-[10px]">returns.estimated_value</code> (null → $0.00).
                  </p>
                </div>
                <span className="inline-flex h-10 w-10 items-center justify-center rounded-xl bg-amber-500/10 text-amber-600 dark:text-amber-400">
                  <DollarSign className="h-5 w-5" />
                </span>
              </div>
            </div>
          </section>

          <section className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-800 dark:bg-slate-950/70">
            <p className="text-sm font-semibold text-slate-900 dark:text-slate-50">Next steps</p>
            <p className="mt-1 text-xs text-muted-foreground">
              Process inbound returns in Returns Processing. File and track marketplace claims in the Claim Engine.
            </p>
            <div className="mt-4 flex flex-wrap gap-3">
              <Link
                href="/returns"
                className="inline-flex items-center gap-2 rounded-xl border border-slate-200 bg-slate-50 px-4 py-2 text-sm font-medium text-slate-800 transition hover:bg-slate-100 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800"
              >
                Returns Processing
              </Link>
              <Link
                href="/claim-engine"
                className="inline-flex items-center gap-2 rounded-xl border border-sky-200 bg-sky-50 px-4 py-2 text-sm font-medium text-sky-800 transition hover:bg-sky-100 dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-200 dark:hover:bg-sky-950/80"
              >
                Claim Engine
              </Link>
              <Link
                href="/settings"
                className="inline-flex items-center gap-2 rounded-xl border border-slate-200 px-4 py-2 text-sm font-medium text-slate-600 transition hover:bg-slate-50 dark:border-slate-700 dark:text-slate-400 dark:hover:bg-slate-900"
              >
                Connected Stores
              </Link>
            </div>
          </section>
        </div>
      </main>
    </>
  );
}
