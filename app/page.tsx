"use client";
import React, { useEffect, useMemo, useState } from "react";
import {
  LayoutDashboard,
  RotateCcw,
  ShieldAlert,
  Settings,
  Bell,
  Search,
  ChevronDown,
  Store,
  Building2,
} from "lucide-react";

import { supabase } from "../src/lib/supabase";

type ClaimStatus = "pending" | "recovered" | "suspicious";

type ClaimRow = {
  id: string;
  amount: number | string;
  status: ClaimStatus;
  created_at: string;
};

type ClaimsSummary = {
  totalRecoverable: number;
  pendingSlas: number;
  suspiciousReturns: number;
};

const initialSummary: ClaimsSummary = {
  totalRecoverable: 0,
  pendingSlas: 0,
  suspiciousReturns: 0,
};

function formatCurrency(value: number): string {
  if (!Number.isFinite(value)) {
    return "$0";
  }

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function toNumber(value: number | string): number {
  if (typeof value === "number") {
    return value;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

async function fetchClaimsSummary(): Promise<ClaimsSummary> {
  const { data, error } = await supabase
    .from("claims")
    .select("id, amount, status, created_at");
  console.log("Fetched data:", data);

  if (error) {
    throw new Error(error.message);
  }

  const rows = (data ?? []) as ClaimRow[];

  let totalRecoverable = 0;
  let pendingSlas = 0;
  let suspiciousReturns = 0;

  for (const row of rows) {
    if (row.status === "recovered") {
      totalRecoverable += toNumber(row.amount);
    } else if (row.status === "pending") {
      pendingSlas += 1;
    } else if (row.status === "suspicious") {
      suspiciousReturns += 1;
    }
  }

  return { totalRecoverable, pendingSlas, suspiciousReturns };
}

const sidebarItems = [
  {
    label: "Dashboard",
    icon: LayoutDashboard,
    active: true,
  },
  {
    label: "Returns Intelligence",
    icon: RotateCcw,
    active: false,
  },
  {
    label: "Claim Engine",
    icon: ShieldAlert,
    active: false,
  },
];
const adapterItems = [
  {
    label: "Amazon",
    icon: Store,
  },
  {
    label: "Walmart",
    icon: Store,
  },
  {
    label: "Costco",
    icon: Building2,
  },
];
const footerItems = [
  {
    label: "Settings",
    icon: Settings,
  },
];
export default function Page() {
  const [summary, setSummary] = useState<ClaimsSummary>(initialSummary);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function run() {
      try {
        setIsLoading(true);
        setErrorMessage(null);
        const result = await fetchClaimsSummary();
        if (!cancelled) {
          setSummary(result);
        }
      } catch (error) {
        if (cancelled) {
          return;
        }
        const message =
          error instanceof Error ? error.message : "Failed to load claims.";
        setErrorMessage(message);
        setSummary(initialSummary);
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    void run();

    return () => {
      cancelled = true;
    };
  }, []);

  const kpis = useMemo(
    () => [
      {
        label: "Total Recoverable",
        value: isLoading
          ? "Syncing..."
          : formatCurrency(summary.totalRecoverable),
        change: isLoading ? "Loading" : "Live from claims",
        trend: "Sum of recovered claim amounts",
        accentClass:
          "from-sky-500/10 via-sky-500/5 to-sky-500/0 border-sky-500/30",
        pillClass: "bg-sky-500/10 text-sky-500 border-sky-500/30",
      },
      {
        label: "Pending SLAs",
        value: isLoading ? "—" : String(summary.pendingSlas),
        change: isLoading ? "Loading" : "Open pending claims",
        trend: "Claims currently in pending status",
        accentClass:
          "from-amber-500/10 via-amber-500/5 to-amber-500/0 border-amber-500/30",
        pillClass: "bg-amber-500/10 text-amber-500 border-amber-500/30",
      },
      {
        label: "Suspicious Returns",
        value: isLoading ? "—" : String(summary.suspiciousReturns),
        change: isLoading ? "Loading" : "Flagged for review",
        trend: "Claims marked as suspicious",
        accentClass:
          "from-rose-500/10 via-rose-500/5 to-rose-500/0 border-rose-500/30",
        pillClass: "bg-rose-500/10 text-rose-500 border-rose-500/30",
      },
    ],
    [isLoading, summary.pendingSlas, summary.suspiciousReturns, summary.totalRecoverable]
  );

  return (
    <div className="flex min-h-screen bg-slate-950 text-slate-50">
      {/* Sidebar */}
      <aside className="flex w-64 flex-col border-r border-slate-800 bg-slate-950/70 backdrop-blur-xl">
        <div className="flex h-16 items-center gap-2 border-b border-slate-800 px-4">
          <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-sky-500/10 ring-1 ring-sky-500/40">
            <span className="text-lg font-semibold text-sky-400">OS</span>
          </div>
          <div className="flex flex-col">
            <span className="text-sm font-semibold tracking-tight text-slate-50">
              E‑commerce OS
            </span>
            <span className="text-xs text-slate-400">
              B2B Returns & Recovery
            </span>
          </div>
        </div>
        <nav className="flex-1 space-y-6 px-3 py-4">
          <div className="space-y-1">
            <p className="px-2 text-xs font-medium uppercase tracking-wide text-slate-500">
              Core
            </p>
            {sidebarItems.map((item) => (
              <button
                key={item.label}
                className={[
                  "group flex w-full items-center gap-2 rounded-lg px-2.5 py-2 text-sm font-medium transition",
                  item.active
                    ? "bg-slate-900 text-slate-50 ring-1 ring-slate-700 shadow-[0_0_0_1px_rgba(15,23,42,0.9)]"
                    : "text-slate-400 hover:bg-slate-900/60 hover:text-slate-100 hover:ring-1 hover:ring-slate-800",
                ].join(" ")}
              >
                <item.icon className="h-4 w-4 text-slate-400 group-hover:text-slate-100" />
                <span>{item.label}</span>
              </button>
            ))}
          </div>
          <div className="space-y-1">
            <p className="px-2 text-xs font-medium uppercase tracking-wide text-slate-500">
              Adapters
            </p>
            {adapterItems.map((item) => (
              <button
                key={item.label}
                className="group flex w-full items-center gap-2 rounded-lg px-2.5 py-2 text-sm font-medium text-slate-400 transition hover:bg-slate-900/60 hover:text-slate-100 hover:ring-1 hover:ring-slate-800"
              >
                <item.icon className="h-4 w-4 text-slate-500 group-hover:text-slate-100" />
                <span>{item.label}</span>
              </button>
            ))}
          </div>
        </nav>
        <div className="border-t border-slate-800 px-3 py-3">
          {footerItems.map((item) => (
            <button
              key={item.label}
              className="group flex w-full items-center gap-2 rounded-lg px-2.5 py-2 text-sm font-medium text-slate-400 transition hover:bg-slate-900/80 hover:text-slate-100 hover:ring-1 hover:ring-slate-800"
            >
              <item.icon className="h-4 w-4 text-slate-500 group-hover:text-slate-100" />
              <span>{item.label}</span>
            </button>
          ))}
        </div>
      </aside>
      {/* Main Column */}
      <div className="flex min-w-0 flex-1 flex-col">
        {/* Header */}
        <header className="flex h-16 items-center justify-between border-b border-slate-800 bg-slate-950/70 px-6 backdrop-blur-xl">
          <div className="flex flex-col">
            <h1 className="text-sm font-semibold tracking-tight text-slate-50">
              Dashboard
            </h1>
            <p className="text-xs text-slate-400">
              Unified control center for returns, claims, and adapters.
            </p>
          </div>
          <div className="flex items-center gap-4">
            {/* Global Search */}
            <div className="relative hidden w-80 items-center md:flex">
              <Search className="pointer-events-none absolute left-3 h-4 w-4 text-slate-500" />
              <input
                type="search"
                placeholder="Search orders, RMAs, claims, SKUs..."
                className="h-9 w-full rounded-lg border border-slate-800 bg-slate-950/70 pl-9 pr-3 text-xs text-slate-100 placeholder:text-slate-500 shadow-sm outline-none ring-0 transition focus:border-sky-500/60 focus:ring-1 focus:ring-sky-500/60"
              />
              <span className="pointer-events-none absolute right-2 hidden items-center gap-1 rounded border border-slate-800 bg-slate-900/60 px-1.5 py-0.5 text-[10px] font-medium text-slate-400 md:inline-flex">
                ⌘K
              </span>
            </div>
            {/* Notification Bell */}
            <button className="relative flex h-9 w-9 items-center justify-center rounded-full border border-slate-800 bg-slate-950/70 text-slate-300 shadow-sm transition hover:border-sky-500/60 hover:text-sky-400">
              <Bell className="h-4 w-4" />
              <span className="absolute right-1 top-1 h-2 w-2 rounded-full bg-rose-500 ring-2 ring-slate-950" />
            </button>
            {/* User Avatar */}
            <button className="flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/70 px-2 py-1.5 text-xs text-slate-200 shadow-sm transition hover:border-sky-500/60">
              <div className="flex h-7 w-7 items-center justify-center rounded-full bg-gradient-to-tr from-sky-500 to-blue-500 text-[11px] font-semibold text-white">
                J
              </div>
              <div className="hidden flex-col items-start leading-tight sm:flex">
                <span className="text-xs font-medium text-slate-50">
                  Jennifer Lee
                </span>
                <span className="text-[10px] text-slate-400">
                  Operations Director
                </span>
              </div>
              <ChevronDown className="hidden h-3 w-3 text-slate-500 sm:block" />
            </button>
          </div>
        </header>
        {/* Main Content */}
        <main className="flex-1 overflow-y-auto bg-gradient-to-br from-slate-950 via-slate-950 to-slate-900">
          <div className="mx-auto flex max-w-6xl flex-col gap-6 px-4 py-6 lg:px-8">
            {/* KPI Cards */}
            <section className="grid gap-4 md:grid-cols-3">
              {kpis.map((kpi) => (
                <div
                  key={kpi.label}
                  className={`relative overflow-hidden rounded-2xl border bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80 transition hover:-translate-y-0.5 hover:shadow-[0_18px_45px_rgba(15,23,42,0.85)] ${kpi.accentClass}`}
                >
                  <div className="pointer-events-none absolute inset-0 bg-gradient-to-br from-slate-100/2 via-slate-100/0 to-slate-100/0" />
                  <div className="relative flex items-start justify-between gap-3">
                    <div className="space-y-2">
                      <p className="text-xs font-medium uppercase tracking-wide text-slate-400">
                        {kpi.label}
                      </p>
                      <div className="flex items-baseline gap-1">
                        <span
                          className={`text-2xl font-semibold tracking-tight ${
                            isLoading ? "text-slate-500" : "text-slate-50"
                          }`}
                        >
                          {kpi.value}
                        </span>
                      </div>
                    </div>
                    <span
                      className={`inline-flex items-center gap-1 rounded-full border px-2 py-1 text-[10px] font-medium ${kpi.pillClass}`}
                    >
                      <span className="h-1.5 w-1.5 rounded-full bg-current" />
                      {kpi.change}
                    </span>
                  </div>
                  <p className="relative mt-3 text-[11px] text-slate-400">
                    {kpi.trend}
                  </p>
                  {isLoading && (
                    <div className="mt-3 h-1.5 w-20 rounded-full bg-slate-800/80">
                      <div className="h-1.5 w-1/2 animate-pulse rounded-full bg-slate-600/80" />
                    </div>
                  )}
                </div>
              ))}
            </section>

            {errorMessage && (
              <div className="rounded-2xl border border-rose-700/60 bg-rose-950/40 px-4 py-3 text-xs text-rose-100">
                <span className="font-semibold">Data warning:</span>{" "}
                {errorMessage}
              </div>
            )}
            {/* Secondary Content */}
            <section className="grid gap-4 lg:grid-cols-3">
              {/* Left: Activity Snapshot */}
              <div className="col-span-2 overflow-hidden rounded-2xl border border-slate-800 bg-slate-950/70 shadow-sm">
                <div className="flex items-center justify-between border-b border-slate-800 px-4 py-3">
                  <div>
                    <p className="text-xs font-semibold tracking-tight text-slate-50">
                      Returns Pipeline
                    </p>
                    <p className="text-[11px] text-slate-400">
                      Live view across marketplaces and carriers.
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <button className="rounded-full bg-slate-900 px-2.5 py-1.5 text-[11px] font-medium text-slate-200 ring-1 ring-slate-700">
                      Last 30 days
                    </button>
                    <button className="rounded-full bg-transparent px-2.5 py-1.5 text-[11px] font-medium text-slate-400 ring-1 ring-slate-800 hover:text-slate-100">
                      Custom
                    </button>
                  </div>
                </div>
                <div className="grid gap-4 px-4 py-4 md:grid-cols-3">
                  <div className="space-y-1">
                    <p className="text-xs text-slate-400">Open RMAs</p>
                    <p className="text-lg font-semibold text-slate-50">196</p>
                    <p className="text-[11px] text-emerald-400">
                      72% within SLA
                    </p>
                  </div>
                  <div className="space-y-1">
                    <p className="text-xs text-slate-400">Claims In Flight</p>
                    <p className="text-lg font-semibold text-slate-50">84</p>
                    <p className="text-[11px] text-sky-400">
                      54% with carrier evidence
                    </p>
                  </div>
                  <div className="space-y-1">
                    <p className="text-xs text-slate-400">High Risk</p>
                    <p className="text-lg font-semibold text-slate-50">23</p>
                    <p className="text-[11px] text-rose-400">
                      Pattern anomalies detected
                    </p>
                  </div>
                </div>
              </div>
              {/* Right: SLA Overview */}
              <div className="overflow-hidden rounded-2xl border border-slate-800 bg-slate-950/70 shadow-sm">
                <div className="border-b border-slate-800 px-4 py-3">
                  <p className="text-xs font-semibold tracking-tight text-slate-50">
                    SLA Overview
                  </p>
                  <p className="text-[11px] text-slate-400">
                    Where your attention is required today.
                  </p>
                </div>
                <div className="space-y-3 px-4 py-4">
                  <div className="flex items-center justify-between rounded-lg bg-slate-900/80 px-3 py-2">
                    <div>
                      <p className="text-xs font-medium text-slate-50">
                        Marketplace responses
                      </p>
                      <p className="text-[11px] text-slate-400">
                        18 tickets require response in the next 4 hours.
                      </p>
                    </div>
                    <span className="text-xs font-semibold text-amber-400">
                      4h
                    </span>
                  </div>
                  <div className="flex items-center justify-between rounded-lg bg-slate-900/60 px-3 py-2">
                    <div>
                      <p className="text-xs font-medium text-slate-50">
                        Carrier evidence
                      </p>
                      <p className="text-[11px] text-slate-400">
                        9 shipments awaiting proof-of-delivery uploads.
                      </p>
                    </div>
                    <span className="text-xs font-semibold text-sky-400">
                      Today
                    </span>
                  </div>
                  <div className="flex items-center justify-between rounded-lg bg-slate-900/60 px-3 py-2">
                    <div>
                      <p className="text-xs font-medium text-slate-50">
                        Policy breaches
                      </p>
                      <p className="text-[11px] text-slate-400">
                        3 high-value returns outside policy guidelines.
                      </p>
                    </div>
                    <span className="text-xs font-semibold text-rose-400">
                      Review
                    </span>
                  </div>
                </div>
              </div>
            </section>
            {/* Bottom Placeholder Section for Future Widgets */}
            <section className="rounded-2xl border border-dashed border-slate-800/80 bg-slate-950/50 px-4 py-4 text-xs text-slate-500">
              This area can host charts, cohort analysis, adapter health, or
              custom widgets tailored to your B2B stack.
            </section>
          </div>
        </main>
      </div>
    </div>
  );
}