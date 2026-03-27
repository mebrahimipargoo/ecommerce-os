"use client";

import Link from "next/link";
import { useMemo } from "react";
import { Bot, Building2, FileText, Shield, User } from "lucide-react";
import type { ClaimHistoryLogRow } from "./claim-crm-actions";
import { ReturnIdentifiersColumn } from "../../components/ReturnIdentifiersColumn";

function formatCurrency(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatWhen(iso: string): string {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(iso));
}

/** Maps DB status → CRM badge label (V16.4.12). */
export function submissionCrmBadgeLabel(status: string): {
  label: string;
  className: string;
} {
  switch (status) {
    case "accepted":
      return {
        label: "Approved",
        className:
          "border-emerald-500/60 bg-emerald-950/60 text-emerald-200",
      };
    case "rejected":
      return {
        label: "Denied",
        className: "border-rose-500/60 bg-rose-950/60 text-rose-200",
      };
    case "evidence_requested":
      return {
        label: "Evidence Requested",
        className: "border-amber-500/60 bg-amber-950/60 text-amber-200",
      };
    case "submitted":
    case "ready_to_send":
    case "draft":
      return {
        label: "Pending Marketplace Action",
        className: "border-sky-500/60 bg-sky-950/60 text-sky-200",
      };
    default:
      return {
        label: status.replace(/_/g, " "),
        className: "border-slate-500/60 bg-slate-900/60 text-slate-200",
      };
  }
}

function bubbleMeta(log: ClaimHistoryLogRow): { title: string; Icon: typeof Bot; align: "left" | "right" } {
  if (log.actor === "marketplace_bot") {
    return { title: "Amazon / Walmart response", Icon: Building2, align: "left" };
  }
  if (log.actor === "agent") {
    return { title: "Agent smart reply", Icon: Bot, align: "right" };
  }
  return { title: "Admin", Icon: User, align: "right" };
}

export function ClaimInvestigationClient({
  submission,
  logs,
  returnRow,
  previewUrl,
}: {
  submission: Record<string, unknown>;
  logs: ClaimHistoryLogRow[];
  returnRow: Record<string, unknown> | null;
  previewUrl: string | null;
}) {
  const status = String(submission.status ?? "");
  const badge = submissionCrmBadgeLabel(status);
  const caseId = (submission.submission_id as string | null) ?? null;
  const amount = Number(submission.claim_amount ?? 0) || 0;
  const prob = submission.success_probability;
  const lastCheck = submission.last_checked_at as string | null;

  const sortedLogs = useMemo(() => [...logs].sort((a, b) => a.created_at.localeCompare(b.created_at)), [logs]);

  return (
    <div className="min-h-screen bg-slate-50 dark:bg-gradient-to-br dark:from-slate-950 dark:via-slate-950 dark:to-slate-900">
      <header className="border-b border-slate-200 bg-white/90 px-4 py-4 backdrop-blur dark:border-slate-800 dark:bg-slate-950/80 md:px-8">
        <div className="mx-auto flex max-w-3xl flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <Link
              href="/claim-engine"
              className="text-xs font-medium text-sky-600 hover:text-sky-500 dark:text-sky-400"
            >
              ← Claim Engine
            </Link>
            <h1 className="mt-2 text-lg font-semibold tracking-tight text-slate-900 dark:text-slate-50">
              Claim investigation
            </h1>
            <p className="text-xs text-muted-foreground">Ticket timeline and marketplace conversation</p>
          </div>
          <span
            className={`inline-flex w-fit items-center rounded-full border px-3 py-1.5 text-xs font-semibold ${badge.className}`}
          >
            {badge.label}
          </span>
        </div>
      </header>

      <main className="mx-auto max-w-3xl px-4 py-6 md:px-8">
        <section className="mb-6 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-800 dark:bg-slate-950/70">
          <div className="flex items-start gap-2 border-b border-slate-100 pb-3 dark:border-slate-800">
            <Shield className="mt-0.5 h-4 w-4 text-sky-500" />
            <div>
              <p className="text-xs font-semibold text-slate-900 dark:text-slate-50">Submission details</p>
              <p className="text-[11px] text-muted-foreground">Identifiers, case ID, and generated evidence.</p>
            </div>
          </div>
          <div className="mt-3 space-y-2 text-sm">
            {returnRow ? (
              <ReturnIdentifiersColumn
                compact
                itemName={returnRow.item_name as string | undefined}
                asin={returnRow.asin as string | undefined}
                fnsku={returnRow.fnsku as string | undefined}
                sku={returnRow.sku as string | undefined}
              />
            ) : (
              <p className="text-xs text-muted-foreground">No return row linked.</p>
            )}
            <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
              <span>
                Case ID:{" "}
                <span className="font-mono text-slate-800 dark:text-slate-200">{caseId ?? "—"}</span>
              </span>
              <span>Claim value: {formatCurrency(amount)}</span>
              {typeof prob === "number" && !Number.isNaN(prob) ? (
                <span>Success probability: {prob.toFixed(0)}%</span>
              ) : null}
              {lastCheck ? <span>Last checked: {formatWhen(lastCheck)}</span> : null}
            </div>
            {previewUrl ? (
              <a
                href={previewUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="mt-2 inline-flex items-center gap-2 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-xs font-semibold text-slate-800 hover:bg-slate-100 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:hover:bg-slate-800"
              >
                <FileText className="h-4 w-4" />
                Open evidence PDF
              </a>
            ) : null}
          </div>
        </section>

        <section className="rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-950/70">
          <div className="border-b border-slate-100 px-4 py-3 dark:border-slate-800">
            <p className="text-xs font-semibold text-slate-900 dark:text-slate-50">Conversation</p>
            <p className="text-[11px] text-muted-foreground">
              Marketplace messages and agent replies from <code className="text-[10px]">claim_history_logs</code>.
            </p>
          </div>
          <div className="flex flex-col gap-4 px-3 py-4 sm:px-4">
            {sortedLogs.length === 0 ? (
              <p className="px-1 text-center text-sm text-muted-foreground">
                No messages yet. When the autonomous agent syncs, history will appear here.
              </p>
            ) : (
              sortedLogs.map((log) => {
                const meta = bubbleMeta(log);
                const isLeft = meta.align === "left";
                const kindLabel =
                  log.message_kind === "marketplace_response"
                    ? "Amazon's response"
                    : log.message_kind === "agent_reply"
                      ? "Agent's smart reply"
                      : meta.title;
                const Icon = meta.Icon;
                return (
                  <div
                    key={log.id}
                    className={`flex w-full ${isLeft ? "justify-start" : "justify-end"}`}
                  >
                    <div
                      className={`max-w-[92%] rounded-2xl border px-3 py-2.5 text-sm shadow-sm sm:max-w-[85%] ${
                        isLeft
                          ? "border-slate-200 bg-slate-100 text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                          : "border-sky-200 bg-sky-50 text-sky-950 dark:border-sky-900 dark:bg-sky-950/50 dark:text-sky-50"
                      }`}
                    >
                      <div className="mb-1 flex flex-wrap items-center gap-2 text-[10px] font-semibold uppercase tracking-wide text-muted-foreground">
                        <Icon className="h-3.5 w-3.5" />
                        <span>{kindLabel}</span>
                        <span className="font-normal opacity-80">· {formatWhen(log.created_at)}</span>
                      </div>
                      <p className="whitespace-pre-wrap text-[13px] leading-relaxed">{log.message_content}</p>
                      <p className="mt-2 text-[10px] text-muted-foreground">
                        Status after: <span className="font-mono">{log.status_at_time}</span>
                      </p>
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </section>
      </main>
    </div>
  );
}
