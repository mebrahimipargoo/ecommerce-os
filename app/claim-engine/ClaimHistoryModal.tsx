"use client";

import { useEffect, useState } from "react";
import { Building2, Bot, Clock, Loader2, User, X } from "lucide-react";
import { getClaimTimelineLogs, type ClaimTimelineRow } from "./claim-history-timeline-actions";

function formatWhen(iso: string): string {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(iso));
}

function timelineIcon(actor: string | null) {
  const a = (actor ?? "").toLowerCase();
  if (a.includes("marketplace") || a.includes("bot")) return Building2;
  if (a.includes("agent")) return Bot;
  return User;
}

function DetailsBlock({ details }: { details: Record<string, unknown> | null }) {
  if (!details || Object.keys(details).length === 0) return null;
  return (
    <pre className="mt-2 max-h-40 overflow-auto rounded-lg border border-slate-200 bg-slate-50/80 p-2 text-[10px] leading-relaxed text-slate-700 dark:border-slate-700 dark:bg-slate-900/50 dark:text-slate-300">
      {JSON.stringify(details, null, 2)}
    </pre>
  );
}

/**
 * Right-side sheet with a vertical timeline for `claim_history_logs` (scoped by organization_id).
 */
export function ClaimHistoryModal({
  open,
  onClose,
  claimId,
  organizationId,
}: {
  open: boolean;
  onClose: () => void;
  claimId: string | null;
  /** Tenant scope — same as `profiles.organization_id` / organization UUID string. */
  organizationId: string;
}) {
  const [logs, setLogs] = useState<ClaimTimelineRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    if (!open || !claimId) {
      setLogs([]);
      setErr(null);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setErr(null);
    void getClaimTimelineLogs(claimId, organizationId).then((r) => {
      if (cancelled) return;
      setLoading(false);
      if (r.ok) setLogs(r.rows);
      else setErr(r.error ?? "Failed to load");
    });
    return () => {
      cancelled = true;
    };
  }, [open, claimId, organizationId]);

  if (!open || !claimId) return null;

  return (
    <div className="fixed inset-0 z-[600] flex justify-end">
      <button type="button" className="absolute inset-0 bg-black/50" aria-label="Close" onClick={onClose} />
      <aside className="relative z-10 flex h-full w-full max-w-md animate-in slide-in-from-right flex-col border-l border-slate-200 bg-white shadow-2xl duration-200 dark:border-slate-800 dark:bg-slate-950 sm:max-w-lg">
        <div className="flex shrink-0 items-center justify-between border-b border-slate-200 px-4 py-3 dark:border-slate-800">
          <div className="flex items-center gap-2">
            <Clock className="h-4 w-4 text-sky-500" aria-hidden />
            <div>
              <p className="text-sm font-semibold text-slate-900 dark:text-slate-50">Claim history</p>
              <p className="font-mono text-[10px] text-muted-foreground">{claimId}</p>
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg p-1.5 text-muted-foreground hover:bg-slate-100 dark:hover:bg-slate-800"
            aria-label="Close"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto px-4 py-4">
          {loading ? (
            <div className="flex justify-center py-12">
              <Loader2 className="h-7 w-7 animate-spin text-sky-500" />
            </div>
          ) : err ? (
            <p className="text-center text-sm text-rose-600">{err}</p>
          ) : logs.length === 0 ? (
            <p className="text-center text-sm text-muted-foreground">No history entries yet.</p>
          ) : (
            <div className="relative pl-2">
              <div
                className="absolute bottom-2 left-[11px] top-2 w-px bg-slate-200 dark:bg-slate-700"
                aria-hidden
              />
              <ul className="relative space-y-6">
                {logs.map((log) => {
                  const Icon = timelineIcon(log.actor);
                  return (
                    <li key={log.id} className="relative pl-8">
                      <span
                        className="absolute left-0 top-1 flex h-[22px] w-[22px] items-center justify-center rounded-full border border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-300"
                        aria-hidden
                      >
                        <Icon className="h-3 w-3" />
                      </span>
                      <div className="rounded-xl border border-slate-200 bg-slate-50/80 px-3 py-2.5 dark:border-slate-700 dark:bg-slate-900/40">
                        <div className="flex flex-wrap items-baseline justify-between gap-2">
                          <p className="text-xs font-semibold text-slate-900 dark:text-slate-100">{log.action}</p>
                          <time className="text-[10px] tabular-nums text-muted-foreground">{formatWhen(log.created_at)}</time>
                        </div>
                        <p className="mt-1 text-[11px] text-muted-foreground">
                          <span className="font-medium text-slate-600 dark:text-slate-400">Actor:</span>{" "}
                          {log.actor ?? "—"}
                        </p>
                        {log.status_at_time ? (
                          <p className="mt-0.5 text-[10px] text-muted-foreground">
                            Status at time: <span className="font-mono">{log.status_at_time}</span>
                          </p>
                        ) : null}
                        <DetailsBlock details={log.details} />
                      </div>
                    </li>
                  );
                })}
              </ul>
            </div>
          )}
        </div>
      </aside>
    </div>
  );
}
