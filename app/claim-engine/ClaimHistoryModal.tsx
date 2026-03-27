"use client";

import { useEffect, useState } from "react";
import { Building2, Bot, Loader2, User, X } from "lucide-react";
import { getClaimHistoryLogsForSubmission, type ClaimHistoryLogRow } from "./claim-crm-actions";

function formatWhen(iso: string): string {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(iso));
}

function bubbleIcon(actor: ClaimHistoryLogRow["actor"]) {
  if (actor === "marketplace_bot") return Building2;
  if (actor === "agent") return Bot;
  return User;
}

export function ClaimHistoryModal({
  open,
  onClose,
  submissionId,
  organizationId,
}: {
  open: boolean;
  onClose: () => void;
  submissionId: string | null;
  organizationId: string;
}) {
  const [logs, setLogs] = useState<ClaimHistoryLogRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    if (!open || !submissionId) {
      setLogs([]);
      setErr(null);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setErr(null);
    void getClaimHistoryLogsForSubmission(submissionId, organizationId).then((r) => {
      if (cancelled) return;
      setLoading(false);
      if (r.ok) setLogs(r.data);
      else setErr(r.error ?? "Failed to load");
    });
    return () => {
      cancelled = true;
    };
  }, [open, submissionId, organizationId]);

  if (!open || !submissionId) return null;

  return (
    <div className="fixed inset-0 z-[600] flex items-end justify-center sm:items-center">
      <button
        type="button"
        className="absolute inset-0 bg-black/50"
        aria-label="Close"
        onClick={onClose}
      />
      <div className="relative z-10 flex max-h-[85vh] w-full max-w-lg flex-col rounded-t-2xl border border-slate-200 bg-white shadow-xl dark:border-slate-800 dark:bg-slate-950 sm:rounded-2xl">
        <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3 dark:border-slate-800">
          <p className="text-sm font-semibold text-slate-900 dark:text-slate-50">Claim history</p>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg p-1 text-muted-foreground hover:bg-slate-100 dark:hover:bg-slate-800"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto px-3 py-3">
          {loading ? (
            <div className="flex justify-center py-8">
              <Loader2 className="h-6 w-6 animate-spin text-sky-500" />
            </div>
          ) : err ? (
            <p className="text-center text-sm text-rose-600">{err}</p>
          ) : logs.length === 0 ? (
            <p className="text-center text-sm text-muted-foreground">No history entries yet.</p>
          ) : (
            <ul className="flex flex-col gap-3">
              {logs.map((log) => {
                const Icon = bubbleIcon(log.actor);
                const left = log.actor === "marketplace_bot";
                return (
                  <li
                    key={log.id}
                    className={`flex ${left ? "justify-start" : "justify-end"}`}
                  >
                    <div
                      className={`max-w-[95%] rounded-xl border px-3 py-2 text-xs ${
                        left
                          ? "border-slate-200 bg-slate-100 text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                          : "border-sky-200 bg-sky-50 text-sky-950 dark:border-sky-900 dark:bg-sky-950/40 dark:text-sky-50"
                      }`}
                    >
                      <div className="mb-1 flex items-center gap-1.5 text-[10px] font-semibold uppercase text-muted-foreground">
                        <Icon className="h-3 w-3" />
                        {log.actor.replace(/_/g, " ")}
                        <span className="font-normal">· {formatWhen(log.created_at)}</span>
                      </div>
                      <p className="whitespace-pre-wrap">{log.message_content}</p>
                      <p className="mt-1 text-[10px] text-muted-foreground">Status: {log.status_at_time}</p>
                    </div>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      </div>
    </div>
  );
}
