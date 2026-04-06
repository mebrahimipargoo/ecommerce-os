"use client";

import { useEffect, useState, type ReactNode } from "react";
import { Building2, Bot, ChevronDown, ChevronRight, Clock, Loader2, User, X } from "lucide-react";
import { getClaimTimelineLogs, type ClaimTimelineRow } from "./claim-history-timeline-actions";

function formatWhen(iso: string): string {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return "—";
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(t));
}

function formatWhenFull(iso: string): string {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return "—";
  return new Intl.DateTimeFormat("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(t));
}

function pickStr(obj: Record<string, unknown> | null, key: string): string | null {
  if (!obj) return null;
  const v = obj[key];
  if (v == null) return null;
  const s = String(v).trim();
  return s || null;
}

function humanizeKey(key: string): string {
  return key
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatActorRole(raw: string | null): string {
  if (!raw) return "";
  const lower = raw.toLowerCase().replace(/\s+/g, "_");
  const map: Record<string, string> = {
    human_admin: "Human admin",
    marketplace_bot: "Marketplace",
    agent: "AI agent",
    system: "System",
  };
  return map[lower] ?? raw.replace(/_/g, " ");
}

function formatMessageKind(raw: string | null): string {
  if (!raw) return "";
  const lower = raw.toLowerCase();
  const map: Record<string, string> = {
    system: "System event",
    user: "User note",
    marketplace: "Marketplace",
    agent_reply: "Agent reply",
  };
  return map[lower] ?? humanizeKey(raw);
}

function statusBadgeClass(status: string): string {
  const s = status.toLowerCase();
  if (/(accept|approved|complete|closed|won)/i.test(s)) {
    return "border-emerald-500/40 bg-emerald-500/15 text-emerald-800 dark:text-emerald-200";
  }
  if (/(reject|denied|failed|cancel)/i.test(s)) {
    return "border-rose-500/40 bg-rose-500/15 text-rose-800 dark:text-rose-200";
  }
  if (/(pending|investigat|open|draft|ready)/i.test(s)) {
    return "border-amber-500/40 bg-amber-500/15 text-amber-900 dark:text-amber-100";
  }
  return "border-slate-300 bg-slate-100 text-slate-800 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-100";
}

function Badge({
  children,
  variant = "neutral",
}: {
  children: ReactNode;
  variant?: "neutral" | "sky" | "violet";
}) {
  const cls =
    variant === "sky"
      ? "border-sky-500/35 bg-sky-500/10 text-sky-900 dark:text-sky-100"
      : variant === "violet"
        ? "border-violet-500/35 bg-violet-500/10 text-violet-900 dark:text-violet-100"
        : "border-slate-300/80 bg-slate-100/90 text-slate-700 dark:border-slate-600 dark:bg-slate-800/80 dark:text-slate-200";
  return (
    <span
      className={`inline-flex max-w-full items-center rounded-md border px-2 py-0.5 text-[10px] font-medium leading-tight ${cls}`}
    >
      {children}
    </span>
  );
}

/** Keys we render as structured UI (not in the "extra" list). */
const STRUCTURED_DETAIL_KEYS = new Set([
  "status",
  "new_status",
  "actor_role",
  "message_kind",
  "status_at_time",
]);

function timelineIcon(actorHint: string | null) {
  const a = (actorHint ?? "").toLowerCase();
  if (a.includes("marketplace") || a.includes("bot")) return Building2;
  if (a.includes("agent") && !a.includes("admin")) return Bot;
  if (a.includes("admin") || a.includes("user") || a.includes("@") || a.includes("human")) return User;
  return User;
}

function TimelineEntryDetails({
  details,
  statusSnapshot,
}: {
  details: Record<string, unknown> | null;
  statusSnapshot: string | null;
}) {
  const [rawOpen, setRawOpen] = useState(false);
  if (!details || Object.keys(details).length === 0) {
    if (!statusSnapshot) return null;
    return (
      <div className="mt-3">
        <p className="text-[10px] font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">Status</p>
        <span
          className={`mt-1 inline-flex rounded-lg border px-2.5 py-1 text-xs font-semibold ${statusBadgeClass(statusSnapshot)}`}
        >
          {statusSnapshot}
        </span>
      </div>
    );
  }

  const actorRole = formatActorRole(pickStr(details, "actor_role"));
  const messageKind = formatMessageKind(pickStr(details, "message_kind"));
  const newStatus = pickStr(details, "new_status");
  const detailStatus = pickStr(details, "status");
  const effectiveStatus = newStatus ?? detailStatus ?? statusSnapshot;

  const extras: [string, string][] = [];
  for (const [k, v] of Object.entries(details)) {
    if (STRUCTURED_DETAIL_KEYS.has(k)) continue;
    if (v == null || v === "") continue;
    const display =
      typeof v === "object" ? JSON.stringify(v) : String(v);
    extras.push([humanizeKey(k), display]);
  }

  const hasStructured =
    !!actorRole || !!messageKind || !!effectiveStatus || newStatus !== detailStatus;

  return (
    <div className="mt-3 space-y-3 border-t border-slate-200/80 pt-3 dark:border-slate-700/80">
      {(actorRole || messageKind) && (
        <div className="flex flex-wrap gap-1.5">
          {messageKind ? <Badge variant="sky">{messageKind}</Badge> : null}
          {actorRole ? <Badge variant="violet">Role: {actorRole}</Badge> : null}
        </div>
      )}

      {effectiveStatus ? (
        <div>
          <p className="text-[10px] font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">
            {newStatus && detailStatus && newStatus !== detailStatus ? "Status change" : "Status"}
          </p>
          <div className="mt-1 flex flex-wrap items-center gap-2">
            <span
              className={`inline-flex rounded-lg border px-2.5 py-1 text-xs font-semibold ${statusBadgeClass(effectiveStatus)}`}
            >
              {effectiveStatus}
            </span>
            {newStatus && detailStatus && newStatus !== detailStatus ? (
              <>
                <span className="text-[10px] text-muted-foreground">from</span>
                <span
                  className={`inline-flex rounded-md border px-2 py-0.5 text-[11px] font-medium opacity-80 ${statusBadgeClass(detailStatus)}`}
                >
                  {detailStatus}
                </span>
              </>
            ) : null}
          </div>
        </div>
      ) : null}

      {extras.length > 0 ? (
        <dl className="space-y-1.5 rounded-lg border border-slate-200/90 bg-white/60 px-2.5 py-2 dark:border-slate-700/90 dark:bg-slate-950/40">
          {extras.map(([label, value]) => (
            <div key={label} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)] gap-x-2 gap-y-0.5 text-[11px]">
              <dt className="text-slate-500 dark:text-slate-400">{label}</dt>
              <dd className="break-words font-medium text-slate-800 dark:text-slate-100">{value}</dd>
            </div>
          ))}
        </dl>
      ) : null}

      {!hasStructured && extras.length === 0 && Object.keys(details).length > 0 ? (
        <p className="text-[11px] text-muted-foreground">No extra fields.</p>
      ) : null}

      {Object.keys(details).length > 0 ? (
        <>
          <button
            type="button"
            onClick={() => setRawOpen((o) => !o)}
            className="flex w-full items-center gap-1 text-left text-[10px] font-medium text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
          >
            {rawOpen ? <ChevronDown className="h-3 w-3 shrink-0" /> : <ChevronRight className="h-3 w-3 shrink-0" />}
            Raw payload (JSON)
          </button>
          {rawOpen ? (
            <pre className="max-h-36 overflow-auto rounded-lg border border-slate-200 bg-slate-50 p-2 font-mono text-[10px] leading-relaxed text-slate-700 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
              {JSON.stringify(details, null, 2)}
            </pre>
          ) : null}
        </>
      ) : null}
    </div>
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
  readOnly = false,
}: {
  open: boolean;
  onClose: () => void;
  claimId: string | null;
  /** Tenant scope — same as `profiles.organization_id` / organization UUID string. */
  organizationId: string;
  /** When true (e.g. closed-claims tab), emphasize view-only UI. */
  readOnly?: boolean;
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
      if (r.ok) setLogs(Array.isArray(r.rows) ? r.rows : []);
      else setErr(r.error ?? "Failed to load");
    });
    return () => {
      cancelled = true;
    };
  }, [open, claimId, organizationId]);

  if (!open || !claimId) return null;

  const claimIdShort =
    claimId.length > 14 ? `${claimId.slice(0, 8)}…${claimId.slice(-6)}` : claimId;

  return (
    <div className="fixed inset-0 z-[600] flex justify-end">
      <button type="button" className="absolute inset-0 bg-black/50" aria-label="Close" onClick={onClose} />
      <aside className="relative z-10 flex h-full w-full max-w-md animate-in slide-in-from-right flex-col border-l border-slate-200 bg-white shadow-2xl duration-200 dark:border-slate-800 dark:bg-slate-950 sm:max-w-lg">
        <div className="flex shrink-0 items-center justify-between border-b border-slate-200 px-4 py-3 dark:border-slate-800">
          <div className="flex min-w-0 flex-1 items-start gap-2">
            <Clock className="mt-0.5 h-4 w-4 shrink-0 text-sky-500" aria-hidden />
            <div className="min-w-0">
              <p className="text-sm font-semibold text-slate-900 dark:text-slate-50">Claim history</p>
              <p className="text-[10px] text-slate-500 dark:text-slate-400">Submission</p>
              <p className="truncate font-mono text-[10px] text-muted-foreground" title={claimId}>
                {claimIdShort}
              </p>
              {readOnly ? (
                <p className="mt-1 inline-flex items-center rounded border border-slate-200 bg-slate-50 px-1.5 py-0.5 text-[10px] font-medium text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
                  View only
                </p>
              ) : null}
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
            <p className="text-center text-sm text-muted-foreground">No history available.</p>
          ) : (
            <div className="relative pl-2">
              <div
                className="absolute bottom-2 left-[11px] top-2 w-px bg-slate-200 dark:bg-slate-700"
                aria-hidden
              />
              <ul className="relative space-y-6">
                {logs.map((log, idx) => {
                  const roleHint = pickStr(log.details, "actor_role") ?? log.actor;
                  const Icon = timelineIcon(roleHint);
                  return (
                    <li key={log.id || `log-${idx}`} className="relative pl-8">
                      <span
                        className="absolute left-0 top-1 flex h-[22px] w-[22px] items-center justify-center rounded-full border border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-300"
                        aria-hidden
                      >
                        <Icon className="h-3 w-3" />
                      </span>
                      <article className="rounded-xl border border-slate-200 bg-slate-50/80 px-3 py-3 shadow-sm dark:border-slate-700 dark:bg-slate-900/40">
                        <div className="flex flex-wrap items-start justify-between gap-2 border-b border-slate-200/80 pb-2 dark:border-slate-700/80">
                          <h3 className="max-w-[85%] text-sm font-semibold leading-snug text-slate-900 dark:text-slate-50">
                            {log.action || "Event"}
                          </h3>
                          <time
                            className="shrink-0 text-right text-[10px] tabular-nums leading-tight text-muted-foreground"
                            dateTime={log.created_at}
                            title={formatWhenFull(log.created_at)}
                          >
                            {formatWhen(log.created_at)}
                          </time>
                        </div>
                        <p className="mt-2 text-[11px] text-slate-600 dark:text-slate-300">
                          <span className="font-medium text-slate-500 dark:text-slate-400">Recorded by</span>{" "}
                          <span className="font-medium text-slate-800 dark:text-slate-100">{log.actor ?? "—"}</span>
                        </p>
                        <TimelineEntryDetails details={log.details} statusSnapshot={log.status_at_time ?? null} />
                      </article>
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
