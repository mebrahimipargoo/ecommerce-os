"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { estimateClaimSuccessProbability } from "./claim-crm-utils";
import { CLAIM_SUBMISSION_RETURN_ID_COLUMN, CLAIM_SUBMISSIONS_TABLE } from "./claim-submissions-constants";
import type { ClaimSubmissionStatus } from "./claim-submission-actions";

const DEFAULT_ORG = "00000000-0000-0000-0000-000000000001";

const CLAIM_HISTORY_TABLE = "claim_history_logs";

export type ClaimHistoryActor = "agent" | "marketplace_bot" | "human_admin";

export type ClaimHistoryMessageKind =
  | "marketplace_response"
  | "agent_reply"
  | "note"
  | "system";

export type ClaimHistoryLogRow = {
  id: string;
  organization_id: string;
  /** Same UUID as `claim_id` on the log row (submission id). */
  submission_id: string;
  actor: ClaimHistoryActor;
  message_content: string;
  attachments: Record<string, unknown>;
  status_at_time: string;
  message_kind: ClaimHistoryMessageKind | null;
  created_at: string;
};

const CLAIM_HISTORY_SELECT =
  "id, organization_id, claim_id, action, details, actor, created_at";

function asDetailsObj(raw: unknown): Record<string, unknown> {
  if (raw != null && typeof raw === "object" && !Array.isArray(raw)) return raw as Record<string, unknown>;
  return {};
}

function mapClaimHistoryLogRow(r: Record<string, unknown>): ClaimHistoryLogRow {
  const d = asDetailsObj(r.details);
  const claimId = String(r.claim_id ?? "");
  const action = String(r.action ?? "").trim();
  const message_content =
    typeof d.message_content === "string" && d.message_content.trim() ? d.message_content.trim() : action;
  const statusRaw = d.status ?? d.status_at_time;
  const status_at_time = typeof statusRaw === "string" && statusRaw.trim() ? statusRaw.trim() : "";
  const mk = d.message_kind;
  const message_kind: ClaimHistoryMessageKind | null =
    mk === "marketplace_response" || mk === "agent_reply" || mk === "note" || mk === "system"
      ? mk
      : null;

  const roleRaw = typeof d.actor_role === "string" ? d.actor_role : "";
  let actor: ClaimHistoryActor = "human_admin";
  if (roleRaw === "marketplace_bot" || roleRaw === "agent" || roleRaw === "human_admin") {
    actor = roleRaw;
  } else {
    const a = String(r.actor ?? "").toLowerCase();
    if (a.includes("marketplace") || a.includes("bot")) actor = "marketplace_bot";
    else if (a.includes("agent") && !a.includes("human")) actor = "agent";
  }

  return {
    id: r.id as string,
    organization_id: r.organization_id as string,
    submission_id: claimId,
    actor,
    message_content,
    attachments: d,
    status_at_time,
    message_kind,
    created_at: r.created_at as string,
  };
}

export type ClaimEngineKpis = {
  totalActiveClaims: number;
  /** Sum of `claim_amount` for non-terminal rows (not accepted / not rejected). */
  totalClaimValueUsd: number;
  /** Sum of `claim_amount` where status is pipeline / in-flight (pending-like + submitted). */
  projectedRecoveryUsd: number;
  /** Sum of actual recoveries: `reimbursement_amount` when set, else `claim_amount` for accepted. */
  totalRecoveredUsd: number;
  successRatePercent: number;
  pendingEvidenceCount: number;
};

function resolveReturnSku(ret: Record<string, unknown> | null): string | null {
  if (!ret) return null;
  const sku = ret.sku;
  if (typeof sku === "string" && sku.trim()) return sku;
  return null;
}

export async function getClaimEngineKpis(
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; data?: ClaimEngineKpis; error?: string }> {
  try {
    const { data, error } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .select("status, claim_amount, reimbursement_amount")
      .eq("organization_id", organizationId);

    if (error) throw new Error(error.message);
    const rows = data ?? [];

    const TERMINAL_STATUSES = new Set(["accepted", "rejected", "failed"]);
    const active = rows.filter((r) => !TERMINAL_STATUSES.has(String(r.status ?? "")));
    const totalActiveClaims = active.length;
    const totalClaimValueUsd = active.reduce((sum, r) => sum + Number(r.claim_amount ?? 0), 0);

    const projectedStatuses = new Set([
      "pending",
      "submitted",
      "draft",
      "ready_to_send",
    ]);
    const projectedRecoveryUsd = rows
      .filter((r) => projectedStatuses.has(String(r.status ?? "")))
      .reduce((sum, r) => sum + Number(r.claim_amount ?? 0), 0);

    const acceptedRows = rows.filter((r) => r.status === "accepted");
    const totalRecoveredUsd = acceptedRows.reduce((sum, r) => {
      const raw = r as { reimbursement_amount?: unknown; claim_amount?: unknown };
      const reimb = Number(raw.reimbursement_amount);
      if (Number.isFinite(reimb) && reimb > 0) return sum + reimb;
      return sum + Number(raw.claim_amount ?? 0);
    }, 0);

    const finished = rows.filter((r) => r.status === "accepted" || r.status === "rejected" || r.status === "failed");
    const accepted = finished.filter((r) => r.status === "accepted").length;
    const denied = finished.filter((r) => r.status === "rejected" || r.status === "failed").length;
    const denom = accepted + denied;
    const successRatePercent = denom > 0 ? Math.round((accepted / denom) * 1000) / 10 : 0;

    const pendingEvidenceCount = rows.filter((r) => r.status === "evidence_requested").length;

    return {
      ok: true,
      data: {
        totalActiveClaims,
        totalClaimValueUsd,
        projectedRecoveryUsd,
        totalRecoveredUsd,
        successRatePercent,
        pendingEvidenceCount,
      },
    };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to load KPIs.",
    };
  }
}

export type ClaimInvestigationPayload = {
  submission: Record<string, unknown>;
  logs: ClaimHistoryLogRow[];
  returnRow: Record<string, unknown> | null;
  preview_url: string | null;
};

async function signedUrlForReport(path: string | null): Promise<string | null> {
  if (!path) return null;
  const { data, error } = await supabaseServer.storage
    .from("claim-reports")
    .createSignedUrl(path, 3600);
  if (error || !data?.signedUrl) return null;
  return data.signedUrl;
}

export async function getClaimHistoryLogsForSubmission(
  submissionId: string,
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; data: ClaimHistoryLogRow[]; error?: string }> {
  try {
    const { data, error } = await supabaseServer
      .from(CLAIM_HISTORY_TABLE)
      .select(CLAIM_HISTORY_SELECT)
      .eq("claim_id", submissionId)
      .eq("organization_id", organizationId)
      .order("created_at", { ascending: true });

    if (error) throw new Error(error.message);

    const logs: ClaimHistoryLogRow[] = (data ?? []).map((row) => mapClaimHistoryLogRow(row as Record<string, unknown>));

    return { ok: true, data: logs };
  } catch (e) {
    return {
      ok: false,
      data: [],
      error: e instanceof Error ? e.message : "Failed to load history.",
    };
  }
}

export async function getClaimInvestigationPayload(
  submissionId: string,
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; data?: ClaimInvestigationPayload; error?: string }> {
  try {
    const { data: sub, error: sErr } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .select("*")
      .eq("id", submissionId)
      .eq("organization_id", organizationId)
      .maybeSingle();

    if (sErr) throw new Error(sErr.message);
    if (!sub) return { ok: false, error: "Submission not found." };

    const subRow = sub as Record<string, unknown>;
    const returnId = subRow[CLAIM_SUBMISSION_RETURN_ID_COLUMN] as string | null | undefined;

    const { data: ret } = returnId
      ? await supabaseServer.from("returns").select("*").eq("id", returnId).maybeSingle()
      : { data: null };

    const { data: logsRaw, error: lErr } = await supabaseServer
      .from(CLAIM_HISTORY_TABLE)
      .select(CLAIM_HISTORY_SELECT)
      .eq("claim_id", submissionId)
      .eq("organization_id", organizationId)
      .order("created_at", { ascending: true });

    if (lErr) throw new Error(lErr.message);

    const logs: ClaimHistoryLogRow[] = (logsRaw ?? []).map((row) =>
      mapClaimHistoryLogRow(row as Record<string, unknown>),
    );

    const preview_url = await signedUrlForReport(subRow.report_url as string | null);

    return {
      ok: true,
      data: {
        submission: subRow,
        logs,
        returnRow: ret ? (ret as Record<string, unknown>) : null,
        preview_url,
      },
    };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to load investigation.",
    };
  }
}

export type SyncMarketplaceLogInput = {
  actor: ClaimHistoryActor;
  message_content: string;
  attachments?: Record<string, unknown>;
  status_at_time: string;
  message_kind?: ClaimHistoryMessageKind;
};

/**
 * Mock / integration hook for an external autonomous agent: append logs, update submission
 * status, last_checked_at, and success_probability from the last meaningful message.
 */
export async function syncMarketplaceStatus(opts: {
  submissionId: string;
  organizationId?: string;
  newStatus: ClaimSubmissionStatus;
  logs: SyncMarketplaceLogInput[];
  /** Used when `logs` is empty or to override probability source text. */
  lastMessageForProbability?: string;
}): Promise<{ ok: boolean; successProbability?: number; error?: string }> {
  const organizationId = opts.organizationId ?? DEFAULT_ORG;

  try {
    const lastText =
      opts.lastMessageForProbability?.trim() ||
      [...opts.logs].reverse().find((l) => l.message_content.trim())?.message_content ||
      "pending";
    const successProbability = estimateClaimSuccessProbability(lastText);

    const { data: exists, error: exErr } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .select("id")
      .eq("id", opts.submissionId)
      .eq("organization_id", organizationId)
      .maybeSingle();
    if (exErr) throw new Error(exErr.message);
    if (!exists) return { ok: false, error: "Submission not found." };

    for (const log of opts.logs) {
      const msg = log.message_content.trim() || "Update";
      const details = {
        ...(log.attachments ?? {}),
        status: log.status_at_time,
        message_kind: log.message_kind ?? null,
        actor_role: log.actor,
      };
      const { error: insErr } = await supabaseServer.from(CLAIM_HISTORY_TABLE).insert({
        organization_id: organizationId,
        claim_id: opts.submissionId,
        action: msg,
        details,
        actor: log.actor.replace(/_/g, " "),
      });
      if (insErr) throw new Error(insErr.message);
    }

    const { error: upErr } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .update({
        status: opts.newStatus,
        last_checked_at: new Date().toISOString(),
        success_probability: successProbability,
      })
      .eq("id", opts.submissionId)
      .eq("organization_id", organizationId);

    if (upErr) throw new Error(upErr.message);

    return { ok: true, successProbability };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "syncMarketplaceStatus failed.",
    };
  }
}

