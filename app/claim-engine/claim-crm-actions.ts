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
  submission_id: string;
  actor: ClaimHistoryActor;
  message_content: string;
  attachments: Record<string, unknown>;
  status_at_time: string;
  message_kind: ClaimHistoryMessageKind | null;
  created_at: string;
};

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
      .select("*")
      .eq("submission_id", submissionId)
      .eq("organization_id", organizationId)
      .order("created_at", { ascending: true });

    if (error) throw new Error(error.message);

    const logs: ClaimHistoryLogRow[] = (data ?? []).map((row) => {
      const r = row as Record<string, unknown>;
      return {
        id: r.id as string,
        organization_id: r.organization_id as string,
        submission_id: r.submission_id as string,
        actor: r.actor as ClaimHistoryActor,
        message_content: r.message_content as string,
        attachments: (r.attachments as Record<string, unknown>) ?? {},
        status_at_time: r.status_at_time as string,
        message_kind: (r.message_kind as ClaimHistoryMessageKind | null) ?? null,
        created_at: r.created_at as string,
      };
    });

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
      .select("*")
      .eq("submission_id", submissionId)
      .eq("organization_id", organizationId)
      .order("created_at", { ascending: true });

    if (lErr) throw new Error(lErr.message);

    const logs: ClaimHistoryLogRow[] = (logsRaw ?? []).map((row) => {
      const r = row as Record<string, unknown>;
      return {
        id: r.id as string,
        organization_id: r.organization_id as string,
        submission_id: r.submission_id as string,
        actor: r.actor as ClaimHistoryActor,
        message_content: r.message_content as string,
        attachments: (r.attachments as Record<string, unknown>) ?? {},
        status_at_time: r.status_at_time as string,
        message_kind: (r.message_kind as ClaimHistoryMessageKind | null) ?? null,
        created_at: r.created_at as string,
      };
    });

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
      const { error: insErr } = await supabaseServer.from(CLAIM_HISTORY_TABLE).insert({
        organization_id: organizationId,
        submission_id: opts.submissionId,
        actor: log.actor,
        message_content: log.message_content,
        attachments: log.attachments ?? {},
        status_at_time: log.status_at_time,
        message_kind: log.message_kind ?? null,
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

