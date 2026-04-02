"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { isUuidString } from "../../lib/uuid";

export type ClaimTimelineRow = {
  id: string;
  claim_id: string;
  action: string;
  details: Record<string, unknown> | null;
  actor: string | null;
  company_id: string | null;
  created_at: string;
  /** Legacy status snapshot when present. */
  status_at_time?: string | null;
};

function asDetails(raw: unknown): Record<string, unknown> | null {
  if (raw == null) return null;
  if (typeof raw === "object" && !Array.isArray(raw)) return raw as Record<string, unknown>;
  return null;
}

/**
 * Loads timeline rows for a claim (submission id). Filters by `company_id` (text)
 * and `claim_id` / `submission_id` match.
 */
export async function getClaimTimelineLogs(
  claimId: string,
  companyId: string,
): Promise<{ ok: true; rows: ClaimTimelineRow[] } | { ok: false; error: string }> {
  if (!isUuidString(claimId)) return { ok: false, error: "Invalid claim id." };
  const cid = companyId.trim();
  if (!cid) return { ok: false, error: "company_id is required." };

  try {
    const selectCols =
      "id, claim_id, submission_id, action, details, actor_label, company_id, created_at, status_at_time, message_content, attachments, actor";

    const orFilter = `claim_id.eq.${claimId},submission_id.eq.${claimId}`;

    const { data, error } = await supabaseServer
      .from("claim_history_logs")
      .select(selectCols)
      .eq("company_id", cid)
      .or(orFilter)
      .order("created_at", { ascending: true });

    if (error) throw new Error(error.message);

    const rows: ClaimTimelineRow[] = (data ?? []).map((row) => {
      const r = row as Record<string, unknown>;
      const action = (r.action as string | null) ?? (r.message_content as string) ?? "";
      const details =
        asDetails(r.details) ??
        asDetails(r.attachments) ??
        null;
      const actor =
        (r.actor_label as string | null)?.trim() ||
        (r.actor != null ? String(r.actor) : null) ||
        null;
      const claimFk = (r.claim_id as string | null) ?? (r.submission_id as string) ?? claimId;
      return {
        id: String(r.id),
        claim_id: claimFk,
        action,
        details,
        actor,
        company_id: (r.company_id as string | null) ?? cid,
        created_at: String(r.created_at ?? ""),
        status_at_time: r.status_at_time != null ? String(r.status_at_time) : null,
      };
    });

    return { ok: true, rows };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to load claim history.",
    };
  }
}

export async function resolveProfileDisplayName(profileId: string | null | undefined): Promise<string> {
  const id = profileId?.trim();
  if (!id || !isUuidString(id)) return "User";
  const { data, error } = await supabaseServer
    .from("profiles")
    .select("full_name, name")
    .eq("id", id)
    .maybeSingle();
  if (error || !data) return "User";
  const row = data as { full_name?: string | null; name?: string | null };
  const n = (row.full_name ?? row.name ?? "").trim();
  return n || "User";
}

/**
 * Appends a timeline row. Sets legacy columns so existing RLS policies and readers keep working.
 */
export async function appendClaimHistoryTimelineEntry(input: {
  claimId: string;
  companyId: string;
  action: string;
  details?: Record<string, unknown> | null;
  statusAtTime: string;
  actorLabel: string;
  actorEnum?: "human_admin" | "agent" | "marketplace_bot";
}): Promise<{ ok: boolean; error?: string }> {
  if (!isUuidString(input.claimId)) return { ok: false, error: "Invalid claim id." };
  const companyId = input.companyId.trim();
  if (!companyId) return { ok: false, error: "company_id is required." };

  const details = input.details ?? {};
  const action = input.action.trim() || "Update";
  const actorLabel = input.actorLabel.trim() || "User";
  const actorEnum = input.actorEnum ?? "human_admin";

  try {
    const { error } = await supabaseServer.from("claim_history_logs").insert({
      company_id: companyId,
      submission_id: input.claimId,
      claim_id: input.claimId,
      action,
      details,
      actor_label: actorLabel,
      actor: actorEnum,
      message_content: action,
      attachments: details,
      status_at_time: input.statusAtTime,
      message_kind: "system",
    });
    if (error) throw new Error(error.message);
    return { ok: true };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to record claim history.",
    };
  }
}
