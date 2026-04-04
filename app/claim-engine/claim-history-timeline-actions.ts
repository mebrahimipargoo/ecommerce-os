"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { isUuidString } from "../../lib/uuid";

export type ClaimTimelineRow = {
  id: string;
  claim_id: string;
  action: string;
  details: Record<string, unknown> | null;
  actor: string | null;
  organization_id: string | null;
  created_at: string;
  /** Status snapshot — read from `details.status` (or legacy `details.status_at_time`). */
  status_at_time?: string | null;
};

function asDetails(raw: unknown): Record<string, unknown> | null {
  if (raw == null) return null;
  if (typeof raw === "object" && !Array.isArray(raw)) return raw as Record<string, unknown>;
  return null;
}

function statusFromDetails(details: Record<string, unknown> | null): string | null {
  if (!details) return null;
  const s = details.status ?? details.status_at_time;
  if (typeof s === "string" && s.trim()) return s.trim();
  return null;
}

/** Map DB `actor` (text or legacy enum-like strings) to a short display label. */
function formatActorDisplay(raw: unknown): string | null {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  if (s.includes("@")) return s;
  const lower = s.toLowerCase();
  if (lower === "human_admin") return "Human admin";
  if (lower === "marketplace_bot") return "Marketplace bot";
  if (lower === "agent") return "Agent";
  if (lower === "system") return "System";
  return s.replace(/_/g, " ");
}

/**
 * Loads timeline rows for a claim (submission id). Filters by `organization_id` and `claim_id`.
 */
export async function getClaimTimelineLogs(
  claimId: string,
  organizationId: string,
): Promise<{ ok: true; rows: ClaimTimelineRow[] } | { ok: false; error: string }> {
  if (!isUuidString(claimId)) return { ok: false, error: "Invalid claim id." };
  const cid = organizationId.trim();
  if (!cid) return { ok: false, error: "organization_id is required." };

  try {
    const selectCols = "id, claim_id, action, details, actor, organization_id, created_at";

    const { data, error } = await supabaseServer
      .from("claim_history_logs")
      .select(selectCols)
      .eq("organization_id", cid)
      .eq("claim_id", claimId)
      .order("created_at", { ascending: true });

    if (error) throw new Error(error.message);

    const rows: ClaimTimelineRow[] = (data ?? []).map((row) => {
      const r = row as Record<string, unknown>;
      const details = asDetails(r.details);
      const action = (r.action as string | null)?.trim() || "";
      const actor = formatActorDisplay(r.actor);
      const claimFk = (r.claim_id as string | null) ?? claimId;
      return {
        id: String(r.id),
        claim_id: claimFk,
        action,
        details,
        actor,
        organization_id: (r.organization_id as string | null) ?? cid,
        created_at: String(r.created_at ?? ""),
        status_at_time: statusFromDetails(details),
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
 * Appends a timeline row. Persists status and kind inside `details` JSONB (no extra table columns).
 */
export async function appendClaimHistoryTimelineEntry(input: {
  claimId: string;
  organizationId: string;
  action: string;
  details?: Record<string, unknown> | null;
  statusAtTime: string;
  actorLabel: string;
}): Promise<{ ok: boolean; error?: string }> {
  if (!isUuidString(input.claimId)) return { ok: false, error: "Invalid claim id." };
  const orgId = input.organizationId.trim();
  if (!orgId) return { ok: false, error: "organization_id is required." };

  const base = input.details ?? {};
  const action = input.action.trim() || "Update";
  const actorLabel = input.actorLabel.trim() || "User";
  const details: Record<string, unknown> = {
    ...base,
    status: input.statusAtTime,
    message_kind: "system",
    actor_role: "human_admin",
  };

  try {
    const { error } = await supabaseServer.from("claim_history_logs").insert({
      organization_id: orgId,
      claim_id: input.claimId,
      action,
      details,
      actor: actorLabel,
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
