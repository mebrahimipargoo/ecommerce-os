"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { resolveOrganizationId } from "../../lib/organization";
import { isUuidString } from "../../lib/uuid";
import { CLAIM_SUBMISSIONS_TABLE, CLAIM_SUBMISSIONS_WITH_RETURNS_EMBED } from "./claim-submissions-constants";

export type ClaimReportHistoryStatusLabel =
  | "Generated"
  | "Submitted"
  | "Denied"
  | "Generating..."
  | "Failed";

export type ClaimReportHistoryRow = {
  id: string;
  report_name: string;
  /** Same source as Claim Engine: `source_payload.claim_type` (adapter / case type). */
  claim_type: string | null;
  /** Resolved from `profiles` when `created_by` is a UUID; else operator text. */
  generated_by: string | null;
  created_at: string;
  report_url: string | null;
  status_label: ClaimReportHistoryStatusLabel;
  raw_status: string;
};

function returnFromSubmissionEmbed(sub: Record<string, unknown>): Record<string, unknown> | null {
  const raw = sub.returns;
  if (!raw) return null;
  return (Array.isArray(raw) ? raw[0] : raw) as Record<string, unknown>;
}

function claimTypeFromRow(sub: Record<string, unknown>, ret: Record<string, unknown> | null): string | null {
  const payload = (sub.source_payload as Record<string, unknown>) ?? {};
  const fromPayload = payload.claim_type;
  if (typeof fromPayload === "string" && fromPayload.trim()) return fromPayload.trim();
  const cond = ret?.conditions;
  if (Array.isArray(cond) && cond.length > 0) {
    const first = cond[0];
    if (typeof first === "string" && first.trim()) return first.trim();
  }
  return null;
}

function resolveStatusLabel(
  reportUrl: string | null,
  status: string,
): ClaimReportHistoryStatusLabel {
  const hasUrl = Boolean(reportUrl?.trim());
  const st = status ?? "";
  /** `"failed"` is always a terminal failure regardless of whether a PDF was generated. */
  if (st === "failed") return "Failed";
  if (!hasUrl) {
    if (st === "rejected") return "Failed";
    return "Generating...";
  }
  if (st === "rejected") return "Denied";
  if (st === "submitted" || st === "evidence_requested" || st === "investigating" || st === "accepted") {
    return "Submitted";
  }
  return "Generated";
}

function buildReportName(sub: Record<string, unknown>, ret: Record<string, unknown> | null): string {
  const orderId = (ret?.order_id as string | null) ?? null;
  const oid = orderId?.trim();
  if (oid) return `Claim PDF for Order #${oid}`;
  return "Claim PDF";
}

async function loadProfileNames(ids: string[]): Promise<Map<string, string>> {
  const uuids = [...new Set(ids.map((x) => x.trim()).filter(isUuidString))];
  if (uuids.length === 0) return new Map();
  const { data, error } = await supabaseServer
    .from("profiles")
    .select("id, full_name, name")
    .in("id", uuids);
  if (error || !data) return new Map();
  const m = new Map<string, string>();
  for (const row of data as { id: string; full_name?: string | null; name?: string | null }[]) {
    const name = (row.full_name ?? row.name ?? "").trim();
    if (row.id && name) m.set(row.id, name);
  }
  return m;
}

export type ListClaimReportHistoryParams = {
  organizationId?: string;
  /** ISO date (inclusive start of day UTC) */
  dateFrom?: string | null;
  /** ISO date (inclusive end of day UTC) */
  dateTo?: string | null;
};

/**
 * Report archive: `claim_submissions` rows that have a stored PDF (`report_url` set).
 * Embedded `returns` for claim “case” type via `source_payload.claim_type` or return conditions.
 * Generator: `profiles.full_name` when `created_by` (submission or return) is a UUID; otherwise the text label.
 */
export async function listClaimReportHistory(
  params: ListClaimReportHistoryParams = {},
): Promise<{ ok: boolean; data: ClaimReportHistoryRow[]; error?: string }> {
  const orgId =
    params.organizationId?.trim() && isUuidString(params.organizationId.trim())
      ? params.organizationId.trim()
      : resolveOrganizationId();
  try {
    let q = supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .select(CLAIM_SUBMISSIONS_WITH_RETURNS_EMBED)
      .eq("organization_id", orgId)
      .not("report_url", "is", null)
      .neq("report_url", "")
      .order("created_at", { ascending: false })
      .limit(500);

    const from = params.dateFrom?.trim();
    const to = params.dateTo?.trim();
    if (from) q = q.gte("created_at", from);
    if (to) q = q.lte("created_at", to);

    const { data: subs, error } = await q;
    if (error) throw new Error(error.message);
    const list = (subs ?? []) as Record<string, unknown>[];

    const creatorCandidates: string[] = [];
    for (const sub of list) {
      const cb = sub.created_by as string | null | undefined;
      if (cb?.trim()) creatorCandidates.push(cb.trim());
      const ret = returnFromSubmissionEmbed(sub);
      const rcb = ret?.created_by as string | null | undefined;
      if (rcb?.trim()) creatorCandidates.push(String(rcb).trim());
    }
    const profileMap = await loadProfileNames(creatorCandidates);

    function resolveGeneratedBy(sub: Record<string, unknown>, ret: Record<string, unknown> | null): string | null {
      const subCb = sub.created_by as string | null | undefined;
      if (subCb && isUuidString(subCb.trim())) {
        const n = profileMap.get(subCb.trim());
        if (n) return n;
      }
      const retCb = ret?.created_by as string | null | undefined;
      if (retCb != null && String(retCb).trim()) {
        const t = String(retCb).trim();
        if (isUuidString(t)) {
          const n = profileMap.get(t);
          if (n) return n;
        }
        return t;
      }
      if (subCb && String(subCb).trim()) return String(subCb).trim();
      return null;
    }

    const rows: ClaimReportHistoryRow[] = [];
    for (const sub of list) {
      const ret = returnFromSubmissionEmbed(sub);
      const st = String(sub.status ?? "draft");
      const reportUrl = (sub.report_url as string | null) ?? null;
      rows.push({
        id: sub.id as string,
        report_name: buildReportName(sub, ret),
        claim_type: claimTypeFromRow(sub, ret),
        generated_by: resolveGeneratedBy(sub, ret),
        created_at: sub.created_at as string,
        report_url: reportUrl,
        status_label: resolveStatusLabel(reportUrl, st),
        raw_status: st,
      });
    }

    return { ok: true, data: rows };
  } catch (e) {
    return {
      ok: false,
      data: [],
      error: e instanceof Error ? e.message : "Failed to load report history.",
    };
  }
}
