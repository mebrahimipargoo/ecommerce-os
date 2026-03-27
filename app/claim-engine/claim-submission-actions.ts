"use server";

import { randomUUID } from "node:crypto";

import { supabaseServer } from "../../lib/supabase-server";
import {
  CLAIM_SUBMISSION_RETURN_ID_COLUMN,
  CLAIM_SUBMISSIONS_TABLE,
  CLAIM_SUBMISSIONS_WITH_RETURNS_EMBED,
} from "./claim-submissions-constants";

const DEFAULT_ORG = "00000000-0000-0000-0000-000000000001";
const BUCKET = "claim-reports";

export type ClaimSubmissionStatus =
  | "draft"
  | "ready_to_send"
  | "submitted"
  | "evidence_requested"
  | "investigating"
  | "accepted"
  | "rejected";

export type ClaimSubmissionListRow = {
  id: string;
  organization_id: string;
  return_id: string;
  store_id: string | null;
  report_url: string | null;
  status: ClaimSubmissionStatus;
  submission_id: string | null;
  claim_amount: number;
  last_checked_at: string | null;
  success_probability: number | null;
  created_at: string;
  updated_at: string;
  preview_url: string | null;
  item_name: string | null;
  asin: string | null;
  fnsku: string | null;
  sku: string | null;
};

async function signedUrlForPath(path: string | null): Promise<string | null> {
  if (!path) return null;
  const { data, error } = await supabaseServer.storage
    .from(BUCKET)
    .createSignedUrl(path, 3600);
  if (error || !data?.signedUrl) return null;
  return data.signedUrl;
}

/**
 * Force-build submission queue: every `ready_for_claim` return gets a `claim_submissions` row
 * (`ready_to_send`) without PDF/identifier validation. Existing rows are upserted back to `ready_to_send`.
 */
export async function approveClaimSubmission(
  submissionId: string,
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; error?: string }> {
  try {
    const { error } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .update({ status: "accepted" })
      .eq("id", submissionId)
      .eq("organization_id", organizationId);
    if (error) throw new Error(error.message);
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Approve failed." };
  }
}

export async function generateDailyClaimReports(
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; generated: number; error?: string }> {
  try {
    const { data: readyRows, error: rErr } = await supabaseServer
      .from("returns")
      .select("id, organization_id, store_id, estimated_value")
      .eq("organization_id", organizationId)
      .eq("status", "ready_for_claim")
      .is("deleted_at", null);

    if (rErr) throw new Error(rErr.message);

    const rows = readyRows ?? [];
    if (rows.length === 0) return { ok: true, generated: 0 };

    let generated = 0;
    const now = new Date().toISOString();

    for (const ret of rows) {
      const returnId = ret.id as string;
      const orgId = String(ret.organization_id ?? organizationId);
      const storeId = (ret.store_id as string | null) ?? null;
      const ev = ret.estimated_value;
      const n = Number(ev);
      const claimAmount = Number.isFinite(n) && n > 0 ? n : 100;
      const submissionId = `test-${randomUUID().replace(/-/g, "").slice(0, 16)}`;

      const { error: upErr } = await supabaseServer.from(CLAIM_SUBMISSIONS_TABLE).upsert(
        {
          organization_id: orgId,
          [CLAIM_SUBMISSION_RETURN_ID_COLUMN]: returnId,
          store_id: storeId,
          report_url: "force-queue",
          status: "ready_to_send",
          submission_id: submissionId,
          claim_amount: claimAmount,
          updated_at: now,
        },
        { onConflict: "return_id" },
      );

      if (!upErr) generated += 1;
    }

    return { ok: true, generated };
  } catch (e) {
    return {
      ok: false,
      generated: 0,
      error: e instanceof Error ? e.message : "generateDailyClaimReports failed.",
    };
  }
}

function resolveReturnSku(ret: Record<string, unknown> | null): string | null {
  if (!ret) return null;
  const sku = ret.sku;
  if (typeof sku === "string" && sku.trim()) return sku;
  return null;
}

function returnRowFromSubmissionEmbed(sub: Record<string, unknown>): Record<string, unknown> | null {
  const raw = sub.returns;
  if (!raw) return null;
  const r = Array.isArray(raw) ? raw[0] : raw;
  return r as Record<string, unknown>;
}

export async function listClaimSubmissions(
  _organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; data: ClaimSubmissionListRow[]; error?: string }> {
  try {
    /** V16.4.23: Temporarily ignore org filter (RLS/testing) — queue shows all tenants' ready_to_send rows. */
    const { data: subs, error } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .select(CLAIM_SUBMISSIONS_WITH_RETURNS_EMBED)
      .eq("status", "ready_to_send")
      .order("created_at", { ascending: false })
      .limit(200);

    console.log("Fetched Claims:", subs, error);
    if (error) throw new Error(error.message);
    const list = subs ?? [];

    const rows: ClaimSubmissionListRow[] = [];
    for (const raw of list) {
      const r = raw as Record<string, unknown>;
      const path = r.report_url as string | null;
      const preview_url = await signedUrlForPath(path);
      const rid = r[CLAIM_SUBMISSION_RETURN_ID_COLUMN] as string;
      const ret = returnRowFromSubmissionEmbed(r);
      rows.push({
        id: r.id as string,
        organization_id: r.organization_id as string,
        return_id: rid,
        store_id: (r.store_id as string | null) ?? null,
        report_url: path,
        status: r.status as ClaimSubmissionStatus,
        submission_id: (r.submission_id as string | null) ?? null,
        claim_amount: Number(r.claim_amount ?? 0) || 0,
        last_checked_at: (r.last_checked_at as string | null) ?? null,
        success_probability:
          r.success_probability === null || r.success_probability === undefined
            ? null
            : Number(r.success_probability),
        created_at: r.created_at as string,
        updated_at: r.updated_at as string,
        preview_url,
        item_name: (ret?.item_name as string) ?? null,
        asin: (ret?.asin as string | null) ?? null,
        fnsku: (ret?.fnsku as string | null) ?? null,
        sku: resolveReturnSku(ret),
      });
    }

    return { ok: true, data: rows };
  } catch (e) {
    return {
      ok: false,
      data: [],
      error: e instanceof Error ? e.message : "Failed to list claim submissions.",
    };
  }
}

export async function refreshClaimReportSignedUrl(
  reportPath: string | null,
): Promise<{ ok: boolean; url?: string | null; error?: string }> {
  try {
    const url = await signedUrlForPath(reportPath);
    return { ok: true, url };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Signed URL failed." };
  }
}

export async function markClaimSubmissionManualSubmit(
  submissionId: string,
  marketplaceCaseId: string,
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; error?: string }> {
  const id = marketplaceCaseId.trim();
  if (!id) return { ok: false, error: "Marketplace case ID is required." };
  try {
    const { error } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .update({
        status: "submitted",
        submission_id: id,
      })
      .eq("id", submissionId)
      .eq("organization_id", organizationId);
    if (error) throw new Error(error.message);
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

/**
 * Agent command: mark submissions as filed. Defaults to all `ready_to_send` when no IDs passed.
 * Writes one `claim_history_logs` row per submission.
 */
export async function bulkSubmitClaimsToMarketplace(
  organizationId: string = DEFAULT_ORG,
  selectedSubmissionIds?: string[] | null,
): Promise<{ ok: boolean; count?: number; error?: string }> {
  const HISTORY_TABLE = "claim_history_logs";
  try {
    let targetIds: string[] = [];

    if (selectedSubmissionIds && selectedSubmissionIds.length > 0) {
      const { data, error } = await supabaseServer
        .from(CLAIM_SUBMISSIONS_TABLE)
        .select("id")
        .eq("organization_id", organizationId)
        .in("id", selectedSubmissionIds)
        .eq("status", "ready_to_send");
      if (error) throw new Error(error.message);
      targetIds = (data ?? []).map((r) => r.id as string);
    } else {
      const { data, error } = await supabaseServer
        .from(CLAIM_SUBMISSIONS_TABLE)
        .select("id")
        .eq("organization_id", organizationId)
        .eq("status", "ready_to_send");
      if (error) throw new Error(error.message);
      targetIds = (data ?? []).map((r) => r.id as string);
    }

    if (targetIds.length === 0) {
      return { ok: false, error: "No ready_to_send submissions to submit." };
    }

    const { error: upErr } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .update({ status: "submitted" })
      .in("id", targetIds)
      .eq("organization_id", organizationId);
    if (upErr) throw new Error(upErr.message);

    const msg = "Batch submission initiated by Admin.";
    const logRows = targetIds.map((submission_id) => ({
      organization_id: organizationId,
      submission_id,
      actor: "human_admin" as const,
      message_content: msg,
      attachments: {},
      status_at_time: "submitted",
      message_kind: "system",
    }));

    const { error: logErr } = await supabaseServer.from(HISTORY_TABLE).insert(logRows);
    if (logErr) throw new Error(logErr.message);

    return { ok: true, count: targetIds.length };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Bulk submit failed.",
    };
  }
}
