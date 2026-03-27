/**
 * Data layer: all Claim Engine reads/writes go through `claim_submissions` (not legacy `claims`).
 * Joins `returns` for ASIN/FNSKU/SKU whenever `return_id` is set.
 */
import { supabaseServer } from "../../lib/supabase-server";
import type { ReturnRecord } from "../returns/actions";
import {
  CLAIM_SUBMISSION_RETURN_ID_COLUMN,
  CLAIM_SUBMISSIONS_TABLE,
  CLAIM_SUBMISSIONS_WITH_RETURNS_EMBED,
} from "./claim-submissions-constants";
import type { ClaimRecord } from "./claim-types";

function normalizeReturnEmbed(raw: unknown): ReturnRecord | null {
  if (!raw) return null;
  const r = raw as Record<string, unknown>;
  const sr = r.stores;
  let stores: ReturnRecord["stores"];
  if (Array.isArray(sr)) {
    const first = sr[0] as { name?: string; platform?: string } | undefined;
    stores =
      first && first.name != null
        ? { name: String(first.name), platform: String(first.platform ?? "") }
        : null;
  } else if (sr && typeof sr === "object" && sr !== null && "name" in sr) {
    const o = sr as { name: string; platform?: string };
    stores = { name: o.name, platform: o.platform ?? "" };
  } else {
    stores = null;
  }
  return { ...r, stores } as ReturnRecord;
}

function resolveSkuFromReturn(ret: ReturnRecord | null): string | null {
  if (!ret) return null;
  const a = ret.sku;
  if (typeof a === "string" && a.trim()) return a;
  return null;
}

/** PostgREST `returns` FK embed on `claim_submissions` (object or one-element array). */
function returnFromSubmissionEmbed(sub: Record<string, unknown>): ReturnRecord | null {
  const raw = sub.returns;
  if (!raw) return null;
  const r = Array.isArray(raw) ? raw[0] : raw;
  return normalizeReturnEmbed(r);
}

/**
 * Claim Engine workspace — marketplace investigation lifecycle only (not `ready_to_send` / pipeline).
 * `pending_amazon` is not a DB enum value; filing-pending rows use `submitted`.
 */
const WORKSPACE_STATUSES = [
  "submitted",
  "evidence_requested",
  "investigating",
  "accepted",
  "rejected",
] as const;

/**
 * Submissions in review / filed states. V16.4.23: org filter removed temporarily so rows are visible under RLS testing.
 */
export async function fetchClaimWorkspaceRows(
  _organizationId: string,
  limit = 200,
): Promise<{ ok: boolean; data: ClaimRecord[]; error?: string }> {
  try {
    const { data: subs, error } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .select(CLAIM_SUBMISSIONS_WITH_RETURNS_EMBED)
      .in("status", [...WORKSPACE_STATUSES])
      .order("updated_at", { ascending: false })
      .limit(limit);

    console.log("Claim workspace submissions:", subs, error);
    if (error) throw new Error(error.message);
    const list = subs ?? [];

    const rows: ClaimRecord[] = list.map((raw) => {
      const sub = raw as Record<string, unknown>;
      const ret = returnFromSubmissionEmbed(sub);
      return mapSubmissionToClaimRecord(sub, ret);
    });

    return { ok: true, data: rows };
  } catch (e) {
    return {
      ok: false,
      data: [],
      error: e instanceof Error ? e.message : "Failed to load claim workspace.",
    };
  }
}

/** Maps `claim_submissions` + optional `returns` row + source_payload → UI `ClaimRecord`. */
export function mapSubmissionToClaimRecord(
  sub: Record<string, unknown>,
  ret: ReturnRecord | null,
): ClaimRecord {
  const payload = (sub.source_payload as Record<string, unknown>) ?? {};
  const amount = Number(sub.claim_amount ?? payload.amount ?? 0) || 0;
  const reimbursementRaw = sub.reimbursement_amount;
  const reimbursement_amount =
    reimbursementRaw === null || reimbursementRaw === undefined
      ? null
      : Number(reimbursementRaw) || 0;
  const rid = sub[CLAIM_SUBMISSION_RETURN_ID_COLUMN] as string | null | undefined;

  return {
    id: sub.id as string,
    organization_id: sub.organization_id as string,
    amount,
    reimbursement_amount,
    status: String(sub.status ?? "draft"),
    claim_type: (payload.claim_type as string) ?? null,
    marketplace_provider: (payload.marketplace_provider as string) ?? null,
    created_at: sub.created_at as string,
    amazon_order_id: ret?.order_id ?? (payload.amazon_order_id as string) ?? null,
    return_id: rid ?? null,
    item_name: ret?.item_name ?? (payload.item_name as string) ?? null,
    asin: ret?.asin ?? (payload.asin as string) ?? null,
    fnsku: ret?.fnsku ?? (payload.fnsku as string) ?? null,
    sku: resolveSkuFromReturn(ret) ?? (payload.sku as string) ?? null,
    marketplace_claim_id: (sub.submission_id as string) ?? (payload.marketplace_claim_id as string) ?? null,
    marketplace_link_status: (payload.marketplace_link_status as string) ?? null,
    store_id: (sub.store_id as string) ?? null,
  };
}

/**
 * Lists submissions with returns embedded (PostgREST FK). Falls back to `select` + batch returns fetch.
 */
export async function fetchClaimSubmissionsWithReturns(
  organizationId: string,
  limit = 100,
): Promise<{ ok: boolean; data: ClaimRecord[]; error?: string }> {
  try {
    const { data: subs, error } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .select(CLAIM_SUBMISSIONS_WITH_RETURNS_EMBED)
      .eq("organization_id", organizationId)
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) throw new Error(error.message);
    const list = subs ?? [];

    const rows: ClaimRecord[] = list.map((raw) => {
      const sub = raw as Record<string, unknown>;
      const ret = returnFromSubmissionEmbed(sub);
      return mapSubmissionToClaimRecord(sub, ret);
    });

    return { ok: true, data: rows };
  } catch (e) {
    return {
      ok: false,
      data: [],
      error: e instanceof Error ? e.message : "Failed to load claim submissions.",
    };
  }
}
