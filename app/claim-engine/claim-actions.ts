"use server";

import { supabaseServer } from "../../lib/supabase-server";
import type { PackageRecord, PalletRecord, ReturnRecord } from "../returns/actions";
import { listPackages, listPallets } from "../returns/actions";
import { RETURN_SELECT } from "../returns/returns-constants";
import { fetchClaimWorkspaceRows, mapSubmissionToClaimRecord } from "./claim-repository";
import { resolveInitialClaimAmountUsd, resolveClaimAmountFromReturnSync } from "./claim-amount-utils";
import { ClaimObject } from "./claim-object";
import { CLAIM_SUBMISSION_RETURN_ID_COLUMN, CLAIM_SUBMISSIONS_TABLE } from "./claim-submissions-constants";
import type { ClaimRecord } from "./claim-types";

export type { ClaimRecord } from "./claim-types";

const DEFAULT_ORG = "00000000-0000-0000-0000-000000000001";

/** PostgREST may embed `stores` as an object or a one-element array — normalize for `ReturnRecord`. */
function normalizeReturnRow(raw: unknown): ReturnRecord {
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

function mapBulkUiStatusToSubmission(status: string): string {
  if (status === "cancelled") return "rejected";
  return status;
}

export async function bulkUpdateClaimsStatus(
  ids: string[],
  status: string,
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; error?: string }> {
  if (ids.length === 0) return { ok: true };
  try {
    const dbStatus = mapBulkUiStatusToSubmission(status);
    const { error } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .update({ status: dbStatus })
      .in("id", ids)
      .eq("organization_id", organizationId);
    if (error) throw new Error(error.message);
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Bulk update failed." };
  }
}

export async function updateClaimFields(
  id: string,
  patch: {
    marketplace_claim_id?: string | null;
    marketplace_link_status?: string | null;
    amount?: number | null;
    reimbursement_amount?: number | null;
  },
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; error?: string }> {
  try {
    const { data: row, error: fetchErr } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .select("source_payload")
      .eq("id", id)
      .eq("organization_id", organizationId)
      .maybeSingle();
    if (fetchErr) throw new Error(fetchErr.message);
    if (!row) return { ok: false, error: "Claim not found." };

    const sp = { ...((row.source_payload as Record<string, unknown>) ?? {}) };
    if (patch.marketplace_link_status !== undefined) {
      sp.marketplace_link_status = patch.marketplace_link_status;
    }

    const updateRow: Record<string, unknown> = {
      source_payload: sp,
    };
    if (patch.amount !== undefined) updateRow.claim_amount = patch.amount;
    if (patch.reimbursement_amount !== undefined) updateRow.reimbursement_amount = patch.reimbursement_amount;
    if (patch.marketplace_claim_id !== undefined) updateRow.submission_id = patch.marketplace_claim_id;

    const { error } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .update(updateRow)
      .eq("id", id)
      .eq("organization_id", organizationId);
    if (error) throw new Error(error.message);
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

export type ClaimDetailPayload = {
  claim: ClaimRecord;
  returnRow: ReturnRecord | null;
  pallet: PalletRecord | null;
  packageRow: PackageRecord | null;
};

export async function getClaimDetail(
  claimId: string,
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; data?: ClaimDetailPayload; error?: string }> {
  try {
    const { data: subRaw, error: cErr } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .select("*")
      .eq("id", claimId)
      .eq("organization_id", organizationId)
      .single();
    if (cErr) throw new Error(cErr.message);
    const sub = subRaw as Record<string, unknown>;

    let returnRow: ReturnRecord | null = null;
    const rid = sub[CLAIM_SUBMISSION_RETURN_ID_COLUMN] as string | null | undefined;
    if (rid) {
      const { data: ret, error: rErr } = await supabaseServer
        .from("returns")
        .select(RETURN_SELECT)
        .eq("id", rid)
        .maybeSingle();
      if (!rErr && ret) returnRow = normalizeReturnRow(ret);
    }

    const claim = mapSubmissionToClaimRecord(sub, returnRow);

    const ret = returnRow;
    const palletId = ret?.pallet_id ?? null;
    const packageId = ret?.package_id ?? null;

    let pallet: PalletRecord | null = null;
    let pkg: PackageRecord | null = null;
    if (palletId) {
      const pr = await listPallets(organizationId);
      if (pr.ok) pallet = pr.data.find((p) => p.id === palletId) ?? null;
    }
    if (packageId) {
      const pk = await listPackages(organizationId);
      if (pk.ok) pkg = pk.data.find((p) => p.id === packageId) ?? null;
    }

    return { ok: true, data: { claim, returnRow: ret, pallet, packageRow: pkg } };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load claim." };
  }
}

function mapStorePlatformToClaimProvider(platform: string | undefined | null): string | null {
  if (!platform) return null;
  const p = platform.toLowerCase();
  if (p === "amazon") return "amazon_sp_api";
  if (p === "walmart") return "walmart_api";
  if (p === "ebay") return "ebay_api";
  return null;
}

/**
 * Builds claim PDF payload from a return row: prefers a synced `claims` row linked by
 * `return_id`; otherwise synthesizes a minimal claim record for pipeline-only items.
 */
export async function getClaimDetailForReturn(
  returnId: string,
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; data?: ClaimDetailPayload; error?: string }> {
  try {
    const { data: retRaw, error: rErr } = await supabaseServer
      .from("returns")
      .select(RETURN_SELECT)
      .eq("id", returnId)
      .eq("organization_id", organizationId)
      .maybeSingle();
    if (rErr) throw new Error(rErr.message);
    if (!retRaw) return { ok: false, error: "Return not found." };
    const returnRow = normalizeReturnRow(retRaw);

    const { data: subRow } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .select("*")
      .eq(CLAIM_SUBMISSION_RETURN_ID_COLUMN, returnId)
      .eq("organization_id", organizationId)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (subRow) {
      return getClaimDetail((subRow as Record<string, unknown>).id as string, organizationId);
    }

    const palletId = returnRow.pallet_id ?? null;
    const packageId = returnRow.package_id ?? null;
    let pallet: PalletRecord | null = null;
    let pkg: PackageRecord | null = null;
    if (palletId) {
      const pr = await listPallets(organizationId);
      if (pr.ok) pallet = pr.data.find((p) => p.id === palletId) ?? null;
    }
    if (packageId) {
      const pk = await listPackages(organizationId);
      if (pk.ok) pkg = pk.data.find((p) => p.id === packageId) ?? null;
    }

    const claimObj = ClaimObject.fromReadyReturn(returnRow, pallet, pkg);
    const syntheticClaim = claimObj.toSyntheticClaimRecord(organizationId);
    syntheticClaim.marketplace_provider = mapStorePlatformToClaimProvider(returnRow.stores?.platform);
    const amount = await resolveInitialClaimAmountUsd(
      supabaseServer,
      returnRow,
      resolveClaimAmountFromReturnSync(returnRow),
    );
    syntheticClaim.amount = amount;

    return {
      ok: true,
      data: {
        claim: syntheticClaim,
        returnRow,
        pallet,
        packageRow: pkg,
      },
    };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to build claim detail." };
  }
}

/** Claim Engine workspace table: submitted / evidence / investigating claims for this tenant only. */
export async function listClaimRowsForClaimEngine(
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; data: ClaimRecord[]; error?: string }> {
  return fetchClaimWorkspaceRows(organizationId);
}

export async function getBulkClaimDetails(
  ids: string[],
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; data: ClaimDetailPayload[]; error?: string }> {
  try {
    const data: ClaimDetailPayload[] = [];
    for (const id of ids) {
      const r = await getClaimDetail(id, organizationId);
      if (r.ok && r.data) data.push(r.data);
    }
    return { ok: true, data };
  } catch (e) {
    return {
      ok: false,
      data: [],
      error: e instanceof Error ? e.message : "Bulk load failed.",
    };
  }
}
