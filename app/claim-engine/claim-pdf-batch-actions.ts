"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { getClaimDetail, type ClaimDetailPayload } from "./claim-actions";
import { CLAIM_SUBMISSIONS_TABLE } from "./claim-submissions-constants";

export type ClaimPdfPagePayload = {
  storeName: string;
  storePlatform: string;
  detail: ClaimDetailPayload;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
};

function storeLabel(platform: string | null | undefined): { name: string; platform: string } {
  const p = (platform ?? "").toLowerCase();
  if (p === "amazon") return { name: "Amazon", platform: "amazon" };
  if (p === "walmart") return { name: "Walmart", platform: "walmart" };
  if (p === "ebay") return { name: "eBay", platform: "ebay" };
  return { name: "Store", platform: p || "custom" };
}

/**
 * Builds pages for @react-pdf preview/download.
 * - With `selectedSubmissionIds`: batch report for those submissions (any status).
 * - With null/empty: master report = all `ready_to_send` rows for the org.
 */
export async function prepareClaimEnginePdfPages(
  organizationId: string,
  selectedSubmissionIds: string[] | null,
): Promise<{
  ok: boolean;
  pages?: ClaimPdfPagePayload[];
  mode?: "batch" | "master";
  /** Rows matched from `claim_submissions` (DB). */
  totalQueried?: number;
  /** Pages successfully built (detail load OK). */
  pagesBuilt?: number;
  error?: string;
}> {
  try {
    let ids: string[] = [];

    if (selectedSubmissionIds && selectedSubmissionIds.length > 0) {
      const { data, error } = await supabaseServer
        .from(CLAIM_SUBMISSIONS_TABLE)
        .select("id")
        .eq("company_id", organizationId)
        .in("id", selectedSubmissionIds);
      if (error) throw new Error(error.message);
      ids = (data ?? []).map((r) => r.id as string);
      if (ids.length === 0) return { ok: false, error: "No matching submissions for the selected rows." };
    } else {
      const { data, error } = await supabaseServer
        .from(CLAIM_SUBMISSIONS_TABLE)
        .select("id")
        .eq("company_id", organizationId)
        .eq("status", "ready_to_send")
        .order("created_at", { ascending: true });
      if (error) throw new Error(error.message);
      ids = (data ?? []).map((r) => r.id as string);
      if (ids.length === 0) {
        return { ok: false, error: "No ready_to_send submissions. Select rows in the queue or workspace, or enqueue pipeline PDFs first." };
      }
    }

    const totalQueried = ids.length;
    const pages: ClaimPdfPagePayload[] = [];
    for (const id of ids) {
      const res = await getClaimDetail(id, organizationId);
      if (!res.ok || !res.data) continue;
      const detail = res.data;
      const st = detail.returnRow?.stores;
      const lbl = storeLabel(st?.platform);
      pages.push({
        storeName: st?.name?.trim() || lbl.name,
        storePlatform: st?.platform?.trim() || lbl.platform,
        detail,
        claimAmountNote: String(detail.claim.amount ?? ""),
        marketplaceClaimIdNote: detail.claim.marketplace_claim_id ?? undefined,
      });
    }

    const pagesBuilt = pages.length;
    if (pagesBuilt === 0) {
      return {
        ok: false,
        totalQueried,
        pagesBuilt: 0,
        error: `Matched ${totalQueried} submission(s) in the database, but claim details could not be loaded for PDF (check return links and org).`,
      };
    }

    return {
      ok: true,
      pages,
      totalQueried,
      pagesBuilt,
      mode: selectedSubmissionIds && selectedSubmissionIds.length > 0 ? "batch" : "master",
    };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to prepare PDF.",
    };
  }
}
