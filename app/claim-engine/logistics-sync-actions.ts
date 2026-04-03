"use server";

import { supabaseServer } from "../../lib/supabase-server";
import {
  shouldAutoEnqueueAmazonClaimSubmission,
  storePlatformFromEmbed,
} from "../returns/claim-queue-helpers";
import { generateDailyClaimReports } from "./claim-submission-actions";
import { CLAIM_SUBMISSION_RETURN_ID_COLUMN, CLAIM_SUBMISSIONS_TABLE } from "./claim-submissions-constants";

const DEFAULT_ORG = "00000000-0000-0000-0000-000000000001";

/**
 * Returns counts for Settings "Logistics AI Agent" sync UI:
 * - `pendingSyncCount`: Amazon `ready_for_claim` returns not yet represented as `ready_to_send` in `claim_submissions`.
 */
export async function getClaimQueueSyncStatus(
  organizationId: string = DEFAULT_ORG,
): Promise<{
  ok: boolean;
  readyForClaimCount?: number;
  pendingSyncCount?: number;
  systemUpToDate?: boolean;
  error?: string;
}> {
  try {
    const { data: retRows, error: rErr } = await supabaseServer
      .from("returns")
      .select("id, marketplace, conditions, stores(platform)")
      .eq("organization_id", organizationId)
      .eq("status", "ready_for_claim")
      .is("deleted_at", null)
      .limit(5000);
    if (rErr) throw new Error(rErr.message);
    const amazonReady = (retRows ?? []).filter((row) => {
      const r = row as { marketplace?: string | null; conditions?: string[] | null; stores?: unknown };
      return shouldAutoEnqueueAmazonClaimSubmission(
        r.marketplace,
        r.conditions ?? [],
        storePlatformFromEmbed(r.stores),
      );
    });
    const readyIds = amazonReady.map((r) => (r as { id: string }).id);
    const readyForClaimCount = readyIds.length;
    if (readyForClaimCount === 0) {
      return { ok: true, readyForClaimCount: 0, pendingSyncCount: 0, systemUpToDate: true };
    }

    const { data: subRows, error: sErr } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .select(CLAIM_SUBMISSION_RETURN_ID_COLUMN)
      .eq("organization_id", organizationId)
      .eq("status", "ready_to_send")
      .in(CLAIM_SUBMISSION_RETURN_ID_COLUMN, readyIds);
    if (sErr) throw new Error(sErr.message);

    const synced = new Set(
      (subRows ?? [])
        .map((r) => r[CLAIM_SUBMISSION_RETURN_ID_COLUMN] as string | null)
        .filter(Boolean),
    );
    const pendingSyncCount = readyIds.filter((id) => !synced.has(id)).length;
    return {
      ok: true,
      readyForClaimCount,
      pendingSyncCount,
      systemUpToDate: pendingSyncCount === 0,
    };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Sync status failed.",
    };
  }
}

/** Enqueues / upserts `claim_submissions` for all `ready_for_claim` returns (same as pipeline builder). */
export async function syncClaimQueueNow(
  organizationId: string = DEFAULT_ORG,
): Promise<{ ok: boolean; generated?: number; error?: string }> {
  return generateDailyClaimReports(organizationId);
}
