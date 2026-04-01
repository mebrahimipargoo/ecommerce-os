"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { resolveWriteOrganizationId } from "../../lib/server-tenant";
import { isUuidString } from "../../lib/uuid";
import { guessLedgerSnapshotDate } from "../../lib/csv-parse-basic";

export { guessLedgerSnapshotDate };

const CHUNK = 500;

export type AmazonLedgerStagingRow = {
  id: string;
  organization_id: string;
  snapshot_date: string | null;
  raw_row: Record<string, unknown>;
  created_at: string;
};

/**
 * Deletes staging rows older than retention window for this tenant.
 * Uses snapshot_date when present, else created_at::date (Neda: always scope by organization_id).
 */
export async function purgeAmazonLedgerStagingRetention(payload: {
  actorProfileId: string | null | undefined;
  requestedOrganizationId?: string | null;
  retentionDays: number;
}): Promise<{ ok: boolean; error?: string }> {
  try {
    const orgId = await resolveWriteOrganizationId(
      payload.actorProfileId,
      payload.requestedOrganizationId,
    );
    if (!isUuidString(orgId)) throw new Error("Invalid organization.");

    const days = Math.max(1, Math.min(3650, Math.floor(payload.retentionDays || 60)));
    const cutoff = new Date();
    cutoff.setUTCDate(cutoff.getUTCDate() - days);
    cutoff.setUTCHours(0, 0, 0, 0);
    const cutoffStr = cutoff.toISOString().slice(0, 10);
    const cutoffIso = cutoff.toISOString();

    const { error: e1 } = await supabaseServer
      .from("amazon_ledger_staging")
      .delete()
      .eq("organization_id", orgId)
      .not("snapshot_date", "is", null)
      .lt("snapshot_date", cutoffStr);
    if (e1) throw new Error(e1.message);

    const { error: e2 } = await supabaseServer
      .from("amazon_ledger_staging")
      .delete()
      .eq("organization_id", orgId)
      .is("snapshot_date", null)
      .lt("created_at", cutoffIso);
    if (e2) throw new Error(e2.message);

    return { ok: true };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Purge failed.",
    };
  }
}

/**
 * Deletes existing staging rows for the given organization whose snapshot_date
 * falls within [startDate, endDate] (inclusive, ISO YYYY-MM-DD strings).
 * Call this before an upload to prevent duplicate rows for the same period.
 */
export async function deleteAmazonLedgerStagingByDateRange(payload: {
  actorProfileId: string | null | undefined;
  requestedOrganizationId?: string | null;
  startDate: string;
  endDate: string;
}): Promise<{ ok: boolean; deleted: number; error?: string }> {
  try {
    const orgId = await resolveWriteOrganizationId(
      payload.actorProfileId,
      payload.requestedOrganizationId,
    );
    if (!isUuidString(orgId)) throw new Error("Invalid organization.");

    const start = payload.startDate.slice(0, 10);
    const end = payload.endDate.slice(0, 10);
    if (!start || !end || start > end) throw new Error("Invalid date range.");

    const { error, count } = await supabaseServer
      .from("amazon_ledger_staging")
      .delete({ count: "exact" })
      .eq("organization_id", orgId)
      .gte("snapshot_date", start)
      .lte("snapshot_date", end);

    if (error) throw new Error(error.message);
    return { ok: true, deleted: count ?? 0 };
  } catch (e) {
    return {
      ok: false,
      deleted: 0,
      error: e instanceof Error ? e.message : "Delete by date range failed.",
    };
  }
}

export async function insertAmazonLedgerStagingBatch(payload: {
  actorProfileId: string | null | undefined;
  requestedOrganizationId?: string | null;
  rows: Record<string, string>[];
}): Promise<{ ok: boolean; inserted: number; error?: string }> {
  try {
    const orgId = await resolveWriteOrganizationId(
      payload.actorProfileId,
      payload.requestedOrganizationId,
    );
    if (!isUuidString(orgId)) throw new Error("Invalid organization.");

    if (payload.rows.length > CHUNK) {
      return {
        ok: false,
        inserted: 0,
        error: `Each batch may have at most ${CHUNK} rows.`,
      };
    }
    const slice = payload.rows;
    if (slice.length === 0) return { ok: true, inserted: 0 };

    const insertRows = slice.map((r) => ({
      organization_id: orgId,
      snapshot_date: guessLedgerSnapshotDate(r),
      raw_row: r as unknown as Record<string, unknown>,
    }));

    const { error } = await supabaseServer.from("amazon_ledger_staging").insert(insertRows);
    if (error) throw new Error(error.message);
    return { ok: true, inserted: insertRows.length };
  } catch (e) {
    return {
      ok: false,
      inserted: 0,
      error: e instanceof Error ? e.message : "Insert failed.",
    };
  }
}

/**
 * Removes a raw CSV file that was previously uploaded to the `raw-reports` bucket
 * during an Amazon ledger import. Scoped to super admins only.
 */
export async function deleteAmazonLedgerStorageFile(payload: {
  actorProfileId: string | null | undefined;
  storagePath: string;
}): Promise<{ ok: boolean; error?: string }> {
  try {
    if (!payload.actorProfileId) throw new Error("Not authenticated.");
    if (!payload.storagePath?.trim()) throw new Error("No storage path provided.");

    const { error } = await supabaseServer.storage
      .from("raw-reports")
      .remove([payload.storagePath]);

    if (error) throw new Error(error.message);
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Storage delete failed." };
  }
}

/** All ledger reads must filter by organization_id (tenant isolation). */
export async function listAmazonLedgerStaging(payload: {
  actorProfileId: string | null | undefined;
  requestedOrganizationId?: string | null;
  limit?: number;
}): Promise<{ ok: boolean; data: AmazonLedgerStagingRow[]; error?: string }> {
  try {
    const orgId = await resolveWriteOrganizationId(
      payload.actorProfileId,
      payload.requestedOrganizationId,
    );
    if (!isUuidString(orgId)) throw new Error("Invalid organization.");

    const lim = Math.min(10_000, Math.max(1, payload.limit ?? 500));
    const { data, error } = await supabaseServer
      .from("amazon_ledger_staging")
      .select("id, organization_id, snapshot_date, raw_row, created_at")
      .eq("organization_id", orgId)
      .order("created_at", { ascending: false })
      .limit(lim);
    if (error) throw new Error(error.message);
    return { ok: true, data: (data ?? []) as AmazonLedgerStagingRow[] };
  } catch (e) {
    return {
      ok: false,
      data: [],
      error: e instanceof Error ? e.message : "Failed to load ledger staging.",
    };
  }
}
