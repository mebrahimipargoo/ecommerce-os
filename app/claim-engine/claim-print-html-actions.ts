"use server";

import { supabaseServer } from "../../lib/supabase-server";
import {
  CLAIM_SUBMISSION_RETURN_ID_COLUMN,
  CLAIM_SUBMISSIONS_TABLE,
} from "./claim-submissions-constants";
import { RETURN_SELECT } from "../returns/returns-constants";
import {
  getReturnPhotoEvidenceUrls,
  type ReturnPhotoEvidenceRow,
} from "../../lib/return-photo-evidence";

function resolveSkuFromReturnRow(ret: Record<string, unknown> | null): string | null {
  if (!ret) return null;
  const sku = ret.sku;
  if (typeof sku === "string" && sku.trim()) return sku;
  return null;
}

/** Collect image URLs from `returns` (`photo_evidence` URL slots + optional `photo_urls` JSON/array). */
export async function collectReturnPhotoUrls(
  ret: Record<string, unknown> | null,
): Promise<string[]> {
  if (!ret) return [];
  const urls: string[] = [];
  const add = (u: unknown) => {
    if (typeof u === "string" && u.trim()) urls.push(u.trim());
  };
  const ev = getReturnPhotoEvidenceUrls(ret.photo_evidence as ReturnPhotoEvidenceRow | undefined);
  add(ev.item_url);
  add(ev.expiry_url);
  add(ev.return_label_url);
  const raw = ret.photo_urls;
  if (Array.isArray(raw)) {
    for (const x of raw) add(x);
  } else if (raw && typeof raw === "object" && !Array.isArray(raw)) {
    for (const v of Object.values(raw as Record<string, unknown>)) add(v);
  } else if (typeof raw === "string") {
    try {
      const p = JSON.parse(raw) as unknown;
      if (Array.isArray(p)) for (const x of p) add(x);
    } catch {
      /* ignore */
    }
  }
  return [...new Set(urls)];
}

export type ReadyToSendPrintRow = {
  id: string;
  item_name: string | null;
  asin: string | null;
  fnsku: string | null;
  sku: string | null;
  claim_amount: number;
  store_name: string | null;
  store_platform: string | null;
  photo_urls: string[];
};

/**
 * Rows eligible for native HTML print — same shape as list enrich, filtered to `ready_to_send`.
 * Testing fallbacks: if none for this org, any org `ready_to_send`; if still none, latest rows regardless of status.
 */
export async function fetchReadyToSendSubmissionsForHtmlPrint(
  organizationId: string,
): Promise<{
  ok: boolean;
  data: ReadyToSendPrintRow[];
  error?: string;
  usedFallback?: boolean;
  /** True when last-resort rows may not be `ready_to_send` — print mark relaxes status filter. */
  anyStatusFallback?: boolean;
}> {
  try {
    const { data: subs, error } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .select("*")
      .eq("organization_id", organizationId)
      .eq("status", "ready_to_send")
      .order("created_at", { ascending: true });

    if (error) throw new Error(error.message);
    let list = subs ?? [];
    let usedFallback = false;
    let anyStatusFallback = false;

    if (list.length === 0) {
      const { data: anyReady, error: e2 } = await supabaseServer
        .from(CLAIM_SUBMISSIONS_TABLE)
        .select("*")
        .eq("status", "ready_to_send")
        .order("created_at", { ascending: true })
        .limit(100);
      if (e2) throw new Error(e2.message);
      list = anyReady ?? [];
      if (list.length > 0) usedFallback = true;
    }

    if (list.length === 0) {
      const { data: anyRow, error: e3 } = await supabaseServer
        .from(CLAIM_SUBMISSIONS_TABLE)
        .select("*")
        .order("created_at", { ascending: false })
        .limit(50);
      if (e3) throw new Error(e3.message);
      list = anyRow ?? [];
      if (list.length > 0) {
        usedFallback = true;
        anyStatusFallback = true;
      }
    }
    const returnIds = [
      ...new Set(
        list.map((s) => String((s as Record<string, unknown>)[CLAIM_SUBMISSION_RETURN_ID_COLUMN] ?? "")),
      ),
    ].filter(Boolean);

    let retMap = new Map<string, Record<string, unknown>>();
    if (returnIds.length > 0) {
      const { data: rets, error: rErr } = await supabaseServer
        .from("returns")
        .select(RETURN_SELECT)
        .in("id", returnIds);
      if (rErr) throw new Error(rErr.message);
      retMap = new Map((rets ?? []).map((row) => [row.id as string, row as Record<string, unknown>]));
    }

    const rows: ReadyToSendPrintRow[] = [];
    for (const raw of list) {
      const r = raw as Record<string, unknown>;
      const rid = r[CLAIM_SUBMISSION_RETURN_ID_COLUMN] as string;
      const ret = retMap.get(rid) ?? null;
      const rawStores = ret?.stores as
        | { name?: string; platform?: string }
        | { name?: string; platform?: string }[]
        | null
        | undefined;
      const stores = Array.isArray(rawStores) ? rawStores[0] : rawStores;
      const storeName = stores?.name?.trim() || null;
      const storePlatform = stores?.platform?.trim() || null;
      rows.push({
        id: r.id as string,
        item_name: (ret?.item_name as string) ?? null,
        asin: (ret?.asin as string | null) ?? null,
        fnsku: (ret?.fnsku as string | null) ?? null,
        sku: resolveSkuFromReturnRow(ret),
        claim_amount: Number(r.claim_amount ?? 0) || 0,
        store_name: storeName,
        store_platform: storePlatform,
        photo_urls: await collectReturnPhotoUrls(ret),
      });
    }

    return { ok: true, data: rows, usedFallback };
  } catch (e) {
    return {
      ok: false,
      data: [],
      error: e instanceof Error ? e.message : "Failed to load ready_to_send submissions.",
    };
  }
}

/**
 * After browser print: mark rows as filed locally (HTML path).
 * Pass `organizationId: undefined` when rows came from cross-org fallback so updates still apply.
 */
export async function markClaimSubmissionsPrintedLocally(
  submissionIds: string[],
  organizationId?: string | null,
  opts?: { relaxStatus?: boolean },
): Promise<{ ok: boolean; updated?: number; error?: string }> {
  if (submissionIds.length === 0) return { ok: true, updated: 0 };
  try {
    let q = supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .update({
        status: "submitted",
        report_url: "generated_locally",
        updated_at: new Date().toISOString(),
      })
      .in("id", submissionIds);
    if (!opts?.relaxStatus) {
      q = q.eq("status", "ready_to_send");
    }
    if (organizationId != null && organizationId !== "") {
      q = q.eq("organization_id", organizationId);
    }
    const { data, error } = await q.select("id");

    if (error) throw new Error(error.message);
    return { ok: true, updated: data?.length ?? submissionIds.length };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Update failed.",
    };
  }
}
