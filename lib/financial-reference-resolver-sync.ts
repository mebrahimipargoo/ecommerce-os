/**
 * Populates `financial_reference_resolver` (and thus `v_trid_resolver`) from
 * settlement, transaction, and reimbursement domain rows. Deterministic keys;
 * failures are logged and rethrown only by callers that want a hard stop.
 */

import type { SupabaseClient } from "@supabase/supabase-js";

import type { AmazonSyncKind } from "./pipeline/amazon-report-registry";

function normStr(v: unknown): string {
  return String(v ?? "").trim();
}

function tryAsinFromRawData(raw: unknown): string | null {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  const o = raw as Record<string, unknown>;
  const keys = ["asin", "asin1", "asin1-value", "product-id", "product_id", "ASIN"];
  for (const k of keys) {
    const v = o[k];
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return null;
}

function tryCurrencyFromRawData(raw: unknown): string | null {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  const o = raw as Record<string, unknown>;
  for (const k of ["currency", "currency-code", "currency_code"]) {
    const v = o[k];
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return null;
}

function tryPostedDateFromRawData(raw: unknown): string | null {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  const o = raw as Record<string, unknown>;
  for (const k of ["posted-date", "posted_date", "approval-date", "approval_date"]) {
    const v = o[k];
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return null;
}

function stableTridKey(parts: unknown[]): string {
  const s = parts.map((p) => normStr(p)).join("|");
  return s.length > 480 ? s.slice(0, 480) : s;
}

function settlementTridKey(row: Record<string, unknown>): string {
  const id = normStr(row.id);
  const line = normStr(row.amazon_line_key);
  const base = line
    ? `amz_line:${line}`
    : stableTridKey([
        "settlement",
        row.settlement_id,
        row.order_id,
        row.sku,
        row.posted_date,
        row.transaction_type,
        row.amount_total,
        row.source_physical_row_number,
        row.source_file_sha256,
      ]);
  return id ? `${base}|rid:${id}` : base;
}

function transactionTridKey(row: Record<string, unknown>): string {
  const id = normStr(row.id);
  const base = stableTridKey([
    "txn",
    row.settlement_id,
    row.order_id,
    row.sku,
    row.posted_date,
    row.transaction_type,
    row.amount,
    row.source_physical_row_number,
    row.source_file_sha256,
  ]);
  return id ? `${base}|rid:${id}` : base;
}

function reimbursementTridKey(row: Record<string, unknown>): string {
  const id = normStr(row.id);
  const base = stableTridKey([
    "reimb",
    row.reimbursement_id,
    row.order_id,
    row.sku,
    row.amount_reimbursed,
    row.source_physical_row_number,
    row.source_file_sha256,
  ]);
  return id ? `${base}|rid:${id}` : base;
}

export async function syncFinancialReferenceResolverForUpload(
  supabase: SupabaseClient,
  organizationId: string,
  uploadId: string,
  kind: AmazonSyncKind,
): Promise<{ upserted: number }> {
  if (kind !== "SETTLEMENT" && kind !== "TRANSACTIONS" && kind !== "REIMBURSEMENTS") {
    return { upserted: 0 };
  }

  const table =
    kind === "SETTLEMENT"
      ? "amazon_settlements"
      : kind === "TRANSACTIONS"
        ? "amazon_transactions"
        : "amazon_reimbursements";

  const { data: rows, error: readErr } = await supabase
    .from(table)
    .select("*")
    .eq("organization_id", organizationId)
    .eq("upload_id", uploadId);

  if (readErr) throw new Error(`financial_reference_resolver: read ${table} failed: ${readErr.message}`);

  const list = (rows ?? []) as Record<string, unknown>[];
  const batch: Record<string, unknown>[] = [];

  for (const row of list) {
    const id = normStr(row.id);
    if (!id) continue;

    let trid_key: string;
    let transaction_type: string | null;
    let amount: number | null;
    let posted_date: string | null;
    let settlement_id: string | null;
    let order_id: string | null;
    let sku: string | null;
    let currency: string | null;
    let reference_group_key: string | null;
    let confidence_score: number;

    const raw_data = row.raw_data;

    if (kind === "SETTLEMENT") {
      trid_key = settlementTridKey(row);
      transaction_type = normStr(row.transaction_type) || null;
      const at = row.amount_total;
      amount = typeof at === "number" && Number.isFinite(at) ? at : at != null ? Number(at) : null;
      if (amount !== null && Number.isNaN(amount)) amount = null;
      const pd = row.posted_date;
      posted_date = pd != null ? String(pd) : null;
      settlement_id = normStr(row.settlement_id) || null;
      order_id = normStr(row.order_id) || null;
      sku = normStr(row.sku) || null;
      currency = normStr(row.currency) || tryCurrencyFromRawData(raw_data);
      reference_group_key = settlement_id ?? order_id;
      confidence_score = normStr(row.amazon_line_key) ? 1 : 0.85;
    } else if (kind === "TRANSACTIONS") {
      trid_key = transactionTridKey(row);
      transaction_type = normStr(row.transaction_type) || null;
      const am = row.amount;
      amount = typeof am === "number" && Number.isFinite(am) ? am : am != null ? Number(am) : null;
      if (amount !== null && Number.isNaN(amount)) amount = null;
      const pd = row.posted_date;
      posted_date = pd != null ? String(pd) : null;
      settlement_id = normStr(row.settlement_id) || null;
      order_id = normStr(row.order_id) || null;
      sku = normStr(row.sku) || null;
      currency = tryCurrencyFromRawData(raw_data);
      reference_group_key = settlement_id ?? order_id;
      confidence_score = 0.85;
    } else {
      trid_key = reimbursementTridKey(row);
      transaction_type = "reimbursement";
      const am = row.amount_reimbursed;
      amount = typeof am === "number" && Number.isFinite(am) ? am : am != null ? Number(am) : null;
      if (amount !== null && Number.isNaN(amount)) amount = null;
      posted_date = null;
      const rawPosted = tryPostedDateFromRawData(raw_data);
      if (rawPosted) posted_date = rawPosted;
      settlement_id = null;
      order_id = normStr(row.order_id) || null;
      sku = normStr(row.sku) || null;
      currency = tryCurrencyFromRawData(raw_data);
      reference_group_key = order_id ?? (normStr(row.reimbursement_id) || null);
      confidence_score = 0.8;
    }

    const asin = tryAsinFromRawData(raw_data);

    batch.push({
      organization_id: organizationId,
      trid_key,
      source_table: table,
      source_row_id: id,
      settlement_id,
      order_id,
      sku,
      asin,
      posted_date,
      transaction_type,
      amount,
      currency,
      reference_group_key,
      confidence_score,
    });
  }

  const CHUNK = 300;
  let upserted = 0;
  for (let off = 0; off < batch.length; off += CHUNK) {
    const chunk = batch.slice(off, off + CHUNK);
    const { error: upErr } = await supabase.from("financial_reference_resolver").upsert(chunk, {
      onConflict: "organization_id,trid_key,source_table,source_row_id",
      ignoreDuplicates: false,
    });
    if (upErr) throw new Error(`financial_reference_resolver upsert failed: ${upErr.message}`);
    upserted += chunk.length;
  }

  console.log(
    JSON.stringify({
      phase: "financial_reference_resolver_sync",
      organization_id: organizationId,
      upload_id: uploadId,
      source_table: table,
      rows_upserted: upserted,
    }),
  );

  return { upserted };
}
