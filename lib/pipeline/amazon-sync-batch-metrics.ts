import type { SupabaseClient } from "@supabase/supabase-js";

import type { AmazonSyncKind } from "./amazon-report-registry";
import { removalAmazonRemovalsBusinessDedupKey } from "./amazon-removals-business-key";

export type SyncKind = AmazonSyncKind;

export type BatchUpsertMetricDelta = {
  rows_synced_new: number;
  rows_synced_updated: number;
  rows_synced_unchanged: number;
  /** Identical to an existing row for the same natural key (re-import / idempotent upsert). */
  rows_duplicate_against_existing: number;
};

const EMPTY_DELTA: BatchUpsertMetricDelta = {
  rows_synced_new: 0,
  rows_synced_updated: 0,
  rows_synced_unchanged: 0,
  rows_duplicate_against_existing: 0,
};

const VOLATILE_KEYS = new Set([
  "id",
  "created_at",
  "updated_at",
  "upload_id",
  "source_upload_id",
  "amazon_staging_id",
]);

function comparableSnapshot(row: Record<string, unknown>): string {
  const o: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    if (VOLATILE_KEYS.has(k)) continue;
    o[k] = v;
  }
  const keys = Object.keys(o).sort();
  return JSON.stringify(keys.map((k) => [k, o[k]]));
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/**
 * Classifies a packed domain batch before upsert: new vs updated vs unchanged duplicate.
 * Best-effort per batch; totals are summed across batches in the sync route.
 */
export async function measureBatchUpsertMetrics(
  supabase: SupabaseClient,
  kind: SyncKind,
  table: string,
  organizationId: string,
  _uploadId: string,
  packedRows: Record<string, unknown>[],
): Promise<BatchUpsertMetricDelta> {
  if (packedRows.length === 0) return { ...EMPTY_DELTA };

  switch (kind) {
    case "FBA_RETURNS":
    case "INVENTORY_LEDGER":
    case "REIMBURSEMENTS":
    case "TRANSACTIONS":
    case "REPORTS_REPOSITORY":
    case "ALL_ORDERS":
    case "REPLACEMENTS":
    case "FBA_GRADE_AND_RESELL":
    case "MANAGE_FBA_INVENTORY":
    case "FBA_INVENTORY":
    case "INBOUND_PERFORMANCE":
    case "AMAZON_FULFILLED_INVENTORY":
    case "RESERVED_INVENTORY":
    case "FEE_PREVIEW":
    case "MONTHLY_STORAGE_FEES":
    case "SETTLEMENT":
    case "SAFET_CLAIMS":
    case "CATEGORY_LISTINGS":
    case "ALL_LISTINGS":
    case "ACTIVE_LISTINGS":
      return await byPhysicalLine(supabase, table, organizationId, packedRows);
    case "REMOVAL_ORDER":
      return await byRemovalBusinessLine(supabase, table, organizationId, packedRows);
    default:
      return { ...EMPTY_DELTA };
  }
}

/** Preload existing rows for idempotent re-import of the same file (same SHA + physical row index). */
/** Align with sync `uq_amazon_*_org_file_row` identity (physical row as truncated integer string). */
function physicalLineMetricKey(organizationId: string, r: Record<string, unknown>): string {
  const org = String(organizationId)
    .trim()
    .replace(/^\{|\}$/g, "")
    .toLowerCase();
  const sha = String(r.source_file_sha256 ?? "").trim().toLowerCase();
  const v = r.source_physical_row_number;
  let pr = "";
  if (v !== null && v !== undefined) {
    if (typeof v === "number" && Number.isFinite(v)) pr = String(Math.trunc(v));
    else {
      const s = String(v).trim();
      if (s !== "") {
        const n = Number(s.replace(/,/g, ""));
        pr = Number.isFinite(n) ? String(Math.trunc(n)) : s;
      }
    }
  }
  return `${org}|${sha}|${pr}`;
}

async function byPhysicalLine(
  supabase: SupabaseClient,
  table: string,
  organizationId: string,
  packedRows: Record<string, unknown>[],
): Promise<BatchUpsertMetricDelta> {
  const keyOf = (r: Record<string, unknown>): string => physicalLineMetricKey(organizationId, r);

  const bySha = new Map<string, Record<string, unknown>[]>();
  for (const r of packedRows) {
    const sha = String(r.source_file_sha256 ?? "").trim().toLowerCase();
    if (!sha) continue;
    const arr = bySha.get(sha) ?? [];
    arr.push(r);
    bySha.set(sha, arr);
  }

  const existingByKey = new Map<string, Record<string, unknown>>();
  for (const [sha, rows] of bySha) {
    const rowNums = [
      ...new Set(
        rows
          .map((r) => Number(r.source_physical_row_number))
          .filter((n) => Number.isFinite(n) && n > 0),
      ),
    ];
    for (const part of chunk(rowNums, 200)) {
      const { data, error } = await supabase
        .from(table)
        .select("*")
        .eq("organization_id", organizationId)
        .eq("source_file_sha256", sha)
        .in("source_physical_row_number", part);
      if (error) {
        console.warn(`[import-metrics] preload ${table} by physical line:`, error.message);
        return { ...EMPTY_DELTA };
      }
      for (const row of data ?? []) {
        existingByKey.set(keyOf(row as Record<string, unknown>), row as Record<string, unknown>);
      }
    }
  }

  return classifyAgainstMap(packedRows, keyOf, existingByKey);
}

async function byColumnIn(
  supabase: SupabaseClient,
  table: string,
  organizationId: string,
  packedRows: Record<string, unknown>[],
  column: string,
  keyFn: (r: Record<string, unknown>) => string,
): Promise<BatchUpsertMetricDelta> {
  const keys = [...new Set(packedRows.map(keyFn).filter(Boolean))];
  if (keys.length === 0) return { ...EMPTY_DELTA };

  const existingByKey = new Map<string, Record<string, unknown>>();
  for (const part of chunk(keys, 200)) {
    const { data, error } = await supabase
      .from(table)
      .select("*")
      .eq("organization_id", organizationId)
      .in(column, part);
    if (error) {
      console.warn(`[import-metrics] preload ${table} by ${column}:`, error.message);
      return { ...EMPTY_DELTA };
    }
    for (const row of data ?? []) {
      const k = keyFn(row as Record<string, unknown>);
      if (k) existingByKey.set(k, row as Record<string, unknown>);
    }
  }

  return classifyAgainstMap(packedRows, keyFn, existingByKey);
}

/**
 * Preload existing `amazon_removals` rows that share the business-line key with this batch
 * (any upload) so new vs updated vs unchanged counts are meaningful across files.
 */
async function byRemovalBusinessLine(
  supabase: SupabaseClient,
  table: string,
  organizationId: string,
  packedRows: Record<string, unknown>[],
): Promise<BatchUpsertMetricDelta> {
  const keyFn = removalAmazonRemovalsBusinessDedupKey;
  const packedKeys = new Set(packedRows.map(keyFn));
  const orderIds = [
    ...new Set(
      packedRows
        .map((r) => String(r.order_id ?? "").trim())
        .filter((x) => x.length > 0),
    ),
  ];
  if (orderIds.length === 0) return { ...EMPTY_DELTA };

  const existingByKey = new Map<string, Record<string, unknown>>();
  for (const part of chunk(orderIds, 200)) {
    const { data, error } = await supabase
      .from(table)
      .select("*")
      .eq("organization_id", organizationId)
      .in("order_id", part);
    if (error) {
      console.warn(`[import-metrics] preload ${table} removal business line:`, error.message);
      return { ...EMPTY_DELTA };
    }
    for (const row of data ?? []) {
      const k = keyFn(row as Record<string, unknown>);
      if (packedKeys.has(k)) existingByKey.set(k, row as Record<string, unknown>);
    }
  }

  return classifyAgainstMap(packedRows, keyFn, existingByKey);
}

function classifyAgainstMap(
  packedRows: Record<string, unknown>[],
  keyFn: (r: Record<string, unknown>) => string,
  existingByKey: Map<string, Record<string, unknown>>,
): BatchUpsertMetricDelta {
  const d: BatchUpsertMetricDelta = {
    rows_synced_new: 0,
    rows_synced_updated: 0,
    rows_synced_unchanged: 0,
    rows_duplicate_against_existing: 0,
  };

  for (const row of packedRows) {
    const k = keyFn(row);
    if (!k) continue;
    const prev = existingByKey.get(k);
    if (!prev) {
      d.rows_synced_new += 1;
      continue;
    }
    const same = comparableSnapshot(row) === comparableSnapshot(prev);
    if (same) {
      d.rows_synced_unchanged += 1;
      d.rows_duplicate_against_existing += 1;
    } else {
      d.rows_synced_updated += 1;
    }
  }

  return d;
}
