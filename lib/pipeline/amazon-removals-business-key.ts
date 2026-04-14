/**
 * REMOVAL_ORDER → `amazon_removals` business identity (`uq_amazon_removals_business_line`).
 * Used for in-batch dedupe, sync metrics, and PostgREST `onConflict` — must stay aligned with the DB index:
 *
 * organization_id, store_id, order_id, sku, fnsku, disposition,
 * requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity,
 * order_date, order_type
 * (NULLS NOT DISTINCT)
 */

import { isUuidString } from "../uuid";

/** Match Postgres NULLS NOT DISTINCT text normalization for unique comparisons. */
export function pgTextUniqueField(val: unknown): string | null {
  if (val === null || val === undefined) return null;
  const s = String(val).trim().normalize("NFC");
  return s === "" ? null : s;
}

/**
 * Integer quantity columns — align with `parseQty()` in import-sync-mappers (`parseInt`, not `parseFloat`).
 * (Used by REMOVAL_SHIPMENT logical-line keying; keep stable.)
 */
export function qtyKey(v: unknown): string {
  if (v === null || v === undefined) return "\0n";
  if (typeof v === "number" && Number.isFinite(v)) return `i:${Math.trunc(v)}`;
  const s = String(v).trim();
  if (s === "") return "\0n";
  const n = parseInt(s, 10);
  if (!Number.isNaN(n)) return `i:${n}`;
  return `s:${s}`;
}

/**
 * Date column — legacy helper for REMOVAL_SHIPMENT `removalLogicalLineDedupKey` only.
 * REMOVAL_ORDER business keys must use `normalizeRemovalOrderDateForBusinessKey`
 * so they stay aligned with `parseIsoDate` + Postgres `date`.
 */
export function dateKey(v: unknown): string {
  if (v === null || v === undefined) return "\0n";
  if (v instanceof Date) {
    if (Number.isNaN(v.getTime())) return "\0n";
    const y = v.getFullYear();
    const m = String(v.getMonth() + 1).padStart(2, "0");
    const d = String(v.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }
  const s = String(v).trim();
  if (s === "") return "\0n";
  const ymd = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (ymd) return ymd[1];
  return "\0n";
}

/**
 * PostgreSQL `float8`/`numeric` → `int4` assignment rounds half away from zero (same family as `rint`).
 * JS `Math.trunc` / `parseInt` disagree (e.g. 10.6 → 10 in JS trunc, 11 in PG), which breaks dedupe vs
 * `uq_amazon_removals_business*` and can produce INSERT batches where PostgREST stores equal integers
 * under distinct in-memory keys.
 */
function pgFloat8ToInt4Assignment(x: number): number {
  if (!Number.isFinite(x)) return 0;
  if (x === 0 || Object.is(x, -0)) return 0;
  const sign = x > 0 ? 1 : -1;
  const ax = Math.abs(x);
  const base = Math.floor(ax);
  const frac = ax - base;
  if (frac < 0.5) return sign * base;
  if (frac > 0.5) return sign * (base + 1);
  return sign * (base + 1);
}

/**
 * Align quantity columns with how Postgres compares them on the unique index (`integer` semantics).
 * Pure integer decimal strings follow `parseQty` / mapper behavior; fractional / float inputs follow PG rounding.
 */
export function normalizeRemovalOrderQtyForBusinessKey(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "bigint") {
    const n = Number(v);
    if (!Number.isFinite(n)) return null;
    return pgFloat8ToInt4Assignment(n);
  }
  if (typeof v === "number" && Number.isFinite(v)) {
    return pgFloat8ToInt4Assignment(v);
  }
  const s0 = String(v).trim();
  if (s0 === "") return null;
  const s = s0.replace(/,/g, "");
  if (/^-?\d+$/.test(s)) {
    const n = parseInt(s, 10);
    return Number.isFinite(n) ? n : null;
  }
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return pgFloat8ToInt4Assignment(n);
}

/**
 * Same rules as `parseIsoDate` in `lib/import-sync-mappers.ts` (Removal Order mapper) — including
 * leading `YYYY-MM-DD` on timestamps so timezone does not shift the calendar day.
 */
function removalOrderBizKeyParseIsoDateString(v: string): string | null {
  if (!v) return null;
  const s = v.trim();
  const ymd = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (ymd) return ymd[1];
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

/**
 * Normalize uuid / json forms so two payloads that Postgres treats as the same `uuid` value
 * never split into different in-memory business keys (e.g. braces, casing).
 */
export function normalizeRemovalOrderUuidForBusinessKey(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s0 = String(v)
    .trim()
    .replace(/^\{|\}$/g, "")
    .toLowerCase();
  if (s0 === "") return null;
  return isUuidString(s0) ? s0 : null;
}

/**
 * Must stay in lockstep with `parseIsoDate` in `lib/import-sync-mappers.ts` (Removal Order mapper output)
 * and with how Postgres compares `date` values on the unique index.
 */
export function normalizeRemovalOrderDateForBusinessKey(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  if (v instanceof Date) {
    if (Number.isNaN(v.getTime())) return null;
    return removalOrderBizKeyParseIsoDateString(v.toISOString());
  }
  return removalOrderBizKeyParseIsoDateString(String(v));
}

export type RemovalOrderBusinessKeyColumns = {
  organization_id: string;
  store_id: string | null;
  order_id: string | null;
  sku: string | null;
  fnsku: string | null;
  disposition: string | null;
  requested_quantity: number | null;
  shipped_quantity: number | null;
  disposed_quantity: number | null;
  cancelled_quantity: number | null;
  order_date: string | null;
  order_type: string | null;
};

/**
 * Single source of truth for the 12 columns in `uq_amazon_removals_business_line`.
 * Values are normalized the same way Postgres will compare them on the unique index.
 */
export function coerceRemovalOrderBusinessKeyColumns(row: Record<string, unknown>): RemovalOrderBusinessKeyColumns {
  const orgUuid = normalizeRemovalOrderUuidForBusinessKey(row.organization_id);
  const organization_id =
    orgUuid ?? String(row.organization_id ?? "").trim().replace(/^\{|\}$/g, "").toLowerCase();

  return {
    organization_id,
    store_id: normalizeRemovalOrderUuidForBusinessKey(row.store_id),
    order_id: pgTextUniqueField(row.order_id),
    sku: pgTextUniqueField(row.sku),
    fnsku: pgTextUniqueField(row.fnsku),
    disposition: pgTextUniqueField(row.disposition),
    requested_quantity: normalizeRemovalOrderQtyForBusinessKey(row.requested_quantity),
    shipped_quantity: normalizeRemovalOrderQtyForBusinessKey(row.shipped_quantity),
    disposed_quantity: normalizeRemovalOrderQtyForBusinessKey(row.disposed_quantity),
    cancelled_quantity: normalizeRemovalOrderQtyForBusinessKey(row.cancelled_quantity),
    order_date: normalizeRemovalOrderDateForBusinessKey(row.order_date),
    order_type: pgTextUniqueField(row.order_type),
  };
}

/**
 * Merge coerced business-key columns onto a full row so PostgREST sends the same scalars the key used.
 */
export function applyCanonicalRemovalOrderBusinessColumns(row: Record<string, unknown>): Record<string, unknown> {
  const c = coerceRemovalOrderBusinessKeyColumns(row);
  return {
    ...row,
    organization_id: c.organization_id,
    store_id: c.store_id,
    order_id: c.order_id,
    sku: c.sku,
    fnsku: c.fnsku,
    disposition: c.disposition,
    requested_quantity: c.requested_quantity,
    shipped_quantity: c.shipped_quantity,
    disposed_quantity: c.disposed_quantity,
    cancelled_quantity: c.cancelled_quantity,
    order_date: c.order_date,
    order_type: c.order_type,
  };
}

/**
 * Canonical business key for `uq_amazon_removals_business_line`:
 * use everywhere (preload, classification, in-batch dedupe, final insert batch).
 *
 * Serialized tuple of the 12 index columns (same order as the DB unique index) — avoids `|`-delimiter
 * collisions when `order_id` / `sku` / `fnsku` contain delimiter characters.
 */
export function removalAmazonRemovalsBusinessDedupKey(row: Record<string, unknown>): string {
  const c = coerceRemovalOrderBusinessKeyColumns(row);
  return JSON.stringify([
    c.organization_id,
    c.store_id,
    c.order_id,
    c.sku,
    c.fnsku,
    c.disposition,
    c.requested_quantity,
    c.shipped_quantity,
    c.disposed_quantity,
    c.cancelled_quantity,
    c.order_date,
    c.order_type,
  ]);
}

/** Invariant helper: count of distinct canonical business keys in a batch. */
export function uniqueBusinessKeyCount(finalInsertRows: Record<string, unknown>[]): number {
  return new Set(finalInsertRows.map(removalAmazonRemovalsBusinessDedupKey)).size;
}

/** Alias for REMOVAL_ORDER insert invariant checks (`finalRows.length === uniqueKeyCount(finalRows)`). */
export function uniqueKeyCount(finalRows: Record<string, unknown>[]): number {
  return uniqueBusinessKeyCount(finalRows);
}

/** Keys that appear more than once under `removalAmazonRemovalsBusinessDedupKey` (for diagnostics). */
export function listDuplicateRemovalOrderBusinessKeys(rows: Record<string, unknown>[]): string[] {
  const counts = new Map<string, number>();
  for (const r of rows) {
    const k = removalAmazonRemovalsBusinessDedupKey(r);
    counts.set(k, (counts.get(k) ?? 0) + 1);
  }
  return [...counts.entries()].filter(([, c]) => c > 1).map(([k]) => k);
}

/** PostgREST `onConflict` — must match `uq_amazon_removals_business_line` exactly. */
export const AMAZON_REMOVALS_BUSINESS_CONFLICT_COLUMNS =
  "organization_id,store_id,order_id,sku,fnsku,disposition,requested_quantity,shipped_quantity,disposed_quantity,cancelled_quantity,order_date,order_type";
