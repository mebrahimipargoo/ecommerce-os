/**
 * POST /api/settings/imports/sync
 *
 * Phase 3 of the 3-phase ETL pipeline.
 *
 * Reads rows from amazon_staging (keyed by upload_id), routes them into the
 * correct amazon_ domain table based on report_type, then deletes the processed
 * staging rows.  Sets status → "synced" on completion.
 *
 * Domain table routing (amazon_ prefix standard):
 *   FBA_RETURNS … MONTHLY_STORAGE_FEES → raw landing upsert on
 *   (organization_id, source_file_sha256, source_physical_row_number) — one domain row per physical CSV line.
 *   REMOVAL_ORDER    → amazon_removals         (business line — see `uq_amazon_removals_business_line`)
 *   REMOVAL_SHIPMENT → amazon_removal_shipments (row_number + cross-upload business skip; Phase 4 only for tree / expected_packages)
 *
 * JSONB Fallback: any CSV column not matched by the typed mapper is stored in
 * the `raw_data` JSONB column — this permanently prevents schema cache crashes.
 *
 * Staging Preservation Rule:
 *   Staging rows are deleted ONLY after their domain batch is successfully upserted.
 *   If any upsert fails, the remaining staging rows are left intact so the user
 *   can fix the issue and retry Phase 3 without re-running Phase 2.
 *
 * Accepts: { upload_id: string }
 * Returns: { ok: true, rowsSynced: number, kind: string }
 */

import { NextResponse } from "next/server";

import {
  applyColumnMappingToRow,
  mapRowToAmazonInventoryLedger,
  mapRowToAmazonReimbursement,
  mapRowToAmazonRemoval,
  mapRowToAmazonRemovalShipment,
  mapRowToAmazonReturn,
  mapRowToAmazonSafetClaim,
  mapRowToAmazonSettlement,
  mapRowToAmazonTransaction,
  mapRowToAmazonReportsRepository,
  mapRowToAmazonRawArchive,
  mapRowToAmazonManageFbaInventory,
  mapRowToAmazonFbaInventory,
  mapRowToAmazonInboundPerformance,
  mapRowToAmazonAmazonFulfilledInventory,
  packPayloadForSupabase,
  NATIVE_COLUMNS_RETURNS,
  NATIVE_COLUMNS_REMOVALS,
  NATIVE_COLUMNS_LEDGER,
  NATIVE_COLUMNS_REIMBURSEMENTS,
  NATIVE_COLUMNS_SETTLEMENTS,
  NATIVE_COLUMNS_SAFET,
  NATIVE_COLUMNS_TRANSACTIONS,
  NATIVE_COLUMNS_REPORTS_REPOSITORY,
  NATIVE_COLUMNS_ALL_ORDERS,
  NATIVE_COLUMNS_REPLACEMENTS,
  NATIVE_COLUMNS_FBA_GRADE_AND_RESELL,
  NATIVE_COLUMNS_MANAGE_FBA_INVENTORY,
  NATIVE_COLUMNS_FBA_INVENTORY,
  NATIVE_COLUMNS_INBOUND_PERFORMANCE,
  NATIVE_COLUMNS_AMAZON_FULFILLED_INVENTORY,
  NATIVE_COLUMNS_RESERVED_INVENTORY,
  NATIVE_COLUMNS_FEE_PREVIEW,
  NATIVE_COLUMNS_MONTHLY_STORAGE_FEES,
} from "../../../../../lib/import-sync-mappers";
import {
  applyCanonicalRemovalOrderBusinessColumns,
  dateKey,
  listDuplicateRemovalOrderBusinessKeys,
  pgTextUniqueField,
  qtyKey,
  removalAmazonRemovalsBusinessDedupKey,
  uniqueBusinessKeyCount,
  uniqueKeyCount,
} from "../../../../../lib/pipeline/amazon-removals-business-key";
import { mapStagingMappedRowToListingRawInsert } from "../../../../../lib/import-listing-physical-lines";
import { logImportPhase } from "../../../../../lib/pipeline/amazon-import-engine-log";
import {
  FPS_KEY_COMPLETE,
  FPS_KEY_FAILED,
  FPS_KEY_SYNC,
  FPS_LABEL_COMPLETE,
  FPS_NEXT_ACTION_LABEL_GENERIC,
  fpsLabelSync,
  fpsNextAfterSync,
  fpsPctPhase3,
} from "../../../../../lib/pipeline/file-processing-status-contract";
import {
  CONFLICT_KEY,
  DOMAIN_TABLE,
  isListingAmazonSyncKind,
  requiresPhase4Generic,
  resolveAmazonImportEngineConfig,
  resolveAmazonImportSyncKind,
  type AmazonImportEngineConfig,
  type AmazonSyncKind,
} from "../../../../../lib/pipeline/amazon-report-registry";
import { completeInventoryLedgerProductIdentifierMapPhase } from "../../../../../lib/inventory-ledger-generic-completion";
import { removalShipmentArchiveBusinessKey } from "../../../../../lib/pipeline/removal-shipment-archive-key";
import {
  measureBatchUpsertMetrics,
  type BatchUpsertMetricDelta,
} from "../../../../../lib/pipeline/amazon-sync-batch-metrics";
import { removeOlderRemovalImportsWithSameFileContent } from "@/app/(admin)/imports/import-actions";
import {
  mergeUploadMetadata,
  type ImportRunMetrics,
} from "../../../../../lib/raw-report-upload-metadata";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";
/** Large listing files — Phase 3 raw upserts only (catalog is Phase 4 Generic). */
export const maxDuration = 60;

const BATCH_SIZE = 500;
/** Max rows per Postgres upsert call — enables granular sync_progress updates. */
const UPSERT_CHUNK_SIZE = 500;
/** Page size for staging reads. Always use range(0, …); do not advance offset after rows are deleted. */
const STAGING_READ_BATCH = 1000;
const STAGING_TABLE = "amazon_staging";

/** File fingerprint from Phase-1 metadata; stable across re-import of the same bytes. */
function resolveSourceFileSha256(meta: unknown, uploadId: string): string {
  const m = meta && typeof meta === "object" ? (meta as Record<string, unknown>) : {};
  const s = String(m.content_sha256 ?? "").trim().toLowerCase();
  if (s) return s;
  return `legacy-upload-${uploadId}`;
}

function attachPhysicalRowIdentity(
  row: Record<string, unknown> | null,
  stagingRowNumber: number,
  fileSha: string,
): void {
  if (!row) return;
  row.source_file_sha256 = fileSha;
  row.source_physical_row_number = stagingRowNumber;
}

/** Aligns with Postgres `integer` / `uuid` comparison on `uq_amazon_returns_org_file_row` (LPN is not identity). */
function normalizeFbaReturnsPhysicalRowNumberForKey(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "number" && Number.isFinite(v)) return String(Math.trunc(v));
  const s = String(v).trim();
  if (s === "") return "";
  const n = Number(s.replace(/,/g, ""));
  if (Number.isFinite(n)) return String(Math.trunc(n));
  return s;
}

/** Canonical tuple for `uq_amazon_returns_org_file_row` — must match `ON CONFLICT` / in-batch dedupe. */
function fbaReturnsFileRowIdentityKey(row: Record<string, unknown>): string {
  const org = String(row.organization_id ?? "")
    .trim()
    .replace(/^\{|\}$/g, "")
    .toLowerCase();
  const sha = String(row.source_file_sha256 ?? "").trim().toLowerCase();
  const pr = normalizeFbaReturnsPhysicalRowNumberForKey(row.source_physical_row_number);
  return `${org}|${sha}|${pr}`;
}

const FBA_RETURNS_DEDUPE_LOG_SAMPLES = 8;

function listDuplicateFbaReturnsFileRowKeys(rows: Record<string, unknown>[]): string[] {
  const counts = new Map<string, number>();
  for (const r of rows) {
    const k = fbaReturnsFileRowIdentityKey(r);
    counts.set(k, (counts.get(k) ?? 0) + 1);
  }
  return [...counts.entries()].filter(([, c]) => c > 1).map(([k]) => k);
}

/**
 * Collapse rows that share the same `(organization_id, source_file_sha256, source_physical_row_number)`;
 * last row wins (same staging line should not appear twice).
 */
function dedupeFbaReturnsRowsByFileRowKey(packed: Record<string, unknown>[]): {
  rows: Record<string, unknown>[];
  collapsedCount: number;
  duplicateKeySamples: string[];
} {
  const seen = new Map<string, Record<string, unknown>>();
  const duplicateKeySamples: string[] = [];
  for (const row of packed) {
    const key = fbaReturnsFileRowIdentityKey(row);
    if (seen.has(key) && duplicateKeySamples.length < FBA_RETURNS_DEDUPE_LOG_SAMPLES) {
      duplicateKeySamples.push(key);
    }
    seen.set(key, row);
  }
  const rows = [...seen.values()];
  return {
    rows,
    collapsedCount: Math.max(0, packed.length - rows.length),
    duplicateKeySamples,
  };
}

/** `finalRows.length === uniqueKeyCount(finalRows)` for FBA_RETURNS physical-line keys. */
function uniqueFbaReturnsFileRowKeyCount(rows: Record<string, unknown>[]): number {
  return new Set(rows.map(fbaReturnsFileRowIdentityKey)).size;
}

function guardFbaReturnsPhysicalRowBatchUniqueness(
  rows: Record<string, unknown>[],
  label: string,
): Record<string, unknown>[] {
  let out = rows;
  for (let iter = 0; iter < 8; iter++) {
    const u = uniqueFbaReturnsFileRowKeyCount(out);
    if (u === out.length) return out;
    const dupKeys = listDuplicateFbaReturnsFileRowKeys(out);
    console.warn(
      `[FBA_RETURNS] ${label}: length ${out.length} !== uniqueKeyCount ${u} — re-collapsing (iter ${iter}); ` +
        `duplicate_file_row_keys(sample)=${JSON.stringify(dupKeys.slice(0, FBA_RETURNS_DEDUPE_LOG_SAMPLES))}`,
    );
    out = dedupeFbaReturnsRowsByFileRowKey(out).rows;
  }
  const u = uniqueFbaReturnsFileRowKeyCount(out);
  if (out.length !== u) {
    const dupKeys = listDuplicateFbaReturnsFileRowKeys(out);
    throw new Error(
      `[FBA_RETURNS] ${label}: finalRows.length (${out.length}) !== uniqueKeyCount (${u}); ` +
        `duplicate_file_row_keys(sample)=${JSON.stringify(dupKeys.slice(0, FBA_RETURNS_DEDUPE_LOG_SAMPLES))}`,
    );
  }
  return out;
}

// =============================================================================
// ── REPORTS_REPOSITORY physical-line guard ──────────────────────────────────
// Same shape as the FBA_RETURNS guard above. The conflict target for
// `amazon_reports_repository` is the unique index
// `uq_amazon_reports_repo_org_file_row (organization_id, source_file_sha256, source_physical_row_number)`.
// We must ensure no two rows in a single INSERT statement collide on this tuple,
// or Postgres would raise either:
//   • "ON CONFLICT DO UPDATE command cannot affect row a second time"  (in-batch duplicate vs the targeted index), or
//   • "duplicate key value violates unique constraint <other>"          (in-batch duplicate vs a stale orphan unique index that
//     the migration history claims to have dropped — this manifests as
//     `uq_amazon_reports_repo_natural` or `uq_amazon_reports_repo_org_line_hash` if the DROP wasn't applied).
//
// Importantly, we key on PHYSICAL identity (source_physical_row_number) — NOT on
// source_line_hash or any natural business tuple — so genuinely distinct
// financial sub-lines (Principal / FBA Fee / Commission rows that happen to
// share date_time/order_id/sku/description) are PRESERVED, not collapsed.
// =============================================================================

const REPORTS_REPO_DEDUPE_LOG_SAMPLES = 8;

function reportsRepoFileRowIdentityKey(row: Record<string, unknown>): string {
  const org = String(row.organization_id ?? "")
    .trim()
    .replace(/^\{|\}$/g, "")
    .toLowerCase();
  const sha = String(row.source_file_sha256 ?? "").trim().toLowerCase();
  const prRaw = row.source_physical_row_number;
  const pr =
    typeof prRaw === "number" && Number.isFinite(prRaw)
      ? String(Math.trunc(prRaw))
      : (() => {
          const s = String(prRaw ?? "").trim();
          if (s === "") return "";
          const n = Number(s.replace(/,/g, ""));
          return Number.isFinite(n) ? String(Math.trunc(n)) : s;
        })();
  return `${org}|${sha}|${pr}`;
}

function listDuplicateReportsRepoFileRowKeys(rows: Record<string, unknown>[]): string[] {
  const counts = new Map<string, number>();
  for (const r of rows) {
    const k = reportsRepoFileRowIdentityKey(r);
    counts.set(k, (counts.get(k) ?? 0) + 1);
  }
  return [...counts.entries()].filter(([, c]) => c > 1).map(([k]) => k);
}

/**
 * Last-row-wins collapse on the physical-line key. Mirrors the FBA_RETURNS
 * helper but never touches business fields (transaction_type, total_amount,
 * description) — only deduplicates rows that share the *same physical CSV line*.
 */
function dedupeReportsRepoRowsByFileRowKey(packed: Record<string, unknown>[]): {
  rows: Record<string, unknown>[];
  collapsedCount: number;
  duplicateKeySamples: string[];
} {
  const seen = new Map<string, Record<string, unknown>>();
  const duplicateKeySamples: string[] = [];
  for (const row of packed) {
    const key = reportsRepoFileRowIdentityKey(row);
    if (seen.has(key) && duplicateKeySamples.length < REPORTS_REPO_DEDUPE_LOG_SAMPLES) {
      duplicateKeySamples.push(key);
    }
    seen.set(key, row);
  }
  const rows = [...seen.values()];
  return {
    rows,
    collapsedCount: Math.max(0, packed.length - rows.length),
    duplicateKeySamples,
  };
}

function uniqueReportsRepoFileRowKeyCount(rows: Record<string, unknown>[]): number {
  return new Set(rows.map(reportsRepoFileRowIdentityKey)).size;
}

/**
 * Hard pre-write guard for REPORTS_REPOSITORY. Runs BEFORE the upsert is
 * dispatched to PostgREST. If any physical-line key collides within the chunk
 * we collapse and try again; if collisions persist after retries we throw a
 * structured diagnostic so the failure surfaces as something the user can act
 * on instead of a raw Postgres "duplicate key" error.
 */
function guardReportsRepoPhysicalRowBatchUniqueness(
  rows: Record<string, unknown>[],
  label: string,
): Record<string, unknown>[] {
  let out = rows;
  for (let iter = 0; iter < 8; iter++) {
    const u = uniqueReportsRepoFileRowKeyCount(out);
    if (u === out.length) return out;
    const dupKeys = listDuplicateReportsRepoFileRowKeys(out);
    console.warn(
      `[REPORTS_REPOSITORY] ${label}: length ${out.length} !== uniqueKeyCount ${u} — re-collapsing (iter ${iter}); ` +
        `duplicate_file_row_keys(sample)=${JSON.stringify(dupKeys.slice(0, REPORTS_REPO_DEDUPE_LOG_SAMPLES))}`,
    );
    out = dedupeReportsRepoRowsByFileRowKey(out).rows;
  }
  const u = uniqueReportsRepoFileRowKeyCount(out);
  if (out.length !== u) {
    const dupKeys = listDuplicateReportsRepoFileRowKeys(out);
    throw new Error(
      `[REPORTS_REPOSITORY] ${label}: finalRows.length (${out.length}) !== uniqueKeyCount (${u}); ` +
        `duplicate_file_row_keys(sample)=${JSON.stringify(dupKeys.slice(0, REPORTS_REPO_DEDUPE_LOG_SAMPLES))}. ` +
        `This means two rows in the same batch share (organization_id, source_file_sha256, source_physical_row_number). ` +
        `Confirm amazon_staging has no duplicate row_number rows for this upload_id.`,
    );
  }
  return out;
}

/**
 * Pre-sync staging integrity assertion for REPORTS_REPOSITORY (and any other
 * report whose conflict target is the physical-line tuple).
 *
 * Verifies in `amazon_staging` for this (org, upload):
 *   1. count(*) > 0                        (cannot sync empty staging)
 *   2. count(*) === count(distinct row_number) — no duplicate physical rows
 *   3. count(*) === max(row_number) - min(row_number) + 1 — no gaps
 *
 * Also LOGS (does NOT fail) the count of duplicate `source_line_hash` values:
 * Reports Repository legitimately repeats sub-line content across distinct
 * physical lines (Principal / FBA Fee / Commission), so duplicate hashes are
 * NOT an error here — they are reported for visibility only.
 *
 * Throws a structured Error if (1)-(3) fail. The outer catch then marks the
 * upload `failed` with a clear message — no silent staged-but-broken state.
 */
async function assertReportsRepoStagingPhysicalIntegrity(
  orgId: string,
  uploadId: string,
): Promise<{
  stagingCount: number;
  duplicateHashCount: number;
  minRowNumber: number;
  maxRowNumber: number;
}> {
  const { count: stagingCountResp, error: cntErr } = await supabaseServer
    .from(STAGING_TABLE)
    .select("*", { count: "exact", head: true })
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);
  if (cntErr) {
    throw new Error(
      `[REPORTS_REPOSITORY] staging integrity check (count) failed: ${cntErr.message}. upload_id=${uploadId}`,
    );
  }
  const stagingCount = typeof stagingCountResp === "number" ? stagingCountResp : 0;
  if (stagingCount === 0) {
    throw new Error(
      `[REPORTS_REPOSITORY] staging is empty for upload_id=${uploadId}. Run Phase 2 (Process) first.`,
    );
  }

  const { data: maxRow, error: maxErr } = await supabaseServer
    .from(STAGING_TABLE)
    .select("row_number")
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId)
    .order("row_number", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (maxErr) {
    throw new Error(`[REPORTS_REPOSITORY] staging integrity check (max) failed: ${maxErr.message}`);
  }
  const { data: minRow, error: minErr } = await supabaseServer
    .from(STAGING_TABLE)
    .select("row_number")
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId)
    .order("row_number", { ascending: true })
    .limit(1)
    .maybeSingle();
  if (minErr) {
    throw new Error(`[REPORTS_REPOSITORY] staging integrity check (min) failed: ${minErr.message}`);
  }
  const maxRowNumber =
    maxRow && typeof (maxRow as { row_number?: unknown }).row_number === "number"
      ? Math.floor((maxRow as { row_number: number }).row_number)
      : 0;
  const minRowNumber =
    minRow && typeof (minRow as { row_number?: unknown }).row_number === "number"
      ? Math.floor((minRow as { row_number: number }).row_number)
      : 0;

  // Gap / duplicate-row-number assertion. If the unique index
  // `uq_amazon_staging_org_upload_row_number` is intact this is impossible; the
  // check is a defense in depth so we surface the issue as a clear pre-flight
  // error instead of a cryptic Postgres "duplicate key" inside an upsert.
  const span = stagingCount === 0 ? 0 : maxRowNumber - minRowNumber + 1;
  if (span !== stagingCount) {
    throw new Error(
      `[REPORTS_REPOSITORY] staging integrity failed for upload_id=${uploadId}: ` +
        `count=${stagingCount}, min(row_number)=${minRowNumber}, max(row_number)=${maxRowNumber}, span=${span}. ` +
        `Either staging has duplicate row_numbers (uq_amazon_staging_org_upload_row_number compromised) ` +
        `or there are gaps. Re-run Phase 2 (Process) to restage.`,
    );
  }

  // INFORMATIONAL ONLY — distinct source_line_hash duplicates are EXPECTED in
  // REPORTS_REPOSITORY (e.g. Principal/FBA Fee/Commission may share content
  // across distinct physical lines). We log the count and a few sample hashes
  // for diagnostics but never fail on this — collapsing here would lose
  // line-level financial granularity.
  let duplicateHashCount = 0;
  try {
    const { data: hashSample } = await supabaseServer
      .from(STAGING_TABLE)
      .select("source_line_hash, row_number")
      .eq("organization_id", orgId)
      .eq("upload_id", uploadId)
      .order("source_line_hash", { ascending: true })
      .limit(50000); // hard cap for the diagnostic — large enough to be useful, small enough not to blow memory
    if (Array.isArray(hashSample)) {
      const counts = new Map<string, number>();
      for (const r of hashSample) {
        const h = String((r as { source_line_hash?: unknown }).source_line_hash ?? "");
        if (!h) continue;
        counts.set(h, (counts.get(h) ?? 0) + 1);
      }
      const dupEntries = [...counts.entries()].filter(([, c]) => c > 1);
      duplicateHashCount = dupEntries.reduce((sum, [, c]) => sum + (c - 1), 0);
      if (duplicateHashCount > 0) {
        console.log(
          JSON.stringify({
            event: "REPORTS_REPOSITORY_staging_duplicate_hash_info",
            note: "Duplicate source_line_hash rows kept on purpose — line-level financial granularity is preserved via source_physical_row_number.",
            upload_id: uploadId,
            organization_id: orgId,
            duplicate_hash_extra_rows_in_sample: duplicateHashCount,
            sample_hashes_with_count: dupEntries
              .slice(0, REPORTS_REPO_DEDUPE_LOG_SAMPLES)
              .map(([h, c]) => ({ hash: h, occurrences: c })),
          }),
        );
      }
    }
  } catch (e) {
    console.warn(
      `[REPORTS_REPOSITORY] staging duplicate-hash sample failed (non-fatal): ${e instanceof Error ? e.message : String(e)}`,
    );
  }

  console.log(
    JSON.stringify({
      event: "REPORTS_REPOSITORY_staging_integrity_ok",
      upload_id: uploadId,
      organization_id: orgId,
      staging_count: stagingCount,
      min_row_number: minRowNumber,
      max_row_number: maxRowNumber,
      duplicate_hash_extra_rows_in_sample: duplicateHashCount,
    }),
  );

  return { stagingCount, duplicateHashCount, minRowNumber, maxRowNumber };
}

type Body = { upload_id?: string };

type SyncKind = AmazonSyncKind;

/** Typed columns on `amazon_listing_report_rows_raw` (overflow not used — payloads are pre-shaped). */
const NATIVE_COLUMNS_LISTING_RAW = new Set([
  "organization_id",
  "store_id",
  "source_upload_id",
  "source_report_type",
  "row_number",
  "source_file_sha256",
  "source_physical_row_number",
  "seller_sku",
  "asin",
  "listing_id",
  "raw_payload",
  "source_line_hash",
  "parse_status",
  "parse_error",
]);

/** NATIVE_COLUMNS set for each sync kind — passed to packPayloadForSupabase(). */
const NATIVE_COLUMNS_MAP: Record<SyncKind, Set<string> | null> = {
  FBA_RETURNS:           NATIVE_COLUMNS_RETURNS,
  REMOVAL_ORDER:         NATIVE_COLUMNS_REMOVALS,
  REMOVAL_SHIPMENT:      NATIVE_COLUMNS_REMOVALS,
  INVENTORY_LEDGER:      NATIVE_COLUMNS_LEDGER,
  REIMBURSEMENTS:        NATIVE_COLUMNS_REIMBURSEMENTS,
  SETTLEMENT:            NATIVE_COLUMNS_SETTLEMENTS,
  SAFET_CLAIMS:          NATIVE_COLUMNS_SAFET,
  TRANSACTIONS:          NATIVE_COLUMNS_TRANSACTIONS,
  REPORTS_REPOSITORY:    NATIVE_COLUMNS_REPORTS_REPOSITORY,
  ALL_ORDERS:            NATIVE_COLUMNS_ALL_ORDERS,
  REPLACEMENTS:          NATIVE_COLUMNS_REPLACEMENTS,
  FBA_GRADE_AND_RESELL:  NATIVE_COLUMNS_FBA_GRADE_AND_RESELL,
  MANAGE_FBA_INVENTORY:  NATIVE_COLUMNS_MANAGE_FBA_INVENTORY,
  FBA_INVENTORY:         NATIVE_COLUMNS_FBA_INVENTORY,
  INBOUND_PERFORMANCE:   NATIVE_COLUMNS_INBOUND_PERFORMANCE,
  AMAZON_FULFILLED_INVENTORY: NATIVE_COLUMNS_AMAZON_FULFILLED_INVENTORY,
  RESERVED_INVENTORY:    NATIVE_COLUMNS_RESERVED_INVENTORY,
  FEE_PREVIEW:           NATIVE_COLUMNS_FEE_PREVIEW,
  MONTHLY_STORAGE_FEES:  NATIVE_COLUMNS_MONTHLY_STORAGE_FEES,
  CATEGORY_LISTINGS:     NATIVE_COLUMNS_LISTING_RAW,
  ALL_LISTINGS:          NATIVE_COLUMNS_LISTING_RAW,
  ACTIVE_LISTINGS:       NATIVE_COLUMNS_LISTING_RAW,
  UNKNOWN:               null,
};

/** Fallback batch key when source_staging_id is missing (REMOVAL_SHIPMENT legacy paths). */
function removalLogicalLineDedupKey(row: Record<string, unknown>): string {
  return [
    String(row.organization_id ?? "").trim(),
    pgTextUniqueField(row.order_id) ?? "",
    pgTextUniqueField(row.sku) ?? "",
    pgTextUniqueField(row.fnsku) ?? "",
    pgTextUniqueField(row.disposition) ?? "",
    qtyKey(row.requested_quantity),
    qtyKey(row.shipped_quantity),
    qtyKey(row.disposed_quantity),
    qtyKey(row.cancelled_quantity),
    dateKey(row.order_date),
    pgTextUniqueField(row.order_type) ?? "",
  ].join("|");
}

/**
 * Imports Target Store on `raw_report_uploads.metadata` (Wave 1).
 * Prefer `import_store_id`; fall back to `ledger_store_id` for older sessions.
 */
function resolveImportStoreId(meta: unknown): string | null {
  const m =
    meta && typeof meta === "object" && !Array.isArray(meta)
      ? (meta as Record<string, unknown>)
      : {};
  const a = typeof m.import_store_id === "string" ? m.import_store_id.trim() : "";
  if (a && isUuidString(a)) return a;
  const b = typeof m.ledger_store_id === "string" ? m.ledger_store_id.trim() : "";
  if (b && isUuidString(b)) return b;
  return null;
}

/**
 * Merge two mapped removal rows that share the same business upsert key (prefer non-null / non-empty).
 * `upload_id` + `source_staging_id` use last-wins so lineage reflects the latest contributing CSV line.
 */
function mergeRemovalOrderRowsPreferNonNull(
  prev: Record<string, unknown>,
  next: Record<string, unknown>,
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...prev };
  for (const [k, v] of Object.entries(next)) {
    if (k === "raw_data") continue;
    if (v === null || v === undefined) continue;
    if (typeof v === "string" && v.trim() === "") continue;
    const ex = out[k];
    const exEmpty =
      ex === null ||
      ex === undefined ||
      (typeof ex === "string" && ex.trim() === "");
    if (exEmpty) out[k] = v;
  }
  const pr = prev.raw_data;
  const nr = next.raw_data;
  if (nr && typeof nr === "object" && !Array.isArray(nr)) {
    const po =
      pr && typeof pr === "object" && !Array.isArray(pr) ? (pr as Record<string, unknown>) : {};
    out.raw_data = { ...po, ...(nr as Record<string, unknown>) };
  }
  const u = next.upload_id;
  if (u !== null && u !== undefined && String(u).trim() !== "") out.upload_id = u;
  const sid = next.source_staging_id;
  if (sid !== null && sid !== undefined && String(sid).trim() !== "") out.source_staging_id = sid;
  return out;
}

/**
 * Raw shipment archive: one row per staging line. Arbiter matches
 * `uq_amazon_removal_shipments_org_upload_staging` (see 20260521_wave1_removal_store_dual_dedupe
 * and migration dropping business-line uniqueness for raw archive).
 */
/** Idempotent per physical CSV line when staging is recreated (see migration20260619). */
const REMOVAL_SHIPMENT_ROW_NUMBER_CONFLICT = "organization_id,upload_id,staging_row_number";

async function loadCrossUploadShipmentBusinessKeySet(opts: {
  orgId: string;
  storeId: string;
  excludeUploadId: string;
  orderIds: string[];
  trackingNumbers: string[];
}): Promise<Set<string>> {
  const keys = new Set<string>();
  const { orgId, storeId, excludeUploadId } = opts;
  const orderIds = [...new Set(opts.orderIds.map((s) => pgTextUniqueField(s) ?? "").filter(Boolean))].slice(0, 2000);
  const trackingNumbers = [
    ...new Set(opts.trackingNumbers.map((s) => pgTextUniqueField(s) ?? "").filter(Boolean)),
  ].slice(0, 2000);

  const ingest = (rows: Record<string, unknown>[] | null) => {
    for (const row of rows ?? []) {
      const k = removalShipmentArchiveBusinessKey(row);
      if (k) keys.add(k);
    }
  };

  for (let i = 0; i < orderIds.length; i += 80) {
    const chunk = orderIds.slice(i, i + 80);
    const { data, error } = await supabaseServer
      .from("amazon_removal_shipments")
      .select(
        "organization_id,store_id,order_id,tracking_number,sku,fnsku,disposition,requested_quantity,shipped_quantity,disposed_quantity,cancelled_quantity,order_date,order_type,carrier,shipment_date",
      )
      .eq("organization_id", orgId)
      .eq("store_id", storeId)
      .neq("upload_id", excludeUploadId)
      .in("order_id", chunk);
    if (error) {
      throw new Error(`[REMOVAL_SHIPMENT] cross-upload shipment key prefetch failed: ${error.message}`);
    }
    ingest(data as Record<string, unknown>[]);
  }

  for (let i = 0; i < trackingNumbers.length; i += 80) {
    const chunk = trackingNumbers.slice(i, i + 80);
    const { data, error } = await supabaseServer
      .from("amazon_removal_shipments")
      .select(
        "organization_id,store_id,order_id,tracking_number,sku,fnsku,disposition,requested_quantity,shipped_quantity,disposed_quantity,cancelled_quantity,order_date,order_type,carrier,shipment_date",
      )
      .eq("organization_id", orgId)
      .eq("store_id", storeId)
      .neq("upload_id", excludeUploadId)
      .in("tracking_number", chunk);
    if (error) {
      throw new Error(`[REMOVAL_SHIPMENT] cross-upload shipment key prefetch failed: ${error.message}`);
    }
    ingest(data as Record<string, unknown>[]);
  }

  return keys;
}

/**
 * Patch `amazon_removals` from typed `amazon_removal_shipments` — fill missing fields only
 * (no null/empty overwrites of existing values).
 */
function buildRemovalFillFromShipment(
  existing: Record<string, unknown>,
  shipment: Record<string, unknown>,
): Record<string, unknown> {
  const payload: Record<string, unknown> = {};
  const tn = pgTextUniqueField(shipment.tracking_number);
  if (tn && !pgTextUniqueField(existing.tracking_number as string | null)) {
    payload.tracking_number = tn;
  }
  const incC = shipment.carrier;
  if (incC !== undefined && incC !== null && String(incC).trim() !== "") {
    if (!pgTextUniqueField(existing.carrier as string | null)) payload.carrier = incC;
  }
  const incSd = shipment.shipment_date;
  if (incSd !== undefined && incSd !== null && String(incSd).trim() !== "") {
    if (existing.shipment_date == null || String(existing.shipment_date).trim() === "") {
      payload.shipment_date = incSd;
    }
  }
  const incOd = shipment.order_date;
  if (incOd !== undefined && incOd !== null && String(incOd).trim() !== "") {
    if (existing.order_date == null || String(existing.order_date).trim() === "") {
      payload.order_date = incOd;
    }
  }
  const incOt = shipment.order_type;
  if (incOt !== undefined && incOt !== null && String(incOt).trim() !== "") {
    if (!pgTextUniqueField(existing.order_type as string | null)) payload.order_type = incOt;
  }
  return payload;
}

function parseStagingRawRow(raw: unknown): Record<string, string> {
  if (raw && typeof raw === "object" && !Array.isArray(raw)) {
    const o = raw as Record<string, unknown>;
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(o)) {
      out[k] = v === null || v === undefined ? "" : String(v);
    }
    return out;
  }
  if (typeof raw === "string") {
    try {
      const j = JSON.parse(raw) as unknown;
      if (j && typeof j === "object" && !Array.isArray(j)) {
        return parseStagingRawRow(j);
      }
    } catch {
      /* ignore */
    }
  }
  return {};
}

type RemovalShipmentSyncOpts = {
  uploadId: string;
  orgId: string;
  storeId: string;
  totalStagingRows: number;
  columnMapping: Record<string, string> | null;
  syncUpserted: { value: number };
  engine: AmazonImportEngineConfig;
  reportType: string;
  duplicateInBatchTotal: { value: number };
};

/** One active REMOVAL_SHIPMENT sync per (org, store) — avoids shipment tree rebuild races. */
async function acquireRemovalPipelineLock(orgId: string, storeId: string, uploadId: string): Promise<void> {
  await supabaseServer.from("import_pipeline_locks").delete().eq("upload_id", uploadId);
  const { error } = await supabaseServer.from("import_pipeline_locks").insert({
    organization_id: orgId,
    store_id: storeId,
    upload_id: uploadId,
  });
  if (!error) return;
  if (error.code === "23505") {
    const { data } = await supabaseServer
      .from("import_pipeline_locks")
      .select("upload_id")
      .eq("organization_id", orgId)
      .eq("store_id", storeId)
      .maybeSingle();
    const existing = data && typeof data === "object" ? String((data as { upload_id?: string }).upload_id ?? "") : "";
    if (existing === uploadId) return;
    throw new Error(
      "Another removal shipment import is running for this store. Wait for it to finish, then retry.",
    );
  }
  throw new Error(`Removal pipeline lock failed: ${error.message}`);
}

async function releaseRemovalPipelineLock(uploadId: string): Promise<void> {
  await supabaseServer.from("import_pipeline_locks").delete().eq("upload_id", uploadId);
}

/**
 * REMOVAL_SHIPMENT Phase 3: land rows in `amazon_removal_shipments` only.
 * Idempotent per upload via `staging_row_number` (stable when staging is recreated).
 * Skips inserts when the same logical shipment line already exists from another upload.
 * Does not call shipment tree / expected_packages RPCs (Phase 4 only).
 */
async function runRemovalShipmentSync(opts: RemovalShipmentSyncOpts): Promise<{
  synced: number;
  mapperNullCount: number;
  rawRowsWritten: number;
  rawRowsSkippedCrossUpload: number;
}> {
  const { uploadId, orgId, storeId, totalStagingRows, columnMapping, syncUpserted, engine, reportType, duplicateInBatchTotal } =
    opts;

  await acquireRemovalPipelineLock(orgId, storeId, uploadId);

  try {
    console.log(
      `[REMOVAL_SHIPMENT] Phase 3 raw sync — destination=amazon_removal_shipments only; ` +
        `expected_packages and shipment tree are not mutated here (Phase 4 Generic). upload_id=${uploadId}`,
    );

    let shipmentArchiveRowsAttempted = 0;
    let shipmentArchiveRowsWritten = 0;

    let archived = 0;
    let mapperNull = 0;
    let rawRowsSkippedCrossUpload = 0;
    const removalsUpdated = 0;
    let loggedShipmentRawKeys = false;
    let loggedShipmentMappedSample = false;

    const REMOVAL_ENRICH_VERIFY_FIELDS = [
      "tracking_number",
      "carrier",
      "shipment_date",
      "order_date",
      "order_type",
    ] as const;

    let verifyAStagingFetched = 0;
    let verifyAShipmentWritten = 0;
    let verifyAStagingDeleted = 0;
    let verifyBEnrichConsidered = 0;
    let verifyBEnrichMatched = 0;
    let verifyBEnrichUpdatedOk = 0;
    const verifyBEnrichFieldInPayload: Record<string, number> = {};
    for (const k of REMOVAL_ENRICH_VERIFY_FIELDS) verifyBEnrichFieldInPayload[k] = 0;

    let removalShipmentBatchIndex = 0;

    while (true) {
      removalShipmentBatchIndex++;
      const { data: stagingRows, error: readErr } = await supabaseServer
        .from(STAGING_TABLE)
        .select("id, row_number, raw_row")
        .eq("upload_id", uploadId)
        .eq("organization_id", orgId)
        .order("row_number", { ascending: true })
        .limit(STAGING_READ_BATCH);

      if (readErr) throw new Error(`Staging read failed: ${readErr.message}`);
      if (!stagingRows || stagingRows.length === 0) break;

      const ids = stagingRows.map((r) => r.id as string);

      type ParsedStagingShipment = {
        sr: (typeof stagingRows)[number];
        rawObj: Record<string, string>;
        mappedRemoval: Record<string, unknown> | null;
        stagingRowNumber: number;
      };

      const parsed: ParsedStagingShipment[] = [];
      const orderScratch: string[] = [];
      const trackingScratch: string[] = [];

      for (const sr of stagingRows) {
        const rawObj = parseStagingRawRow(sr.raw_row);
        if (!loggedShipmentRawKeys && Object.keys(rawObj).length > 0) {
          loggedShipmentRawKeys = true;
          console.log(
            JSON.stringify({
              phase: "REMOVAL_SHIPMENT_raw_row_keys",
              upload_id: uploadId,
              keys: Object.keys(rawObj).sort(),
            }),
          );
        }
        const mappedRow = applyColumnMappingToRow(rawObj, columnMapping);
        const mappedRemoval = mapRowToAmazonRemovalShipment(mappedRow, orgId, uploadId, storeId) as Record<
          string,
          unknown
        > | null;
        if (!loggedShipmentMappedSample && mappedRemoval) {
          loggedShipmentMappedSample = true;
          console.log(
            JSON.stringify({
              phase: "REMOVAL_SHIPMENT_mapped_sample_before_upsert",
              upload_id: uploadId,
              store_id: storeId,
              sample: {
                order_id: mappedRemoval.order_id,
                tracking_number: mappedRemoval.tracking_number,
                carrier: mappedRemoval.carrier,
                shipment_date: mappedRemoval.shipment_date,
                order_date: mappedRemoval.order_date,
                order_type: mappedRemoval.order_type,
                sku: mappedRemoval.sku,
                fnsku: mappedRemoval.fnsku,
                disposition: mappedRemoval.disposition,
                requested_quantity: mappedRemoval.requested_quantity,
                shipped_quantity: mappedRemoval.shipped_quantity,
                disposed_quantity: mappedRemoval.disposed_quantity,
                cancelled_quantity: mappedRemoval.cancelled_quantity,
              },
            }),
          );
        }

        const rowNumRaw = (sr as { row_number?: unknown }).row_number;
        const stagingRowNumber =
          typeof rowNumRaw === "number" && Number.isFinite(rowNumRaw)
            ? rowNumRaw
            : parseInt(String(rowNumRaw ?? ""), 10);
        if (!Number.isFinite(stagingRowNumber) || stagingRowNumber < 1) {
          throw new Error(
            `[REMOVAL_SHIPMENT] Invalid amazon_staging.row_number for upload_id=${uploadId} staging_id=${sr.id}`,
          );
        }

        if (mappedRemoval) {
          const oid = mappedRemoval.order_id != null ? String(mappedRemoval.order_id).trim() : "";
          if (oid) orderScratch.push(oid);
          const tr = mappedRemoval.tracking_number != null ? String(mappedRemoval.tracking_number).trim() : "";
          if (tr) trackingScratch.push(tr);
        }

        parsed.push({ sr, rawObj, mappedRemoval, stagingRowNumber });
      }

      const crossUploadKeys = await loadCrossUploadShipmentBusinessKeySet({
        orgId,
        storeId,
        excludeUploadId: uploadId,
        orderIds: orderScratch,
        trackingNumbers: trackingScratch,
      });

      const archiveRows: Record<string, unknown>[] = [];
      for (const { sr, rawObj, mappedRemoval, stagingRowNumber } of parsed) {
        const baseArchive: Record<string, unknown> = {
          organization_id: orgId,
          upload_id: uploadId,
          amazon_staging_id: sr.id,
          store_id: storeId,
          staging_row_number: stagingRowNumber,
          raw_row: rawObj,
        };
        if (mappedRemoval) {
          Object.assign(baseArchive, {
            order_id: mappedRemoval.order_id ?? null,
            sku: mappedRemoval.sku ?? null,
            fnsku: mappedRemoval.fnsku ?? null,
            disposition: mappedRemoval.disposition ?? null,
            tracking_number: mappedRemoval.tracking_number ?? null,
            carrier: mappedRemoval.carrier ?? null,
            shipment_date: mappedRemoval.shipment_date ?? null,
            order_date: mappedRemoval.order_date ?? null,
            order_type: mappedRemoval.order_type ?? null,
            requested_quantity: mappedRemoval.requested_quantity ?? null,
            shipped_quantity: mappedRemoval.shipped_quantity ?? null,
            disposed_quantity: mappedRemoval.disposed_quantity ?? null,
            cancelled_quantity: mappedRemoval.cancelled_quantity ?? null,
          });
        }

        if (mappedRemoval) {
          const bizKey = removalShipmentArchiveBusinessKey(baseArchive);
          if (bizKey && crossUploadKeys.has(bizKey)) {
            rawRowsSkippedCrossUpload++;
            continue;
          }
        } else {
          mapperNull++;
        }

        archiveRows.push(baseArchive);
      }

      shipmentArchiveRowsAttempted += archiveRows.length;

      for (let i = 0; i < archiveRows.length; i += UPSERT_CHUNK_SIZE) {
        const chunk = archiveRows.slice(i, i + UPSERT_CHUNK_SIZE);
        if (chunk.length === 0) continue;
        const { error: upsertErr } = await supabaseServer.from("amazon_removal_shipments").upsert(chunk, {
          onConflict: REMOVAL_SHIPMENT_ROW_NUMBER_CONFLICT,
          ignoreDuplicates: false,
        });
        if (upsertErr) {
          throw new Error(
            `[REMOVAL_SHIPMENT] upsert into amazon_removal_shipments failed: ${upsertErr.message}. ` +
              `Apply migration 20260619_removal_shipment_staging_row_number.sql if the column or unique index is missing.`,
          );
        }
      }

      shipmentArchiveRowsWritten += archiveRows.length;

      archived += stagingRows.length;
      await bumpSyncProgressMetadata(
        {
          uploadId,
          orgId,
          totalStagingRows,
          upserted: syncUpserted,
          engine,
          reportType,
          duplicateInBatchTotal,
        },
        stagingRows.length,
        {
          rawRowsWritten: shipmentArchiveRowsWritten,
          rawRowsSkippedCrossUpload,
        },
      );

      const enrichmentShipmentsConsidered = 0;
      const enrichmentRemovalsMatched = 0;
      const enrichmentRemovalsUpdated = 0;
      const enrichPayloadFieldCounts: Record<string, number> = {};
      for (const k of REMOVAL_ENRICH_VERIFY_FIELDS) enrichPayloadFieldCounts[k] = 0;

      const removalShipmentsUpsertRowCount = archiveRows.length;
      await deleteFromStaging(ids, orgId);

      verifyAStagingFetched += stagingRows.length;
      verifyAShipmentWritten += removalShipmentsUpsertRowCount;
      verifyAStagingDeleted += ids.length;
      verifyBEnrichConsidered += enrichmentShipmentsConsidered;
      verifyBEnrichMatched += enrichmentRemovalsMatched;
      verifyBEnrichUpdatedOk += enrichmentRemovalsUpdated;
      for (const fk of REMOVAL_ENRICH_VERIFY_FIELDS) {
        verifyBEnrichFieldInPayload[fk] += enrichPayloadFieldCounts[fk];
      }

      console.log(
        JSON.stringify({
          checkpoint: "REMOVAL_PIPELINE",
          stage: "AB_per_batch",
          upload_id: uploadId,
          organization_id: orgId,
          store_id: storeId,
          batch_index: removalShipmentBatchIndex,
          A_sync: {
            staging_rows_fetched: stagingRows.length,
            shipment_rows_upserted: removalShipmentsUpsertRowCount,
            shipment_rows_skipped_cross_upload_duplicate: rawRowsSkippedCrossUpload,
            shipment_archive_rows_attempted: shipmentArchiveRowsAttempted,
            shipment_archive_rows_written: shipmentArchiveRowsWritten,
            staging_rows_deleted: ids.length,
          },
          B_enrichment: {
            legacy_bypassed: true,
            note: "Phase 4 Generic runs rebuild_shipment_tree + expected_packages enrich — not invoked in Phase 3.",
            shipment_rows_considered: enrichmentShipmentsConsidered,
            removal_rows_matched: enrichmentRemovalsMatched,
            removal_rows_updated: enrichmentRemovalsUpdated,
            removal_updates_including_field: enrichPayloadFieldCounts,
          },
        }),
      );
    }

    const stagingCountReconciles = totalStagingRows <= 0 || archived === totalStagingRows;

    const pStoreIdForRpc = isUuidString(storeId) ? storeId : null;
    console.log(
      `[REMOVAL_SHIPMENT] raw archive sync completed organization_id=${orgId} p_store_id=${pStoreIdForRpc ?? "null"} ` +
        `(shipment tree + expected_packages deferred to Phase 4 Generic)`,
    );

    console.log(
      JSON.stringify({
        checkpoint: "REMOVAL_PIPELINE",
        stage: "AB_upload_totals",
        upload_id: uploadId,
        organization_id: orgId,
        store_id: storeId,
        A_sync_totals: {
          staging_rows_fetched: verifyAStagingFetched,
          shipment_rows_upserted: verifyAShipmentWritten,
          shipment_rows_skipped_cross_upload_duplicate: rawRowsSkippedCrossUpload,
          shipment_archive_rows_attempted: shipmentArchiveRowsAttempted,
          shipment_archive_rows_written: shipmentArchiveRowsWritten,
          staging_rows_deleted: verifyAStagingDeleted,
          initial_staging_row_count: totalStagingRows,
          staging_rows_processed: archived,
          staging_fetch_equals_delete: verifyAStagingFetched === verifyAStagingDeleted,
        },
        B_enrichment_totals: {
          legacy_bypassed: true,
          shipment_rows_considered: verifyBEnrichConsidered,
          removal_rows_matched: verifyBEnrichMatched,
          removal_rows_updated: verifyBEnrichUpdatedOk,
          removal_updates_including_field: verifyBEnrichFieldInPayload,
        },
        integrity: {
          staging_rows_not_skipped_by_traversal: stagingCountReconciles,
          mapper_null_rows: mapperNull,
        },
      }),
    );

    console.log(
      JSON.stringify({
        phase: "REMOVAL_SHIPMENT_wave1_reconciliation",
        store_id: storeId,
        shipment_lines_processed: archived,
        shipment_lines_upserted: shipmentArchiveRowsWritten,
        skipped_cross_upload_duplicate: rawRowsSkippedCrossUpload,
        removals_tracking_updates: removalsUpdated,
        mapper_null: mapperNull,
      }),
    );
    console.log(
      `[REMOVAL_SHIPMENT] Done: processed=${archived} upserted=${shipmentArchiveRowsWritten} ` +
        `skipped_cross_upload=${rawRowsSkippedCrossUpload} mapper_null=${mapperNull}`,
    );

    return {
      synced: archived,
      mapperNullCount: mapperNull,
      rawRowsWritten: shipmentArchiveRowsWritten,
      rawRowsSkippedCrossUpload,
    };
  } finally {
    await releaseRemovalPipelineLock(uploadId);
  }
}

async function audit(
  orgId: string,
  action: string,
  entityId: string,
  detail?: Record<string, unknown>,
): Promise<void> {
  await supabaseServer.from("raw_report_import_audit").insert({
    organization_id: orgId,
    user_profile_id: null,
    action,
    entity_id: entityId,
    detail: detail ?? null,
  });
}

/** Write a "failed" status back to the upload row (best-effort, never throws). */
async function markFailed(uploadId: string, orgId: string, message: string): Promise<void> {
  try {
    const { data: prevRow } = await supabaseServer
      .from("raw_report_uploads")
      .select("metadata")
      .eq("id", uploadId)
      .maybeSingle();

    await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "failed",
        metadata: mergeUploadMetadata(
          (prevRow as { metadata?: unknown } | null)?.metadata,
          { error_message: message, failed_phase: "sync", import_metrics: { current_phase: "failed", failure_reason: message } },
        ),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId);

    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: uploadId,
        organization_id: orgId,
        status: "failed",
        current_phase: "failed",
        phase_key: FPS_KEY_FAILED,
        phase_label: message.slice(0, 200),
        next_action_key: null,
        next_action_label: null,
        error_message: message,
      },
      { onConflict: "upload_id" },
    );
  } catch (inner) {
    console.error("[sync] markFailed write error:", inner);
  }
}

export async function POST(req: Request): Promise<Response> {
  let uploadIdForFail: string | null = null;
  let orgId = "";

  try {
    const body = (await req.json()) as Body;
    const uploadId = typeof body.upload_id === "string" ? body.upload_id.trim() : "";
    if (!isUuidString(uploadId)) {
      return NextResponse.json({ ok: false, error: "Invalid upload_id." }, { status: 400 });
    }
    uploadIdForFail = uploadId;

    const { data: row, error: fetchErr } = await supabaseServer
      .from("raw_report_uploads")
      .select("id, organization_id, metadata, status, report_type, column_mapping, file_name")
      .eq("id", uploadId)
      .maybeSingle();

    if (fetchErr || !row) {
      return NextResponse.json({ ok: false, error: "Upload session not found." }, { status: 404 });
    }

    orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
    if (!isUuidString(orgId)) {
      return NextResponse.json(
        { ok: false, error: "Invalid upload row (organization_id)." },
        { status: 500 },
      );
    }

    const status = String((row as { status?: unknown }).status ?? "");
    if (status !== "staged" && status !== "failed" && status !== "processing") {
      return NextResponse.json(
        {
          ok: false,
          error: `Phase 3 (Sync) requires status "staged" (or "failed"/"processing" for retry). Current status is "${status}".${
            status === "mapped" || status === "ready"
              ? " Run Phase 2 (Process) first."
              : status === "needs_mapping"
                ? ' Use "Map Columns" first, then Process, then Sync.'
                : status === "raw_synced" || status === "synced" || status === "complete"
                  ? " Sync has already completed for this upload."
                  : ""
          }`,
        },
        { status: 409 },
      );
    }

    const kind = resolveAmazonImportSyncKind((row as { report_type?: string }).report_type);
    const reportTypeRawEarly = String((row as { report_type?: string }).report_type ?? "").trim();

    if (kind === "UNKNOWN") {
      return NextResponse.json(
        {
          ok: false,
          error:
            "Cannot sync: report type is not set. " +
            "Open the History table, set the correct report type from the dropdown, then re-run Process and Sync.",
        },
        { status: 422 },
      );
    }

    const engine = resolveAmazonImportEngineConfig(kind);
    const hasDomainTable = DOMAIN_TABLE[kind] !== null;

    const columnMapping =
      (row as { column_mapping?: unknown }).column_mapping &&
      typeof (row as { column_mapping?: unknown }).column_mapping === "object" &&
      !Array.isArray((row as { column_mapping?: unknown }).column_mapping)
        ? ((row as { column_mapping?: unknown }).column_mapping as Record<string, string>)
        : null;

    const meta = (row as { metadata?: unknown }).metadata;
    const sourceFileSha256 = resolveSourceFileSha256(meta, uploadId);

    const importStoreId = resolveImportStoreId(meta);
    if ((kind === "REMOVAL_ORDER" || kind === "REMOVAL_SHIPMENT") && !importStoreId) {
      return NextResponse.json(
        {
          ok: false,
          error:
            "Imports Target Store is required for removal reports. Choose a target store in the importer, save classification, then run Sync again.",
        },
        { status: 422 },
      );
    }

    // ── Optimistic lock — prevents concurrent clicks from double-syncing ───────
    const { data: locked, error: lockErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "processing",
        metadata: mergeUploadMetadata(meta, {
          process_progress: 0,
          sync_progress: 0,
          etl_phase: "sync",
          error_message: "",
        }),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId)
      .in("status", ["staged", "failed"])
      .select("id");

    if (lockErr) {
      return NextResponse.json({ ok: false, error: lockErr.message }, { status: 500 });
    }
    if (!locked || locked.length === 0) {
      return NextResponse.json(
        {
          ok: false,
          error: "Upload is not in a syncable state (another operation may be running).",
        },
        { status: 409 },
      );
    }

    await audit(orgId, "import.sync_started", uploadId, {
      fileName: (row as { file_name?: string }).file_name,
      kind,
      domainTable: DOMAIN_TABLE[kind] ?? "none",
    });

    // REMOVAL_ORDER: same bytes re-uploaded → remove prior upload rows that share `metadata.content_sha256`.
    // REMOVAL_SHIPMENT: append-safe by default — same-file purge only when metadata flag is set (destructive).
    const replaceRemovalShipmentSameFileSha =
      meta &&
      typeof meta === "object" &&
      !Array.isArray(meta) &&
      (meta as Record<string, unknown>).removal_shipment_replace_same_file_sha === true;

    if (kind === "REMOVAL_ORDER") {
      const rep = await removeOlderRemovalImportsWithSameFileContent(orgId, uploadId, meta, "REMOVAL_ORDER");
      if (rep.ok && rep.removedUploadIds.length > 0) {
        console.log(
          `[sync][REMOVAL_ORDER] Replaced ${rep.removedUploadIds.length} prior import(s) with the same file (SHA-256).`,
        );
        await audit(orgId, "import.removal_same_file_replaced_prior_uploads", uploadId, {
          removed_upload_ids: rep.removedUploadIds,
        });
      } else if (!rep.ok) {
        throw new Error(`Same-file cleanup failed: ${rep.error}`);
      }
    } else if (kind === "REMOVAL_SHIPMENT") {
      if (!replaceRemovalShipmentSameFileSha) {
        console.log(
          "[sync][REMOVAL_SHIPMENT] Same-file SHA purge skipped (append-safe default). " +
            "Set metadata.removal_shipment_replace_same_file_sha=true on the upload to delete prior imports with identical content_sha256.",
        );
      } else {
        const rep = await removeOlderRemovalImportsWithSameFileContent(
          orgId,
          uploadId,
          meta,
          "REMOVAL_SHIPMENT",
        );
        if (rep.ok && rep.removedUploadIds.length > 0) {
          console.log(
            `[sync][REMOVAL_SHIPMENT] Replaced ${rep.removedUploadIds.length} prior import(s) with the same file (SHA-256) — explicit replace flag.`,
          );
          await audit(orgId, "import.removal_same_file_replaced_prior_uploads", uploadId, {
            removed_upload_ids: rep.removedUploadIds,
          });
        } else if (!rep.ok) {
          throw new Error(`Same-file cleanup failed: ${rep.error}`);
        }
      }
    }

    const { count: stagingRowCount } = await supabaseServer
      .from(STAGING_TABLE)
      .select("*", { count: "exact", head: true })
      .eq("upload_id", uploadId)
      .eq("organization_id", orgId);

    const totalStagingRows = typeof stagingRowCount === "number" ? stagingRowCount : 0;

    // ── REPORTS_REPOSITORY pre-flight: assert physical-line integrity in staging ─
    // Throws a clear, structured error if the staging table is missing rows,
    // has duplicate row_numbers, or has gaps. Logs (does not fail) the count
    // of duplicate source_line_hash rows so the operator can see them — those
    // are kept on purpose to preserve Principal/FBA Fee/Commission granularity.
    if (kind === "REPORTS_REPOSITORY") {
      await assertReportsRepoStagingPhysicalIntegrity(orgId, uploadId);
    }
    const syncUpserted = { value: 0 };

    const syncMetricTotals: BatchUpsertMetricDelta = {
      rows_synced_new: 0,
      rows_synced_updated: 0,
      rows_synced_unchanged: 0,
      rows_duplicate_against_existing: 0,
    };

    const duplicateInBatchRef = { value: 0 };
    const syncProgressBase: SyncProgressOpts = {
      uploadId,
      orgId,
      totalStagingRows,
      upserted: syncUpserted,
      metricTotals: syncMetricTotals,
      duplicateInBatchTotal: duplicateInBatchRef,
      engine,
      reportType: reportTypeRawEarly,
    };

    console.log(`[sync][${kind}] Starting sync — staging rows: ${totalStagingRows}, domain table: ${DOMAIN_TABLE[kind] ?? "none"}`);

    // ── Phase 3 core: read → map → upsert → delete (strictly sequenced) ───────
    //
    // STAGING PRESERVATION RULE:
    //   Staging rows are deleted only AFTER their corresponding domain batch is
    //   confirmed written.  If flushDomainBatch() throws at any point, the
    //   remaining staging rows are left untouched so the user can retry.
    //
    // REMOVAL_SHIPMENT: dedicated path — full raw rows → amazon_removal_shipments only (Phase 4 Generic: tree + expected_packages).
    //
    // Errors propagate immediately — no swallowing, no silent fallbacks.
    let synced = 0;
    // Tracks staging rows where the mapper returned null (missing required anchor
    // field). These are removed from staging in the final cleanup but are never
    // written to the domain table — logged as a warning so data loss is visible.
    let mapperNullCount = 0;
    let removalShipmentRawWritten = 0;
    let removalShipmentSkippedCross = 0;
    let removalShipmentLinesForGeneric = 0;
    /** Staging lines merged in JS because they shared a conflict key in the same Postgres batch. */

    const rawTargetLabel = engine.sync_target_table ?? DOMAIN_TABLE[kind] ?? "domain";
    const syncLabelStart = fpsLabelSync(engine.sync_target_table);
    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: uploadId,
        organization_id: orgId,
        status: "syncing",
        current_phase: "sync",
        phase_key: FPS_KEY_SYNC,
        phase_label: syncLabelStart,
        current_phase_label: syncLabelStart,
        next_action_key: null,
        next_action_label: null,
        stage_target_table: engine.stage_target_table,
        sync_target_table: engine.sync_target_table,
        generic_target_table: engine.generic_target_table,
        current_target_table: rawTargetLabel,
        sync_pct: 0,
        upload_pct: 100,
        process_pct: 100,
        phase1_upload_pct: 100,
        phase2_stage_pct: 100,
        phase3_raw_sync_pct: 0,
        phase4_generic_pct: 0,
        total_rows: totalStagingRows,
        processed_rows: 0,
        staged_rows_written: totalStagingRows,
        phase3_status: "running",
        phase3_started_at: new Date().toISOString(),
      },
      { onConflict: "upload_id" },
    );

    logImportPhase({
      report_type: reportTypeRawEarly,
      upload_id: uploadId,
      phase: FPS_KEY_SYNC,
      phase_key: FPS_KEY_SYNC,
      rows_processed: 0,
      rows_written: 0,
      target_table: rawTargetLabel,
    });

    if (kind === "REMOVAL_SHIPMENT") {
      const r = await runRemovalShipmentSync({
        uploadId,
        orgId,
        storeId: importStoreId!,
        totalStagingRows,
        columnMapping,
        syncUpserted,
        engine,
        reportType: reportTypeRawEarly,
        duplicateInBatchTotal: duplicateInBatchRef,
      });
      synced = r.synced;
      mapperNullCount = r.mapperNullCount;
      removalShipmentRawWritten = r.rawRowsWritten;
      removalShipmentSkippedCross = r.rawRowsSkippedCrossUpload;
    } else while (true) {
      // ── Read next chunk: always from the start — prior rows were deleted from staging. ──
      const { data: stagingRows, error: readErr } = await supabaseServer
        .from(STAGING_TABLE)
        .select("id, row_number, raw_row, source_line_hash")
        .eq("upload_id", uploadId)
        .eq("organization_id", orgId)
        .range(0, STAGING_READ_BATCH - 1);

      if (readErr) throw new Error(`Staging read failed: ${readErr.message}`);
      if (!stagingRows || stagingRows.length === 0) break;

      const domainBatch: Record<string, unknown>[] = [];
      const batchStagingIds: string[] = [];

      for (const sr of stagingRows as {
        id: string;
        row_number: number;
        raw_row: Record<string, string>;
        source_line_hash?: string;
      }[]) {
        if (hasDomainTable) {
          const rawRow = (sr.raw_row ?? {}) as Record<string, string>;
          const mappedRow = applyColumnMappingToRow(rawRow, columnMapping);

          let insertRow: Record<string, unknown> | null = null;

          if (isListingAmazonSyncKind(kind)) {
            insertRow = mapStagingMappedRowToListingRawInsert({
              mappedRow,
              organizationId: orgId,
              storeId: importStoreId,
              sourceUploadId: uploadId,
              sourceReportType: reportTypeRawEarly,
              fileLineNumber: sr.row_number,
              stagingSourceLineHash: String(sr.source_line_hash ?? ""),
            });
          } else if (kind === "FBA_RETURNS") {
            insertRow = mapRowToAmazonReturn(mappedRow, orgId, uploadId) as Record<string, unknown>;
          } else if (kind === "REMOVAL_ORDER") {
            insertRow = mapRowToAmazonRemoval(mappedRow, orgId, uploadId, importStoreId!) as Record<string, unknown> | null;
            if (insertRow) insertRow.source_staging_id = sr.id;
          } else if (kind === "INVENTORY_LEDGER") {
            insertRow = mapRowToAmazonInventoryLedger(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "REIMBURSEMENTS") {
            insertRow = mapRowToAmazonReimbursement(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "SETTLEMENT") {
            insertRow = mapRowToAmazonSettlement(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "SAFET_CLAIMS") {
            insertRow = mapRowToAmazonSafetClaim(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "TRANSACTIONS") {
            insertRow = mapRowToAmazonTransaction(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "REPORTS_REPOSITORY") {
            insertRow = mapRowToAmazonReportsRepository(mappedRow, orgId, uploadId) as Record<
              string,
              unknown
            > | null;
          } else if (kind === "MANAGE_FBA_INVENTORY") {
            insertRow = mapRowToAmazonManageFbaInventory(
              mappedRow,
              orgId,
              uploadId,
              importStoreId,
            ) as Record<string, unknown> | null;
          } else if (kind === "FBA_INVENTORY") {
            insertRow = mapRowToAmazonFbaInventory(
              mappedRow,
              orgId,
              uploadId,
              importStoreId,
            ) as Record<string, unknown> | null;
          } else if (kind === "INBOUND_PERFORMANCE") {
            insertRow = mapRowToAmazonInboundPerformance(
              mappedRow,
              orgId,
              uploadId,
              importStoreId,
            ) as Record<string, unknown> | null;
          } else if (kind === "AMAZON_FULFILLED_INVENTORY") {
            insertRow = mapRowToAmazonAmazonFulfilledInventory(
              mappedRow,
              orgId,
              uploadId,
              importStoreId,
            ) as Record<string, unknown> | null;
          } else if (
            kind === "ALL_ORDERS" ||
            kind === "REPLACEMENTS" ||
            kind === "FBA_GRADE_AND_RESELL" ||
            kind === "RESERVED_INVENTORY" ||
            kind === "FEE_PREVIEW" ||
            kind === "MONTHLY_STORAGE_FEES"
          ) {
            insertRow = mapRowToAmazonRawArchive(mappedRow, orgId, uploadId, importStoreId) as Record<string, unknown> | null;
          }

          if (insertRow && kind !== "REMOVAL_ORDER") {
            attachPhysicalRowIdentity(insertRow, sr.row_number, sourceFileSha256);
          }

          if (insertRow) {
            domainBatch.push(insertRow);
            batchStagingIds.push(sr.id);
          } else {
            // Mapper returned null — required anchor field (order_id, lpn, etc.) is
            // missing. Count these so we can warn after the loop.
            mapperNullCount++;
          }

          // ── Flush domain batch at BATCH_SIZE ─────────────────────────────
          // On error flushDomainBatch() throws.  The staging IDs for THIS
          // batch are not yet in batchStagingIds_flushed so they are preserved.
          if (domainBatch.length >= BATCH_SIZE) {
            const inputBatch = domainBatch.splice(0, BATCH_SIZE);
            const stagingIdsForBatch = batchStagingIds.splice(0, inputBatch.length);
            const flushResult = await flushDomainBatch(kind, inputBatch, syncProgressBase, syncMetricTotals);
            synced += flushResult.flushed;
            duplicateInBatchRef.value += flushResult.collapsedInBatch;
            await deleteFromStaging(stagingIdsForBatch, orgId);
          }
        } else {
          // Recognized kind but no domain table — acknowledge as synced
          synced += 1;
          batchStagingIds.push(sr.id);
        }
      }

      // ── Flush remainder of this staging page ───────────────────────────────
      if (domainBatch.length > 0) {
        const inputBatch = domainBatch.splice(0);
        const stagingIdsForBatch = batchStagingIds.splice(0, inputBatch.length);
        const flushResult = await flushDomainBatch(kind, inputBatch, syncProgressBase, syncMetricTotals);
        synced += flushResult.flushed;
        duplicateInBatchRef.value += flushResult.collapsedInBatch;
        await deleteFromStaging(stagingIdsForBatch, orgId);
      }

      // Acknowledge no-domain rows
      if (batchStagingIds.length > 0) {
        await deleteFromStaging(batchStagingIds, orgId);
      }
    }

    // ── Row-count verification ────────────────────────────────────────────────
    // mapper-null rows are dropped; JS dedup further reduces rows within a batch.
    // Both are expected in different circumstances — surfaced here for visibility.
    if (mapperNullCount > 0) {
      if (kind === "REMOVAL_SHIPMENT") {
        console.warn(
          `[sync][REMOVAL_SHIPMENT] ${mapperNullCount} staging row(s) could not be mapped ` +
            `(missing order_id / anchor); those lines were still archived to amazon_removal_shipments with raw_row only. ` +
            `Phase 3 does not touch amazon_removals or expected_packages.`,
        );
      } else {
        console.warn(
          `[sync][${kind}] WARNING: ${mapperNullCount} staging row(s) were not written to ` +
            `${DOMAIN_TABLE[kind] ?? "domain table"} because the mapper could not find a required ` +
            `anchor field (order_id, lpn, etc.). ` +
            `These rows are removed from staging but have NO domain table entry.`,
        );
      }
    }
    if (kind !== "REMOVAL_SHIPMENT") {
      const jsDedupedAway = totalStagingRows - synced - mapperNullCount;
      if (jsDedupedAway > 0) {
        console.log(
          `[sync][${kind}] ${jsDedupedAway} staging row(s) were merged by within-batch deduplication ` +
            `(same conflict key in batch) — this is expected for Amazon reports that repeat rows.`,
        );
      }
      console.log(
        `[sync][${kind}] Row count summary: staging=${totalStagingRows} ` +
          `written=${synced} mapper_null=${mapperNullCount} deduped_in_batch=${Math.max(0, jsDedupedAway)}`,
      );
      if (isListingAmazonSyncKind(kind)) {
        const archived = syncMetricTotals.rows_synced_new + syncMetricTotals.rows_synced_updated;
        const skippedExisting = syncMetricTotals.rows_duplicate_against_existing;
        console.log(
          `[sync][${kind}] Phase 3 listing raw summary: raw archived ${archived}, raw skipped_existing ${skippedExisting}`,
        );
      }
    } else {
      console.log(
        `[sync][REMOVAL_SHIPMENT] Row count summary: staging=${totalStagingRows} ` +
          `staging_lines_processed=${synced} raw_upserted=${removalShipmentRawWritten} ` +
          `skipped_cross_upload_duplicate=${removalShipmentSkippedCross} mapper_null=${mapperNullCount}`,
      );
    }

    if (kind === "REMOVAL_SHIPMENT") {
      const { count: shipLineCnt } = await supabaseServer
        .from("amazon_removal_shipments")
        .select("*", { count: "exact", head: true })
        .eq("upload_id", uploadId)
        .eq("organization_id", orgId);
      removalShipmentLinesForGeneric = typeof shipLineCnt === "number" ? shipLineCnt : removalShipmentRawWritten;
    }

    // ── Final safety cleanup: delete any residual staging rows for this upload ─
    // Rows whose mapper returned null (e.g. missing required fields) are never
    // added to domainBatch/batchStagingIds, so they would survive the loop.
    // This single DELETE by upload_id catches all of them without touching
    // rows belonging to other concurrent uploads.
    {
      const { error: cleanupErr } = await supabaseServer
        .from(STAGING_TABLE)
        .delete()
        .eq("upload_id", uploadId)
        .eq("organization_id", orgId);
      if (cleanupErr) {
        console.warn("[sync] final staging cleanup warning:", cleanupErr.message);
        // Non-fatal: domain data is already written; stale staging rows will be
        // cleaned up on the next Process run for this upload.
      }
    }

    // ── Mark upload as synced ─────────────────────────────────────────────────
    const { data: prevRow } = await supabaseServer
      .from("raw_report_uploads")
      .select("metadata")
      .eq("id", uploadId)
      .maybeSingle();

    const syncCollapsedByDedupe =
      kind === "REMOVAL_SHIPMENT"
        ? removalShipmentSkippedCross
        : Math.max(0, totalStagingRows - synced - mapperNullCount);

    const needsPhase4 = requiresPhase4Generic(kind);
    const finalRawW =
      kind === "REMOVAL_SHIPMENT"
        ? removalShipmentRawWritten
        : syncMetricTotals.rows_synced_new +
          syncMetricTotals.rows_synced_updated +
          syncMetricTotals.rows_synced_unchanged;
    const finalRawSkip =
      kind === "REMOVAL_SHIPMENT"
        ? removalShipmentSkippedCross
        : syncMetricTotals.rows_duplicate_against_existing;
    const finalPhase3Pct = fpsPctPhase3(finalRawW, finalRawSkip, totalStagingRows);
    const importMetrics: ImportRunMetrics = {
      physical_lines_seen: totalStagingRows,
      data_rows_seen: totalStagingRows,
      rows_staged: totalStagingRows,
      rows_synced_upserted: kind === "REMOVAL_SHIPMENT" ? removalShipmentRawWritten : synced,
      rows_mapper_invalid: mapperNullCount,
      rows_duplicate_in_file: duplicateInBatchRef.value,
      rows_net_collapsed_vs_staging: syncCollapsedByDedupe,
      rows_synced_new: syncMetricTotals.rows_synced_new,
      rows_synced_updated: syncMetricTotals.rows_synced_updated,
      rows_synced_unchanged: syncMetricTotals.rows_synced_unchanged,
      rows_duplicate_against_existing:
        kind === "REMOVAL_SHIPMENT"
          ? removalShipmentSkippedCross
          : syncMetricTotals.rows_duplicate_against_existing,
      current_phase: needsPhase4 ? "raw_synced" : "complete",
    };

    const wave1Extra =
      kind === "REMOVAL_ORDER" || kind === "REMOVAL_SHIPMENT"
        ? {
            wave1_import_store_id: importStoreId,
            wave1_sync_reconciliation: {
              kind,
              staging_row_count: totalStagingRows,
              domain_rows_written: kind === "REMOVAL_SHIPMENT" ? removalShipmentRawWritten : synced,
              mapper_null: mapperNullCount,
              collapsed_by_business_dedupe: syncCollapsedByDedupe,
              ...(kind === "REMOVAL_SHIPMENT"
                ? {
                    removal_shipment_skipped_cross_upload: removalShipmentSkippedCross,
                    removal_shipment_lines_for_generic: removalShipmentLinesForGeneric,
                  }
                : {}),
            },
          }
        : {};

    const finalStatus = needsPhase4 ? "raw_synced" : "synced";
    const { error: markErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: finalStatus,
        import_pipeline_completed_at: needsPhase4 ? null : new Date().toISOString(),
        metadata: mergeUploadMetadata(
          (prevRow as { metadata?: unknown } | null)?.metadata,
          {
            row_count: kind === "REMOVAL_SHIPMENT" ? removalShipmentRawWritten : synced,
            staging_row_count: totalStagingRows,
            sync_row_count: kind === "REMOVAL_SHIPMENT" ? removalShipmentRawWritten : synced,
            sync_mapper_null_count: mapperNullCount,
            sync_collapsed_by_dedupe: syncCollapsedByDedupe,
            sync_duplicate_in_batch_rows: duplicateInBatchRef.value,
            import_metrics: importMetrics,
            process_progress: 100,
            sync_progress: 100,
            etl_phase: needsPhase4 ? "raw_synced" : "sync",
            ...(kind === "REMOVAL_SHIPMENT"
              ? {
                  removal_shipment_phase3_raw_written: removalShipmentRawWritten,
                  removal_shipment_phase3_skipped_cross_upload: removalShipmentSkippedCross,
                  removal_shipment_lines_for_generic: removalShipmentLinesForGeneric,
                }
              : {}),
            ...(isListingAmazonSyncKind(kind)
              ? { catalog_listing_import_phase: "raw_archived" as const }
              : {}),
            error_message: "",
            ...wave1Extra,
          },
        ),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId);

    if (markErr) throw new Error(`Sync succeeded but failed to save status: ${markErr.message}`);

    const domainLabel = DOMAIN_TABLE[kind] ?? "none";

    let rowsEligibleGeneric = 0;
    if (isListingAmazonSyncKind(kind)) {
      const { count } = await supabaseServer
        .from("amazon_listing_report_rows_raw")
        .select("*", { count: "exact", head: true })
        .eq("organization_id", orgId)
        .eq("source_upload_id", uploadId);
      rowsEligibleGeneric = typeof count === "number" ? count : 0;
    } else if (kind === "INVENTORY_LEDGER") {
      const { count } = await supabaseServer
        .from("amazon_inventory_ledger")
        .select("*", { count: "exact", head: true })
        .eq("organization_id", orgId)
        .eq("upload_id", uploadId);
      rowsEligibleGeneric = typeof count === "number" ? count : 0;
    } else if (kind === "SETTLEMENT" || kind === "TRANSACTIONS" || kind === "REIMBURSEMENTS") {
      const t = DOMAIN_TABLE[kind];
      if (t) {
        const { count } = await supabaseServer
          .from(t)
          .select("*", { count: "exact", head: true })
          .eq("organization_id", orgId)
          .eq("upload_id", uploadId);
        rowsEligibleGeneric = typeof count === "number" ? count : 0;
      }
    } else if (kind === "REMOVAL_SHIPMENT") {
      rowsEligibleGeneric = removalShipmentLinesForGeneric;
    }

    const syncLabelDone = fpsLabelSync(engine.sync_target_table);
    const nextGeneric = fpsNextAfterSync(engine.supports_generic);
    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: uploadId,
        organization_id: orgId,
        status: needsPhase4 ? "processing" : "complete",
        current_phase: needsPhase4 ? "raw_synced" : "complete",
        phase_key: needsPhase4 ? FPS_KEY_SYNC : FPS_KEY_COMPLETE,
        phase_label: needsPhase4 ? syncLabelDone : FPS_LABEL_COMPLETE,
        next_action_key: nextGeneric,
        next_action_label: needsPhase4 ? FPS_NEXT_ACTION_LABEL_GENERIC : null,
        current_phase_label: needsPhase4 ? syncLabelDone : FPS_LABEL_COMPLETE,
        stage_target_table: engine.stage_target_table,
        sync_target_table: engine.sync_target_table,
        generic_target_table: engine.generic_target_table,
        current_target_table: domainLabel,
        upload_pct: 100,
        process_pct: 100,
        sync_pct: 100,
        phase1_upload_pct: 100,
        phase2_stage_pct: 100,
        phase3_raw_sync_pct: finalPhase3Pct,
        /**
         * Phase 5 contract:
         *   • supports_generic=true  → leave Phase 4 at 0% pending until Generic runs.
         *   • supports_generic=false → Phase 4 is an explicit no-op; mark 100% complete
         *     so the UI shows the 5-phase pipeline as done (not "pending forever").
         */
        phase4_generic_pct: needsPhase4 ? 0 : 100,
        phase4_status: needsPhase4 ? "pending" : "complete",
        phase4_completed_at: needsPhase4 ? null : new Date().toISOString(),
        processed_rows: totalStagingRows,
        total_rows: totalStagingRows,
        raw_rows_written: kind === "REMOVAL_SHIPMENT" ? removalShipmentRawWritten : finalRawW,
        raw_rows_skipped_existing:
          kind === "REMOVAL_SHIPMENT"
            ? removalShipmentSkippedCross
            : syncMetricTotals.rows_duplicate_against_existing,
        duplicate_rows_skipped: duplicateInBatchRef.value,
        rows_eligible_for_generic: rowsEligibleGeneric,
        staged_rows_written: totalStagingRows,
        error_message: null,
        import_metrics: importMetrics,
        phase3_status: "complete",
        phase3_completed_at: new Date().toISOString(),
      },
      { onConflict: "upload_id" },
    );

    let inventoryLedgerRanInlineGeneric = false;
    if (kind === "INVENTORY_LEDGER" && needsPhase4) {
      await completeInventoryLedgerProductIdentifierMapPhase({
        supabase: supabaseServer,
        organizationId: orgId,
        uploadId,
        storeId: importStoreId ?? null,
        reportTypeRaw: reportTypeRawEarly,
        engine,
      });
      inventoryLedgerRanInlineGeneric = true;
    }

    await audit(orgId, "import.sync_completed", uploadId, {
      rowsSynced: kind === "REMOVAL_SHIPMENT" ? removalShipmentRawWritten : synced,
      kind,
      domainTable: DOMAIN_TABLE[kind] ?? "none",
    });

    const syncLogPhaseKey =
      inventoryLedgerRanInlineGeneric || !needsPhase4 ? FPS_KEY_COMPLETE : FPS_KEY_SYNC;

    logImportPhase({
      report_type: reportTypeRawEarly,
      upload_id: uploadId,
      phase: syncLogPhaseKey,
      phase_key: syncLogPhaseKey,
      target_table: engine.sync_target_table ?? domainLabel,
      rows_written: kind === "REMOVAL_SHIPMENT" ? removalShipmentRawWritten : finalRawW,
      rows_skipped_existing: finalRawSkip,
      duplicates_skipped: duplicateInBatchRef.value,
      rows_processed: kind === "REMOVAL_SHIPMENT" ? removalShipmentRawWritten : synced,
    });

    return NextResponse.json({
      ok: true,
      rowsSynced: kind === "REMOVAL_SHIPMENT" ? removalShipmentRawWritten : synced,
      rowsSkippedCrossUploadDuplicate:
        kind === "REMOVAL_SHIPMENT" ? removalShipmentSkippedCross : undefined,
      kind,
      ...(isListingAmazonSyncKind(kind)
        ? {
            listingPhase3: {
              rawArchived: syncMetricTotals.rows_synced_new + syncMetricTotals.rows_synced_updated,
              rawSkippedExisting: syncMetricTotals.rows_duplicate_against_existing,
            },
          }
        : {}),
    });

  } catch (e) {
    const message = e instanceof Error ? e.message : "Sync failed.";
    console.error("[sync] error:", message);

    // ── Write failed status back — staging rows are NOT touched ───────────────
    // Any staging rows not yet deleted remain intact so the user can retry
    // Phase 3 after fixing the underlying issue.
    if (uploadIdForFail && isUuidString(uploadIdForFail) && isUuidString(orgId)) {
      await markFailed(uploadIdForFail, orgId, message);
    }

    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

// =============================================================================
// ── Helpers ───────────────────────────────────────────────────────────────────
// =============================================================================

/**
 * Deletes staging rows by ID in chunks of 200.
 * Only called AFTER the corresponding domain batch has been confirmed written.
 */
async function deleteFromStaging(ids: string[], organizationId: string): Promise<void> {
  if (ids.length === 0) return;
  const chunk_size = 200;
  for (let i = 0; i < ids.length; i += chunk_size) {
    const chunk = ids.slice(i, i + chunk_size);
    const { error } = await supabaseServer
      .from(STAGING_TABLE)
      .delete()
      .in("id", chunk)
      .eq("organization_id", organizationId);
    if (error) throw new Error(`Staging cleanup failed: ${error.message}`);
  }
}

type SyncProgressOpts = {
  uploadId: string;
  orgId: string;
  totalStagingRows: number;
  /** Cumulative staging lines finished this sync (for progress bar denominator). */
  upserted: { value: number };
  metricTotals?: BatchUpsertMetricDelta;
  duplicateInBatchTotal?: { value: number };
  engine?: AmazonImportEngineConfig;
  reportType?: string;
};

async function bumpSyncProgressMetadata(
  opts: SyncProgressOpts,
  chunkRowCount: number,
  removalShipment?: { rawRowsWritten: number; rawRowsSkippedCrossUpload: number },
): Promise<void> {
  if (opts.totalStagingRows <= 0 || chunkRowCount <= 0) return;
  opts.upserted.value += chunkRowCount;
  const { data: prevRow } = await supabaseServer
    .from("raw_report_uploads")
    .select("metadata")
    .eq("id", opts.uploadId)
    .maybeSingle();
  const pct = Math.min(
    99,
    Math.round((opts.upserted.value / Math.max(1, opts.totalStagingRows)) * 100),
  );
  const prevMeta = (prevRow as { metadata?: unknown } | null)?.metadata;
  const prevIm =
    prevMeta && typeof prevMeta === "object" && prevMeta !== null && "import_metrics" in prevMeta
      ? {
          ...((prevMeta as { import_metrics?: Record<string, unknown> }).import_metrics ?? {}),
        }
      : {};

  const engine = opts.engine;
  const syncTarget = engine?.sync_target_table ?? null;
  const label = fpsLabelSync(syncTarget);

  let rawW: number;
  let rawSkip: number;
  if (removalShipment) {
    rawW = removalShipment.rawRowsWritten;
    rawSkip = removalShipment.rawRowsSkippedCrossUpload;
  } else if (opts.metricTotals) {
    const m = opts.metricTotals;
    rawW = m.rows_synced_new + m.rows_synced_updated + m.rows_synced_unchanged;
    rawSkip = m.rows_duplicate_against_existing;
  } else {
    rawW = opts.upserted.value;
    rawSkip = 0;
  }
  const dupBatch = opts.duplicateInBatchTotal?.value ?? 0;
  const phase3Pct = fpsPctPhase3(rawW, rawSkip, opts.totalStagingRows);

  await supabaseServer
    .from("raw_report_uploads")
    .update({
      metadata: mergeUploadMetadata(prevMeta, {
        sync_progress: pct,
        etl_phase: "sync",
        import_metrics: {
          ...prevIm,
          current_phase: "sync",
          rows_synced: opts.upserted.value,
          total_staging_rows: opts.totalStagingRows,
          ...(removalShipment
            ? {
                removal_shipment_raw_rows_written: removalShipment.rawRowsWritten,
                removal_shipment_skipped_cross_upload: removalShipment.rawRowsSkippedCrossUpload,
              }
            : {}),
        },
      }),
      updated_at: new Date().toISOString(),
    })
    .eq("id", opts.uploadId)
    .eq("organization_id", opts.orgId);

  await supabaseServer
    .from("file_processing_status")
    .upsert(
      {
        upload_id: opts.uploadId,
        organization_id: opts.orgId,
        status: "syncing",
        current_phase: "sync",
        phase_key: FPS_KEY_SYNC,
        phase_label: label,
        current_phase_label: label,
        next_action_key: null,
        next_action_label: null,
        stage_target_table: engine?.stage_target_table,
        sync_target_table: engine?.sync_target_table,
        generic_target_table: engine?.generic_target_table,
        current_target_table: syncTarget,
        upload_pct: 100,
        process_pct: 100,
        sync_pct: phase3Pct,
        phase1_upload_pct: 100,
        phase2_stage_pct: 100,
        phase3_raw_sync_pct: phase3Pct,
        processed_rows: opts.upserted.value,
        total_rows: opts.totalStagingRows,
        staged_rows_written: opts.totalStagingRows,
        raw_rows_written: rawW,
        raw_rows_skipped_existing: rawSkip,
        duplicate_rows_skipped: dupBatch,
        import_metrics: {
          current_phase: "sync",
          rows_synced: opts.upserted.value,
          total_staging_rows: opts.totalStagingRows,
          ...(removalShipment
            ? {
                removal_shipment_raw_rows_written: removalShipment.rawRowsWritten,
                removal_shipment_skipped_cross_upload: removalShipment.rawRowsSkippedCrossUpload,
              }
            : {}),
        },
      },
      { onConflict: "upload_id" },
    );
}

/** Max business-key samples logged when duplicates are collapsed (REMOVAL_ORDER debug). */
const REMOVAL_ORDER_DEDUPE_LOG_SAMPLES = 8;

/** Ignore lineage + surrogate keys when deciding “unchanged” vs DB row (cross-file re-import). */
const REMOVAL_ORDER_COMPARABLE_VOLATILE = new Set([
  "id",
  "created_at",
  "updated_at",
  "upload_id",
  "source_staging_id",
]);

function removalOrderComparableSnapshot(row: Record<string, unknown>): string {
  const o: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    if (REMOVAL_ORDER_COMPARABLE_VOLATILE.has(k)) continue;
    o[k] = v;
  }
  const keys = Object.keys(o).sort();
  return JSON.stringify(keys.map((k) => [k, o[k]]));
}

function chunkArray<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/**
 * Preload `amazon_removals` for the batch’s `order_id` set, then split:
 * - inserts: business key not present in DB
 * - upserts: key exists and payload differs (lineage excluded) from DB row
 * - skipped: key exists and payload is unchanged → no write
 */
async function partitionRemovalOrderRowsAgainstDatabase(
  orgId: string,
  deduped: Record<string, unknown>[],
): Promise<{
  inserts: Record<string, unknown>[];
  upserts: Record<string, unknown>[];
  skippedUnchanged: number;
}> {
  const orderIds = [
    ...new Set(deduped.map((r) => String(r.order_id ?? "").trim()).filter((x) => x.length > 0)),
  ];
  const existingByBizKey = new Map<string, Record<string, unknown>>();

  for (const part of chunkArray(orderIds, 200)) {
    const { data, error } = await supabaseServer
      .from("amazon_removals")
      .select("*")
      .eq("organization_id", orgId)
      .in("order_id", part);
    if (error) {
      throw new Error(`[REMOVAL_ORDER] preload amazon_removals failed: ${error.message}`);
    }
    for (const row of data ?? []) {
      const rec = row as Record<string, unknown>;
      const recCanon = applyCanonicalRemovalOrderBusinessColumns(rec);
      const k = removalAmazonRemovalsBusinessDedupKey(recCanon);
      if (!existingByBizKey.has(k)) existingByBizKey.set(k, recCanon);
    }
  }

  const insertByBizKey = new Map<string, Record<string, unknown>>();
  const upsertByBizKey = new Map<string, Record<string, unknown>>();
  let skippedUnchanged = 0;

  for (const row of deduped) {
    const canon = applyCanonicalRemovalOrderBusinessColumns(row);
    const k = removalAmazonRemovalsBusinessDedupKey(canon);
    const ex = existingByBizKey.get(k);
    if (!ex) {
      const prev = insertByBizKey.get(k);
      insertByBizKey.set(
        k,
        prev
          ? applyCanonicalRemovalOrderBusinessColumns(mergeRemovalOrderRowsPreferNonNull(prev, canon))
          : canon,
      );
      continue;
    }
    if (removalOrderComparableSnapshot(canon) === removalOrderComparableSnapshot(ex)) {
      skippedUnchanged++;
      continue;
    }
    const prevU = upsertByBizKey.get(k);
    upsertByBizKey.set(
      k,
      prevU
        ? applyCanonicalRemovalOrderBusinessColumns(mergeRemovalOrderRowsPreferNonNull(prevU, canon))
        : canon,
    );
  }

  return {
    inserts: [...insertByBizKey.values()],
    upserts: [...upsertByBizKey.values()],
    skippedUnchanged,
  };
}

/**
 * Final guard: Postgres INSERT rejects duplicate unique keys in one statement.
 * Collapse by `removalAmazonRemovalsBusinessDedupKey` (same as `uq_amazon_removals_business_line`).
 * Always runs before insert/upsert — not only when a Set detects duplicate *strings* (the old guard
 * missed cases where two payloads matched the same DB tuple but keyed differently).
 */
function finalizeRemovalOrderWriteBatchForUniqueness(
  rows: Record<string, unknown>[],
  label: "insert" | "upsert",
): Record<string, unknown>[] {
  let out = rows.map(applyCanonicalRemovalOrderBusinessColumns);
  for (let iter = 0; iter < 8; iter++) {
    const step = dedupeRemovalOrderPayloadsForUpsert(out);
    out = step.rows;
    const u = uniqueBusinessKeyCount(out);
    if (u === out.length) {
      if (step.collapsedCount > 0) {
        console.warn(
          `[REMOVAL_ORDER] finalize ${label}: collapsed ${step.collapsedCount} duplicate row(s); ` +
            `duplicate_key_samples=${JSON.stringify(step.duplicateKeySamples)}`,
        );
      }
      return out;
    }
    const dupKeys = listDuplicateRemovalOrderBusinessKeys(out);
    console.warn(
      `[REMOVAL_ORDER] invariant ${label}: length ${out.length} !== uniqueBusinessKeyCount ${u} — ` +
        `re-collapsing (iter ${iter}); duplicate_business_keys(sample)=${JSON.stringify(dupKeys.slice(0, REMOVAL_ORDER_DEDUPE_LOG_SAMPLES))}`,
    );
  }
  const dupKeys = listDuplicateRemovalOrderBusinessKeys(out);
  throw new Error(
    `[REMOVAL_ORDER] invariant ${label}: cannot collapse to unique business keys after retries; ` +
      `duplicate_business_keys(sample)=${JSON.stringify(dupKeys.slice(0, REMOVAL_ORDER_DEDUPE_LOG_SAMPLES))}`,
  );
}

/**
 * Last line of defense before PostgREST: length must equal distinct `removalAmazonRemovalsBusinessDedupKey` values.
 * If not, log duplicates, collapse with merge rules, canonicalize again, and retry.
 */
function enforceRemovalOrderChunkBusinessKeyInvariant(
  rows: Record<string, unknown>[],
  label: "insert" | "upsert",
): Record<string, unknown>[] {
  let out = rows;
  for (let iter = 0; iter < 8; iter++) {
    const u = uniqueBusinessKeyCount(out);
    if (u === out.length) return out;
    const dupKeys = listDuplicateRemovalOrderBusinessKeys(out);
    console.warn(
      `[REMOVAL_ORDER] ${label} chunk invariant: length ${out.length} !== uniqueBusinessKeyCount ${u} — ` +
        `collapsing (iter ${iter}); duplicate_business_keys(sample)=${JSON.stringify(dupKeys.slice(0, REMOVAL_ORDER_DEDUPE_LOG_SAMPLES))}`,
    );
    const step = dedupeRemovalOrderPayloadsForUpsert(out);
    out = step.rows.map(applyCanonicalRemovalOrderBusinessColumns);
  }
  const u = uniqueBusinessKeyCount(out);
  const dupKeys = listDuplicateRemovalOrderBusinessKeys(out);
  if (u !== out.length) {
    throw new Error(
      `[REMOVAL_ORDER] ${label} chunk: still have duplicate business keys after collapse; ` +
        `length=${out.length} unique=${u} duplicate_business_keys(sample)=${JSON.stringify(dupKeys.slice(0, REMOVAL_ORDER_DEDUPE_LOG_SAMPLES))}`,
    );
  }
  return out;
}

/**
 * Final guard before a PostgREST statement: `finalRows.length === uniqueKeyCount(finalRows)` for the business line.
 * Used for both INSERT and UPSERT chunks so Postgres never sees duplicate `uq_amazon_removals_business_line` keys.
 */
function guardFinalRemovalOrderInsertArray(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  if (rows.length === 0) return rows;
  let out = rows.map(applyCanonicalRemovalOrderBusinessColumns);
  for (let iter = 0; iter < 8; iter++) {
    const u = uniqueKeyCount(out);
    if (u === out.length) {
      return out;
    }
    const dupKeys = listDuplicateRemovalOrderBusinessKeys(out);
    console.warn(
      `[REMOVAL_ORDER] final INSERT array: length ${out.length} !== uniqueKeyCount ${u} — ` +
        `re-collapsing (iter ${iter}); duplicate_business_keys(sample)=${JSON.stringify(dupKeys.slice(0, REMOVAL_ORDER_DEDUPE_LOG_SAMPLES))}`,
    );
    out = dedupeRemovalOrderPayloadsForUpsert(out).rows.map(applyCanonicalRemovalOrderBusinessColumns);
  }
  const u = uniqueKeyCount(out);
  if (out.length !== u) {
    const dupKeys = listDuplicateRemovalOrderBusinessKeys(out);
    throw new Error(
      `[REMOVAL_ORDER] final INSERT invariant: finalRows.length (${out.length}) !== uniqueKeyCount (${u}); ` +
        `duplicate_business_keys(sample)=${JSON.stringify(dupKeys.slice(0, REMOVAL_ORDER_DEDUPE_LOG_SAMPLES))}`,
    );
  }
  return out;
}

async function writeRemovalOrderPartitionedBatches(
  partition: { inserts: Record<string, unknown>[]; upserts: Record<string, unknown>[] },
  conflictKey: string,
  syncProgress?: SyncProgressOpts,
): Promise<void> {
  for (let off = 0; off < partition.inserts.length; off += UPSERT_CHUNK_SIZE) {
    const rawChunk = partition.inserts.slice(off, off + UPSERT_CHUNK_SIZE);
    let chunk = finalizeRemovalOrderWriteBatchForUniqueness(rawChunk, "insert");
    chunk = enforceRemovalOrderChunkBusinessKeyInvariant(chunk, "insert");
    chunk = guardFinalRemovalOrderInsertArray(chunk);
    if (chunk.length === 0) continue;
    const { error } = await supabaseServer.from("amazon_removals").insert(chunk);
    if (error) {
      throw new Error(`[REMOVAL_ORDER] insert into amazon_removals failed: ${error.message} (chunk size: ${chunk.length})`);
    }
    if (syncProgress) await bumpSyncProgressMetadata(syncProgress, chunk.length);
  }

  for (let off = 0; off < partition.upserts.length; off += UPSERT_CHUNK_SIZE) {
    const rawChunk = partition.upserts.slice(off, off + UPSERT_CHUNK_SIZE);
    let chunk = finalizeRemovalOrderWriteBatchForUniqueness(rawChunk, "upsert");
    chunk = enforceRemovalOrderChunkBusinessKeyInvariant(chunk, "upsert");
    chunk = guardFinalRemovalOrderInsertArray(chunk);
    if (chunk.length === 0) continue;
    const { error } = await supabaseServer
      .from("amazon_removals")
      .upsert(chunk, { onConflict: conflictKey, ignoreDuplicates: false });
    if (error) {
      throw new Error(
        `[REMOVAL_ORDER] upsert into amazon_removals failed: ${error.message}` +
          ` (conflict key: ${conflictKey}, chunk size: ${chunk.length})`,
      );
    }
    if (syncProgress) await bumpSyncProgressMetadata(syncProgress, chunk.length);
  }
}

/**
 * Collapse rows that share the same `uq_amazon_removals_business_line` key before a single upsert statement.
 * (Postgres rejects two identical business keys in one INSERT; PostgREST upsert sends one statement per chunk.)
 */
function dedupeRemovalOrderPayloadsForUpsert(packed: Record<string, unknown>[]): {
  rows: Record<string, unknown>[];
  collapsedCount: number;
  duplicateKeySamples: string[];
} {
  const seen = new Map<string, Record<string, unknown>>();
  const duplicateKeySamples: string[] = [];

  for (const row of packed) {
    const canon = applyCanonicalRemovalOrderBusinessColumns(row);
    const key = removalAmazonRemovalsBusinessDedupKey(canon);
    const existing = seen.get(key);
    if (existing) {
      if (duplicateKeySamples.length < REMOVAL_ORDER_DEDUPE_LOG_SAMPLES) {
        duplicateKeySamples.push(key);
      }
      seen.set(
        key,
        applyCanonicalRemovalOrderBusinessColumns(mergeRemovalOrderRowsPreferNonNull(existing, canon)),
      );
    } else {
      seen.set(key, canon);
    }
  }

  const rows = [...seen.values()];
  return {
    rows,
    collapsedCount: Math.max(0, packed.length - rows.length),
    duplicateKeySamples,
  };
}

/**
 * Packs and upserts a batch of mapped domain rows into the correct amazon_ table.
 *
 *  Step 1 — packPayloadForSupabase():
 *    Any key NOT in NATIVE_COLUMNS_MAP[kind] is redirected into the raw_data
 *    JSONB column.  This is the permanent guard against schema cache errors.
 *
 *  Step 2 — deduplication:
 *    REMOVAL_ORDER: `dedupeRemovalOrderPayloadsForUpsert()` (business key for `uq_amazon_removals_business_line`).
 *    FBA_RETURNS: `deduplicateByConflictKey()` + `guardFbaReturnsPhysicalRowBatchUniqueness()` (file row key; LPN not used).
 *    Other kinds: `deduplicateByConflictKey()`.
 *
 *  Step 3 — DB write:
 *    REMOVAL_ORDER: preload by `order_id`, partition into insert (new keys) / upsert (changed) / skip (unchanged).
 *    Other kinds: supabase.upsert({ onConflict }) or insert in chunks of UPSERT_CHUNK_SIZE.
 *
 * @returns Rows written after batch dedupe + how many staging lines collapsed in-batch.
 */
async function flushDomainBatch(
  kind: SyncKind,
  rows: Record<string, unknown>[],
  syncProgress?: SyncProgressOpts,
  metricTotals?: BatchUpsertMetricDelta,
): Promise<{ flushed: number; collapsedInBatch: number }> {
  if (rows.length === 0) return { flushed: 0, collapsedInBatch: 0 };

  const table = DOMAIN_TABLE[kind];
  if (!table) return { flushed: rows.length, collapsedInBatch: 0 }; // no-op for UNKNOWN

  // ── Step 1: JSONB packing ──────────────────────────────────────────────────
  const nativeCols = NATIVE_COLUMNS_MAP[kind];
  const packed = nativeCols ? packPayloadForSupabase(rows, nativeCols) : rows;

  // ── Step 2: JS-level deduplication (same conflict key in one INSERT must be collapsed)
  let deduped: Record<string, unknown>[];
  let collapsedInBatch: number;

  if (kind === "REMOVAL_ORDER") {
    const ro = dedupeRemovalOrderPayloadsForUpsert(packed);
    deduped = ro.rows;
    collapsedInBatch = ro.collapsedCount;
    let uniqKeys = uniqueBusinessKeyCount(deduped);
    if (uniqKeys !== deduped.length) {
      console.warn(
        `[REMOVAL_ORDER] invariant: ${deduped.length} rows but ${uniqKeys} unique business keys after dedupe — forcing second collapse`,
      );
      const again = dedupeRemovalOrderPayloadsForUpsert(deduped);
      deduped = again.rows;
      collapsedInBatch += again.collapsedCount;
      uniqKeys = uniqueBusinessKeyCount(deduped);
    }
    console.log(
      `[REMOVAL_ORDER] sync dedupe: mapped_rows=${packed.length} after_business_dedupe=${deduped.length} ` +
        `unique_business_keys=${uniqKeys} collapsed=${collapsedInBatch} ` +
        `duplicate_key_samples=${JSON.stringify(ro.duplicateKeySamples)}`,
    );
  } else {
    deduped = deduplicateByConflictKey(kind, packed);
    collapsedInBatch = Math.max(0, packed.length - deduped.length);
    if (kind === "FBA_RETURNS") {
      const before = deduped.length;
      deduped = guardFbaReturnsPhysicalRowBatchUniqueness(deduped, "pre-write");
      collapsedInBatch += Math.max(0, before - deduped.length);
    }
    if (kind === "REPORTS_REPOSITORY") {
      const before = deduped.length;
      deduped = guardReportsRepoPhysicalRowBatchUniqueness(deduped, "pre-write");
      const dropped = Math.max(0, before - deduped.length);
      collapsedInBatch += dropped;
      if (dropped > 0) {
        console.warn(
          `[REPORTS_REPOSITORY] within-batch dedupe collapsed ${dropped} row(s) on (organization_id, source_file_sha256, source_physical_row_number). ` +
            `This is unexpected — investigate amazon_staging for duplicate row_numbers for this upload.`,
        );
      }
    }
    console.log(`[${kind}] Original batch size: ${packed.length}, Cleaned batch size: ${deduped.length}`);
  }

  if (kind === "REMOVAL_ORDER") {
    if (!syncProgress) {
      throw new Error("[REMOVAL_ORDER] syncProgress is required for partitioned amazon_removals write");
    }
    const partition = await partitionRemovalOrderRowsAgainstDatabase(syncProgress.orgId, deduped);
    const insertsCanonical = finalizeRemovalOrderWriteBatchForUniqueness(partition.inserts, "insert");
    const upsertsCanonical = finalizeRemovalOrderWriteBatchForUniqueness(partition.upserts, "upsert");
    if (
      insertsCanonical.length !== partition.inserts.length ||
      upsertsCanonical.length !== partition.upserts.length
    ) {
      console.warn(
        `[REMOVAL_ORDER] post-partition canonical finalize: inserts ${partition.inserts.length}->${insertsCanonical.length}, ` +
          `upserts ${partition.upserts.length}->${upsertsCanonical.length}`,
      );
    }
    console.log(
      `[REMOVAL_ORDER] partition: inserts=${insertsCanonical.length} upserts=${upsertsCanonical.length} ` +
        `skipped_unchanged=${partition.skippedUnchanged}`,
    );

    if (metricTotals && syncProgress) {
      metricTotals.rows_synced_new += insertsCanonical.length;
      metricTotals.rows_synced_updated += upsertsCanonical.length;
      metricTotals.rows_synced_unchanged += partition.skippedUnchanged;
      metricTotals.rows_duplicate_against_existing += partition.skippedUnchanged;
    }

    const roConflict = CONFLICT_KEY.REMOVAL_ORDER;
    if (!roConflict) {
      throw new Error("[REMOVAL_ORDER] CONFLICT_KEY.REMOVAL_ORDER is not configured");
    }
    await writeRemovalOrderPartitionedBatches(
      { inserts: insertsCanonical, upserts: upsertsCanonical },
      roConflict,
      syncProgress,
    );
    return { flushed: deduped.length, collapsedInBatch };
  }

  if (metricTotals && syncProgress && deduped.length > 0) {
    const d = await measureBatchUpsertMetrics(
      supabaseServer,
      kind,
      table,
      syncProgress.orgId,
      syncProgress.uploadId,
      deduped,
    );
    metricTotals.rows_synced_new += d.rows_synced_new;
    metricTotals.rows_synced_updated += d.rows_synced_updated;
    metricTotals.rows_synced_unchanged += d.rows_synced_unchanged;
    metricTotals.rows_duplicate_against_existing += d.rows_duplicate_against_existing;
  }

  // ── Step 3: chunked upsert / insert ────────────────────────────────────────
  const conflictKey = CONFLICT_KEY[kind];

  if (conflictKey) {
    for (let off = 0; off < deduped.length; off += UPSERT_CHUNK_SIZE) {
      const chunk = deduped.slice(off, off + UPSERT_CHUNK_SIZE);
      const { error } = await supabaseServer
        .from(table)
        .upsert(chunk, { onConflict: conflictKey, ignoreDuplicates: false });

      if (error) {
        // Structured diagnostic for REPORTS_REPOSITORY duplicate-key failures:
        // surface the chunk size, distinct physical-line count, sample
        // conflicting keys, and a hint to check for orphan unique indexes
        // (uq_amazon_reports_repo_natural / uq_amazon_reports_repo_org_line_hash)
        // that the migration history claims to drop but may still exist in
        // some environments — those would also raise duplicate-key errors
        // even though the application's own ON CONFLICT target is fine.
        if (kind === "REPORTS_REPOSITORY") {
          const distinctPhysical = uniqueReportsRepoFileRowKeyCount(chunk);
          const dupKeys = listDuplicateReportsRepoFileRowKeys(chunk);
          console.error(
            JSON.stringify({
              event: "REPORTS_REPOSITORY_upsert_failure",
              upload_id: syncProgress?.uploadId,
              organization_id: syncProgress?.orgId,
              chunk_size: chunk.length,
              distinct_physical_line_keys_in_chunk: distinctPhysical,
              duplicate_physical_line_keys_in_chunk_sample: dupKeys.slice(0, REPORTS_REPO_DEDUPE_LOG_SAMPLES),
              chunk_first_physical_row_number: (chunk[0] as { source_physical_row_number?: unknown } | undefined)
                ?.source_physical_row_number,
              chunk_last_physical_row_number: (chunk[chunk.length - 1] as { source_physical_row_number?: unknown } | undefined)
                ?.source_physical_row_number,
              postgres_error: error.message,
              hint:
                "If chunk_size === distinct_physical_line_keys_in_chunk, the failing constraint is NOT the application's " +
                "ON CONFLICT target (organization_id, source_file_sha256, source_physical_row_number). Likely cause is a " +
                "stale orphan unique index. Run the verification SQL in the patch deliverable to inspect pg_indexes for " +
                "amazon_reports_repository and drop any non-current unique index(es).",
            }),
          );
          throw new Error(
            `[REPORTS_REPOSITORY] upsert into ${table} failed: ${error.message} ` +
              `(conflict key: ${conflictKey}, chunk size: ${chunk.length}, distinct physical lines in chunk: ${distinctPhysical}). ` +
              `If chunk_size === distinct, a stale unique index on amazon_reports_repository is the cause — see server logs for diagnostic and verification SQL.`,
          );
        }
        throw new Error(
          `[${kind}] upsert into ${table} failed: ${error.message}` +
            ` (conflict key: ${conflictKey}, chunk size: ${chunk.length})`,
        );
      }
      if (syncProgress) await bumpSyncProgressMetadata(syncProgress, chunk.length);
    }
  } else {
    for (let off = 0; off < deduped.length; off += UPSERT_CHUNK_SIZE) {
      const chunk = deduped.slice(off, off + UPSERT_CHUNK_SIZE);
      const { error } = await supabaseServer.from(table).insert(chunk);
      if (error) {
        throw new Error(`[${kind}] insert into ${table} failed: ${error.message}`);
      }
      if (syncProgress) await bumpSyncProgressMetadata(syncProgress, chunk.length);
    }
  }

  return { flushed: deduped.length, collapsedInBatch };
}

/**
 * Same tuple as raw-landing `ON CONFLICT` targets — for FBA_RETURNS this is `uq_amazon_returns_org_file_row`
 * `(organization_id, source_file_sha256, source_physical_row_number)`.
 */
function physicalLineDedupKey(row: Record<string, unknown>): string {
  const norm = (v: unknown): string => String(v ?? "").trim().toLowerCase();
  return `${norm(row.organization_id)}|${String(row.source_file_sha256 ?? "").trim().toLowerCase()}|${String(row.source_physical_row_number ?? "")}`;
}

/**
 * Removes duplicate rows within a batch using the same composite key that
 * Postgres would use on ON CONFLICT.
 *
 * Raw landing tables: (organization_id, source_file_sha256, source_physical_row_number).
 * REMOVAL_ORDER: handled only in `flushDomainBatch` via `dedupeRemovalOrderPayloadsForUpsert()`.
 * REMOVAL_SHIPMENT: staging-line keys unchanged.
 *
 * Prevents: "ON CONFLICT DO UPDATE command cannot affect row a second time".
 */
function deduplicateByConflictKey(
  kind: SyncKind,
  rows: Record<string, unknown>[],
): Record<string, unknown>[] {
  /** Normalise a scalar field value for safe key comparison. */
  const norm = (v: unknown): string =>
    String(v ?? "").trim().toLowerCase();

  const seen = new Map<string, Record<string, unknown>>();

  for (const row of rows) {
    let key: string;
    switch (kind) {
      case "FBA_RETURNS": // uq_amazon_returns_org_file_row — never key on LPN
        key = fbaReturnsFileRowIdentityKey(row);
        break;
      case "INVENTORY_LEDGER":
      case "REIMBURSEMENTS":
      case "SETTLEMENT":
      case "SAFET_CLAIMS":
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
        key = physicalLineDedupKey(row);
        break;
      case "REMOVAL_ORDER":
        throw new Error(
          "REMOVAL_ORDER deduplication is handled in flushDomainBatch via dedupeRemovalOrderPayloadsForUpsert()",
        );
      case "REMOVAL_SHIPMENT":
        key =
          row.source_staging_id != null && String(row.source_staging_id).trim() !== ""
            ? `sid|${String(row.organization_id)}|${String(row.upload_id)}|${String(row.source_staging_id)}`
            : removalLogicalLineDedupKey(row);
        break;
      case "CATEGORY_LISTINGS":
      case "ALL_LISTINGS":
      case "ACTIVE_LISTINGS":
        key = physicalLineDedupKey(row);
        break;
      default:
        // UNKNOWN — give every row a unique key so nothing is silently dropped
        key = `${norm(row.organization_id)}|__unknown__|${Math.random()}`;
    }

    seen.set(key, row);
  }

  return [...seen.values()];
}
