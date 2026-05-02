/**
 * Phase 2 — Process raw file → `amazon_staging` only (unified for all Amazon report types).
 * Listing exports: same as other reports — Sync lands `amazon_listing_report_rows_raw`, Generic merges `catalog_products`.
 *
 * Shared by:
 *   POST /api/settings/imports/stage  (thin wrapper; rejects listing kinds)
 *   POST /api/settings/imports/process
 */

import csv from "csv-parser";
import { Readable } from "node:stream";
import { NextResponse } from "next/server";

import { createConcatenatedPartsReadable } from "../import-raw-report-stream";
import {
  computeListingPhysicalStagingLineHash,
  splitDataLineIntoCells,
  splitPhysicalLines,
  streamToBuffer,
} from "../import-listing-physical-lines";
import { applyColumnMappingToRow, computeSourceLineHash, normalizeAmazonReportRowKeys } from "../import-sync-mappers";
import {
  mergeUploadMetadata,
  parseRawReportMetadata,
  type ImportRunMetrics,
} from "../raw-report-upload-metadata";
import { supabaseServer } from "../supabase-server";
import { isUuidString } from "../uuid";
import { isListingReportType } from "../raw-report-types";
import {
  FPS_KEY_FAILED,
  FPS_KEY_PROCESS,
  FPS_LABEL_PROCESS,
  FPS_NEXT_ACTION_LABEL_SYNC,
} from "./file-processing-status-contract";
import { logImportPhase } from "./amazon-import-engine-log";
import { resolveAmazonImportEngineConfig, resolveAmazonImportSyncKind } from "./amazon-report-registry";

const STAGING_TABLE = "amazon_staging";
/**
 * Conflict target for amazon_staging upserts.
 * Backs index `uq_amazon_staging_org_upload_row_number` (organization_id, upload_id, row_number).
 * We must NEVER drop or rename this index — chunked + idempotent staging depends on it.
 */
const STAGING_CONFLICT = "organization_id,upload_id,row_number";
/**
 * Conservative batch size for `amazon_staging` upserts.
 * Empirically: 500 hit Postgres statement_timeout on wide REPORTS_REPOSITORY /
 * INVENTORY_LEDGER rows around ~100K rows; 250 was better but still occasionally
 * timed out on the very widest rows. 200 is the safe default. Tune via env var
 * `PHASE2_BATCH_SIZE` (clamped to 50..500) without redeploy if needed.
 * Each batch is its own implicit transaction (single PostgREST upsert call) so
 * a failure in batch N+1 cannot roll back batch N.
 */
const BATCH_SIZE = (() => {
  const raw = process.env.PHASE2_BATCH_SIZE;
  if (!raw) return 200;
  const n = Number(raw);
  if (!Number.isFinite(n)) return 200;
  return Math.min(500, Math.max(50, Math.floor(n)));
})();
/**
 * Maximum age of a `processing` lock before we treat it as stale and allow Phase 2 to take over.
 * Must exceed the maxDuration of /process (300s) plus a safety margin.
 */
const PROCESSING_STALE_MS = 6 * 60 * 1000;
/**
 * Soft time budget per request — wall-clock cap for one "Process" HTTP call.
 *
 * - **Vercel** (`VERCEL=1`): default ~285s so we stop *before* the platform
 *   `maxDuration` hard kill and can persist a clean resume state. Large files on
 *   Hobby (300s cap) may still need multiple Process clicks unless you raise
 *   `PHASE2_SOFT_BUDGET_MS` and match `maxDuration` on `/imports/process`.
 * - **Non-Vercel** (local `next dev`, Docker, bare Node): default **8 hours** —
 *   there is no serverless wall clock; the old 270s default only caused pointless
 *   "time budget exhausted" loops for big CSVs.
 *
 * Override anytime: `PHASE2_SOFT_BUDGET_MS` (milliseconds, min 30s).
 */
const SOFT_BUDGET_MS = ((): number => {
  const raw = process.env.PHASE2_SOFT_BUDGET_MS;
  if (raw) {
    const n = Number(raw);
    if (Number.isFinite(n)) return Math.max(30 * 1000, Math.floor(n));
  }
  const onVercel = process.env.VERCEL === "1";
  if (!onVercel) {
    return 8 * 60 * 60 * 1000;
  }
  return 285 * 1000;
})();
/**
 * Throttle progress writes. Without this, every batch caused 3 round-trips
 * (select metadata + update raw_report_uploads + upsert file_processing_status),
 * and on a 150K-row file that becomes hundreds of seconds of pure progress overhead
 * — directly contributing to mid-run timeouts. Forced writes (per-batch via flushProgress(true))
 * still bypass the throttle for the final completion record.
 */
const PROGRESS_MIN_INTERVAL_MS = 2000;
/**
 * How often to re-anchor `staged_rows_written` / `processed_rows` to the live
 * `count(*)` from the DB instead of the in-memory accumulator. Cheap because
 * Supabase exposes count via head=true (no row payload). Anchored counts
 * eliminate any UI/DB drift caused by silent in-memory off-by-ones.
 */
const DB_COUNT_REANCHOR_INTERVAL_MS = 10 * 1000;
/** Legacy "every N rows" fallback gate (kept so a 0ms-elapsed retry still emits a write occasionally). */
const PROGRESS_EVERY = 25;
const LISTING_PROGRESS_EVERY = 8;

function trimHeaderCell(h: string): string {
  return h.replace(/^\uFEFF/, "").trim();
}

export type StageRequestBody = {
  upload_id?: string;
  start_date?: string | null;
  end_date?: string | null;
  import_full_file?: boolean | null;
};

function num(v: unknown, fallback = 0): number {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return fallback;
}

const DATE_COLUMN_CANDIDATES = [
  "date/time",
  "Date/Time",
  "date_time",
  "posted-date",
  "Posted Date",
  "date",
  "Date",
  "return-date",
  "Return Date",
  "requested-date",
  "Requested Date",
  "settlement-start-date",
  "Settlement Start Date",
  "event-date",
  "Event Date",
];

function rowMatchesDateRange(
  row: Record<string, string>,
  startDate: Date | null,
  endDate: Date | null,
): boolean {
  if (!startDate && !endDate) return true;
  for (const col of DATE_COLUMN_CANDIDATES) {
    const raw = row[col]?.trim();
    if (!raw) continue;
    const d = new Date(raw);
    if (Number.isNaN(d.getTime())) continue;
    if (startDate && d < startDate) return false;
    if (endDate && d > endDate) return false;
    return true;
  }
  return true;
}

/** Page size for scanning row_number values when computing the watermark on a corrupt staging set. */
const WATERMARK_SCAN_PAGE = 5000;

/**
 * Returns the live cardinality of `amazon_staging` for (org, upload):
 * - `count`  — total rows for this upload
 * - `min`    — smallest row_number (0 if empty)
 * - `max`    — largest row_number (0 if empty)
 * Cheap: 3 head=true queries.
 */
async function readStagingCardinality(
  orgId: string,
  uploadId: string,
): Promise<{ count: number; min: number; max: number }> {
  const { count: cntResp, error: cntErr } = await supabaseServer
    .from(STAGING_TABLE)
    .select("*", { count: "exact", head: true })
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);
  if (cntErr) {
    throw new Error(`Staging cardinality (count) failed: ${cntErr.message}. upload_id=${uploadId}`);
  }
  const count = typeof cntResp === "number" ? cntResp : 0;
  if (count === 0) return { count: 0, min: 0, max: 0 };

  const { data: maxRow, error: maxErr } = await supabaseServer
    .from(STAGING_TABLE)
    .select("row_number")
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId)
    .order("row_number", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (maxErr) {
    throw new Error(`Staging cardinality (max) failed: ${maxErr.message}. upload_id=${uploadId}`);
  }
  const max =
    maxRow && typeof (maxRow as { row_number?: unknown }).row_number === "number"
      ? Math.floor((maxRow as { row_number: number }).row_number)
      : 0;

  const { data: minRow, error: minErr } = await supabaseServer
    .from(STAGING_TABLE)
    .select("row_number")
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId)
    .order("row_number", { ascending: true })
    .limit(1)
    .maybeSingle();
  if (minErr) {
    throw new Error(`Staging cardinality (min) failed: ${minErr.message}. upload_id=${uploadId}`);
  }
  const min =
    minRow && typeof (minRow as { row_number?: unknown }).row_number === "number"
      ? Math.floor((minRow as { row_number: number }).row_number)
      : 0;

  return { count, min, max };
}

/**
 * Returns the highest N such that {1, 2, …, N} are ALL present in
 * amazon_staging for this (org, upload). Walks row_numbers ascending in pages
 * and stops at the first gap. Falls back to a single-row LIMIT 1 read when no
 * rows exist (returns 0).
 *
 * For the common case (no gaps, contiguous from 1) this is one ascending page
 * read. For corrupt staging (gaps), it pages through until the first gap is
 * found. Memory is bounded to one page (5000 ints).
 */
async function findContiguousPrefixWatermark(
  orgId: string,
  uploadId: string,
  expectedMax: number,
): Promise<number> {
  if (expectedMax === 0) return 0;
  let next = 1;
  let scanned = 0;
  while (next <= expectedMax) {
    const upper = Math.min(expectedMax, next + WATERMARK_SCAN_PAGE - 1);
    const { data, error } = await supabaseServer
      .from(STAGING_TABLE)
      .select("row_number")
      .eq("organization_id", orgId)
      .eq("upload_id", uploadId)
      .gte("row_number", next)
      .lte("row_number", upper)
      .order("row_number", { ascending: true })
      .limit(WATERMARK_SCAN_PAGE);
    if (error) {
      throw new Error(`Watermark scan failed: ${error.message}. upload_id=${uploadId}`);
    }
    const rows = (data ?? []) as { row_number: number }[];
    if (rows.length === 0) {
      // No rows in [next, upper]. The watermark is next-1.
      return next - 1;
    }
    for (const r of rows) {
      const rn = Math.floor(Number(r.row_number));
      if (rn !== next) {
        // First gap is at `next`; watermark stops at next-1.
        return next - 1;
      }
      next += 1;
      scanned += 1;
    }
    if (rows.length < WATERMARK_SCAN_PAGE) {
      // No more rows beyond what we just scanned — watermark is (next - 1).
      return next - 1;
    }
  }
  // Walked all the way to expectedMax with no gaps.
  void scanned;
  return expectedMax;
}

/**
 * Resume support: returns the contiguous-prefix watermark and the live row
 * count for `amazon_staging`. Used to:
 *   1) skip the re-DELETE on retry (avoids statement_timeout on big files),
 *   2) skip parser rows that were already inserted up to the WATERMARK
 *      (NOT max(row_number) — the previous behavior would skip past gaps),
 *   3) seed progress counters so reported %/staged_rows reflect actual DB state.
 *
 * The watermark guarantees lossless resume: every parser row from
 * `watermark+1` onward is offered to the upsert. Rows already present beyond
 * the gap are no-oped by `ignoreDuplicates: true`; rows in the gap are
 * actually inserted. After the run completes, the gap is filled.
 */
async function getStagingResumeState(
  orgId: string,
  uploadId: string,
): Promise<{
  watermark: number;
  rowCount: number;
  minRowNumber: number;
  maxRowNumber: number;
  hasGaps: boolean;
}> {
  const card = await readStagingCardinality(orgId, uploadId);
  if (card.count === 0) {
    return { watermark: 0, rowCount: 0, minRowNumber: 0, maxRowNumber: 0, hasGaps: false };
  }
  const expectedSpan = card.max - card.min + 1;
  const noGapsAndStartsAtOne = card.min === 1 && card.count === expectedSpan;
  if (noGapsAndStartsAtOne) {
    // Fast path: contiguous from 1..max with no gaps.
    return {
      watermark: card.max,
      rowCount: card.count,
      minRowNumber: card.min,
      maxRowNumber: card.max,
      hasGaps: false,
    };
  }
  // Slow path: scan ascending to locate the first gap. Watermark is the
  // highest contiguous-from-1 row_number. If min > 1, watermark is 0 (no
  // contiguous prefix at all — the parser will offer all rows from 1, the
  // existing rows beyond will be no-oped by ignoreDuplicates upsert).
  const watermark =
    card.min === 1 ? await findContiguousPrefixWatermark(orgId, uploadId, card.max) : 0;
  return {
    watermark,
    rowCount: card.count,
    minRowNumber: card.min,
    maxRowNumber: card.max,
    hasGaps: true,
  };
}

/**
 * If a prior Phase 2 worker died mid-run, status remains `processing` forever
 * (Vercel maxDuration cap kills the request without notifying the DB). Free
 * such locks once they exceed PROCESSING_STALE_MS so the next click can resume.
 * Returns true when this call performed the unstuck step (caller uses this for
 * logging only — the normal lock acquisition still runs after).
 */
async function unstickStaleProcessingLock(
  orgId: string,
  uploadId: string,
  startedAtIso: string | null | undefined,
): Promise<boolean> {
  if (!startedAtIso) return false;
  const startedAt = new Date(startedAtIso).getTime();
  if (!Number.isFinite(startedAt)) return false;
  if (Date.now() - startedAt < PROCESSING_STALE_MS) return false;

  const { data: prevRow } = await supabaseServer
    .from("raw_report_uploads")
    .select("metadata")
    .eq("id", uploadId)
    .maybeSingle();

  const { data: flipped } = await supabaseServer
    .from("raw_report_uploads")
    .update({
      status: "failed",
      metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
        error_message: "Recovered stale processing lock (worker timed out).",
        failed_phase: "process",
        import_metrics: { current_phase: "failed", failure_reason: "stale_processing_lock_recovered" },
      }),
      updated_at: new Date().toISOString(),
    })
    .eq("id", uploadId)
    .eq("organization_id", orgId)
    .eq("status", "processing")
    .select("id");
  return Array.isArray(flipped) && flipped.length > 0;
}

/**
 * Hard verification — run after staging finishes (or after a resume completes).
 * Reads the **actual DB state** for (org, upload_id) and asserts:
 *   - `count(*) === expectedCount`
 *   - `max(row_number) === expectedMaxRowNumber`
 *   - For non-listing (contiguous) reports: `min(row_number) === 1`
 *     and `count === max - min + 1` (no gaps)
 *
 * Throws `Error` on any mismatch. The outer catch in
 * `executeAmazonPhase2Staging` will then mark the upload `failed` with the
 * verification message — we never silently mark `staged` when rows are
 * missing or duplicated.
 */
async function assertStagingComplete(opts: {
  orgId: string;
  uploadId: string;
  expectedCount: number;
  expectedMaxRowNumber: number;
  isListing: boolean;
}): Promise<{ dbCount: number; dbMin: number; dbMax: number }> {
  const { orgId, uploadId, expectedCount, expectedMaxRowNumber, isListing } = opts;

  const { count: countResp, error: countErr } = await supabaseServer
    .from(STAGING_TABLE)
    .select("*", { count: "exact", head: true })
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);
  if (countErr) {
    throw new Error(
      `Staging verification failed (count query): ${countErr.message}. upload_id=${uploadId}`,
    );
  }
  const dbCount = typeof countResp === "number" ? countResp : 0;

  const { data: maxRow, error: maxErr } = await supabaseServer
    .from(STAGING_TABLE)
    .select("row_number")
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId)
    .order("row_number", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (maxErr) {
    throw new Error(
      `Staging verification failed (max query): ${maxErr.message}. upload_id=${uploadId}`,
    );
  }
  const dbMax =
    maxRow && typeof (maxRow as { row_number?: unknown }).row_number === "number"
      ? Math.floor((maxRow as { row_number: number }).row_number)
      : 0;

  const { data: minRow, error: minErr } = await supabaseServer
    .from(STAGING_TABLE)
    .select("row_number")
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId)
    .order("row_number", { ascending: true })
    .limit(1)
    .maybeSingle();
  if (minErr) {
    throw new Error(
      `Staging verification failed (min query): ${minErr.message}. upload_id=${uploadId}`,
    );
  }
  const dbMin =
    minRow && typeof (minRow as { row_number?: unknown }).row_number === "number"
      ? Math.floor((minRow as { row_number: number }).row_number)
      : 0;

  const ctx = `upload_id=${uploadId} expected_count=${expectedCount} db_count=${dbCount} expected_max=${expectedMaxRowNumber} db_max=${dbMax} db_min=${dbMin} listing=${isListing}`;

  if (dbCount !== expectedCount) {
    throw new Error(
      `Staging verification failed: row count mismatch (expected ${expectedCount}, found ${dbCount} in amazon_staging). ${ctx}`,
    );
  }
  if (dbMax !== expectedMaxRowNumber) {
    throw new Error(
      `Staging verification failed: max(row_number) mismatch (expected ${expectedMaxRowNumber}, found ${dbMax}). ${ctx}`,
    );
  }
  if (!isListing) {
    if (dbCount > 0 && dbMin !== 1) {
      throw new Error(
        `Staging verification failed: min(row_number) expected 1 for contiguous report, found ${dbMin}. ${ctx}`,
      );
    }
    const span = dbCount === 0 ? 0 : dbMax - dbMin + 1;
    if (span !== dbCount) {
      throw new Error(
        `Staging verification failed: row_number gaps detected (count=${dbCount}, span=${span}). ${ctx}`,
      );
    }
  }

  console.log(
    JSON.stringify({
      event: "phase2_staging_verified",
      upload_id: uploadId,
      organization_id: orgId,
      db_count: dbCount,
      db_min_row_number: dbMin,
      db_max_row_number: dbMax,
      is_listing: isListing,
    }),
  );

  return { dbCount, dbMin, dbMax };
}

/**
 * Structured failure log captured at the moment a batch upsert throws.
 * Records what we tried, what is actually committed in the DB right now,
 * and where the next retry will resume from. Best-effort — diagnostic
 * failures are themselves swallowed so they cannot mask the real error.
 */
async function logBatchFailureDiagnostic(opts: {
  orgId: string;
  uploadId: string;
  batchStartRowNumber: number | null;
  batchEndRowNumber: number | null;
  batchSize: number;
  inMemoryStagedBefore: number;
  errorMessage: string;
}): Promise<void> {
  const { orgId, uploadId, batchStartRowNumber, batchEndRowNumber, batchSize, inMemoryStagedBefore, errorMessage } =
    opts;
  try {
    const { count: committedAfter } = await supabaseServer
      .from(STAGING_TABLE)
      .select("*", { count: "exact", head: true })
      .eq("organization_id", orgId)
      .eq("upload_id", uploadId);
    const next = await getStagingResumeState(orgId, uploadId);
    console.error(
      JSON.stringify({
        event: "phase2_staging_batch_failure",
        upload_id: uploadId,
        organization_id: orgId,
        batch_start_row_number: batchStartRowNumber,
        batch_end_row_number: batchEndRowNumber,
        batch_size: batchSize,
        committed_count_before_attempt: inMemoryStagedBefore,
        committed_count_after_failure: typeof committedAfter === "number" ? committedAfter : null,
        next_resume_from_row_number: next.maxRowNumber,
        next_resume_row_count: next.rowCount,
        error: errorMessage,
      }),
    );
  } catch (logErr) {
    console.error(
      `[phase2-staging] Diagnostic log itself failed: ${logErr instanceof Error ? logErr.message : String(logErr)}`,
    );
  }
}

async function audit(orgId: string, action: string, entityId: string, detail?: Record<string, unknown>): Promise<void> {
  await supabaseServer.from("raw_report_import_audit").insert({
    organization_id: orgId,
    user_profile_id: null,
    action,
    entity_id: entityId,
    detail: detail ?? null,
  });
}

const KNOWN_TYPES = new Set([
  "FBA_RETURNS",
  "REMOVAL_ORDER",
  "REMOVAL_SHIPMENT",
  "INVENTORY_LEDGER",
  "REIMBURSEMENTS",
  "SETTLEMENT",
  "SAFET_CLAIMS",
  "TRANSACTIONS",
  "REPORTS_REPOSITORY",
  "PRODUCT_IDENTITY",
  "ALL_ORDERS",
  "REPLACEMENTS",
  "FBA_GRADE_AND_RESELL",
  "MANAGE_FBA_INVENTORY",
  "FBA_INVENTORY",
  "INBOUND_PERFORMANCE",
  "AMAZON_FULFILLED_INVENTORY",
  "RESERVED_INVENTORY",
  "FEE_PREVIEW",
  "MONTHLY_STORAGE_FEES",
  "CATEGORY_LISTINGS",
  "ALL_LISTINGS",
  "ACTIVE_LISTINGS",
  "fba_customer_returns",
  "inventory_ledger",
  "safe_t_claims",
  "reimbursements",
  "settlement_repository",
  "transaction_view",
]);

/**
 * Runs Phase 2 staging ETL. Caller must not wrap in listing/ledger special cases.
 * Pass a plain body if the HTTP body was already consumed upstream.
 */
export async function executeAmazonPhase2Staging(reqOrBody: Request | StageRequestBody): Promise<Response> {
  let uploadIdForFail: string | null = null;
  let orgId = "";

  try {
    const body =
      reqOrBody instanceof Request
        ? ((await reqOrBody.json()) as StageRequestBody)
        : (reqOrBody as StageRequestBody);

    const importFullFile = body.import_full_file === true;
    const filterStartDate = !importFullFile && body.start_date ? new Date(body.start_date) : null;
    const filterEndDate = !importFullFile && body.end_date ? new Date(body.end_date) : null;
    if (filterEndDate && !Number.isNaN(filterEndDate.getTime())) {
      filterEndDate.setHours(23, 59, 59, 999);
    }
    const uploadId = typeof body.upload_id === "string" ? body.upload_id.trim() : "";
    if (!isUuidString(uploadId)) {
      return NextResponse.json({ ok: false, error: "Invalid upload_id." }, { status: 400 });
    }
    uploadIdForFail = uploadId;

    const { data: row, error: fetchErr } = await supabaseServer
      .from("raw_report_uploads")
      .select(
        "id, organization_id, metadata, status, report_type, column_mapping, file_name, import_pipeline_started_at",
      )
      .eq("id", uploadId)
      .maybeSingle();

    if (fetchErr || !row) {
      return NextResponse.json({ ok: false, error: "Upload session not found." }, { status: 404 });
    }

    orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
    if (!isUuidString(orgId)) {
      return NextResponse.json({ ok: false, error: "Invalid upload row (organization_id)." }, { status: 500 });
    }

    let status = String((row as { status?: unknown }).status ?? "");

    // Recover stale `processing` locks (worker died past Vercel maxDuration). After
    // the flip, status becomes `failed` and the normal retry path takes over.
    if (status === "processing") {
      const startedAt =
        (row as { import_pipeline_started_at?: string | null }).import_pipeline_started_at ?? null;
      const recovered = await unstickStaleProcessingLock(orgId, uploadId, startedAt);
      if (recovered) {
        status = "failed";
      }
    }

    const retryableStatuses = ["mapped", "ready", "uploaded", "pending", "failed"];
    if (!retryableStatuses.includes(status)) {
      return NextResponse.json(
        {
          ok: false,
          error: `Phase 2 (Stage) requires a processable status (mapped, ready, uploaded, pending, or failed for retry). Current status is "${status}".${
            status === "needs_mapping"
              ? ' Use "Map Columns" in the History table first.'
              : status === "staged"
                ? " This file is already staged — use Sync."
                : ""
          }`,
        },
        { status: 409 },
      );
    }

    const reportTypeRaw = String((row as { report_type?: unknown }).report_type ?? "").trim();
    if (!reportTypeRaw || reportTypeRaw === "UNKNOWN" || !KNOWN_TYPES.has(reportTypeRaw)) {
      return NextResponse.json(
        {
          ok: false,
          error:
            'Report type is not set or is "UNKNOWN". Open "Map Columns" in the History table, select the correct type, and save before processing.',
        },
        { status: 422 },
      );
    }

    const engine = resolveAmazonImportEngineConfig(resolveAmazonImportSyncKind(reportTypeRaw));

    const meta = (row as { metadata?: unknown }).metadata;
    const parsed = parseRawReportMetadata(meta);
    if (parsed.uploadProgress < 100) {
      return NextResponse.json(
        { ok: false, error: "Upload is not complete yet (wait for 100% upload progress)." },
        { status: 400 },
      );
    }
    const metaObj =
      meta && typeof meta === "object" && !Array.isArray(meta) ? (meta as Record<string, unknown>) : {};

    const storagePrefix = parsed.storagePrefix?.trim() ?? "";
    const rawFilePath = typeof metaObj.raw_file_path === "string" ? metaObj.raw_file_path.trim() : "";

    if (!storagePrefix && !rawFilePath) {
      return NextResponse.json({ ok: false, error: "Missing storage prefix or raw_file_path in upload metadata." }, { status: 400 });
    }

    const totalParts =
      num(metaObj.total_parts, 0) > 0
        ? Math.floor(num(metaObj.total_parts, 0))
        : Math.max(1, Math.floor(num(metaObj.upload_chunks_count, 0)));

    if (!Number.isFinite(totalParts) || totalParts < 1) {
      return NextResponse.json({ ok: false, error: "Missing part count in upload metadata." }, { status: 400 });
    }

    const columnMapping =
      (row as { column_mapping?: unknown }).column_mapping &&
      typeof (row as { column_mapping?: unknown }).column_mapping === "object" &&
      !Array.isArray((row as { column_mapping?: unknown }).column_mapping)
        ? ((row as { column_mapping?: unknown }).column_mapping as Record<string, string>)
        : null;

    const totalRowsFromMeta =
      typeof metaObj.total_rows === "number" && Number.isFinite(metaObj.total_rows) && metaObj.total_rows > 0
        ? Math.floor(metaObj.total_rows)
        : typeof metaObj.total_rows === "string" && metaObj.total_rows.trim() !== ""
          ? Math.floor(num(metaObj.total_rows, 0))
          : 0;
    // Planned row count must come from Phase-1 total_rows only — never parsed.rowCount, which prefers
    // live row_count and can be stale from a prior run on the same upload row.
    const estimatedRows = totalRowsFromMeta > 0 ? totalRowsFromMeta : null;

    // Lock without resetting progress fields — those will be refreshed below from
    // the resume state so the UI never regresses from "150,000 staged" to "0 staged".
    const { data: locked, error: lockErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "processing",
        metadata: mergeUploadMetadata(meta, {
          error_message: "",
          import_metrics: { current_phase: "staging" },
        }),
        import_pipeline_started_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId)
      .in("status", ["mapped", "ready", "uploaded", "pending", "failed"])
      .select("id");

    if (lockErr) {
      return NextResponse.json({ ok: false, error: lockErr.message }, { status: 500 });
    }
    if (!locked || locked.length === 0) {
      return NextResponse.json(
        { ok: false, error: "Upload is not in a stageable state (another operation may be running)." },
        { status: 409 },
      );
    }

    /**
     * Resume-aware staging: never blindly DELETE the existing rows for this upload.
     * On a fresh run there are no rows (no-op cost). On retry of a partially-staged
     * large file, deleting tens/hundreds of thousands of rows is exactly what was
     * causing statement_timeout — and the subsequent re-INSERT then collided with
     * surviving rows on uq_amazon_staging_org_upload_row_number.
     *
     * CRITICAL: resume from the CONTIGUOUS-PREFIX WATERMARK, NOT max(row_number).
     * If a prior run (or a manual reset, or a partial sync that deleted some
     * staging rows) left gaps, the previous max-only resume would skip past the
     * gap and the missing rows would never be re-staged — leaving the upload
     * permanently broken. The watermark is the highest N such that {1..N} are
     * ALL present in staging; the parser re-offers everything from N+1, the
     * upsert with `ignoreDuplicates: true` no-ops on rows that already exist
     * past the gap, and the gap itself gets filled in this run.
     */
    const resumeState = await getStagingResumeState(orgId, uploadId);
    const resumeFromRowNumber = resumeState.watermark;
    const resumeRowCount = resumeState.rowCount;

    if (resumeState.hasGaps) {
      console.warn(
        JSON.stringify({
          event: "phase2_staging_corruption_detected",
          severity: "warn",
          message:
            "amazon_staging for this upload is non-contiguous. Resume will start at the watermark and the upsert will fill the gap; a non-empty gap is usually caused by a prior partial sync that deleted some staging rows or a manual SQL edit.",
          upload_id: uploadId,
          organization_id: orgId,
          db_min_row_number: resumeState.minRowNumber,
          db_max_row_number: resumeState.maxRowNumber,
          db_row_count: resumeState.rowCount,
          contiguous_prefix_watermark: resumeFromRowNumber,
          gap_total: Math.max(0, resumeState.maxRowNumber - resumeState.rowCount),
        }),
      );
    }
    if (resumeFromRowNumber > 0) {
      console.log(
        `[phase2-staging] Resuming upload ${uploadId} from contiguous-prefix watermark=${resumeFromRowNumber} ` +
          `(rows_in_staging=${resumeRowCount}, max_row_number=${resumeState.maxRowNumber}, gaps=${resumeState.hasGaps}).`,
      );
    } else if (resumeRowCount > 0) {
      console.log(
        `[phase2-staging] Staging has ${resumeRowCount} rows for upload ${uploadId} but no contiguous prefix from row_number=1 ` +
          `(min=${resumeState.minRowNumber}, max=${resumeState.maxRowNumber}). Re-parsing from row 1; existing rows past the gap will be no-oped by upsert.`,
      );
    }

    await audit(orgId, "import.stage_started", uploadId, {
      fileName: (row as { file_name?: string }).file_name,
      totalParts,
      pipeline: "unified_phase2_staging",
      resume_from_row_number: resumeFromRowNumber,
      resume_row_count: resumeRowCount,
      db_min_row_number: resumeState.minRowNumber,
      db_max_row_number: resumeState.maxRowNumber,
      has_gaps: resumeState.hasGaps,
      batch_size: BATCH_SIZE,
      soft_budget_ms: SOFT_BUDGET_MS,
    });

    /**
     * Soft time budget — the worker self-terminates a few seconds before
     * Vercel's hard `maxDuration` so we can write a clean "click Process again
     * to resume" failure state instead of getting hard-killed mid-batch.
     */
    const requestStartedAt = Date.now();
    const isSoftBudgetExhausted = (): boolean => Date.now() - requestStartedAt > SOFT_BUDGET_MS;

    // Seed FPS with cumulative counters so the UI does not flash back to 0% on retry.
    // Compute target pct from estimatedRows + resumeRowCount, then take MAX with the
    // existing FPS row's process_pct and the existing metadata.process_progress so
    // progress can never visibly regress — no matter where the prior worker died.
    const computedInitialPctRaw =
      estimatedRows && estimatedRows > 0 && resumeRowCount > 0
        ? Math.min(99, Math.round((resumeRowCount / Math.max(estimatedRows, resumeRowCount)) * 100))
        : 0;

    const { data: existingFps } = await supabaseServer
      .from("file_processing_status")
      .select("process_pct, phase2_stage_pct, processed_rows, staged_rows_written")
      .eq("upload_id", uploadId)
      .maybeSingle();

    const existingProcessPct =
      existingFps && typeof (existingFps as { process_pct?: unknown }).process_pct === "number"
        ? Math.max(0, Math.min(99, Math.floor((existingFps as { process_pct: number }).process_pct)))
        : 0;
    const existingPhase2Pct =
      existingFps && typeof (existingFps as { phase2_stage_pct?: unknown }).phase2_stage_pct === "number"
        ? Math.max(0, Math.min(99, Math.floor((existingFps as { phase2_stage_pct: number }).phase2_stage_pct)))
        : 0;
    const existingProcessedRows =
      existingFps && typeof (existingFps as { processed_rows?: unknown }).processed_rows === "number"
        ? Math.max(0, Math.floor((existingFps as { processed_rows: number }).processed_rows))
        : 0;
    const existingStagedRowsWritten =
      existingFps && typeof (existingFps as { staged_rows_written?: unknown }).staged_rows_written === "number"
        ? Math.max(0, Math.floor((existingFps as { staged_rows_written: number }).staged_rows_written))
        : 0;

    const metaPrevPct = (() => {
      const v = metaObj.process_progress;
      if (typeof v === "number" && Number.isFinite(v)) return Math.max(0, Math.min(99, Math.floor(v)));
      if (typeof v === "string" && v.trim() !== "") {
        const n = Number(v);
        if (Number.isFinite(n)) return Math.max(0, Math.min(99, Math.floor(n)));
      }
      return 0;
    })();

    const initialProcessPct = Math.max(computedInitialPctRaw, existingProcessPct, existingPhase2Pct, metaPrevPct);
    const initialProcessedRows = Math.max(resumeRowCount, existingProcessedRows);
    const initialStagedRowsWritten = Math.max(resumeRowCount, existingStagedRowsWritten);

    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: uploadId,
        organization_id: orgId,
        status: "processing",
        current_phase: "staging",
        phase_key: FPS_KEY_PROCESS,
        phase_label: FPS_LABEL_PROCESS,
        stage_target_table: engine.stage_target_table,
        sync_target_table: engine.sync_target_table,
        generic_target_table: engine.generic_target_table,
        next_action_key: "sync",
        next_action_label: FPS_NEXT_ACTION_LABEL_SYNC,
        current_phase_label: FPS_LABEL_PROCESS,
        upload_pct: 100,
        process_pct: initialProcessPct,
        phase2_stage_pct: initialProcessPct,
        sync_pct: 0,
        processed_rows: initialProcessedRows,
        staged_rows_written: initialStagedRowsWritten,
        total_rows: estimatedRows && estimatedRows > 0 ? estimatedRows : null,
        error_message: null,
        import_metrics: { current_phase: "staging", rows_staged: initialStagedRowsWritten },
      },
      { onConflict: "upload_id" },
    );

    const headerRowIndex =
      typeof metaObj.header_row_index === "number" && metaObj.header_row_index > 0
        ? Math.floor(metaObj.header_row_index as number)
        : 0;

    /**
     * Synthesised headers — set by the importer when the source file has no
     * header row (e.g. headerless Amazon Inventory Ledger export). When
     * present, csv-parser is configured with these directly and row 0 is
     * treated as data, not as a header.
     */
    const synthesizedHeaders =
      Array.isArray(metaObj.synthesized_headers)
        ? (metaObj.synthesized_headers as unknown[])
            .map((h) => String(h ?? "").trim())
            .filter((h) => h !== "")
        : null;

    const rawFileExt = rawFilePath ? (rawFilePath.split(".").pop() ?? "").toLowerCase() : "";
    const metaFileExt =
      typeof metaObj.file_extension === "string" ? metaObj.file_extension.replace(/^\./, "").toLowerCase() : "";
    const fileExt = metaFileExt || rawFileExt;
    const csvSeparator: string = fileExt === "txt" ? "\t" : ",";

    if (fileExt === "xlsx") {
      return NextResponse.json(
        { ok: false, error: "Excel imports are not processed by this pipeline. Export as CSV and re-upload." },
        { status: 415 },
      );
    }
    if (fileExt && fileExt !== "csv" && fileExt !== "txt") {
      return NextResponse.json(
        { ok: false, error: `Unsupported file type for processing: .${fileExt || "unknown"}` },
        { status: 415 },
      );
    }

    let source: NodeJS.ReadableStream;
    if (rawFilePath) {
      const { data: blob, error: dlErr } = await supabaseServer.storage.from("raw-reports").download(rawFilePath);
      if (dlErr || !blob) {
        throw new Error(dlErr?.message ?? `Could not download file from storage: ${rawFilePath}`);
      }
      // CRITICAL: stream the blob — DO NOT buffer the entire file. The previous
      // `Buffer.from(await blob.arrayBuffer())` materialized the whole file in
      // memory which OOM-killed the worker on >300MB CSVs (the symptom: status
      // stuck at `processing` with 0 rows in amazon_staging). Streaming via
      // Readable.fromWeb keeps memory bounded to BATCH_SIZE * row_size + the
      // csv-parser internal buffer, regardless of file size.
      // Listing reports still go through the buffered path (`streamToBuffer`)
      // because they need the full text for header-aware physical-line splitting;
      // they're handled below in their own branch and outside this code path.
      const webStream = blob.stream() as unknown as ReadableStream<Uint8Array>;
      source = Readable.fromWeb(webStream as Parameters<typeof Readable.fromWeb>[0]);
    } else {
      source = createConcatenatedPartsReadable(supabaseServer, storagePrefix, totalParts);
    }

    // ── Listing exports: physical lines → amazon_staging (Phase 2 only) ─────
    if (isListingReportType(reportTypeRaw)) {
      const sep = csvSeparator as "\t" | ",";
      const fileBuf = await streamToBuffer(source as Readable);
      let lines = splitPhysicalLines(fileBuf.toString("utf8"));
      if (headerRowIndex > 0 && headerRowIndex < lines.length) {
        lines = lines.slice(headerRowIndex);
      }
      if (lines.length === 0) {
        return NextResponse.json({ ok: false, error: "Empty file." }, { status: 400 });
      }

      let skippedEmpty = 0;
      let dataLinesPassed = 0;
      let lastFileLineNumberStaged = resumeFromRowNumber; // tracks expected MAX(row_number) for verify
      const fileRowsTotal = Math.max(0, lines.length - 1);
      const dataRowsTotal = fileRowsTotal;
      // Seed with rows already in DB so progress and counters keep climbing across resumes.
      let approxStaged = resumeRowCount;
      let batch: Record<string, unknown>[] = [];
      let lastListingProgressWrite = 0;
      let lastListingProgressTs = 0;

      const flushListingProgress = async (force: boolean, physicalDone: number) => {
        if (!force) {
          const rowsDelta = physicalDone - lastListingProgressWrite;
          const msSinceLast = Date.now() - lastListingProgressTs;
          // Throttle: skip unless either the row-stride OR the time-stride has elapsed.
          if (rowsDelta < LISTING_PROGRESS_EVERY && msSinceLast < PROGRESS_MIN_INTERVAL_MS) return;
        }
        lastListingProgressWrite = physicalDone;
        lastListingProgressTs = Date.now();
        // Cumulative for resume — physicalDone counts only this-run new rows.
        const cumulativeDone = resumeRowCount + physicalDone;
        const denom = Math.max(1, dataRowsTotal, estimatedRows ?? 0, cumulativeDone);
        const pct = Math.min(99, Math.round((cumulativeDone / denom) * 100));
        const { data: prevRow } = await supabaseServer
          .from("raw_report_uploads")
          .select("metadata")
          .eq("id", uploadId)
          .maybeSingle();
        const cumulativeDataSeen = resumeRowCount + dataLinesPassed;
        await supabaseServer
          .from("raw_report_uploads")
          .update({
            metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
              row_count: cumulativeDone,
              total_rows: dataRowsTotal,
              process_progress: pct,
              physical_lines_seen: fileRowsTotal,
              data_rows_seen: cumulativeDataSeen,
              import_metrics: {
                current_phase: "staging",
                physical_lines_seen: fileRowsTotal,
                data_rows_seen: cumulativeDataSeen,
                rows_staged: approxStaged + batch.length,
              },
            }),
            updated_at: new Date().toISOString(),
          })
          .eq("id", uploadId)
          .eq("organization_id", orgId);

        await supabaseServer.from("file_processing_status").upsert(
          {
            upload_id: uploadId,
            organization_id: orgId,
            status: "processing",
            current_phase: "staging",
            phase_key: FPS_KEY_PROCESS,
            phase_label: FPS_LABEL_PROCESS,
            stage_target_table: engine.stage_target_table,
            sync_target_table: engine.sync_target_table,
            generic_target_table: engine.generic_target_table,
            next_action_key: "sync",
            next_action_label: FPS_NEXT_ACTION_LABEL_SYNC,
            current_phase_label: FPS_LABEL_PROCESS,
            upload_pct: 100,
            process_pct: pct,
            phase1_upload_pct: 100,
            phase2_stage_pct: pct,
            phase3_raw_sync_pct: 0,
            phase4_generic_pct: 0,
            sync_pct: 0,
            processed_rows: cumulativeDone,
            total_rows: denom,
            staged_rows_written: approxStaged + batch.length,
            file_rows_total: fileRowsTotal,
            data_rows_total: dataRowsTotal,
            phase2_status: "running",
            phase2_started_at: new Date().toISOString(),
            import_metrics: { current_phase: "staging" },
          },
          { onConflict: "upload_id" },
        );
      };

      const flushBatch = async (rows: Record<string, unknown>[]) => {
        if (rows.length === 0) return;
        // Idempotent insert: ignore rows already staged for this (org, upload, row_number).
        // Each upsert is a single, independently-committed PostgREST request. A failure
        // here cannot roll back any prior batch — the next attempt resumes from the
        // highest existing row_number.
        const startRn = (rows[0] as { row_number?: number } | undefined)?.row_number ?? null;
        const endRn = (rows[rows.length - 1] as { row_number?: number } | undefined)?.row_number ?? null;
        const inMemoryStagedBefore = approxStaged;
        const { error: insErr } = await supabaseServer
          .from(STAGING_TABLE)
          .upsert(rows, { onConflict: STAGING_CONFLICT, ignoreDuplicates: true });
        if (insErr) {
          await logBatchFailureDiagnostic({
            orgId,
            uploadId,
            batchStartRowNumber: startRn,
            batchEndRowNumber: endRn,
            batchSize: rows.length,
            inMemoryStagedBefore,
            errorMessage: insErr.message,
          });
          throw new Error(
            `Upsert into ${STAGING_TABLE} failed (rows ${startRn}..${endRn}, batch_size=${rows.length}): ${insErr.message}`,
          );
        }
        approxStaged += rows.length;
        if (typeof endRn === "number") lastFileLineNumberStaged = endRn;
        // Throttled progress (no longer forced per batch — see PROGRESS_MIN_INTERVAL_MS).
        await flushListingProgress(false, dataLinesPassed);
      };

      const headerLine = lines[0] ?? "";
      const headerCells = splitDataLineIntoCells(trimHeaderCell(headerLine), sep).map(trimHeaderCell);

      for (let lineIdx = 1; lineIdx < lines.length; lineIdx++) {
        const rawLine = lines[lineIdx] ?? "";
        if (rawLine.trim() === "") {
          skippedEmpty += 1;
          void flushListingProgress(false, dataLinesPassed).catch(() => {});
          continue;
        }

        /** 1-based physical line index in the original file (matches listing raw `row_number`). */
        const fileLineNumber = headerRowIndex + lineIdx + 1;
        // Resume: this physical line was already staged by a prior partial run.
        // dataLinesPassed only counts this-run NEW rows so cumulative math elsewhere stays correct.
        if (resumeFromRowNumber > 0 && fileLineNumber <= resumeFromRowNumber) {
          continue;
        }
        dataLinesPassed += 1;
        const dataCells = splitDataLineIntoCells(rawLine, sep);
        const row: Record<string, string> = {};
        for (let j = 0; j < headerCells.length; j++) {
          row[headerCells[j] ?? `column_${j}`] = dataCells[j] ?? "";
        }
        const mappedRow = applyColumnMappingToRow(normalizeAmazonReportRowKeys(row), columnMapping);
        const source_line_hash = computeListingPhysicalStagingLineHash(uploadId, fileLineNumber, orgId, mappedRow);

        const stagingRow: Record<string, unknown> = {
          organization_id: orgId,
          upload_id: uploadId,
          report_type: reportTypeRaw,
          row_number: fileLineNumber,
          source_line_hash,
          raw_row: mappedRow,
        };
        const dateVal =
          mappedRow["Date"] ??
          mappedRow["date"] ??
          mappedRow["Date-Time"] ??
          mappedRow["date-time"] ??
          mappedRow["date_time"] ??
          mappedRow["date/time"] ??
          "";
        if (dateVal) {
          const d = new Date(String(dateVal));
          if (!Number.isNaN(d.getTime())) {
            stagingRow.snapshot_date = d.toISOString().slice(0, 10);
          }
        }

        batch.push(stagingRow);
        if (batch.length >= BATCH_SIZE) {
          const chunk = batch;
          batch = [];
          await flushBatch(chunk);
        }
        void flushListingProgress(false, dataLinesPassed).catch(() => {});
      }

      await flushBatch(batch);
      batch = [];

      // HARD VERIFICATION — must run before we mark the upload as `staged`.
      // For listing exports the row_number space is sparse (empty physical lines are
      // skipped) so we only assert count + max(row_number); gap-detection does not apply.
      const expectedListingCount = resumeRowCount + dataLinesPassed;
      const verified = await assertStagingComplete({
        orgId,
        uploadId,
        expectedCount: expectedListingCount,
        expectedMaxRowNumber: lastFileLineNumberStaged,
        isListing: true,
      });
      const rowsInDb = verified.dbCount;

      const { data: prevRowListing } = await supabaseServer
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadId)
        .maybeSingle();

      // Cumulative completion counters: dataLinesPassed counts only this-run NEW rows
      // after my resume edit, so use rowsInDb (authoritative DB count) for the totals.
      const cumulativeDataSeenAtEnd = rowsInDb;

      await supabaseServer
        .from("raw_report_uploads")
        .update({
          status: "staged",
          import_pipeline_completed_at: null,
          metadata: mergeUploadMetadata((prevRowListing as { metadata?: unknown } | null)?.metadata, {
            row_count: cumulativeDataSeenAtEnd,
            total_rows: cumulativeDataSeenAtEnd,
            processed_rows: rowsInDb,
            process_progress: 100,
            physical_lines_seen: fileRowsTotal,
            data_rows_seen: cumulativeDataSeenAtEnd,
            staging_row_count: rowsInDb,
            catalog_listing_file_rows_seen: fileRowsTotal,
            catalog_listing_data_rows_seen: cumulativeDataSeenAtEnd,
            import_metrics: {
              current_phase: "staged",
              physical_lines_seen: fileRowsTotal,
              data_rows_seen: cumulativeDataSeenAtEnd,
              rows_staged: rowsInDb,
              rows_skipped_empty: skippedEmpty,
              rows_skipped_malformed: 0,
            },
          }),
          updated_at: new Date().toISOString(),
        })
        .eq("id", uploadId)
        .eq("organization_id", orgId);

      await supabaseServer.from("file_processing_status").upsert(
        {
          upload_id: uploadId,
          organization_id: orgId,
          status: "processing",
          current_phase: "staged",
          phase_key: FPS_KEY_PROCESS,
          phase_label: FPS_LABEL_PROCESS,
          current_phase_label: FPS_LABEL_PROCESS,
          stage_target_table: engine.stage_target_table,
          sync_target_table: engine.sync_target_table,
          generic_target_table: engine.generic_target_table,
          next_action_key: "sync",
          next_action_label: FPS_NEXT_ACTION_LABEL_SYNC,
          upload_pct: 100,
          process_pct: 100,
          sync_pct: 0,
          phase1_upload_pct: 100,
          phase2_stage_pct: 100,
          phase3_raw_sync_pct: 0,
          phase4_generic_pct: 0,
          processed_rows: rowsInDb,
          total_rows: Math.max(rowsInDb, cumulativeDataSeenAtEnd, 1),
          staged_rows_written: rowsInDb,
          file_rows_total: fileRowsTotal,
          data_rows_total: dataRowsTotal,
          phase2_status: "complete",
          phase2_completed_at: new Date().toISOString(),
          error_message: null,
          import_metrics: {
            current_phase: "staged",
            physical_lines_seen: fileRowsTotal,
            data_rows_seen: cumulativeDataSeenAtEnd,
            rows_staged: rowsInDb,
          },
        },
        { onConflict: "upload_id" },
      );

      logImportPhase({
        report_type: reportTypeRaw,
        upload_id: uploadId,
        phase: FPS_KEY_PROCESS,
        phase_key: FPS_KEY_PROCESS,
        target_table: engine.stage_target_table,
        rows_written: rowsInDb,
        rows_processed: rowsInDb,
      });

      await audit(orgId, "import.listing_stage_completed", uploadId, {
        rowsStaged: rowsInDb,
        totalSeen: fileRowsTotal,
        listing: true,
        skippedEmpty,
      });

      return NextResponse.json({
        ok: true,
        rowsStaged: rowsInDb,
        totalRows: fileRowsTotal,
        pipeline: "phase2_staging_listing",
      });
    }

    const parser = synthesizedHeaders
      ? csv({
          // Headerless input — supply headers up front and treat row 0 as data.
          headers: synthesizedHeaders,
          skipLines: 0,
          separator: csvSeparator,
        })
      : csv({
          mapHeaders: ({ header }) => String(header).replace(/^\uFEFF/, "").trim(),
          skipLines: headerRowIndex,
          separator: csvSeparator,
        });

    /** Lines that passed the date filter (before per-line dedupe). */
    let dataLinesPassed = 0;
    /** Rows successfully inserted into amazon_staging (1:1 with physical data lines). */
    let approxStaged = resumeRowCount;
    let totalSeen = 0;
    let dataRowNumber = 0;
    let batch: Record<string, unknown>[] = [];
    let lastProgressWrite = 0;
    let lastProgressTs = 0;
    /** Last time we re-read count(*) from amazon_staging — controls how often DB anchoring runs. */
    let lastDbCountReanchorTs = 0;
    /** Authoritative `count(*)` for this upload — refreshed periodically, used in progress writes so the UI never drifts. */
    let dbAnchoredStagedCount = resumeRowCount;

    const flushProgress = async (force = false) => {
      if (!force) {
        const rowsDelta = dataLinesPassed - lastProgressWrite;
        const msSinceLast = Date.now() - lastProgressTs;
        // Throttle: skip unless either the row-stride OR the time-stride elapsed.
        if (rowsDelta < PROGRESS_EVERY && msSinceLast < PROGRESS_MIN_INTERVAL_MS) return;
      }
      lastProgressWrite = dataLinesPassed;
      lastProgressTs = Date.now();

      // Periodically re-anchor `staged_rows_written` and `processed_rows` to the
      // ACTUAL `count(*)` from amazon_staging — not the in-memory accumulator.
      // This is what eliminates the "UI count vs DB count mismatch" symptom and
      // satisfies requirement 6 ("Progress must come from real DB state").
      if (force || Date.now() - lastDbCountReanchorTs > DB_COUNT_REANCHOR_INTERVAL_MS) {
        lastDbCountReanchorTs = Date.now();
        try {
          const { count: liveCount } = await supabaseServer
            .from(STAGING_TABLE)
            .select("*", { count: "exact", head: true })
            .eq("organization_id", orgId)
            .eq("upload_id", uploadId);
          if (typeof liveCount === "number") {
            // Anchor monotonically — never let a transient query glitch decrease it.
            dbAnchoredStagedCount = Math.max(dbAnchoredStagedCount, liveCount);
          }
        } catch {
          // Non-fatal — we keep the previous anchored count.
        }
      }
      /**
       * Cumulative counters (resume-aware): `dataRowNumber` already includes
       * rows from a prior partial run. `dbAnchoredStagedCount` is the live
       * `count(*)` from the DB so reported progress is always >= reality.
       */
      const cumulativeDataRows = dataRowNumber;
      const cumulativePhysicalSeen = Math.max(totalSeen, cumulativeDataRows);
      // For the progress denominators we use the LARGER of in-memory counter
      // and DB anchor, so the UI never regresses.
      const cumulativeStagedReported = Math.max(approxStaged, dbAnchoredStagedCount);
      const baseEst = estimatedRows && estimatedRows > 0 ? estimatedRows : null;
      const live = Math.max(1, cumulativeDataRows, cumulativePhysicalSeen);
      let denom = live;
      if (baseEst != null) {
        if (baseEst <= live * 1.25) {
          denom = Math.max(live, baseEst);
        } else {
          denom = Math.max(live, Math.ceil(live + (baseEst - live) * (live / Math.max(baseEst, 1))));
        }
      }
      const pct = Math.min(99, Math.round((cumulativeDataRows / denom) * 100));
      const { data: prevRow } = await supabaseServer
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadId)
        .maybeSingle();
      await supabaseServer
        .from("raw_report_uploads")
        .update({
          metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
            row_count: cumulativeDataRows,
            process_progress: pct,
            etl_phase: "staging",
            physical_lines_seen: cumulativePhysicalSeen,
            data_rows_seen: cumulativeDataRows,
            staging_row_count: cumulativeStagedReported,
            import_metrics: {
              current_phase: "staging",
              physical_lines_seen: cumulativePhysicalSeen,
              data_rows_seen: cumulativeDataRows,
              rows_staged: cumulativeStagedReported,
            },
          }),
          updated_at: new Date().toISOString(),
        })
        .eq("id", uploadId)
        .eq("organization_id", orgId);

      await supabaseServer.from("file_processing_status").upsert(
        {
          upload_id: uploadId,
          organization_id: orgId,
          status: "processing",
          current_phase: "staging",
          phase_key: FPS_KEY_PROCESS,
          phase_label: FPS_LABEL_PROCESS,
          stage_target_table: engine.stage_target_table,
          sync_target_table: engine.sync_target_table,
          generic_target_table: engine.generic_target_table,
          next_action_key: "sync",
          next_action_label: FPS_NEXT_ACTION_LABEL_SYNC,
          current_phase_label: FPS_LABEL_PROCESS,
          upload_pct: 100,
          process_pct: pct,
          phase1_upload_pct: 100,
          phase2_stage_pct: pct,
          phase3_raw_sync_pct: 0,
          phase4_generic_pct: 0,
          sync_pct: 0,
          processed_rows: cumulativeStagedReported,
          total_rows: denom,
          staged_rows_written: cumulativeStagedReported,
          data_rows_total: cumulativeDataRows,
          import_metrics: {
            physical_lines_seen: cumulativePhysicalSeen,
            data_rows_seen: cumulativeDataRows,
            rows_staged: cumulativeStagedReported,
            current_phase: "staging",
          },
        },
        { onConflict: "upload_id" },
      );
    };

    const flushBatch = async (rows: Record<string, unknown>[]) => {
      if (rows.length === 0) return;
      // Idempotent insert: ignore rows already staged for this (org, upload, row_number).
      // Each upsert is a single, independently-committed PostgREST request — a failure
      // here cannot roll back any prior batch. Next attempt resumes from the WATERMARK.
      const startRn = (rows[0] as { row_number?: number } | undefined)?.row_number ?? null;
      const endRn = (rows[rows.length - 1] as { row_number?: number } | undefined)?.row_number ?? null;
      const inMemoryStagedBefore = approxStaged;
      const { error: insErr } = await supabaseServer
        .from(STAGING_TABLE)
        .upsert(rows, { onConflict: STAGING_CONFLICT, ignoreDuplicates: true });
      if (insErr) {
        await logBatchFailureDiagnostic({
          orgId,
          uploadId,
          batchStartRowNumber: startRn,
          batchEndRowNumber: endRn,
          batchSize: rows.length,
          inMemoryStagedBefore,
          errorMessage: insErr.message,
        });
        throw new Error(
          `Upsert into ${STAGING_TABLE} failed (rows ${startRn}..${endRn}, batch_size=${rows.length}): ${insErr.message}`,
        );
      }
      approxStaged += rows.length;
      // Throttled (no longer forced per batch) — avoids hundreds of progress round-trips
      // on large files which were themselves a source of mid-run timeouts.
      await flushProgress(false);
    };

    /**
     * Sentinel thrown when SOFT_BUDGET_MS is exhausted. Caught at the outer
     * level and converted to a "click Process again" failure state — never
     * rethrown to Vercel as a generic 500.
     */
    const SOFT_BUDGET_SENTINEL = "PHASE2_SOFT_BUDGET_EXHAUSTED";

    let stagedRowCountForResponse = 0;

    let softTerminationTriggered = false;

    await new Promise<void>((resolve, reject) => {
      source.on("error", reject);
      parser.on("error", reject);

      parser.on("data", (csvRow: Record<string, string>) => {
        if (softTerminationTriggered) return;
        totalSeen += 1;

        if (filterStartDate || filterEndDate) {
          if (!rowMatchesDateRange(csvRow, filterStartDate, filterEndDate)) return;
        }

        const normalizedRow = normalizeAmazonReportRowKeys(csvRow);
        const mappedRow = applyColumnMappingToRow(normalizedRow, columnMapping);
        dataRowNumber += 1;
        // Resume: skip rows whose row_number is at or below the contiguous-prefix watermark.
        // We still increment dataRowNumber so the row_number we WOULD assign matches the
        // file order regardless of resume — guaranteeing 1..N contiguous semantics.
        if (resumeFromRowNumber > 0 && dataRowNumber <= resumeFromRowNumber) {
          return;
        }
        dataLinesPassed += 1;
        const source_line_hash = computeSourceLineHash(orgId, mappedRow);

        const stagingRow: Record<string, unknown> = {
          organization_id: orgId,
          upload_id: uploadId,
          report_type: reportTypeRaw,
          row_number: dataRowNumber,
          source_line_hash,
          raw_row: mappedRow,
        };

        const dateVal =
          mappedRow["Date"] ??
          mappedRow["date"] ??
          mappedRow["Date-Time"] ??
          mappedRow["date-time"] ??
          mappedRow["date_time"] ??
          mappedRow["date/time"] ??
          "";
        if (dateVal) {
          const d = new Date(String(dateVal));
          if (!Number.isNaN(d.getTime())) {
            stagingRow.snapshot_date = d.toISOString().slice(0, 10);
          }
        }

        batch.push(stagingRow);

        if (batch.length >= BATCH_SIZE) {
          parser.pause();
          const chunk = batch;
          batch = [];
          void flushBatch(chunk)
            .then(async () => {
              // Soft budget check between batches — stop accepting new rows
              // and let the parser drain so we can mark the upload "failed,
              // please click Process again to resume from row N" cleanly.
              if (isSoftBudgetExhausted()) {
                softTerminationTriggered = true;
                parser.removeAllListeners("data");
                try {
                  source.unpipe?.(parser);
                } catch {
                  /* ignore */
                }
                reject(new Error(SOFT_BUDGET_SENTINEL));
                return;
              }
              parser.resume();
            })
            .catch(reject);
        }
      });

      parser.on("end", () => {
        void (async () => {
          try {
            await flushBatch(batch);
            batch = [];

            // HARD VERIFICATION — must run before we mark the upload `staged`.
            // CSV (non-listing) reports get contiguous row_number 1..N, so we assert
            // count, max, AND no-gaps. Any mismatch throws and the outer catch flips
            // status to `failed` with the structured message — never silently `staged`.
            const verified = await assertStagingComplete({
              orgId,
              uploadId,
              expectedCount: dataRowNumber,
              expectedMaxRowNumber: dataRowNumber,
              isListing: false,
            });
            const rowsInDb = verified.dbCount;

            const { data: prevRow } = await supabaseServer
              .from("raw_report_uploads")
              .select("metadata")
              .eq("id", uploadId)
              .maybeSingle();

            // Cumulative counters (resume-aware): dataRowNumber includes rows whose
            // row_number was assigned (incl. resumed/skipped ones); dataLinesPassed only
            // includes rows actually inserted this run.
            const cumulativeDataSeen = dataRowNumber;
            const cumulativePhysicalSeen = Math.max(totalSeen, cumulativeDataSeen);

            await supabaseServer
              .from("raw_report_uploads")
              .update({
                status: "staged",
                import_pipeline_completed_at: null,
                metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
                  row_count: cumulativeDataSeen,
                  total_rows: cumulativeDataSeen,
                  processed_rows: rowsInDb,
                  process_progress: 100,
                  physical_lines_seen: cumulativePhysicalSeen,
                  data_rows_seen: cumulativeDataSeen,
                  staging_row_count: rowsInDb,
                  error_message: undefined,
                  import_metrics: {
                    current_phase: "staged",
                    physical_lines_seen: cumulativePhysicalSeen,
                    data_rows_seen: cumulativeDataSeen,
                    rows_staged: rowsInDb,
                    rows_skipped_empty: 0,
                    rows_skipped_malformed: 0,
                  },
                  ...(filterStartDate ? { start_date: body.start_date } : {}),
                  ...(filterEndDate ? { end_date: body.end_date } : {}),
                  ...(filterStartDate || filterEndDate ? { import_full_file: false } : { import_full_file: true }),
                }),
                updated_at: new Date().toISOString(),
              })
              .eq("id", uploadId)
              .eq("organization_id", orgId);

            await supabaseServer.from("file_processing_status").upsert(
              {
                upload_id: uploadId,
                organization_id: orgId,
                status: "processing",
                current_phase: "staged",
                phase_key: FPS_KEY_PROCESS,
                phase_label: FPS_LABEL_PROCESS,
                current_phase_label: FPS_LABEL_PROCESS,
                stage_target_table: engine.stage_target_table,
                sync_target_table: engine.sync_target_table,
                generic_target_table: engine.generic_target_table,
                next_action_key: "sync",
                next_action_label: FPS_NEXT_ACTION_LABEL_SYNC,
                upload_pct: 100,
                process_pct: 100,
                sync_pct: 0,
                phase1_upload_pct: 100,
                phase2_stage_pct: 100,
                phase3_raw_sync_pct: 0,
                phase4_generic_pct: 0,
                processed_rows: rowsInDb,
                total_rows: Math.max(rowsInDb, cumulativeDataSeen, 1),
                staged_rows_written: rowsInDb,
                file_rows_total: cumulativePhysicalSeen,
                data_rows_total: cumulativeDataSeen,
                phase2_status: "complete",
                phase2_completed_at: new Date().toISOString(),
                error_message: null,
                import_metrics: {
                  physical_lines_seen: cumulativePhysicalSeen,
                  data_rows_seen: cumulativeDataSeen,
                  rows_staged: rowsInDb,
                  current_phase: "staged",
                },
              },
              { onConflict: "upload_id" },
            );

            logImportPhase({
              report_type: reportTypeRaw,
              upload_id: uploadId,
              phase: FPS_KEY_PROCESS,
              phase_key: FPS_KEY_PROCESS,
              target_table: engine.stage_target_table,
              rows_written: rowsInDb,
              rows_processed: rowsInDb,
            });

            stagedRowCountForResponse = rowsInDb;

            await audit(orgId, "import.stage_completed", uploadId, {
              rowsStaged: rowsInDb,
              totalSeen,
              dateFiltered: !!(filterStartDate || filterEndDate),
            });
            resolve();
          } catch (e) {
            reject(e instanceof Error ? e : new Error(String(e)));
          }
        })();
      });

      source.pipe(parser);
    });

    return NextResponse.json({
      ok: true,
      rowsStaged: stagedRowCountForResponse,
      totalRows: totalSeen,
      pipeline: "phase2_staging",
    });
  } catch (e) {
    const rawMessage = e instanceof Error ? e.message : "Staging failed.";
    // Convert the soft-budget sentinel into a friendly, recoverable failure
    // state that explicitly tells the user (and any automation) the next step.
    const isSoftBudget = rawMessage === "PHASE2_SOFT_BUDGET_EXHAUSTED";
    const message = isSoftBudget
      ? process.env.VERCEL === "1"
        ? "Staging hit the Vercel wall-clock limit for one Process request. Click Process again to resume from the last staged row — or raise `maxDuration` on `/api/settings/imports/process` and set `PHASE2_SOFT_BUDGET_MS` (ms) a bit below that."
        : "Staging time budget exhausted. Click Process again to resume from the last successfully staged row."
      : rawMessage;
    if (uploadIdForFail && isUuidString(uploadIdForFail) && isUuidString(orgId)) {
      // Read live cardinality after the failure so the metadata reflects DB truth.
      let postFailureCount = 0;
      let postFailureMaxRn = 0;
      try {
        const card = await readStagingCardinality(orgId, uploadIdForFail);
        postFailureCount = card.count;
        postFailureMaxRn = card.max;
      } catch {
        // Diagnostic-only — never let a count failure mask the original failure.
      }
      const { data: prevRow } = await supabaseServer
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadIdForFail)
        .maybeSingle();
      // The diagnostic-only keys `phase2_*` are stored as runtime extras inside
      // the JSONB `metadata` column. They are not part of the strict
      // `RawReportUploadMetadata` typed surface, so the patch object is cast at
      // the call site.
      const failurePatch = {
        error_message: message,
        failed_phase: "process",
        staging_row_count: postFailureCount,
        import_metrics: {
          current_phase: "failed",
          failure_reason: message,
        },
        phase2_recoverable: isSoftBudget,
        phase2_committed_rows_at_failure: postFailureCount,
        phase2_max_row_number_at_failure: postFailureMaxRn,
      } as unknown as Parameters<typeof mergeUploadMetadata>[1];
      await supabaseServer
        .from("raw_report_uploads")
        .update({
          status: "failed",
          import_pipeline_failed_at: new Date().toISOString(),
          metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, failurePatch),
          updated_at: new Date().toISOString(),
        })
        .eq("id", uploadIdForFail)
        .eq("organization_id", orgId);

      await supabaseServer.from("file_processing_status").upsert(
        {
          upload_id: uploadIdForFail,
          organization_id: orgId,
          status: "failed",
          phase_key: FPS_KEY_FAILED,
          phase_label: message.slice(0, 200),
          next_action_key: null,
          next_action_label: null,
          upload_pct: 100,
          // Don't drop process_pct to 0 on soft-budget exit — the rows we DID
          // stage are real; the next click resumes from the watermark.
          ...(isSoftBudget
            ? {}
            : { process_pct: 0 }),
          processed_rows: postFailureCount,
          staged_rows_written: postFailureCount,
          error_message: message,
          // FPS `import_metrics` column is JSONB and accepts ad-hoc keys for
          // diagnostic visibility; cast to satisfy the strict typed-client
          // overload while preserving the runtime fields.
          import_metrics: {
            current_phase: "failed",
            failure_reason: message,
            recoverable: isSoftBudget,
            committed_rows_at_failure: postFailureCount,
            max_row_number_at_failure: postFailureMaxRn,
          } as unknown as ImportRunMetrics,
        },
        { onConflict: "upload_id" },
      );

      console.log(
        JSON.stringify({
          event: "phase2_staging_terminated",
          upload_id: uploadIdForFail,
          organization_id: orgId,
          recoverable: isSoftBudget,
          committed_rows_at_failure: postFailureCount,
          max_row_number_at_failure: postFailureMaxRn,
          message,
        }),
      );
    }
    // Soft-budget termination is a planned, recoverable outcome — return 200
    // with `ok: false, recoverable: true` so the UI can surface a "Click
    // Process again" hint instead of a generic 500. Hard failures still 500.
    if (isSoftBudget) {
      return NextResponse.json({ ok: false, error: message, recoverable: true }, { status: 200 });
    }
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
