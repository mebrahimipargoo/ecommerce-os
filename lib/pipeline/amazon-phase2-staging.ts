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
 * Batch size for `amazon_staging` upserts (Nano / WAL friendly with
 * `PHASE2_INTER_BATCH_SLEEP_MS`). Tune via `PHASE2_BATCH_SIZE` (clamped 50..500).
 * Each batch is its own implicit transaction (single PostgREST upsert call) so
 * a failure in batch N+1 cannot roll back batch N.
 */
const BATCH_SIZE = (() => {
  const raw = process.env.PHASE2_BATCH_SIZE;
  if (!raw) return 500;
  const n = Number(raw);
  if (!Number.isFinite(n)) return 500;
  return Math.min(500, Math.max(50, Math.floor(n)));
})();

/** Pause after each successful staging batch to smooth Disk I/O (0 = off). */
const INTER_BATCH_SLEEP_MS = (() => {
  const raw = process.env.PHASE2_INTER_BATCH_SLEEP_MS;
  if (!raw) return 2000;
  const n = Number(raw);
  if (!Number.isFinite(n)) return 2000;
  return Math.min(120_000, Math.max(0, Math.floor(n)));
})();

function sleep(ms: number): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((r) => setTimeout(r, ms));
}
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
 * Throttle progress writes. Progress flushes update `raw_report_uploads` + upsert
 * `file_processing_status` (metadata is merged in-memory to skip a per-flush metadata SELECT).
 * Forced writes (`flushProgress(true)`) bypass the time/row throttle for the final completion record.
 */
const PROGRESS_MIN_INTERVAL_MS = (() => {
  const raw = process.env.PROGRESS_MIN_INTERVAL_MS;
  if (!raw) return 5000;
  const n = Number(raw);
  if (!Number.isFinite(n)) return 5000;
  return Math.min(120_000, Math.max(500, Math.floor(n)));
})();
/** Backoff between attempts after a transient failure (batch upsert, reads, verify). */
const IMPORT_RETRY_BACKOFF_MS = [5000, 15_000, 30_000, 60_000, 120_000] as const;
/** First attempt + up to five delayed retries ⇒ six attempts total. */
const BATCH_UPSERT_MAX_ATTEMPTS = 6;
const VERIFY_READ_MAX_ATTEMPTS = 6;

const PHASE2_BATCH_UPSERT_EXHAUSTED = "PHASE2_BATCH_UPSERT_EXHAUSTED:";
const STAGING_VERIFY_MISMATCH = "STAGING_VERIFY_MISMATCH:";
/** Legacy "every N rows" fallback gate (kept so a 0ms-elapsed retry still emits a write occasionally). */
const PROGRESS_EVERY = 25;
const LISTING_PROGRESS_EVERY = 8;

function trimHeaderCell(h: string): string {
  return h.replace(/^\uFEFF/, "").trim();
}

/**
 * One-shot data-row count for progress denominator when `metadata.total_rows` / `row_count`
 * are absent. Same csv-parser rules as the main staging stream (second read of storage).
 */
async function countCsvDataRowsForStagingProgress(opts: {
  rawFilePath: string;
  storagePrefix: string;
  totalParts: number;
  headerRowIndex: number;
  synthesizedHeaders: string[] | null;
  csvSeparator: string;
}): Promise<number> {
  let source: NodeJS.ReadableStream;
  if (opts.rawFilePath) {
    const { data: blob, error: dlErr } = await supabaseServer.storage.from("raw-reports").download(opts.rawFilePath);
    if (dlErr || !blob) {
      throw new Error(dlErr?.message ?? `Could not download file for row count: ${opts.rawFilePath}`);
    }
    const webStream = blob.stream() as unknown as ReadableStream<Uint8Array>;
    source = Readable.fromWeb(webStream as Parameters<typeof Readable.fromWeb>[0]);
  } else {
    source = createConcatenatedPartsReadable(supabaseServer, opts.storagePrefix, opts.totalParts);
  }

  const parser =
    opts.synthesizedHeaders && opts.synthesizedHeaders.length > 0
      ? csv({
          headers: opts.synthesizedHeaders,
          skipLines: 0,
          separator: opts.csvSeparator,
        })
      : csv({
          mapHeaders: ({ header }) => String(header).replace(/^\uFEFF/, "").trim(),
          skipLines: opts.headerRowIndex,
          separator: opts.csvSeparator,
        });

  return await new Promise<number>((resolve, reject) => {
    let n = 0;
    parser.on("data", () => {
      n += 1;
    });
    parser.on("end", () => resolve(n));
    parser.on("error", reject);
    source.pipe(parser);
  });
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

/** Short backoff for a single paginated watermark read (Nano hiccups). */
const WATERMARK_PAGE_RETRY_BACKOFF_MS = [800, 2000, 5000] as const;

function isTransientImportError(message: string): boolean {
  const m = message.toLowerCase();
  return (
    m.includes("timeout") ||
    m.includes("timed out") ||
    m.includes("statement timeout") ||
    m.includes("deadlock") ||
    m.includes("too many connections") ||
    m.includes("econnreset") ||
    m.includes("etimedout") ||
    m.includes("eai_again") ||
    m.includes("fetch failed") ||
    m.includes("network") ||
    m.includes("socket") ||
    m.includes("502") ||
    m.includes("503") ||
    m.includes("504") ||
    m.includes("bad gateway") ||
    m.includes("gateway timeout") ||
    m.includes("service unavailable") ||
    m.includes("internal server error") ||
    m.includes("cloudflare") ||
    m.includes("premature close")
  );
}

async function withImportOperationRetries<T>(label: string, fn: () => Promise<T>): Promise<T> {
  let lastErr: Error | null = null;
  for (let attempt = 1; attempt <= VERIFY_READ_MAX_ATTEMPTS; attempt++) {
    try {
      return await fn();
    } catch (e) {
      const err = e instanceof Error ? e : new Error(String(e));
      lastErr = err;
      const transient = isTransientImportError(err.message);
      if (!transient || attempt >= VERIFY_READ_MAX_ATTEMPTS) {
        throw err;
      }
      const delay = IMPORT_RETRY_BACKOFF_MS[attempt - 1] ?? 120_000;
      console.warn(
        JSON.stringify({
          event: "phase2_import_op_retry",
          label,
          attempt,
          max: VERIFY_READ_MAX_ATTEMPTS,
          delay_ms: delay,
          message: err.message,
        }),
      );
      await sleep(delay);
    }
  }
  throw lastErr ?? new Error(`${label}: retries exhausted`);
}

/**
 * Returns the live cardinality of `amazon_staging` for (org, upload):
 * - `count`  — total rows for this upload
 * - `min`    — smallest row_number (0 if empty)
 * - `max`    — largest row_number (0 if empty)
 * Cheap: 3 head=true/limit queries. Throws on PostgREST error.
 */
async function readStagingCardinalityOnce(
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

async function readStagingCardinality(orgId: string, uploadId: string): Promise<{ count: number; min: number; max: number }> {
  return withImportOperationRetries(`readStagingCardinality:${uploadId}`, () => readStagingCardinalityOnce(orgId, uploadId));
}

/** Best-effort cardinality — returns `null` if all read attempts fail (resume must not throw). */
async function readStagingCardinalityNullable(
  orgId: string,
  uploadId: string,
): Promise<{ count: number; min: number; max: number } | null> {
  try {
    return await readStagingCardinality(orgId, uploadId);
  } catch {
    return null;
  }
}

async function readStagingMaxRowNumberNullable(orgId: string, uploadId: string): Promise<number | null> {
  try {
    const { data: maxRow, error: maxErr } = await supabaseServer
      .from(STAGING_TABLE)
      .select("row_number")
      .eq("organization_id", orgId)
      .eq("upload_id", uploadId)
      .order("row_number", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (maxErr) return null;
    if (maxRow && typeof (maxRow as { row_number?: unknown }).row_number === "number") {
      return Math.floor((maxRow as { row_number: number }).row_number);
    }
    return null;
  } catch {
    return null;
  }
}

async function readStagingMinRowNumberNullable(orgId: string, uploadId: string): Promise<number | null> {
  try {
    const { data: minRow, error: minErr } = await supabaseServer
      .from(STAGING_TABLE)
      .select("row_number")
      .eq("organization_id", orgId)
      .eq("upload_id", uploadId)
      .order("row_number", { ascending: true })
      .limit(1)
      .maybeSingle();
    if (minErr) return null;
    if (minRow && typeof (minRow as { row_number?: unknown }).row_number === "number") {
      return Math.floor((minRow as { row_number: number }).row_number);
    }
    return null;
  } catch {
    return null;
  }
}

function parseMetadataStagingFallbackRowCount(metadataFallback: unknown): number | null {
  if (!metadataFallback || typeof metadataFallback !== "object" || Array.isArray(metadataFallback)) return null;
  const o = metadataFallback as Record<string, unknown>;
  const wm = o.staging_contiguous_watermark;
  if (typeof wm === "number" && Number.isFinite(wm) && wm >= 0) return Math.floor(wm);
  const direct = o.staging_row_count;
  if (typeof direct === "number" && Number.isFinite(direct) && direct >= 0) return Math.floor(direct);
  const im = o.import_metrics;
  if (im && typeof im === "object" && !Array.isArray(im)) {
    const rs = (im as Record<string, unknown>).rows_staged;
    if (typeof rs === "number" && Number.isFinite(rs) && rs >= 0) return Math.floor(rs);
  }
  return null;
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
    let data: unknown[] | null = null;
    for (let p = 0; p < WATERMARK_PAGE_RETRY_BACKOFF_MS.length; p++) {
      const { data: pageData, error } = await supabaseServer
        .from(STAGING_TABLE)
        .select("row_number")
        .eq("organization_id", orgId)
        .eq("upload_id", uploadId)
        .gte("row_number", next)
        .lte("row_number", upper)
        .order("row_number", { ascending: true })
        .limit(WATERMARK_SCAN_PAGE);
      if (!error) {
        data = (pageData ?? []) as unknown[];
        break;
      }
      if (!isTransientImportError(error.message) || p >= WATERMARK_PAGE_RETRY_BACKOFF_MS.length - 1) {
        throw new Error(`Watermark scan failed: ${error.message}. upload_id=${uploadId}`);
      }
      await sleep(WATERMARK_PAGE_RETRY_BACKOFF_MS[p] ?? 5000);
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
 *
 * **Never throws** — on repeated PostgREST failures uses `metadataFallback`
 * row counts and best-effort min/max reads so a Process click can still resume.
 */
async function getStagingResumeState(
  orgId: string,
  uploadId: string,
  metadataFallback?: unknown | null,
): Promise<{
  watermark: number;
  rowCount: number;
  minRowNumber: number;
  maxRowNumber: number;
  hasGaps: boolean;
}> {
  const card = await readStagingCardinalityNullable(orgId, uploadId);
  if (card && card.count === 0) {
    return { watermark: 0, rowCount: 0, minRowNumber: 0, maxRowNumber: 0, hasGaps: false };
  }
  if (card) {
    const expectedSpan = card.max - card.min + 1;
    const noGapsAndStartsAtOne = card.min === 1 && card.count === expectedSpan;
    if (noGapsAndStartsAtOne) {
      return {
        watermark: card.max,
        rowCount: card.count,
        minRowNumber: card.min,
        maxRowNumber: card.max,
        hasGaps: false,
      };
    }
    let watermark = 0;
    try {
      watermark = card.min === 1 ? await findContiguousPrefixWatermark(orgId, uploadId, card.max) : 0;
    } catch (wmErr) {
      console.warn(
        JSON.stringify({
          event: "phase2_watermark_scan_degraded",
          upload_id: uploadId,
          organization_id: orgId,
          message: wmErr instanceof Error ? wmErr.message : String(wmErr),
        }),
      );
      watermark = 0;
    }
    return {
      watermark,
      rowCount: card.count,
      minRowNumber: card.min,
      maxRowNumber: card.max,
      hasGaps: true,
    };
  }

  const metaCount = parseMetadataStagingFallbackRowCount(metadataFallback);
  const maxRn = (await readStagingMaxRowNumberNullable(orgId, uploadId)) ?? 0;
  const minRn = (await readStagingMinRowNumberNullable(orgId, uploadId)) ?? 0;
  const rowCount = metaCount ?? maxRn;
  if (rowCount <= 0 && maxRn <= 0) {
    return { watermark: 0, rowCount: metaCount ?? 0, minRowNumber: 0, maxRowNumber: 0, hasGaps: false };
  }
  let watermark = 0;
  try {
    watermark = minRn === 1 && maxRn > 0 ? await findContiguousPrefixWatermark(orgId, uploadId, maxRn) : 0;
  } catch {
    watermark = 0;
  }
  console.warn(
    JSON.stringify({
      event: "phase2_resume_cardinality_degraded",
      upload_id: uploadId,
      organization_id: orgId,
      fallback_row_count: rowCount,
      db_max_row_number: maxRn,
      db_min_row_number: minRn,
      watermark,
    }),
  );
  return {
    watermark,
    rowCount: rowCount,
    minRowNumber: minRn,
    maxRowNumber: maxRn,
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

async function readVerificationSnapshotOnce(
  orgId: string,
  uploadId: string,
): Promise<{ dbCount: number; dbMin: number; dbMax: number }> {
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

  return { dbCount, dbMin, dbMax };
}

function verifyStagingSnapshotAgainstExpected(
  snap: { dbCount: number; dbMin: number; dbMax: number },
  opts: { uploadId: string; expectedCount: number; expectedMaxRowNumber: number; isListing: boolean },
): void {
  const { uploadId, expectedCount, expectedMaxRowNumber, isListing } = opts;
  const { dbCount, dbMin, dbMax } = snap;
  const ctx = `upload_id=${uploadId} expected_count=${expectedCount} db_count=${dbCount} expected_max=${expectedMaxRowNumber} db_max=${dbMax} db_min=${dbMin} listing=${isListing}`;

  if (dbCount !== expectedCount) {
    throw new Error(
      `${STAGING_VERIFY_MISMATCH}row count mismatch (expected ${expectedCount}, found ${dbCount} in amazon_staging). ${ctx}`,
    );
  }
  if (dbMax !== expectedMaxRowNumber) {
    throw new Error(
      `${STAGING_VERIFY_MISMATCH}max(row_number) mismatch (expected ${expectedMaxRowNumber}, found ${dbMax}). ${ctx}`,
    );
  }
  if (!isListing) {
    if (dbCount > 0 && dbMin !== 1) {
      throw new Error(
        `${STAGING_VERIFY_MISMATCH}min(row_number) expected 1 for contiguous report, found ${dbMin}. ${ctx}`,
      );
    }
    const span = dbCount === 0 ? 0 : dbMax - dbMin + 1;
    if (span !== dbCount) {
      throw new Error(
        `${STAGING_VERIFY_MISMATCH}row_number gaps detected (count=${dbCount}, span=${span}). ${ctx}`,
      );
    }
  }
}

/**
 * Hard verification — run after staging finishes (or after a resume completes).
 * Retries transient PostgREST failures. **Data mismatches** throw immediately
 * (no retry). If reads keep failing after retries, returns parser-trusted
 * counts with `verificationRelaxed: true` so the run can complete with
 * `staging_final_count_verify_pending` instead of failing the whole import.
 */
async function assertStagingComplete(opts: {
  orgId: string;
  uploadId: string;
  expectedCount: number;
  expectedMaxRowNumber: number;
  isListing: boolean;
}): Promise<{ dbCount: number; dbMin: number; dbMax: number; verificationRelaxed: boolean }> {
  const { orgId, uploadId, expectedCount, expectedMaxRowNumber, isListing } = opts;

  for (let attempt = 1; attempt <= VERIFY_READ_MAX_ATTEMPTS; attempt++) {
    try {
      const snap = await readVerificationSnapshotOnce(orgId, uploadId);
      verifyStagingSnapshotAgainstExpected(snap, opts);
      console.log(
        JSON.stringify({
          event: "phase2_staging_verified",
          upload_id: uploadId,
          organization_id: orgId,
          db_count: snap.dbCount,
          db_min_row_number: snap.dbMin,
          db_max_row_number: snap.dbMax,
          is_listing: isListing,
        }),
      );
      return { ...snap, verificationRelaxed: false };
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.startsWith(STAGING_VERIFY_MISMATCH)) {
        throw e;
      }
      const transient = isTransientImportError(msg);
      if (!transient) {
        throw e instanceof Error ? e : new Error(msg);
      }
      if (attempt >= VERIFY_READ_MAX_ATTEMPTS) {
        break;
      }
      const delay = IMPORT_RETRY_BACKOFF_MS[attempt - 1] ?? 120_000;
      console.warn(
        JSON.stringify({
          event: "phase2_staging_verify_retry",
          upload_id: uploadId,
          organization_id: orgId,
          attempt,
          delay_ms: delay,
          message: msg,
        }),
      );
      await sleep(delay);
    }
  }

  console.warn(
    JSON.stringify({
      event: "phase2_staging_verify_deferred",
      upload_id: uploadId,
      organization_id: orgId,
      expected_count: expectedCount,
      expected_max_row_number: expectedMaxRowNumber,
      is_listing: isListing,
      message: "DB verification reads failed after retries; trusting parser counters for completion.",
    }),
  );

  return {
    dbCount: expectedCount,
    dbMin: isListing ? 0 : expectedCount > 0 ? 1 : 0,
    dbMax: expectedMaxRowNumber,
    verificationRelaxed: true,
  };
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
    const next = await getStagingResumeState(orgId, uploadId, null);
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

async function upsertStagingBatchWithRetries(opts: {
  rows: Record<string, unknown>[];
  orgId: string;
  uploadId: string;
  pipeline: "listing" | "csv_stream";
  batchNo: number;
  inMemoryStagedBefore: number;
  patchPhase2Ui: (p: Record<string, unknown>) => void;
  persistRollingUploadMetadataOnly: () => Promise<void>;
}): Promise<{ durationMs: number }> {
  const { rows, orgId, uploadId, pipeline, batchNo, inMemoryStagedBefore, patchPhase2Ui, persistRollingUploadMetadataOnly } =
    opts;
  if (rows.length === 0) return { durationMs: 0 };
  const startRn = (rows[0] as { row_number?: number } | undefined)?.row_number ?? null;
  const endRn = (rows[rows.length - 1] as { row_number?: number } | undefined)?.row_number ?? null;
  const rowRangeStr =
    typeof startRn === "number" && typeof endRn === "number" ? `${startRn}–${endRn}` : "unknown";
  let lastMsg = "";

  for (let attempt = 1; attempt <= BATCH_UPSERT_MAX_ATTEMPTS; attempt++) {
    patchPhase2Ui({
      phase2_operator_state: attempt > 1 ? "retrying_batch" : "processing",
      phase2_operator_line:
        attempt === 1
          ? "Processing — writing staging batches"
          : `Retrying batch ${batchNo} (${rowRangeStr}), attempt ${attempt}/${BATCH_UPSERT_MAX_ATTEMPTS}`,
      phase2_operator_batch: batchNo,
      phase2_operator_row_range: rowRangeStr,
      phase2_operator_batch_attempts: attempt,
    });
    await persistRollingUploadMetadataOnly();

    const t0 = Date.now();
    const { error: insErr } = await supabaseServer
      .from(STAGING_TABLE)
      .upsert(rows, { onConflict: STAGING_CONFLICT, ignoreDuplicates: true });
    const durationMs = Date.now() - t0;

    if (!insErr) {
      patchPhase2Ui({
        phase2_operator_state: "processing",
        phase2_operator_line: "Processing — writing staging batches",
        phase2_operator_batch: batchNo,
        phase2_operator_row_range: rowRangeStr,
        phase2_operator_batch_attempts: attempt,
      });
      return { durationMs };
    }

    lastMsg = insErr.message;
    const transient = isTransientImportError(insErr.message);
    if (!transient || attempt >= BATCH_UPSERT_MAX_ATTEMPTS) {
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
        `${PHASE2_BATCH_UPSERT_EXHAUSTED}${JSON.stringify({
          pipeline,
          batch: batchNo,
          row_range: rowRangeStr,
          attempts: attempt,
          message: insErr.message,
          transient,
        })}`,
      );
    }

    patchPhase2Ui({
      phase2_operator_state: "waiting_before_retry",
      phase2_operator_line: `Waiting before retry (batch ${batchNo}, attempt ${attempt}/${BATCH_UPSERT_MAX_ATTEMPTS})`,
      phase2_operator_batch: batchNo,
      phase2_operator_row_range: rowRangeStr,
      phase2_operator_batch_attempts: attempt,
    });
    await persistRollingUploadMetadataOnly();
    await sleep(IMPORT_RETRY_BACKOFF_MS[attempt - 1] ?? 120_000);
  }

  throw new Error(
    `${PHASE2_BATCH_UPSERT_EXHAUSTED}${JSON.stringify({
      pipeline,
      batch: batchNo,
      row_range: rowRangeStr,
      attempts: BATCH_UPSERT_MAX_ATTEMPTS,
      message: lastMsg || "unknown",
      transient: true,
    })}`,
  );
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
  /** Fixed file/plan total for FPS `total_rows` — set once staging begins (survives catch for diagnostics). */
  let progressFileTotalRowsSnapshot: number | null = null;

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
    const rowCountFromMeta =
      typeof metaObj.row_count === "number" && Number.isFinite(metaObj.row_count) && metaObj.row_count > 0
        ? Math.floor(metaObj.row_count)
        : typeof metaObj.row_count === "string" && String(metaObj.row_count).trim() !== ""
          ? Math.floor(num(metaObj.row_count, 0))
          : 0;

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
    const resumeState = await getStagingResumeState(orgId, uploadId, meta);
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
      const webStream = blob.stream() as unknown as ReadableStream<Uint8Array>;
      source = Readable.fromWeb(webStream as Parameters<typeof Readable.fromWeb>[0]);
    } else {
      source = createConcatenatedPartsReadable(supabaseServer, storagePrefix, totalParts);
    }

    let progressFileTotalRows: number | null =
      totalRowsFromMeta > 0 ? totalRowsFromMeta : rowCountFromMeta > 0 ? rowCountFromMeta : null;

    let listingPhase2FileBuf: Buffer | null = null;

    if (isListingReportType(reportTypeRaw)) {
      listingPhase2FileBuf = await streamToBuffer(source as Readable);
      const tmpLines = splitPhysicalLines(listingPhase2FileBuf.toString("utf8"));
      const linesForCount =
        headerRowIndex > 0 && headerRowIndex < tmpLines.length ? tmpLines.slice(headerRowIndex) : tmpLines;
      const listingDataRowsFromFile = linesForCount.length > 0 ? Math.max(0, linesForCount.length - 1) : 0;
      if (progressFileTotalRows == null || progressFileTotalRows < 1) {
        progressFileTotalRows = Math.max(1, listingDataRowsFromFile);
      }
    } else {
      if (progressFileTotalRows == null || progressFileTotalRows < 1) {
        try {
          const scanned = await countCsvDataRowsForStagingProgress({
            rawFilePath,
            storagePrefix,
            totalParts,
            headerRowIndex,
            synthesizedHeaders,
            csvSeparator,
          });
          progressFileTotalRows = Number.isFinite(scanned) && scanned > 0 ? scanned : null;
        } catch (countErr) {
          console.warn(
            JSON.stringify({
              event: "phase2_progress_row_count_failed",
              upload_id: uploadId,
              message: countErr instanceof Error ? countErr.message : String(countErr),
            }),
          );
          progressFileTotalRows = null;
        }
      }
      if (rawFilePath) {
        const { data: blob, error: dlErr } = await supabaseServer.storage.from("raw-reports").download(rawFilePath);
        if (dlErr || !blob) {
          throw new Error(dlErr?.message ?? `Could not download file from storage: ${rawFilePath}`);
        }
        const webStream = blob.stream() as unknown as ReadableStream<Uint8Array>;
        source = Readable.fromWeb(webStream as Parameters<typeof Readable.fromWeb>[0]);
      } else {
        source = createConcatenatedPartsReadable(supabaseServer, storagePrefix, totalParts);
      }
    }

    progressFileTotalRowsSnapshot = progressFileTotalRows;

    const computedInitialPctRaw =
      progressFileTotalRows != null && progressFileTotalRows > 0 && resumeFromRowNumber > 0
        ? Math.min(99, Math.round((resumeFromRowNumber / progressFileTotalRows) * 100))
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
    const initialProcessedRows = Math.max(resumeFromRowNumber, existingProcessedRows);
    const initialStagedRowsWritten = Math.max(resumeFromRowNumber, existingStagedRowsWritten);

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
        total_rows: progressFileTotalRows != null && progressFileTotalRows > 0 ? progressFileTotalRows : null,
        error_message: null,
        import_metrics: { current_phase: "staging", rows_staged: initialStagedRowsWritten },
      },
      { onConflict: "upload_id" },
    );

    /** In-memory metadata merge chain — avoids a `select metadata` round-trip on every progress flush. */
    let rollingUploadMetadata: unknown = mergeUploadMetadata(meta, {
      error_message: "",
      ...(progressFileTotalRows != null && progressFileTotalRows > 0 ? { total_rows: progressFileTotalRows } : {}),
      import_metrics: { current_phase: "staging" },
    });

    const patchPhase2Ui = (imPatch: Partial<ImportRunMetrics>) => {
      const r = rollingUploadMetadata as Record<string, unknown>;
      const prevIm =
        r.import_metrics && typeof r.import_metrics === "object" && !Array.isArray(r.import_metrics)
          ? { ...(r.import_metrics as Record<string, unknown>) }
          : {};
      rollingUploadMetadata = mergeUploadMetadata(rollingUploadMetadata, {
        import_metrics: { ...prevIm, ...imPatch } as ImportRunMetrics,
      });
    };

    const mergeRollingImportMetrics = (next: ImportRunMetrics): ImportRunMetrics => {
      const r = rollingUploadMetadata as Record<string, unknown>;
      const prevIm =
        r.import_metrics && typeof r.import_metrics === "object" && !Array.isArray(r.import_metrics)
          ? { ...(r.import_metrics as Record<string, unknown>) }
          : {};
      return { ...prevIm, ...next } as ImportRunMetrics;
    };

    const persistRollingUploadMetadataOnly = async (): Promise<void> => {
      try {
        await supabaseServer
          .from("raw_report_uploads")
          .update({
            metadata: rollingUploadMetadata,
            updated_at: new Date().toISOString(),
          })
          .eq("id", uploadId)
          .eq("organization_id", orgId);
      } catch {
        /* best-effort — must not block batch retries */
      }
    };

    // ── Listing exports: physical lines → amazon_staging (Phase 2 only) ─────
    if (isListingReportType(reportTypeRaw)) {
      const sep = csvSeparator as "\t" | ",";
      if (!listingPhase2FileBuf) {
        throw new Error("Listing file buffer was not prepared — internal Phase 2 error.");
      }
      const fileBuf = listingPhase2FileBuf;
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
      let listingBatchSeq = 0;

      const flushListingProgress = async (force: boolean, physicalDone: number) => {
        if (!force) {
          const rowsDelta = physicalDone - lastListingProgressWrite;
          const msSinceLast = Date.now() - lastListingProgressTs;
          // Throttle: skip unless either the row-stride OR the time-stride has elapsed.
          if (rowsDelta < LISTING_PROGRESS_EVERY && msSinceLast < PROGRESS_MIN_INTERVAL_MS) return;
        }
        lastListingProgressWrite = physicalDone;
        lastListingProgressTs = Date.now();
        const cumulativeDataSeen = resumeRowCount + dataLinesPassed;
        const pendingTailRn =
          batch.length > 0
            ? Math.max(
                lastFileLineNumberStaged,
                Math.floor(
                  Number((batch[batch.length - 1] as { row_number?: unknown }).row_number ?? lastFileLineNumberStaged),
                ),
              )
            : lastFileLineNumberStaged;
        const fileTotalForProgress =
          progressFileTotalRows != null && progressFileTotalRows > 0 ? progressFileTotalRows : dataRowsTotal;
        const pct =
          fileTotalForProgress > 0
            ? Math.min(99, Math.round((pendingTailRn / fileTotalForProgress) * 100))
            : Math.min(99, Math.round((physicalDone / Math.max(1, dataRowsTotal)) * 100));
        const listingProgressImportMetrics = mergeRollingImportMetrics({
          current_phase: "staging",
          physical_lines_seen: fileRowsTotal,
          data_rows_seen: cumulativeDataSeen,
          rows_staged: pendingTailRn,
        });
        rollingUploadMetadata = mergeUploadMetadata(rollingUploadMetadata, {
          row_count: pendingTailRn,
          total_rows: fileTotalForProgress,
          process_progress: pct,
          physical_lines_seen: fileRowsTotal,
          data_rows_seen: cumulativeDataSeen,
          import_metrics: listingProgressImportMetrics,
        });
        await supabaseServer
          .from("raw_report_uploads")
          .update({
            metadata: rollingUploadMetadata,
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
            processed_rows: pendingTailRn,
            total_rows: fileTotalForProgress,
            staged_rows_written: pendingTailRn,
            file_rows_total: fileRowsTotal,
            data_rows_total: dataRowsTotal,
            phase2_status: "running",
            phase2_started_at: new Date().toISOString(),
            import_metrics: listingProgressImportMetrics,
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
        listingBatchSeq += 1;
        const batchNo = listingBatchSeq;
        const startRn = (rows[0] as { row_number?: number } | undefined)?.row_number ?? null;
        const endRn = (rows[rows.length - 1] as { row_number?: number } | undefined)?.row_number ?? null;
        const inMemoryStagedBefore = approxStaged;
        const { durationMs } = await upsertStagingBatchWithRetries({
          rows,
          orgId,
          uploadId,
          pipeline: "listing",
          batchNo,
          inMemoryStagedBefore,
          patchPhase2Ui,
          persistRollingUploadMetadataOnly,
        });
        console.log(
          JSON.stringify({
            event: "phase2_staging_batch",
            pipeline: "listing",
            upload_id: uploadId,
            organization_id: orgId,
            batch: batchNo,
            rows: rows.length,
            row_number_start: startRn,
            row_number_end: endRn,
            duration_ms: durationMs,
          }),
        );
        approxStaged += rows.length;
        if (typeof endRn === "number") lastFileLineNumberStaged = endRn;
        // Throttled progress (no longer forced per batch — see PROGRESS_MIN_INTERVAL_MS).
        await flushListingProgress(false, dataLinesPassed);
        await sleep(INTER_BATCH_SLEEP_MS);
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

      const cumulativeDataSeenAtEnd = resumeRowCount + dataLinesPassed;
      const listingFileTotalPlan =
        progressFileTotalRows != null && progressFileTotalRows > 0
          ? progressFileTotalRows
          : Math.max(1, dataRowsTotal);

      rollingUploadMetadata = mergeUploadMetadata(rollingUploadMetadata, {
        row_count: lastFileLineNumberStaged,
        total_rows: listingFileTotalPlan,
        processed_rows: lastFileLineNumberStaged,
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
          ...(verified.verificationRelaxed
            ? {
                staging_final_count_verify_pending: true,
                phase2_operator_state: "final_verification_pending" as const,
                phase2_operator_line:
                  "Final verification pending — database could not confirm counts; parser totals were used.",
              }
            : {
                staging_final_count_verify_pending: false,
                phase2_operator_state: "completed" as const,
                phase2_operator_line: "Staging completed.",
              }),
        },
      });

      await supabaseServer
        .from("raw_report_uploads")
        .update({
          status: "staged",
          import_pipeline_completed_at: null,
          metadata: rollingUploadMetadata,
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
          processed_rows: lastFileLineNumberStaged,
          total_rows: listingFileTotalPlan,
          staged_rows_written: lastFileLineNumberStaged,
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
            ...(verified.verificationRelaxed
              ? {
                  staging_final_count_verify_pending: true,
                  phase2_operator_state: "final_verification_pending" as const,
                  phase2_operator_line:
                    "Final verification pending — database could not confirm counts; parser totals were used.",
                }
              : {
                  staging_final_count_verify_pending: false,
                  phase2_operator_state: "completed" as const,
                  phase2_operator_line: "Staging completed.",
                }),
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
        totalRows:
          progressFileTotalRows != null && progressFileTotalRows > 0 ? progressFileTotalRows : fileRowsTotal,
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
    /** Diagnostic: rows inserted this run (batch lengths); not used for FPS `processed_rows`. */
    let approxStaged = resumeRowCount;
    let totalSeen = 0;
    let dataRowNumber = 0;
    let batch: Record<string, unknown>[] = [];
    let lastProgressWrite = 0;
    let lastProgressTs = 0;
    /** Max `row_number` after the last successful batch upsert — FPS progress without `count(*)`. */
    let lastCommittedMaxRowNumber = resumeFromRowNumber;
    let mainBatchSeq = 0;

    const flushProgress = async (force = false) => {
      if (!force) {
        const rowsDelta = dataLinesPassed - lastProgressWrite;
        const msSinceLast = Date.now() - lastProgressTs;
        // Throttle: skip unless either the row-stride OR the time-stride elapsed.
        if (rowsDelta < PROGRESS_EVERY && msSinceLast < PROGRESS_MIN_INTERVAL_MS) return;
      }
      lastProgressWrite = dataLinesPassed;
      lastProgressTs = Date.now();

      const cumulativeDataRows = dataRowNumber;
      const cumulativePhysicalSeen = Math.max(totalSeen, cumulativeDataRows);
      const pct =
        progressFileTotalRows != null && progressFileTotalRows > 0
          ? Math.min(99, Math.round((lastCommittedMaxRowNumber / progressFileTotalRows) * 100))
          : Math.min(99, Math.round((cumulativeDataRows / Math.max(1, cumulativePhysicalSeen)) * 100));

      const progressImportMetrics = mergeRollingImportMetrics({
        current_phase: "staging",
        physical_lines_seen: cumulativePhysicalSeen,
        data_rows_seen: cumulativeDataRows,
        rows_staged: lastCommittedMaxRowNumber,
      });
      rollingUploadMetadata = mergeUploadMetadata(rollingUploadMetadata, {
        row_count: lastCommittedMaxRowNumber,
        process_progress: pct,
        etl_phase: "staging",
        physical_lines_seen: cumulativePhysicalSeen,
        data_rows_seen: cumulativeDataRows,
        staging_contiguous_watermark: lastCommittedMaxRowNumber,
        ...(progressFileTotalRows != null && progressFileTotalRows > 0 ? { total_rows: progressFileTotalRows } : {}),
        import_metrics: progressImportMetrics,
      });
      await supabaseServer
        .from("raw_report_uploads")
        .update({
          metadata: rollingUploadMetadata,
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
          processed_rows: lastCommittedMaxRowNumber,
          total_rows: progressFileTotalRows != null && progressFileTotalRows > 0 ? progressFileTotalRows : null,
          staged_rows_written: lastCommittedMaxRowNumber,
          data_rows_total: cumulativeDataRows,
          import_metrics: progressImportMetrics,
        },
        { onConflict: "upload_id" },
      );
    };

    const flushBatch = async (rows: Record<string, unknown>[]) => {
      if (rows.length === 0) return;
      // Idempotent insert: ignore rows already staged for this (org, upload, row_number).
      // Each upsert is a single, independently-committed PostgREST request — a failure
      // here cannot roll back any prior batch. Next attempt resumes from the WATERMARK.
      mainBatchSeq += 1;
      const batchNo = mainBatchSeq;
      const startRn = (rows[0] as { row_number?: number } | undefined)?.row_number ?? null;
      const endRn = (rows[rows.length - 1] as { row_number?: number } | undefined)?.row_number ?? null;
      const inMemoryStagedBefore = approxStaged;
      const { durationMs } = await upsertStagingBatchWithRetries({
        rows,
        orgId,
        uploadId,
        pipeline: "csv_stream",
        batchNo,
        inMemoryStagedBefore,
        patchPhase2Ui,
        persistRollingUploadMetadataOnly,
      });
      console.log(
        JSON.stringify({
          event: "phase2_staging_batch",
          pipeline: "csv_stream",
          upload_id: uploadId,
          organization_id: orgId,
          batch: batchNo,
          rows: rows.length,
          row_number_start: startRn,
          row_number_end: endRn,
          duration_ms: durationMs,
        }),
      );
      approxStaged += rows.length;
      if (typeof endRn === "number" && Number.isFinite(endRn)) {
        lastCommittedMaxRowNumber = Math.max(lastCommittedMaxRowNumber, Math.floor(endRn));
      }
      // Throttled (no longer forced per batch) — avoids hundreds of progress round-trips
      // on large files which were themselves a source of mid-run timeouts.
      await flushProgress(false);
      await sleep(INTER_BATCH_SLEEP_MS);
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

            const cumulativeDataSeen = dataRowNumber;
            const cumulativePhysicalSeen = Math.max(totalSeen, cumulativeDataSeen);
            const csvFileTotalPlan =
              progressFileTotalRows != null && progressFileTotalRows > 0
                ? progressFileTotalRows
                : Math.max(1, cumulativeDataSeen);

            rollingUploadMetadata = mergeUploadMetadata(rollingUploadMetadata, {
              row_count: lastCommittedMaxRowNumber,
              total_rows: csvFileTotalPlan,
              processed_rows: lastCommittedMaxRowNumber,
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
                ...(verified.verificationRelaxed
                  ? {
                      staging_final_count_verify_pending: true,
                      phase2_operator_state: "final_verification_pending" as const,
                      phase2_operator_line:
                        "Final verification pending — database could not confirm counts; parser totals were used.",
                    }
                  : {
                      staging_final_count_verify_pending: false,
                      phase2_operator_state: "completed" as const,
                      phase2_operator_line: "Staging completed.",
                    }),
              },
              ...(filterStartDate ? { start_date: body.start_date } : {}),
              ...(filterEndDate ? { end_date: body.end_date } : {}),
              ...(filterStartDate || filterEndDate ? { import_full_file: false } : { import_full_file: true }),
            });

            await supabaseServer
              .from("raw_report_uploads")
              .update({
                status: "staged",
                import_pipeline_completed_at: null,
                metadata: rollingUploadMetadata,
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
                processed_rows: lastCommittedMaxRowNumber,
                total_rows: csvFileTotalPlan,
                staged_rows_written: lastCommittedMaxRowNumber,
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
                  ...(verified.verificationRelaxed
                    ? {
                        staging_final_count_verify_pending: true,
                        phase2_operator_state: "final_verification_pending" as const,
                        phase2_operator_line:
                          "Final verification pending — database could not confirm counts; parser totals were used.",
                      }
                    : {
                        staging_final_count_verify_pending: false,
                        phase2_operator_state: "completed" as const,
                        phase2_operator_line: "Staging completed.",
                      }),
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
      totalRows: progressFileTotalRows != null && progressFileTotalRows > 0 ? progressFileTotalRows : totalSeen,
      pipeline: "phase2_staging",
    });
  } catch (e) {
    const rawMessage = e instanceof Error ? e.message : "Staging failed.";
    // Convert the soft-budget sentinel into a friendly, recoverable failure
    // state that explicitly tells the user (and any automation) the next step.
    const isSoftBudget = rawMessage === "PHASE2_SOFT_BUDGET_EXHAUSTED";
    const isBatchExhausted = rawMessage.startsWith(PHASE2_BATCH_UPSERT_EXHAUSTED);
    type BatchUpsertDiag = {
      pipeline?: string;
      batch?: number;
      row_range?: string;
      attempts?: number;
      message?: string;
    };
    let batchDiag: BatchUpsertDiag | null = null;
    if (isBatchExhausted) {
      try {
        batchDiag = JSON.parse(rawMessage.slice(PHASE2_BATCH_UPSERT_EXHAUSTED.length)) as BatchUpsertDiag;
      } catch {
        batchDiag = null;
      }
    }
    const message = isSoftBudget
      ? process.env.VERCEL === "1"
        ? "Staging hit the Vercel wall-clock limit for one Process request. Click Process again to resume from the last staged row — or raise `maxDuration` on `/api/settings/imports/process` and set `PHASE2_SOFT_BUDGET_MS` (ms) a bit below that."
        : "Staging time budget exhausted. Click Process again to resume from the last successfully staged row."
      : isBatchExhausted
        ? `Staging batch upsert failed after ${batchDiag?.attempts ?? BATCH_UPSERT_MAX_ATTEMPTS} attempt(s) (batch ${batchDiag?.batch ?? "?"}, rows ${batchDiag?.row_range ?? "?"}): ${batchDiag?.message ?? "unknown error"}`
        : rawMessage;
    let httpRecoverable = isSoftBudget;
    if (uploadIdForFail && isUuidString(uploadIdForFail) && isUuidString(orgId)) {
      // Read live cardinality after the failure so the metadata reflects DB truth.
      let postFailureCount = 0;
      let postFailureMaxRn = 0;
      const cardNullable = await readStagingCardinalityNullable(orgId, uploadIdForFail);
      if (cardNullable) {
        postFailureCount = cardNullable.count;
        postFailureMaxRn = cardNullable.max;
      }
      const { data: prevRow } = await supabaseServer
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadIdForFail)
        .maybeSingle();
      const prevMeta = (prevRow as { metadata?: unknown } | null)?.metadata;
      const prevRec = prevMeta && typeof prevMeta === "object" && !Array.isArray(prevMeta) ? (prevMeta as Record<string, unknown>) : {};
      const failPreserveTotalRows = ((): number | null => {
        if (progressFileTotalRowsSnapshot != null && progressFileTotalRowsSnapshot > 0) {
          return progressFileTotalRowsSnapshot;
        }
        const tr = prevRec.total_rows;
        if (typeof tr === "number" && Number.isFinite(tr) && tr > 0) return Math.floor(tr);
        if (typeof tr === "string" && tr.trim() !== "") {
          const n = Number(tr);
          if (Number.isFinite(n) && n > 0) return Math.floor(n);
        }
        return null;
      })();
      const failProgressWatermark = postFailureMaxRn > 0 ? postFailureMaxRn : 0;
      const prevImRaw = prevRec.import_metrics;
      const prevIm =
        prevImRaw && typeof prevImRaw === "object" && !Array.isArray(prevImRaw)
          ? { ...(prevImRaw as Record<string, unknown>) }
          : {};
      // The diagnostic-only keys `phase2_*` are stored as runtime extras inside
      // the JSONB `metadata` column. They are not part of the strict
      // `RawReportUploadMetadata` typed surface, so the patch object is cast at
      // the call site.
      const failurePatch = {
        error_message: message,
        failed_phase: "process",
        staging_row_count: postFailureCount,
        import_metrics: {
          ...prevIm,
          current_phase: "failed",
          failure_reason: message,
          ...(isSoftBudget
            ? {
                phase2_operator_state: "resume_available" as const,
                phase2_operator_line: "Resume available — click Process again to continue staging.",
              }
            : {}),
          ...(isBatchExhausted
            ? {
                phase2_operator_state: "failed_after_retries" as const,
                phase2_operator_line: `Failed after retries — batch ${batchDiag?.batch ?? "?"}, rows ${batchDiag?.row_range ?? "?"}.`,
                phase2_operator_batch: batchDiag?.batch,
                phase2_operator_row_range: batchDiag?.row_range,
                phase2_operator_batch_attempts: batchDiag?.attempts,
              }
            : {}),
        },
        phase2_recoverable: isSoftBudget || (isBatchExhausted && postFailureCount > 0),
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
          ...(isSoftBudget || (isBatchExhausted && postFailureCount > 0)
            ? {}
            : { process_pct: 0 }),
          ...(failPreserveTotalRows != null ? { total_rows: failPreserveTotalRows } : {}),
          processed_rows: failProgressWatermark,
          staged_rows_written: failProgressWatermark,
          error_message: message,
          // FPS `import_metrics` column is JSONB and accepts ad-hoc keys for
          // diagnostic visibility; cast to satisfy the strict typed-client
          // overload while preserving the runtime fields.
          import_metrics: {
            current_phase: "failed",
            failure_reason: message,
            recoverable: isSoftBudget || (isBatchExhausted && postFailureCount > 0),
            committed_rows_at_failure: postFailureCount,
            max_row_number_at_failure: postFailureMaxRn,
            ...(isSoftBudget
              ? {
                  phase2_operator_state: "resume_available" as const,
                  phase2_operator_line: "Resume available — click Process again to continue staging.",
                }
              : {}),
            ...(isBatchExhausted
              ? {
                  phase2_operator_state: "failed_after_retries" as const,
                  phase2_operator_line: `Failed after retries — batch ${batchDiag?.batch ?? "?"}, rows ${batchDiag?.row_range ?? "?"}.`,
                  phase2_operator_batch: batchDiag?.batch,
                  phase2_operator_row_range: batchDiag?.row_range,
                  phase2_operator_batch_attempts: batchDiag?.attempts,
                }
              : {}),
          } as unknown as ImportRunMetrics,
        },
        { onConflict: "upload_id" },
      );

      httpRecoverable = isSoftBudget || (isBatchExhausted && postFailureCount > 0);
      console.log(
        JSON.stringify({
          event: "phase2_staging_terminated",
          upload_id: uploadIdForFail,
          organization_id: orgId,
          recoverable: httpRecoverable,
          batch_exhausted: isBatchExhausted,
          committed_rows_at_failure: postFailureCount,
          max_row_number_at_failure: postFailureMaxRn,
          message,
        }),
      );
    }
    // Soft-budget termination is a planned, recoverable outcome — return 200
    // with `ok: false, recoverable: true` so the UI can surface a "Click
    // Process again" hint instead of a generic 500. Hard failures still 500.
    if (httpRecoverable) {
      return NextResponse.json({ ok: false, error: message, recoverable: true }, { status: 200 });
    }
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
