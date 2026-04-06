/**
 * POST /api/settings/imports/stage
 *
 * Phase 2 of the 3-phase ETL pipeline.
 *
 * Reads the CSV from Supabase Storage (or single raw_file_path), applies the
 * saved column_mapping, optionally filters rows by date range, and inserts
 * matching rows into amazon_staging. Sets status -> "staged" on completion.
 *
 * Accepts: {
 *   upload_id:        string   — UUID of the raw_report_uploads row
 *   start_date?:      string   — ISO date (YYYY-MM-DD); rows before this are skipped
 *   end_date?:        string   — ISO date (YYYY-MM-DD); rows after this are skipped
 *   import_full_file?: boolean — when true, date filter is ignored
 * }
 * Returns: { ok: true, rowsStaged: number, totalRows: number }
 */

import csv from "csv-parser";
import { NextResponse } from "next/server";

import { createConcatenatedPartsReadable } from "../../../../../lib/import-raw-report-stream";
import {
  applyColumnMappingToRow,
  normalizeAmazonReportRowKeys,
} from "../../../../../lib/import-sync-mappers";
import { mergeUploadMetadata, parseRawReportMetadata } from "../../../../../lib/raw-report-upload-metadata";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

/** Staging table name — renamed from amazon_ledger_staging in migration 20260430. */
const STAGING_TABLE = "amazon_staging";

export const runtime = "nodejs";
export const maxDuration = 300;

const BATCH_SIZE = 500;
const PROGRESS_EVERY = 500;

type Body = {
  upload_id?: string;
  start_date?: string | null;
  end_date?: string | null;
  import_full_file?: boolean | null;
};

/** Date columns to probe (in priority order) when applying a date range filter. */
const DATE_COLUMN_CANDIDATES = [
  "date/time", "Date/Time",
  "date_time",
  "posted-date", "Posted Date",
  "date", "Date",
  "return-date", "Return Date",
  "requested-date", "Requested Date",
  "settlement-start-date", "Settlement Start Date",
  "event-date", "Event Date",
];

/**
 * Checks whether a CSV row falls within [startDate, endDate] (inclusive).
 * Returns true if no date range is set, or if no date column is found in the row.
 */
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
  // No date column found — include the row (don't silently drop data)
  return true;
}

function num(v: unknown, fallback = 0): number {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return fallback;
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

export async function POST(req: Request): Promise<Response> {
  let uploadIdForFail: string | null = null;
  let orgId = "";

  try {
    const body = (await req.json()) as Body;

    // Parse optional date filter from request body
    const importFullFile = body.import_full_file === true;
    const filterStartDate =
      !importFullFile && body.start_date ? new Date(body.start_date) : null;
    const filterEndDate =
      !importFullFile && body.end_date ? new Date(body.end_date) : null;
    // Correct end-date to end-of-day so inclusive comparison works
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
      .select("id, organization_id, metadata, status, report_type, column_mapping, file_name")
      .eq("id", uploadId)
      .maybeSingle();

    if (fetchErr || !row) {
      return NextResponse.json({ ok: false, error: "Upload session not found." }, { status: 404 });
    }

    orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
    if (!isUuidString(orgId)) {
      return NextResponse.json({ ok: false, error: "Invalid upload row (organization_id)." }, { status: 500 });
    }

    const status = String((row as { status?: unknown }).status ?? "");
    const retryableStatuses = ["mapped", "ready", "uploaded", "failed"];
    if (!retryableStatuses.includes(status)) {
      return NextResponse.json(
        {
          ok: false,
          error: `Phase 2 (Stage) requires status "mapped" (or "failed" for retry). Current status is "${status}".${
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

    // Validate report_type early so Phase 3 (Sync) won't fail with "unknown type".
    const reportTypeRaw = String((row as { report_type?: unknown }).report_type ?? "").trim();
    const knownTypes = [
      "FBA_RETURNS", "REMOVAL_ORDER", "INVENTORY_LEDGER",
      "REIMBURSEMENTS", "SETTLEMENT", "SAFET_CLAIMS", "TRANSACTIONS", "REPORTS_REPOSITORY",
      "fba_customer_returns", "inventory_ledger", "safe_t_claims",
      "reimbursements", "settlement_repository", "transaction_view",
    ];
    if (!reportTypeRaw || reportTypeRaw === "UNKNOWN" || !knownTypes.includes(reportTypeRaw)) {
      return NextResponse.json(
        {
          ok: false,
          error:
            'Report type is not set or is "UNKNOWN". Open "Map Columns" in the History table, select the correct type, and save before processing.',
        },
        { status: 422 },
      );
    }

    const meta = (row as { metadata?: unknown }).metadata;
    const parsed = parseRawReportMetadata(meta);
    const metaObj =
      meta && typeof meta === "object" && !Array.isArray(meta)
        ? (meta as Record<string, unknown>)
        : {};

    const storagePrefix = parsed.storagePrefix?.trim() ?? "";
    if (!storagePrefix) {
      return NextResponse.json({ ok: false, error: "Missing storage prefix in upload metadata." }, { status: 400 });
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

    const estimatedRows = parsed.rowCount ?? null;

    // Optimistic lock: set status -> "processing" so concurrent clicks are no-ops
    const { data: locked, error: lockErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "processing",
        metadata: mergeUploadMetadata(meta, { process_progress: 0, error_message: "" }),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId)
      .in("status", ["mapped", "ready", "uploaded", "failed"])
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

    await audit(orgId, "import.stage_started", uploadId, {
      fileName: (row as { file_name?: string }).file_name,
      totalParts,
    });

    // ── Pre-stage cleanup ─────────────────────────────────────────────────────
    // Delete any staging rows left over from a previous (failed/retried) attempt
    // for this upload so we never accumulate duplicate ledger rows on retry.
    await supabaseServer
      .from(STAGING_TABLE)
      .delete()
      .eq("upload_id", uploadId)
      .eq("organization_id", orgId);

    // Stream CSV from Storage.
    // If a raw_file_path is stored in metadata (single-file upload from Phase 1),
    // download that file directly. Otherwise fall back to the concatenated-parts reader.
    const rawFilePath =
      typeof metaObj.raw_file_path === "string" ? metaObj.raw_file_path.trim() : "";

    // ── Header row skip: Amazon Reports Repository files prepend metadata rows ─
    // Phase 1 saved the zero-based index of the real header row detected by
    // findHeaderRowIndex.  We pass it as skipLines to csv-parser so the
    // metadata preamble rows are silently discarded before header parsing.
    const headerRowIndex =
      typeof metaObj.header_row_index === "number" && metaObj.header_row_index > 0
        ? Math.floor(metaObj.header_row_index as number)
        : 0;

    // ── TSV / Flat-File delimiter detection ──────────────────────────────────
    // Amazon distributes some reports (e.g. Transactions from Reports Repository)
    // as .txt Tab-Separated Values files.  csv-parser defaults to "," which would
    // collapse every TSV row into a single broken column.
    //
    // Detection order (first match wins):
    //   1. file_extension saved in metadata during Phase 1 (most reliable)
    //   2. Extension of raw_file_path storage object
    //   3. Default to "," (standard CSV)
    const rawFileExt = rawFilePath
      ? (rawFilePath.split(".").pop() ?? "").toLowerCase()
      : "";
    const metaFileExt =
      typeof metaObj.file_extension === "string"
        ? metaObj.file_extension.replace(/^\./, "").toLowerCase()
        : "";
    const fileExt = metaFileExt || rawFileExt;
    const csvSeparator: string = fileExt === "txt" ? "\t" : ",";

    let source: NodeJS.ReadableStream;
    if (rawFilePath) {
      const { data: blob, error: dlErr } = await supabaseServer.storage
        .from("raw-reports")
        .download(rawFilePath);
      if (dlErr || !blob) {
        throw new Error(dlErr?.message ?? `Could not download file from storage: ${rawFilePath}`);
      }
      const { Readable } = await import("stream");
      const arrayBuf = await blob.arrayBuffer();
      source = Readable.from(Buffer.from(arrayBuf));
    } else {
      source = createConcatenatedPartsReadable(supabaseServer, storagePrefix, totalParts);
    }
    const parser = csv({
      mapHeaders: ({ header }) => String(header).replace(/^\uFEFF/, "").trim(),
      skipLines: headerRowIndex,
      separator: csvSeparator,
    });

    let staged = 0;      // rows actually inserted (passed date filter)
    let totalSeen = 0;   // all rows encountered in the CSV
    let batch: Record<string, unknown>[] = [];
    let lastProgressWrite = 0;

    const flushProgress = async () => {
      if (staged - lastProgressWrite < PROGRESS_EVERY) return;
      lastProgressWrite = staged;
      const denom = estimatedRows && estimatedRows > 0 ? estimatedRows : totalSeen || 1;
      const pct = Math.min(99, Math.round((staged / denom) * 100));
      const { data: prevRow } = await supabaseServer
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadId)
        .maybeSingle();
      await supabaseServer
        .from("raw_report_uploads")
        .update({
          metadata: mergeUploadMetadata(
            (prevRow as { metadata?: unknown } | null)?.metadata,
            { row_count: staged, process_progress: pct, etl_phase: "staging" },
          ),
          updated_at: new Date().toISOString(),
        })
        .eq("id", uploadId)
        .eq("organization_id", orgId);
    };

    const flushBatch = async (rows: Record<string, unknown>[]) => {
      if (rows.length === 0) return;
      console.log(`[stage] Starting insert to ${STAGING_TABLE}`, { count: rows.length, uploadId, orgId });
      const { error: insErr } = await supabaseServer
        .from(STAGING_TABLE)
        .insert(rows);
      if (insErr) {
        console.error(`[stage] Insert into ${STAGING_TABLE} failed:`, insErr.message, { uploadId, orgId });
        throw new Error(`Insert into ${STAGING_TABLE} failed: ${insErr.message}`);
      }
      console.log(`[stage] Insert to ${STAGING_TABLE} succeeded`, { count: rows.length });
    };

    await new Promise<void>((resolve, reject) => {
      source.on("error", reject);
      parser.on("error", reject);

      parser.on("data", (csvRow: Record<string, string>) => {
        totalSeen += 1;

        // Apply date range filter if specified
        if (filterStartDate || filterEndDate) {
          if (!rowMatchesDateRange(csvRow, filterStartDate, filterEndDate)) return;
        }

        const normalizedRow = normalizeAmazonReportRowKeys(csvRow);
        const mappedRow = applyColumnMappingToRow(normalizedRow, columnMapping);
        const stagingRow: Record<string, unknown> = {
          organization_id: orgId,
          upload_id: uploadId,
          raw_row: mappedRow,
        };

        // Extract snapshot_date from common date columns
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
        staged += 1;

        if (batch.length >= BATCH_SIZE) {
          parser.pause();
          const chunk = batch;
          batch = [];
          void flushBatch(chunk)
            .then(() => flushProgress())
            .then(() => parser.resume())
            .catch(reject);
        }
      });

      parser.on("end", () => {
        void (async () => {
          try {
            await flushBatch(batch);
            batch = [];

            const { data: prevRow } = await supabaseServer
              .from("raw_report_uploads")
              .select("metadata")
              .eq("id", uploadId)
              .maybeSingle();

            await supabaseServer
              .from("raw_report_uploads")
              .update({
                status: "staged",
                metadata: mergeUploadMetadata(
                  (prevRow as { metadata?: unknown } | null)?.metadata,
                  {
                    row_count: staged,
                    processed_rows: staged,
                    process_progress: 100,
                    error_message: undefined,
                    // Only persist filter info if a filter was actually applied
                    ...(filterStartDate ? { start_date: body.start_date } : {}),
                    ...(filterEndDate   ? { end_date:   body.end_date   } : {}),
                    ...(filterStartDate || filterEndDate ? { import_full_file: false } : { import_full_file: true }),
                  },
                ),
                updated_at: new Date().toISOString(),
              })
              .eq("id", uploadId)
              .eq("organization_id", orgId);

            await audit(orgId, "import.stage_completed", uploadId, {
              rowsStaged: staged,
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

    return NextResponse.json({ ok: true, rowsStaged: staged, totalRows: totalSeen });
  } catch (e) {
    const message = e instanceof Error ? e.message : "Staging failed.";
    if (uploadIdForFail && isUuidString(uploadIdForFail) && isUuidString(orgId)) {
      const { data: prevRow } = await supabaseServer
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadIdForFail)
        .maybeSingle();
      await supabaseServer
        .from("raw_report_uploads")
        .update({
          status: "failed",
          metadata: mergeUploadMetadata(
            (prevRow as { metadata?: unknown } | null)?.metadata,
            { error_message: message, failed_phase: "process" },
          ),
          updated_at: new Date().toISOString(),
        })
        .eq("id", uploadIdForFail)
        .eq("organization_id", orgId);
    }
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
