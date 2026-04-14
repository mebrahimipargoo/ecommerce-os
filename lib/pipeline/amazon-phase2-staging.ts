/**
 * Phase 2 — Process raw file → `amazon_staging` (unified for all Amazon report types
 * except listing exports, which use the listing branch in /api/settings/imports/process).
 *
 * Single implementation shared by:
 *   POST /api/settings/imports/stage  (thin wrapper)
 *   POST /api/settings/imports/process (non-listing path)
 */

import csv from "csv-parser";
import { NextResponse } from "next/server";

import { createConcatenatedPartsReadable } from "../import-raw-report-stream";
import { applyColumnMappingToRow, computeSourceLineHash, normalizeAmazonReportRowKeys } from "../import-sync-mappers";
import { mergeUploadMetadata, parseRawReportMetadata } from "../raw-report-upload-metadata";
import { supabaseServer } from "../supabase-server";
import { isUuidString } from "../uuid";
import { isListingReportType } from "../raw-report-types";

const STAGING_TABLE = "amazon_staging";
const BATCH_SIZE = 500;
/** Minimum interval between progress writes when not forced (e.g. per batch forces a write). */
const PROGRESS_EVERY = 25;

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
  "ALL_ORDERS",
  "REPLACEMENTS",
  "FBA_GRADE_AND_RESELL",
  "MANAGE_FBA_INVENTORY",
  "FBA_INVENTORY",
  "RESERVED_INVENTORY",
  "FEE_PREVIEW",
  "MONTHLY_STORAGE_FEES",
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
    if (isListingReportType(reportTypeRaw)) {
      return NextResponse.json(
        {
          ok: false,
          error:
            "Listing exports use the unified Process route with the listing pipeline, not amazon_staging.",
        },
        { status: 422 },
      );
    }
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

    const { data: locked, error: lockErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "processing",
        metadata: mergeUploadMetadata(meta, {
          process_progress: 0,
          row_count: 0,
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

    await audit(orgId, "import.stage_started", uploadId, {
      fileName: (row as { file_name?: string }).file_name,
      totalParts,
      pipeline: "unified_phase2_staging",
    });

    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: uploadId,
        organization_id: orgId,
        status: "processing",
        current_phase: "staging",
        upload_pct: 100,
        process_pct: 0,
        sync_pct: 0,
        processed_rows: 0,
        total_rows: estimatedRows && estimatedRows > 0 ? estimatedRows : null,
        error_message: null,
        import_metrics: { current_phase: "staging" },
      },
      { onConflict: "upload_id" },
    );

    await supabaseServer.from(STAGING_TABLE).delete().eq("upload_id", uploadId).eq("organization_id", orgId);

    const headerRowIndex =
      typeof metaObj.header_row_index === "number" && metaObj.header_row_index > 0
        ? Math.floor(metaObj.header_row_index as number)
        : 0;

    const rawFileExt = rawFilePath ? (rawFilePath.split(".").pop() ?? "").toLowerCase() : "";
    const metaFileExt =
      typeof metaObj.file_extension === "string" ? metaObj.file_extension.replace(/^\./, "").toLowerCase() : "";
    const fileExt = metaFileExt || rawFileExt;
    const csvSeparator: string = fileExt === "txt" ? "\t" : ",";

    let source: NodeJS.ReadableStream;
    if (rawFilePath) {
      const { data: blob, error: dlErr } = await supabaseServer.storage.from("raw-reports").download(rawFilePath);
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

    /** Lines that passed the date filter (before per-line dedupe). */
    let dataLinesPassed = 0;
    /** Rows successfully inserted into amazon_staging (1:1 with physical data lines). */
    let approxStaged = 0;
    let totalSeen = 0;
    let dataRowNumber = 0;
    let batch: Record<string, unknown>[] = [];
    let lastProgressWrite = 0;

    const flushProgress = async (force = false) => {
      if (!force && dataLinesPassed - lastProgressWrite < PROGRESS_EVERY) return;
      lastProgressWrite = dataLinesPassed;
      /**
       * Denominator for process % — scoped to this upload only (estimatedRows from this row's metadata).
       * If Phase-1 total_rows was wildly higher than rows we are actually processing (stale/wrong guess),
       * blend toward live counts so the bar keeps moving instead of freezing near ~50%.
       */
      const baseEst = estimatedRows && estimatedRows > 0 ? estimatedRows : null;
      const live = Math.max(1, dataLinesPassed, totalSeen);
      let denom = live;
      if (baseEst != null) {
        if (baseEst <= live * 1.25) {
          denom = Math.max(live, baseEst);
        } else {
          denom = Math.max(live, Math.ceil(live + (baseEst - live) * (live / Math.max(baseEst, 1))));
        }
      }
      const pct = Math.min(99, Math.round((dataLinesPassed / denom) * 100));
      const { data: prevRow } = await supabaseServer
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadId)
        .maybeSingle();
      await supabaseServer
        .from("raw_report_uploads")
        .update({
          metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
            row_count: dataLinesPassed,
            process_progress: pct,
            etl_phase: "staging",
            physical_lines_seen: totalSeen,
            data_rows_seen: dataLinesPassed,
            import_metrics: {
              current_phase: "staging",
              physical_lines_seen: totalSeen,
              data_rows_seen: dataLinesPassed,
              rows_staged: approxStaged,
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
          upload_pct: 100,
          process_pct: pct,
          sync_pct: 0,
          processed_rows: dataLinesPassed,
          total_rows: denom,
          import_metrics: {
            physical_lines_seen: totalSeen,
            data_rows_seen: dataLinesPassed,
            rows_staged: approxStaged,
            current_phase: "staging",
          },
        },
        { onConflict: "upload_id" },
      );
    };

    const flushBatch = async (rows: Record<string, unknown>[]) => {
      if (rows.length === 0) return;
      const { error: insErr } = await supabaseServer.from(STAGING_TABLE).insert(rows);
      if (insErr) {
        console.error(`[phase2-staging] Insert into ${STAGING_TABLE} failed:`, insErr.message, { uploadId, orgId });
        throw new Error(`Insert into ${STAGING_TABLE} failed: ${insErr.message}`);
      }
      approxStaged += rows.length;
      await flushProgress(true);
    };

    let stagedRowCountForResponse = 0;

    await new Promise<void>((resolve, reject) => {
      source.on("error", reject);
      parser.on("error", reject);

      parser.on("data", (csvRow: Record<string, string>) => {
        totalSeen += 1;

        if (filterStartDate || filterEndDate) {
          if (!rowMatchesDateRange(csvRow, filterStartDate, filterEndDate)) return;
        }

        const normalizedRow = normalizeAmazonReportRowKeys(csvRow);
        const mappedRow = applyColumnMappingToRow(normalizedRow, columnMapping);
        dataRowNumber += 1;
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
          void flushBatch(chunk).then(() => parser.resume())
            .catch(reject);
        }
      });

      parser.on("end", () => {
        void (async () => {
          try {
            await flushBatch(batch);
            batch = [];

            const { count: stagingCount } = await supabaseServer
              .from(STAGING_TABLE)
              .select("*", { count: "exact", head: true })
              .eq("upload_id", uploadId)
              .eq("organization_id", orgId);
            const rowsInDb = typeof stagingCount === "number" ? stagingCount : approxStaged;

            const { data: prevRow } = await supabaseServer
              .from("raw_report_uploads")
              .select("metadata")
              .eq("id", uploadId)
              .maybeSingle();

            await supabaseServer
              .from("raw_report_uploads")
              .update({
                status: "staged",
                import_pipeline_completed_at: null,
                metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
                  row_count: dataLinesPassed,
                  total_rows: dataLinesPassed,
                  processed_rows: rowsInDb,
                  process_progress: 100,
                  physical_lines_seen: totalSeen,
                  data_rows_seen: dataLinesPassed,
                  staging_row_count: rowsInDb,
                  error_message: undefined,
                  import_metrics: {
                    current_phase: "staged",
                    physical_lines_seen: totalSeen,
                    data_rows_seen: dataLinesPassed,
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
                upload_pct: 100,
                process_pct: 100,
                sync_pct: 0,
                processed_rows: rowsInDb,
                total_rows: Math.max(rowsInDb, dataLinesPassed, 1),
                error_message: null,
                import_metrics: {
                  physical_lines_seen: totalSeen,
                  data_rows_seen: dataLinesPassed,
                  rows_staged: rowsInDb,
                  current_phase: "staged",
                },
              },
              { onConflict: "upload_id" },
            );

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
          import_pipeline_failed_at: new Date().toISOString(),
          metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
            error_message: message,
            failed_phase: "process",
            import_metrics: { current_phase: "failed", failure_reason: message },
          }),
          updated_at: new Date().toISOString(),
        })
        .eq("id", uploadIdForFail)
        .eq("organization_id", orgId);

      await supabaseServer.from("file_processing_status").upsert(
        {
          upload_id: uploadIdForFail,
          organization_id: orgId,
          status: "failed",
          upload_pct: 100,
          process_pct: 0,
          error_message: message,
          import_metrics: { current_phase: "failed", failure_reason: message },
        },
        { onConflict: "upload_id" },
      );
    }
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
