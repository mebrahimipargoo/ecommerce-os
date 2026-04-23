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
import { mergeUploadMetadata, parseRawReportMetadata } from "../raw-report-upload-metadata";
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
const BATCH_SIZE = 500;
/**
 * Maximum age of a `processing` lock before we treat it as stale and allow Phase 2 to take over.
 * Must exceed the maxDuration of /process (300s) plus a safety margin.
 */
const PROCESSING_STALE_MS = 6 * 60 * 1000;
/** Minimum interval between progress writes when not forced (e.g. per batch forces a write). */
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

/**
 * Resume support: returns the highest already-staged `row_number` for this upload,
 * plus the count of rows currently in `amazon_staging` for it. Used to:
 *   1) skip the re-DELETE on retry (avoids statement_timeout on big files),
 *   2) skip parser rows that were already inserted (no duplicate-key on
 *      `uq_amazon_staging_org_upload_row_number`),
 *   3) seed progress counters so reported %/staged_rows reflect actual DB state.
 */
async function getStagingResumeState(
  orgId: string,
  uploadId: string,
): Promise<{ maxRowNumber: number; rowCount: number }> {
  const { data: maxRow } = await supabaseServer
    .from(STAGING_TABLE)
    .select("row_number")
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId)
    .order("row_number", { ascending: false })
    .limit(1)
    .maybeSingle();
  const maxRowNumber =
    maxRow && typeof (maxRow as { row_number?: unknown }).row_number === "number"
      ? Math.floor((maxRow as { row_number: number }).row_number)
      : 0;

  if (maxRowNumber === 0) return { maxRowNumber: 0, rowCount: 0 };

  const { count } = await supabaseServer
    .from(STAGING_TABLE)
    .select("*", { count: "exact", head: true })
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);
  return { maxRowNumber, rowCount: typeof count === "number" ? count : 0 };
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
     * surviving rows on uq_amazon_staging_org_upload_row_number. Instead we resume
     * from the highest already-staged row_number and let upsert handle any overlap.
     */
    const resumeState = await getStagingResumeState(orgId, uploadId);
    const resumeFromRowNumber = resumeState.maxRowNumber;
    const resumeRowCount = resumeState.rowCount;
    if (resumeFromRowNumber > 0) {
      console.log(
        `[phase2-staging] Resuming upload ${uploadId} from row_number=${resumeFromRowNumber} (${resumeRowCount} rows already staged).`,
      );
    }

    await audit(orgId, "import.stage_started", uploadId, {
      fileName: (row as { file_name?: string }).file_name,
      totalParts,
      pipeline: "unified_phase2_staging",
      resume_from_row_number: resumeFromRowNumber,
      resume_row_count: resumeRowCount,
    });

    // Seed FPS with cumulative counters so the UI does not flash back to 0% on retry.
    const initialPctNum =
      estimatedRows && estimatedRows > 0 && resumeRowCount > 0
        ? Math.min(99, Math.round((resumeRowCount / Math.max(estimatedRows, resumeRowCount)) * 100))
        : 0;

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
        process_pct: initialPctNum,
        phase2_stage_pct: initialPctNum,
        sync_pct: 0,
        processed_rows: resumeRowCount,
        staged_rows_written: resumeRowCount,
        total_rows: estimatedRows && estimatedRows > 0 ? estimatedRows : null,
        error_message: null,
        import_metrics: { current_phase: "staging", rows_staged: resumeRowCount },
      },
      { onConflict: "upload_id" },
    );

    const headerRowIndex =
      typeof metaObj.header_row_index === "number" && metaObj.header_row_index > 0
        ? Math.floor(metaObj.header_row_index as number)
        : 0;

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
      const arrayBuf = await blob.arrayBuffer();
      source = Readable.from(Buffer.from(arrayBuf));
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
      const fileRowsTotal = Math.max(0, lines.length - 1);
      const dataRowsTotal = fileRowsTotal;
      // Seed with rows already in DB so progress and counters keep climbing across resumes.
      let approxStaged = resumeRowCount;
      let batch: Record<string, unknown>[] = [];
      let lastListingProgressWrite = 0;

      const flushListingProgress = async (force: boolean, physicalDone: number) => {
        if (!force && physicalDone - lastListingProgressWrite < LISTING_PROGRESS_EVERY) return;
        lastListingProgressWrite = physicalDone;
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
        // This is what makes large-file retries safe — the prior run's rows are kept
        // and only the missing tail gets written.
        const { error: insErr } = await supabaseServer
          .from(STAGING_TABLE)
          .upsert(rows, { onConflict: STAGING_CONFLICT, ignoreDuplicates: true });
        if (insErr) {
          throw new Error(`Upsert into ${STAGING_TABLE} failed: ${insErr.message}`);
        }
        approxStaged += rows.length;
        await flushListingProgress(true, dataLinesPassed);
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

      const { count: stagingCount } = await supabaseServer
        .from(STAGING_TABLE)
        .select("*", { count: "exact", head: true })
        .eq("upload_id", uploadId)
        .eq("organization_id", orgId);
      const rowsInDb = typeof stagingCount === "number" ? stagingCount : approxStaged;

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

    const parser = csv({
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

    const flushProgress = async (force = false) => {
      if (!force && dataLinesPassed - lastProgressWrite < PROGRESS_EVERY) return;
      lastProgressWrite = dataLinesPassed;
      /**
       * Cumulative counters (resume-aware): `dataRowNumber` and `approxStaged` already
       * include rows from a prior partial run. Use them — not `dataLinesPassed` (this-run
       * only) — so process_progress/staged_rows do not regress on retry.
       */
      const cumulativeDataRows = dataRowNumber;
      const cumulativePhysicalSeen = Math.max(totalSeen, cumulativeDataRows);
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
            import_metrics: {
              current_phase: "staging",
              physical_lines_seen: cumulativePhysicalSeen,
              data_rows_seen: cumulativeDataRows,
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
          processed_rows: cumulativeDataRows,
          total_rows: denom,
          staged_rows_written: approxStaged,
          data_rows_total: cumulativeDataRows,
          import_metrics: {
            physical_lines_seen: cumulativePhysicalSeen,
            data_rows_seen: cumulativeDataRows,
            rows_staged: approxStaged,
            current_phase: "staging",
          },
        },
        { onConflict: "upload_id" },
      );
    };

    const flushBatch = async (rows: Record<string, unknown>[]) => {
      if (rows.length === 0) return;
      // Idempotent insert: ignore rows already staged for this (org, upload, row_number).
      // Each batch is its own implicit transaction so a timeout in one batch does not
      // roll back all prior work — the next attempt resumes from the highest row_number.
      const { error: insErr } = await supabaseServer
        .from(STAGING_TABLE)
        .upsert(rows, { onConflict: STAGING_CONFLICT, ignoreDuplicates: true });
      if (insErr) {
        console.error(`[phase2-staging] Upsert into ${STAGING_TABLE} failed:`, insErr.message, { uploadId, orgId });
        throw new Error(`Upsert into ${STAGING_TABLE} failed: ${insErr.message}`);
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
        // Resume: skip rows already present in amazon_staging from a prior partial run.
        // We still increment dataRowNumber (and totalSeen above) so the row_number for
        // newly-staged rows matches what they would have been on a fresh run.
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
          phase_key: FPS_KEY_FAILED,
          phase_label: message.slice(0, 200),
          next_action_key: null,
          next_action_label: null,
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
