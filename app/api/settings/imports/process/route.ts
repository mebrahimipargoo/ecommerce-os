import { Readable } from "node:stream";
import { NextResponse } from "next/server";

import { createConcatenatedPartsReadable } from "../../../../../lib/import-raw-report-stream";
import { syncListingRawRowsToCatalogProducts } from "../../../../../lib/import-listing-canonical-sync";
import { executeAmazonPhase2Staging } from "../../../../../lib/pipeline/amazon-phase2-staging";
import {
  buildRawRowInsertForPhysicalLine,
  splitPhysicalLines,
  streamToBuffer,
} from "../../../../../lib/import-listing-physical-lines";
import {
  AMAZON_LEDGER_UPLOAD_SOURCE,
  mergeUploadMetadata,
  parseRawReportMetadata,
} from "../../../../../lib/raw-report-upload-metadata";
import { isListingReportType } from "../../../../../lib/raw-report-types";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";

/** Large CSV processing can exceed default serverless limits on some hosts. */
export const maxDuration = 300;

const BATCH_SIZE = 1000;
/** Listing imports: flush progress more often so the bar tracks rows read, not only batch boundaries. */
/** How often to persist listing pass-1 progress (physical lines); smaller = smoother bar. */
const LISTING_PROGRESS_EVERY = 8;

type Body = {
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

function resolveImportStoreId(meta: Record<string, unknown>): string | null {
  const a = typeof meta.import_store_id === "string" ? meta.import_store_id.trim() : "";
  if (a && isUuidString(a)) return a;
  const b = typeof meta.ledger_store_id === "string" ? meta.ledger_store_id.trim() : "";
  if (b && isUuidString(b)) return b;
  return null;
}

/**
 * Pipeline kind from `raw_report_uploads.report_type` only (canonical + legacy slugs).
 * Kept aligned with `resolveImportKind` in sync/route.ts for Amazon reports; listing types
 * are listed explicitly so Process routing matches the sync/staging registry.
 */
function resolveImportKind(
  reportType: string | null | undefined,
):
  | "FBA_RETURNS"
  | "REMOVAL_ORDER"
  | "REMOVAL_SHIPMENT"
  | "INVENTORY_LEDGER"
  | "REIMBURSEMENTS"
  | "SETTLEMENT"
  | "SAFET_CLAIMS"
  | "TRANSACTIONS"
  | "REPORTS_REPOSITORY"
  | "ALL_ORDERS"
  | "REPLACEMENTS"
  | "FBA_GRADE_AND_RESELL"
  | "MANAGE_FBA_INVENTORY"
  | "FBA_INVENTORY"
  | "RESERVED_INVENTORY"
  | "FEE_PREVIEW"
  | "MONTHLY_STORAGE_FEES"
  | "CATEGORY_LISTINGS"
  | "ALL_LISTINGS"
  | "ACTIVE_LISTINGS"
  | "UNKNOWN" {
  const rt = String(reportType ?? "").trim();
  if (rt === "FBA_RETURNS" || rt === "fba_customer_returns") return "FBA_RETURNS";
  if (rt === "REMOVAL_ORDER") return "REMOVAL_ORDER";
  if (rt === "REMOVAL_SHIPMENT") return "REMOVAL_SHIPMENT";
  if (rt === "INVENTORY_LEDGER" || rt === "inventory_ledger") return "INVENTORY_LEDGER";
  if (rt === "REIMBURSEMENTS" || rt === "reimbursements") return "REIMBURSEMENTS";
  if (rt === "SETTLEMENT" || rt === "settlement_repository") return "SETTLEMENT";
  if (rt === "SAFET_CLAIMS" || rt === "safe_t_claims") return "SAFET_CLAIMS";
  if (rt === "TRANSACTIONS" || rt === "transaction_view") return "TRANSACTIONS";
  if (rt === "REPORTS_REPOSITORY") return "REPORTS_REPOSITORY";
  if (rt === "ALL_ORDERS") return "ALL_ORDERS";
  if (rt === "REPLACEMENTS") return "REPLACEMENTS";
  if (rt === "FBA_GRADE_AND_RESELL") return "FBA_GRADE_AND_RESELL";
  if (rt === "MANAGE_FBA_INVENTORY") return "MANAGE_FBA_INVENTORY";
  if (rt === "FBA_INVENTORY") return "FBA_INVENTORY";
  if (rt === "RESERVED_INVENTORY") return "RESERVED_INVENTORY";
  if (rt === "FEE_PREVIEW") return "FEE_PREVIEW";
  if (rt === "MONTHLY_STORAGE_FEES") return "MONTHLY_STORAGE_FEES";
  if (rt === "CATEGORY_LISTINGS") return "CATEGORY_LISTINGS";
  if (rt === "ALL_LISTINGS") return "ALL_LISTINGS";
  if (rt === "ACTIVE_LISTINGS") return "ACTIVE_LISTINGS";
  return "UNKNOWN";
}

async function audit(
  orgId: string,
  userId: string | null,
  action: string,
  entityId: string,
  detail?: Record<string, unknown>,
): Promise<void> {
  await supabaseServer.from("raw_report_import_audit").insert({
    organization_id: orgId,
    user_profile_id: userId,
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

    const meta = (row as { metadata?: unknown }).metadata;
    const parsed = parseRawReportMetadata(meta);
    const metaObj = meta && typeof meta === "object" && !Array.isArray(meta) ? (meta as Record<string, unknown>) : {};
    const rawFilePath =
      typeof metaObj.raw_file_path === "string" ? metaObj.raw_file_path.trim() : "";

    // Ledger uploads are processed in the browser during import; History "Sync" is a no-op once complete.
    if (metaObj.source === AMAZON_LEDGER_UPLOAD_SOURCE) {
      const st = String((row as { status?: unknown }).status ?? "");
      if (st === "synced" || st === "complete") {
        return NextResponse.json({ ok: true, rowsProcessed: 0, ledgerSkipped: true });
      }
      return NextResponse.json(
        {
          ok: false,
          error:
            "This ledger import is still uploading or processing in the browser. Wait until it finishes, then use Delete if you need to remove it.",
        },
        { status: 409 },
      );
    }

    if (parsed.uploadProgress < 100) {
      return NextResponse.json(
        { ok: false, error: "Upload is not complete yet (wait for 100% upload progress)." },
        { status: 400 },
      );
    }

    const status = String((row as { status?: unknown }).status ?? "");
    const reportTypeRaw = String((row as { report_type?: string | null }).report_type ?? "").trim();
    const listingImport = isListingReportType(reportTypeRaw);
    /** Unified Phase-2 entry: same statuses for listing and Amazon staging (History + uploader). */
    const processableStatuses = ["mapped", "ready", "uploaded", "pending", "failed"];
    if (!processableStatuses.includes(status)) {
      if (status === "needs_mapping") {
        return NextResponse.json(
          {
            ok: false,
            error:
              'This upload needs column mapping before it can be processed. Click "Map Columns" in the History table to assign fields.',
          },
          { status: 409 },
        );
      }
      return NextResponse.json(
        {
          ok: false,
          error: `Cannot process while status is "${status}". Expected one of: ${processableStatuses.join(", ")}.`,
        },
        { status: 409 },
      );
    }

    let extRaw = typeof metaObj.file_extension === "string" ? metaObj.file_extension.trim().toLowerCase() : "";
    if (!extRaw && rawFilePath) {
      extRaw = (rawFilePath.split(".").pop() ?? "").toLowerCase();
    }
    const ext = extRaw.replace(/^\./, "");
    if (ext === "xlsx") {
      return NextResponse.json(
        { ok: false, error: "Excel imports are not processed by this pipeline. Export as CSV and re-upload." },
        { status: 415 },
      );
    }
    if (ext && ext !== "csv" && ext !== "txt") {
      return NextResponse.json(
        { ok: false, error: `Unsupported file type for processing: .${ext || "unknown"}` },
        { status: 415 },
      );
    }

    const storagePrefix = parsed.storagePrefix?.trim() ?? "";

    let totalParts =
      num(metaObj.total_parts, 0) > 0
        ? Math.floor(num(metaObj.total_parts, 0))
        : Math.max(1, Math.floor(num(metaObj.upload_chunks_count, 0)));

    // Universal Importer (and similar) store one object at metadata.raw_file_path (e.g. …/original.txt).
    // Chunked uploads use storage_prefix + part-000000 without raw_file_path.
    if (rawFilePath) {
      if (!Number.isFinite(totalParts) || totalParts < 1) {
        totalParts = 1;
      }
    } else {
      if (!storagePrefix) {
        return NextResponse.json({ ok: false, error: "Missing storage prefix in upload metadata." }, { status: 400 });
      }
      if (!Number.isFinite(totalParts) || totalParts < 1) {
        return NextResponse.json({ ok: false, error: "Missing part count in upload metadata." }, { status: 400 });
      }
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
        : typeof metaObj.total_rows === "string" && String(metaObj.total_rows).trim() !== ""
          ? Math.floor(num(metaObj.total_rows, 0))
          : 0;
    const estimatedRows = totalRowsFromMeta > 0 ? totalRowsFromMeta : null;

    const kind = resolveImportKind(reportTypeRaw);
    if (kind === "UNKNOWN") {
      return NextResponse.json(
        {
          ok: false,
          error:
            "Could not determine import kind (FBA returns, removal order, inventory ledger, or listing export). Set the Type in History or re-upload so headers can be classified.",
        },
        { status: 422 },
      );
    }

    /** All non-listing reports: single Phase-2 implementation (staging); must run before we set status=processing here. */
    if (!listingImport) {
      return executeAmazonPhase2Staging(body);
    }

    const { data: locked, error: lockErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "processing",
        metadata: mergeUploadMetadata(meta, {
          process_progress: 0,
          row_count: 0,
          error_message: "",
        }),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId)
      .in("status", processableStatuses)
      .select("id");

    if (lockErr) {
      return NextResponse.json({ ok: false, error: lockErr.message }, { status: 500 });
    }
    if (!locked || locked.length === 0) {
      return NextResponse.json(
        { ok: false, error: "Upload is not in a syncable state (already processing or completed)." },
        { status: 409 },
      );
    }

    await audit(orgId, null, "import.process_started", uploadId, {
      fileName: (row as { file_name?: string }).file_name,
      totalParts,
      kind,
    });

    // Initialise / reset the dedicated real-time progress row for this run.
    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: uploadId,
        organization_id: orgId,
        status: "processing",
        current_phase: "staging",
        upload_pct: 100,
        process_pct: 0,
        total_rows: estimatedRows ?? null,
        processed_rows: 0,
        error_message: null,
        import_metrics: { current_phase: "staging" },
      },
      { onConflict: "upload_id" },
    );

    const importStoreId = resolveImportStoreId(metaObj);

    const RAW_REPORTS_BUCKET = "raw-reports";

    let source: Readable;
    if (rawFilePath) {
      console.info(
        JSON.stringify({
          tag: "import.process.storage_source",
          upload_id: uploadId,
          mode: "raw_file_path",
          path: rawFilePath,
        }),
      );
      const { data: blob, error: dlErr } = await supabaseServer.storage.from(RAW_REPORTS_BUCKET).download(rawFilePath);
      if (dlErr || !blob) {
        throw new Error(dlErr?.message ?? `Object not found or inaccessible at raw_file_path: ${rawFilePath}`);
      }
      const arrayBuf = await blob.arrayBuffer();
      source = Readable.from(Buffer.from(arrayBuf));
    } else {
      console.info(
        JSON.stringify({
          tag: "import.process.storage_source",
          upload_id: uploadId,
          mode: "concatenated_parts",
          storage_prefix: storagePrefix,
          total_parts: totalParts,
        }),
      );
      source = createConcatenatedPartsReadable(supabaseServer, storagePrefix, totalParts);
    }

    // ── Listing imports: pass 1 = one raw DB row per *physical* file line (not csv-parser logical rows).
    //    Pass 2 = canonical merge into catalog_products only (identifiers optional in raw).
    if (listingImport) {
      const { error: rawDelErr } = await supabaseServer
        .from("amazon_listing_report_rows_raw")
        .delete()
        .eq("source_upload_id", uploadId);
      if (rawDelErr) {
        return NextResponse.json({ ok: false, error: rawDelErr.message }, { status: 500 });
      }

      const fileBuf = await streamToBuffer(source);
      const text = fileBuf.toString("utf8");
      const lines = splitPhysicalLines(text);
      if (lines.length === 0) {
        return NextResponse.json({ ok: false, error: "Empty file." }, { status: 400 });
      }

      const sep = (ext === "txt" ? "\t" : ",") as "\t" | ",";
      const pass1Metrics = {
        file_rows_seen: Math.max(0, lines.length - 1),
        raw_rows_stored: 0,
        raw_rows_skipped_empty: 0,
        raw_rows_skipped_malformed: 0,
      };

      let rawBatch: Record<string, unknown>[] = [];
      let lastListingProgressWrite = 0;

      const flushRawRowsBatch = async () => {
        if (rawBatch.length === 0) return;
        const chunk = rawBatch;
        rawBatch = [];
        const { error: insErr } = await supabaseServer.from("amazon_listing_report_rows_raw").insert(chunk);
        if (insErr) throw new Error(insErr.message);
        pass1Metrics.raw_rows_stored += chunk.length;
      };

      const flushListingProgressPass1 = async (force: boolean, physicalLinesProcessed: number) => {
        if (!force && physicalLinesProcessed - lastListingProgressWrite < LISTING_PROGRESS_EVERY) return;
        lastListingProgressWrite = physicalLinesProcessed;
        // Pass 1 maps to 0–50% of overall process_progress; pass 2 (canonical) uses 50–99%.
        const totalPhysical = Math.max(1, pass1Metrics.file_rows_seen);
        let pct = Math.min(50, Math.ceil((physicalLinesProcessed / totalPhysical) * 50));
        if (physicalLinesProcessed > 0 && pct < 1) pct = 1;
        const { data: prevRow } = await supabaseServer
          .from("raw_report_uploads")
          .select("metadata")
          .eq("id", uploadId)
          .eq("organization_id", orgId)
          .maybeSingle();
        const prevMeta = (prevRow as { metadata?: unknown } | null)?.metadata;
        const dataRowsSeenLive = Math.max(
          0,
          pass1Metrics.file_rows_seen - pass1Metrics.raw_rows_skipped_empty,
        );
        await supabaseServer
          .from("raw_report_uploads")
          .update({
            metadata: mergeUploadMetadata(prevMeta, {
              row_count: physicalLinesProcessed,
              total_rows: pass1Metrics.file_rows_seen,
              process_progress: pct,
              catalog_listing_import_phase: "raw_archive",
              catalog_listing_file_rows_seen: pass1Metrics.file_rows_seen,
              catalog_listing_data_rows_seen: dataRowsSeenLive,
              catalog_listing_raw_rows_stored: pass1Metrics.raw_rows_stored + rawBatch.length,
              catalog_listing_raw_rows_skipped_empty: pass1Metrics.raw_rows_skipped_empty,
              catalog_listing_raw_rows_skipped_malformed: pass1Metrics.raw_rows_skipped_malformed,
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
            processed_rows: physicalLinesProcessed,
            total_rows: pass1Metrics.file_rows_seen,
            import_metrics: { current_phase: "staging" },
          },
          { onConflict: "upload_id" },
        );
      };

      let physicalLinesProcessed = 0;
      for (let lineIdx = 1; lineIdx < lines.length; lineIdx++) {
        physicalLinesProcessed += 1;
        const rawLine = lines[lineIdx] ?? "";
        if (rawLine.trim() === "") {
          pass1Metrics.raw_rows_skipped_empty += 1;
          void flushListingProgressPass1(false, physicalLinesProcessed).catch(() => {});
          continue;
        }

        const insert = buildRawRowInsertForPhysicalLine({
          lines,
          lineIdx,
          rawLine,
          separator: sep,
          columnMapping,
          organizationId: orgId,
          storeId: importStoreId,
          sourceUploadId: uploadId,
          sourceReportType: reportTypeRaw,
        });
        if (!insert) continue;

        if ((insert as { parse_status?: string }).parse_status === "skipped_malformed") {
          pass1Metrics.raw_rows_skipped_malformed += 1;
        }

        rawBatch.push(insert);

        if (rawBatch.length >= BATCH_SIZE) {
          await flushRawRowsBatch();
          await flushListingProgressPass1(true, physicalLinesProcessed);
        }
        void flushListingProgressPass1(false, physicalLinesProcessed).catch(() => {});
      }

      await flushRawRowsBatch();
      await flushListingProgressPass1(true, physicalLinesProcessed);

      const dataRowsSeen = Math.max(
        0,
        pass1Metrics.file_rows_seen - pass1Metrics.raw_rows_skipped_empty,
      );

      const canonicalMetrics = await syncListingRawRowsToCatalogProducts({
        supabase: supabaseServer,
        organizationId: orgId,
        storeId: importStoreId,
        sourceUploadId: uploadId,
        reportTypeRaw,
        fileRowsSeen: pass1Metrics.file_rows_seen,
        storedRawRows: pass1Metrics.raw_rows_stored,
        onProgress: async (pct, pass2RowsDone) => {
          const approxRows = dataRowsSeen + pass2RowsDone;
          const listingPassTotal = dataRowsSeen + pass1Metrics.raw_rows_stored;
          const { data: prevRow } = await supabaseServer
            .from("raw_report_uploads")
            .select("metadata")
            .eq("id", uploadId)
            .eq("organization_id", orgId)
            .maybeSingle();
          await supabaseServer
            .from("raw_report_uploads")
            .update({
              metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
                row_count: approxRows,
                total_rows: listingPassTotal,
                process_progress: pct,
                catalog_listing_import_phase: "canonical_sync",
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
              current_phase: "processing",
              upload_pct: 100,
              process_pct: pct,
              processed_rows: approxRows,
              total_rows: listingPassTotal,
              import_metrics: { current_phase: "processing" },
            },
            { onConflict: "upload_id" },
          );
        },
      });

      const { data: prevRowFinal } = await supabaseServer
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadId)
        .eq("organization_id", orgId)
        .maybeSingle();

      const finalRowCount = dataRowsSeen;
      await supabaseServer
        .from("raw_report_uploads")
        .update({
          status: "synced",
          metadata: mergeUploadMetadata((prevRowFinal as { metadata?: unknown } | null)?.metadata, {
            row_count: finalRowCount,
            process_progress: 100,
            error_message: undefined,
            catalog_listing_import_phase: "done",
            catalog_listing_file_rows_seen: pass1Metrics.file_rows_seen,
            catalog_listing_data_rows_seen: dataRowsSeen,
            catalog_listing_raw_rows_stored: pass1Metrics.raw_rows_stored,
            catalog_listing_raw_rows_skipped_empty: pass1Metrics.raw_rows_skipped_empty,
            catalog_listing_raw_rows_skipped_malformed: pass1Metrics.raw_rows_skipped_malformed,
            catalog_listing_canonical_rows_new: canonicalMetrics.canonical_rows_new,
            catalog_listing_canonical_rows_updated: canonicalMetrics.canonical_rows_updated,
            catalog_listing_canonical_rows_unchanged: canonicalMetrics.canonical_rows_unchanged,
            catalog_listing_canonical_rows_invalid_for_merge: canonicalMetrics.canonical_rows_invalid_for_merge,
            catalog_listing_canonical_rows_inserted: canonicalMetrics.canonical_rows_new,
            catalog_listing_canonical_rows_unchanged_or_merged: canonicalMetrics.canonical_rows_unchanged,
            total_rows: dataRowsSeen,
          }),
          updated_at: new Date().toISOString(),
        })
        .eq("id", uploadId)
        .eq("organization_id", orgId);

      await supabaseServer.from("file_processing_status").upsert(
        {
          upload_id: uploadId,
          organization_id: orgId,
          status: "complete",
          upload_pct: 100,
          process_pct: 100,
          processed_rows: finalRowCount,
          total_rows: dataRowsSeen,
          error_message: null,
        },
        { onConflict: "upload_id" },
      );

      await audit(orgId, null, "import.process_completed", uploadId, {
        kind,
        listingImport: {
          ...pass1Metrics,
          data_rows_seen: dataRowsSeen,
          ...canonicalMetrics,
        },
      });

      const catalogListingPayload = {
        data_rows_seen: dataRowsSeen,
        physical_lines_after_header: pass1Metrics.file_rows_seen,
        raw_rows_stored: pass1Metrics.raw_rows_stored,
        raw_rows_skipped_empty: pass1Metrics.raw_rows_skipped_empty,
        raw_rows_skipped_malformed: pass1Metrics.raw_rows_skipped_malformed,
        canonical_new: canonicalMetrics.canonical_rows_new,
        canonical_updated: canonicalMetrics.canonical_rows_updated,
        canonical_unchanged: canonicalMetrics.canonical_rows_unchanged,
        canonical_invalid_for_merge: canonicalMetrics.canonical_rows_invalid_for_merge,
        message: [
          `Data rows: ${dataRowsSeen.toLocaleString()}`,
          `Raw stored: ${pass1Metrics.raw_rows_stored.toLocaleString()}`,
          `Empty skipped: ${pass1Metrics.raw_rows_skipped_empty.toLocaleString()}`,
          `Malformed skipped: ${pass1Metrics.raw_rows_skipped_malformed.toLocaleString()}`,
          `Canonical new: ${canonicalMetrics.canonical_rows_new.toLocaleString()}`,
          `Canonical updated: ${canonicalMetrics.canonical_rows_updated.toLocaleString()}`,
          `Canonical unchanged: ${canonicalMetrics.canonical_rows_unchanged.toLocaleString()}`,
          `Canonical invalid: ${canonicalMetrics.canonical_rows_invalid_for_merge.toLocaleString()}`,
        ].join("\n"),
      };

      return NextResponse.json({
        ok: true,
        rowsProcessed: dataRowsSeen,
        totalRows: dataRowsSeen,
        kind,
        catalogListing: catalogListingPayload,
      });
    }

    return NextResponse.json(
      { ok: false, error: "Expected listing import path after non-listing early return." },
      { status: 500 },
    );
  } catch (e) {
    const message = e instanceof Error ? e.message : "Processing failed.";
    if (uploadIdForFail && isUuidString(uploadIdForFail)) {
      let failOrgId = orgId;
      if (!isUuidString(failOrgId)) {
        const { data: r } = await supabaseServer
          .from("raw_report_uploads")
          .select("organization_id")
          .eq("id", uploadIdForFail)
          .maybeSingle();
        failOrgId = String((r as { organization_id?: unknown } | null)?.organization_id ?? "").trim();
      }
      if (isUuidString(failOrgId)) {
        const { data: prevRow } = await supabaseServer
          .from("raw_report_uploads")
          .select("metadata")
          .eq("id", uploadIdForFail)
          .eq("organization_id", failOrgId)
          .maybeSingle();
        await supabaseServer
          .from("raw_report_uploads")
          .update({
            status: "failed",
            metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
              error_message: message,
              process_progress: 0,
            }),
            updated_at: new Date().toISOString(),
          })
          .eq("id", uploadIdForFail)
          .eq("organization_id", failOrgId);

        await supabaseServer.from("file_processing_status").upsert(
          {
            upload_id: uploadIdForFail,
            organization_id: failOrgId,
            status: "failed",
            upload_pct: 100,
            process_pct: 0,
            error_message: message,
          },
          { onConflict: "upload_id" },
        );

        await audit(failOrgId, null, "import.process_failed", uploadIdForFail, { message });
      }
    }
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
