import { NextResponse } from "next/server";
import { Readable } from "node:stream";

import { executeAmazonPhase2Staging } from "../../../../../lib/pipeline/amazon-phase2-staging";
import { resolveAmazonImportSyncKind } from "../../../../../lib/pipeline/amazon-report-registry";
import {
  AMAZON_LEDGER_UPLOAD_SOURCE,
  mergeUploadMetadata,
  parseRawReportMetadata,
} from "../../../../../lib/raw-report-upload-metadata";
import {
  PRODUCT_IDENTITY_REPORT_TYPE,
  readProductIdentityCsvRowsFromStream,
  runProductIdentityImport,
  type ProductIdentityColumnMapping,
} from "../../../../../lib/product-identity-import";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";

/** Large CSV processing can exceed default serverless limits on some hosts. */
export const maxDuration = 300;

type Body = {
  upload_id?: string;
  start_date?: string | null;
  end_date?: string | null;
  import_full_file?: boolean | null;
};

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function numericMeta(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return fallback;
}

function productIdentityColumnMapping(value: unknown): ProductIdentityColumnMapping | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const result: ProductIdentityColumnMapping = {};
  for (const key of ["upc", "vendor", "seller_sku", "mfg_part_number", "fnsku", "asin", "product_name"] as const) {
    const mapped = (value as Record<string, unknown>)[key];
    if (typeof mapped === "string" && mapped.trim()) result[key] = mapped.trim();
  }
  return Object.keys(result).length > 0 ? result : null;
}

async function failProductIdentityImport(params: {
  uploadId: string;
  organizationId: string;
  metadata: unknown;
  message: string;
}): Promise<void> {
  const now = new Date().toISOString();
  const metadata = mergeUploadMetadata(params.metadata, {
    error_message: params.message,
    failed_phase: "process",
    import_metrics: {
      current_phase: "failed",
      failure_reason: params.message,
    },
  });

  await supabaseServer
    .from("raw_report_uploads")
    .update({
      status: "failed",
      import_pipeline_failed_at: now,
      metadata,
      updated_at: now,
    })
    .eq("id", params.uploadId)
    .eq("organization_id", params.organizationId);

  await supabaseServer.from("file_processing_status").upsert(
    {
      upload_id: params.uploadId,
      organization_id: params.organizationId,
      status: "failed",
      current_phase: "failed",
      upload_pct: 100,
      process_pct: 0,
      sync_pct: 0,
      error_message: params.message,
      import_metrics: {
        current_phase: "failed",
        failure_reason: params.message,
      },
    },
    { onConflict: "upload_id" },
  );
}

async function processProductIdentityUpload(params: {
  uploadId: string;
  organizationId: string;
  row: Record<string, unknown>;
  metadata: Record<string, unknown>;
}): Promise<Response> {
  const { uploadId, organizationId, row, metadata } = params;
  const now = new Date().toISOString();
  const storeId =
    typeof metadata.import_store_id === "string" && isUuidString(metadata.import_store_id.trim())
      ? metadata.import_store_id.trim()
      : typeof metadata.ledger_store_id === "string" && isUuidString(metadata.ledger_store_id.trim())
        ? metadata.ledger_store_id.trim()
        : "";

  if (!isUuidString(storeId)) {
    return NextResponse.json(
      { ok: false, error: "Product Identity import requires a selected target store." },
      { status: 400 },
    );
  }

  const rawFilePath = typeof metadata.raw_file_path === "string" ? metadata.raw_file_path.trim() : "";
  if (!rawFilePath) {
    return NextResponse.json(
      { ok: false, error: "Missing raw_file_path in upload metadata for Product Identity import." },
      { status: 400 },
    );
  }

  const contentSha256 =
    typeof metadata.content_sha256 === "string" && /^[a-f0-9]{64}$/i.test(metadata.content_sha256.trim())
      ? metadata.content_sha256.trim().toLowerCase()
      : "";
  if (!contentSha256) {
    return NextResponse.json(
      { ok: false, error: "Missing content SHA-256 for Product Identity import." },
      { status: 400 },
    );
  }

  const rawFileExt = rawFilePath.split(".").pop()?.toLowerCase() ?? "";
  const metaFileExt =
    typeof metadata.file_extension === "string" ? metadata.file_extension.replace(/^\./, "").toLowerCase() : "";
  const fileExt = metaFileExt || rawFileExt || "csv";
  if (fileExt === "xlsx" || fileExt === "xls") {
    return NextResponse.json(
      { ok: false, error: "Product Identity import currently requires CSV or TXT. Export Excel as CSV and re-upload." },
      { status: 415 },
    );
  }
  if (fileExt !== "csv" && fileExt !== "txt") {
    return NextResponse.json(
      { ok: false, error: `Unsupported Product Identity file type: .${fileExt}` },
      { status: 415 },
    );
  }

  const { data: locked, error: lockErr } = await supabaseServer
    .from("raw_report_uploads")
    .update({
      status: "processing",
      metadata: mergeUploadMetadata(metadata, {
        error_message: "",
        process_progress: 0,
        sync_progress: 0,
        import_metrics: { current_phase: "process" },
      }),
      import_pipeline_started_at: now,
      import_pipeline_failed_at: null,
      updated_at: now,
    })
    .eq("id", uploadId)
    .eq("organization_id", organizationId)
    .in("status", ["mapped", "ready", "uploaded", "pending", "failed"])
    .select("id");

  if (lockErr) {
    return NextResponse.json({ ok: false, error: lockErr.message }, { status: 500 });
  }
  if (!locked || locked.length === 0) {
    return NextResponse.json(
      { ok: false, error: "Upload is not in a processable state (another operation may be running)." },
      { status: 409 },
    );
  }

  await supabaseServer.from("file_processing_status").upsert(
    {
      upload_id: uploadId,
      organization_id: organizationId,
      status: "processing",
      current_phase: "process",
      current_phase_label: "Product Identity import",
      stage_target_table: "products",
      sync_target_table: "product_identifier_map",
      generic_target_table: null,
      upload_pct: 100,
      process_pct: 5,
      phase1_upload_pct: 100,
      phase2_stage_pct: 5,
      phase3_raw_sync_pct: 0,
      sync_pct: 0,
      processed_rows: 0,
      total_rows: numericMeta(metadata.total_rows, 0) || null,
      import_metrics: { current_phase: "process" },
      error_message: null,
    },
    { onConflict: "upload_id" },
  );

  try {
    const { data: blob, error: dlErr } = await supabaseServer.storage.from("raw-reports").download(rawFilePath);
    if (dlErr || !blob) {
      throw new Error(dlErr?.message ?? `Could not download file from storage: ${rawFilePath}`);
    }

    const webStream = blob.stream() as unknown as ReadableStream<Uint8Array>;
    const source = Readable.fromWeb(webStream as Parameters<typeof Readable.fromWeb>[0]);
    const headerRowIndex =
      typeof metadata.header_row_index === "number" && metadata.header_row_index > 0
        ? Math.floor(metadata.header_row_index)
        : 0;
    const csvRows = await readProductIdentityCsvRowsFromStream(source, {
      skipLines: headerRowIndex,
      separator: fileExt === "txt" ? "\t" : ",",
    });

    const stats = await runProductIdentityImport({
      supabase: supabaseServer,
      organizationId,
      storeId,
      uploadId,
      csvRows,
      columnMapping: productIdentityColumnMapping(row.column_mapping),
      sourceFileSha256: contentSha256,
    });

    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: uploadId,
        organization_id: organizationId,
        status: "complete",
        current_phase: "complete",
        current_phase_label: "Complete",
        stage_target_table: "products",
        sync_target_table: "product_identifier_map",
        generic_target_table: null,
        next_action_key: null,
        next_action_label: null,
        upload_pct: 100,
        process_pct: 100,
        sync_pct: 100,
        phase1_upload_pct: 100,
        phase2_stage_pct: 100,
        phase3_raw_sync_pct: 100,
        phase4_generic_pct: 0,
        processed_rows: stats.rowsRead,
        staged_rows_written: stats.productsInserted + stats.productsUpdated,
        raw_rows_written: stats.identifiersInserted,
        total_rows: Math.max(stats.rowsRead, 1),
        data_rows_total: stats.rowsRead,
        phase2_status: "complete",
        phase2_completed_at: new Date().toISOString(),
        phase3_status: "complete",
        phase3_completed_at: new Date().toISOString(),
        error_message: null,
        import_metrics: {
          current_phase: "complete",
          data_rows_seen: stats.rowsRead,
          rows_synced_upserted:
            stats.productsInserted +
            stats.productsUpdated +
            stats.catalogProductsInserted +
            stats.catalogProductsUpdated +
            stats.identifiersInserted,
          rows_invalid: stats.invalidIdentifierCount,
        },
      },
      { onConflict: "upload_id" },
    );

    return NextResponse.json({
      ok: true,
      rowsProcessed: stats.rowsRead,
      totalRows: stats.rowsRead,
      pipeline: "product_identity_import",
      productIdentity: { stats },
    });
  } catch (e) {
    const message = e instanceof Error ? e.message : "Product Identity import failed.";
    await failProductIdentityImport({ uploadId, organizationId, metadata, message });
    throw e;
  }
}

/**
 * POST /api/settings/imports/process
 *
 * Phase 2 only: parse file → `amazon_staging`. Sync and Generic are separate routes (registry-driven).
 */
export async function POST(req: Request): Promise<Response> {
  try {
    const body = (await req.json()) as Body;
    const uploadId = typeof body.upload_id === "string" ? body.upload_id.trim() : "";
    if (!isUuidString(uploadId)) {
      return NextResponse.json({ ok: false, error: "Invalid upload_id." }, { status: 400 });
    }

    const { data: row, error: fetchErr } = await supabaseServer
      .from("raw_report_uploads")
      .select("id, organization_id, metadata, status, report_type, import_pipeline_started_at")
      .eq("id", uploadId)
      .maybeSingle();

    if (fetchErr || !row) {
      return NextResponse.json({ ok: false, error: "Upload session not found." }, { status: 404 });
    }

    const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
    if (!isUuidString(orgId)) {
      return NextResponse.json({ ok: false, error: "Invalid upload row (organization_id)." }, { status: 500 });
    }

    // Stale-lock recovery: a prior worker that exceeded Vercel's maxDuration leaves
    // status="processing" forever. After PROCESSING_STALE_MS minutes flip it to
    // "failed" so the user can re-run Process. The actual Phase 2 logic in
    // amazon-phase2-staging then resumes from the highest already-staged row_number,
    // so no rows are lost or duplicated.
    {
      const stStr = String((row as { status?: unknown }).status ?? "");
      if (stStr === "processing") {
        const startedAtIso =
          (row as { import_pipeline_started_at?: string | null }).import_pipeline_started_at ?? null;
        const startedAt = startedAtIso ? new Date(startedAtIso).getTime() : NaN;
        const STALE_MS = 6 * 60 * 1000;
        if (Number.isFinite(startedAt) && Date.now() - startedAt >= STALE_MS) {
          await supabaseServer
            .from("raw_report_uploads")
            .update({ status: "failed", updated_at: new Date().toISOString() })
            .eq("id", uploadId)
            .eq("organization_id", orgId)
            .eq("status", "processing");
          (row as { status?: string }).status = "failed";
        }
      }
    }

    const meta = (row as { metadata?: unknown }).metadata;
    const metaObj = meta && typeof meta === "object" && !Array.isArray(meta) ? (meta as Record<string, unknown>) : {};

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

    const parsed = parseRawReportMetadata(meta);
    if (parsed.uploadProgress < 100) {
      return NextResponse.json(
        { ok: false, error: "Upload is not complete yet (wait for 100% upload progress)." },
        { status: 400 },
      );
    }

    const status = String((row as { status?: unknown }).status ?? "");
    const reportTypeRaw = String((row as { report_type?: string | null }).report_type ?? "").trim();

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

    const kind = resolveAmazonImportSyncKind(reportTypeRaw);
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

    if (kind === PRODUCT_IDENTITY_REPORT_TYPE) {
      return processProductIdentityUpload({
        uploadId,
        organizationId: orgId,
        row: asRecord(row),
        metadata: metaObj,
      });
    }

    return executeAmazonPhase2Staging(body);
  } catch (e) {
    const message = e instanceof Error ? e.message : "Processing failed.";
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
