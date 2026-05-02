import { NextResponse } from "next/server";

import { executeAmazonPhase2Staging } from "../../../../../lib/pipeline/amazon-phase2-staging";
import { resolveAmazonImportSyncKind } from "../../../../../lib/pipeline/amazon-report-registry";
import {
  AMAZON_LEDGER_UPLOAD_SOURCE,
  mergeUploadMetadata,
  parseRawReportMetadata,
} from "../../../../../lib/raw-report-upload-metadata";
import {
  PRODUCT_IDENTITY_REPORT_TYPE,
  processProductIdentityToStaging,
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

function resolveImportStoreId(meta: Record<string, unknown>): string | null {
  const a = typeof meta.import_store_id === "string" ? meta.import_store_id.trim() : "";
  if (a && isUuidString(a)) return a;
  const b = typeof meta.ledger_store_id === "string" ? meta.ledger_store_id.trim() : "";
  if (b && isUuidString(b)) return b;
  return null;
}

async function validateImportStoreBelongsToOrg(params: {
  organizationId: string;
  metadata: Record<string, unknown>;
}): Promise<{ ok: true; storeId: string | null } | { ok: false; error: string }> {
  const storeId = resolveImportStoreId(params.metadata);
  if (!storeId) return { ok: true, storeId: null };
  const { data, error } = await supabaseServer
    .from("stores")
    .select("id, organization_id")
    .eq("id", storeId)
    .maybeSingle();
  if (error) return { ok: false, error: `Store validation failed: ${error.message}` };
  if (!data) return { ok: false, error: "Selected target store does not exist." };
  const ownerOrg = String((data as { organization_id?: unknown }).organization_id ?? "").trim();
  if (ownerOrg !== params.organizationId) {
    return {
      ok: false,
      error: "Selected target store belongs to a different organization than the active import organization.",
    };
  }
  return { ok: true, storeId };
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

/**
 * Phase 2 — parse CSV → product_identity_staging_rows only.
 *
 * Does NOT write to products / catalog_products / product_identifier_map.
 * On completion: status = 'staged', next action = Sync.
 */
async function processProductIdentityUpload(params: {
  uploadId: string;
  organizationId: string;
  row: Record<string, unknown>;
  metadata: Record<string, unknown>;
}): Promise<Response> {
  const { uploadId, organizationId, row, metadata } = params;
  const now = new Date().toISOString();

  const detectedHeadersStart = Array.isArray(metadata.csv_headers)
    ? (metadata.csv_headers as unknown[]).map((h) => String(h ?? "")).filter(Boolean)
    : [];
  const estimatedTotalRows = numericMeta(metadata.total_rows, 0);

  // ── Optimistic lock ───────────────────────────────────────────────────────
  const { data: locked, error: lockErr } = await supabaseServer
    .from("raw_report_uploads")
    .update({
      status: "processing",
      metadata: mergeUploadMetadata(metadata, {
        error_message: "",
        process_progress: 0,
        sync_progress: 0,
        import_metrics: {
          current_phase: "process",
          detected_headers: detectedHeadersStart,
          detected_report_type: "PRODUCT_IDENTITY",
          stage_target_table: "product_identity_staging_rows",
          note: "Phase 2 — writing to staging only. Final tables written in Phase 3 (Sync).",
        },
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
    return NextResponse.json({ ok: false, error: lockErr.message, details: lockErr.message, uploadId, phase: "process" }, { status: 500 });
  }
  if (!locked || locked.length === 0) {
    return NextResponse.json({
      ok: false,
      error: "Upload is not in a processable state (another operation may be running).",
      details: "raw_report_uploads.status was not in {mapped, ready, uploaded, pending, failed}.",
      uploadId,
      phase: "process",
    }, { status: 409 });
  }

  // ── Seed file_processing_status ───────────────────────────────────────────
  await supabaseServer.from("file_processing_status").upsert({
    upload_id: uploadId,
    organization_id: organizationId,
    status: "processing",
    current_phase: "process",
    current_phase_label: "Product Identity — staging CSV rows",
    stage_target_table: "product_identity_staging_rows",
    sync_target_table: "product_identifier_map",
    generic_target_table: null,
    upload_pct: 100,
    phase1_upload_pct: 100,
    phase2_stage_pct: 1,
    phase3_raw_sync_pct: 0,
    process_pct: 1,
    sync_pct: 0,
    processed_rows: 0,
    staged_rows_written: 0,
    total_rows: estimatedTotalRows || null,
    phase2_status: "running",
    phase2_started_at: now,
    phase3_status: "pending",
    import_metrics: { current_phase: "process" },
    error_message: null,
  }, { onConflict: "upload_id" });

  // ── Run Phase 2 ───────────────────────────────────────────────────────────
  try {
    const result = await processProductIdentityToStaging({
      supabase: supabaseServer,
      organizationId,
      uploadId,
      uploadRow: row,
      metadata,
      onChunkProgress: async ({ staged, total }) => {
        const denominator =
          estimatedTotalRows > 0 && total > 0
            ? Math.max(estimatedTotalRows, total)
            : estimatedTotalRows > 0
              ? estimatedTotalRows
              : total;
        const pct = denominator > 0 ? Math.min(99, Math.round((staged / denominator) * 100)) : 1;
        await supabaseServer.from("file_processing_status").upsert({
          upload_id: uploadId,
          organization_id: organizationId,
          status: "processing",
          current_phase: "process",
          phase2_stage_pct: pct,
          process_pct: pct,
          staged_rows_written: staged,
          processed_rows: staged,
          total_rows: denominator > 0 ? denominator : null,
          import_metrics: { current_phase: "process", rows_staged: staged },
        }, { onConflict: "upload_id" });
      },
    });

    if (!result.ok) {
      await failProductIdentityImport({ uploadId, organizationId, metadata, message: result.error });
      return NextResponse.json({ ok: false, error: result.error, details: result.error, uploadId, phase: "process" }, { status: result.status });
    }

    const { stats } = result;
    const doneAt = new Date().toISOString();

    // Phase 2 complete: status=staged so Sync button lights up.
    await supabaseServer.from("file_processing_status").upsert({
      upload_id: uploadId,
      organization_id: organizationId,
      status: "pending",
      current_phase: "staged",
      current_phase_label: "Ready for Sync",
      phase2_stage_pct: 100,
      phase2_status: "complete",
      phase2_completed_at: doneAt,
      process_pct: 100,
      phase3_status: "pending",
      phase3_raw_sync_pct: 0,
      sync_pct: 0,
      staged_rows_written: stats.rowsRead,
      processed_rows: stats.rowsRead,
      total_rows: stats.rowsRead,
      data_rows_total: stats.rowsRead,
      next_action_key: "sync",
      next_action_label: "Sync",
      import_metrics: {
        current_phase: "staged",
        data_rows_seen: stats.rowsRead,
        rows_staged: stats.rowsRead,
        rows_missing_seller_sku: stats.rowsMissingSellerSku,
        rows_invalid_seller_sku: stats.rowsInvalidSellerSku,
        rows_skipped: stats.rowsSkipped,
        detected_report_type: "PRODUCT_IDENTITY",
        stage_target_table: "product_identity_staging_rows",
      },
      error_message: null,
    }, { onConflict: "upload_id" });

    return NextResponse.json({
      ok: true,
      rowsStaged: stats.rowsRead,
      rowsSkipped: stats.rowsSkipped,
      totalRows: stats.rowsRead,
      phase: "process",
      pipeline: "product_identity_staging",
      nextAction: "sync",
    });

  } catch (e) {
    const message = e instanceof Error ? e.message : "Product Identity staging failed.";
    console.error("[ProductIdentityProcess] Phase 2 failed", {
      uploadId, organizationId,
      storeId: resolveImportStoreId(metadata),
      reportType: "PRODUCT_IDENTITY", phase: "process",
      error: message, stack: e instanceof Error ? e.stack : undefined,
    });
    await failProductIdentityImport({ uploadId, organizationId, metadata, message });
    return NextResponse.json({ ok: false, error: message, details: e instanceof Error ? e.stack ?? message : String(e), uploadId, phase: "process" }, { status: 500 });
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
      return NextResponse.json(
        { ok: false, error: "Invalid upload_id.", details: "upload_id must be a UUID.", uploadId: null, phase: "process" },
        { status: 400 },
      );
    }

    const { data: row, error: fetchErr } = await supabaseServer
      .from("raw_report_uploads")
      .select("id, organization_id, metadata, status, report_type, import_pipeline_started_at")
      .eq("id", uploadId)
      .maybeSingle();

    if (fetchErr || !row) {
      return NextResponse.json(
        {
          ok: false,
          error: "Upload session not found.",
          details: fetchErr?.message ?? "raw_report_uploads.id did not match any row.",
          uploadId,
          phase: "process",
        },
        { status: 404 },
      );
    }

    const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
    if (!isUuidString(orgId)) {
      return NextResponse.json(
        {
          ok: false,
          error: "Invalid upload row (organization_id).",
          details: "raw_report_uploads.organization_id is not a UUID.",
          uploadId,
          phase: "process",
        },
        { status: 500 },
      );
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

    const storeValidation = await validateImportStoreBelongsToOrg({
      organizationId: orgId,
      metadata: metaObj,
    });
    if (!storeValidation.ok) {
      return NextResponse.json(
        { ok: false, error: storeValidation.error, details: storeValidation.error, uploadId, phase: "process" },
        { status: 422 },
      );
    }

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
          details: `Ledger source upload status="${st}".`,
          uploadId,
          phase: "process",
        },
        { status: 409 },
      );
    }

    const parsed = parseRawReportMetadata(meta);
    if (parsed.uploadProgress < 100) {
      return NextResponse.json(
        {
          ok: false,
          error: "Upload is not complete yet (wait for 100% upload progress).",
          details: `metadata.upload_progress=${parsed.uploadProgress}`,
          uploadId,
          phase: "process",
        },
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
            details: `raw_report_uploads.status="${status}"`,
            uploadId,
            phase: "process",
          },
          { status: 409 },
        );
      }
      return NextResponse.json(
        {
          ok: false,
          error: `Cannot process while status is "${status}". Expected one of: ${processableStatuses.join(", ")}.`,
          details: `raw_report_uploads.status="${status}"`,
          uploadId,
          phase: "process",
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
          details: `raw_report_uploads.report_type="${reportTypeRaw}"`,
          uploadId,
          phase: "process",
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
    console.error("[imports/process] unhandled failure", {
      phase: "process",
      error: message,
      stack: e instanceof Error ? e.stack : undefined,
    });
    return NextResponse.json(
      {
        ok: false,
        error: message,
        details: e instanceof Error ? e.stack ?? message : String(e),
        uploadId: null,
        phase: "process",
      },
      { status: 500 },
    );
  }
}
