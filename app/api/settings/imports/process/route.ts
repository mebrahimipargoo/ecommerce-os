import csv from "csv-parser";
import { NextResponse } from "next/server";

import { createConcatenatedPartsReadable } from "../../../../../lib/import-raw-report-stream";
import {
  applyColumnMappingToRow,
  mapRowToExpectedRemoval,
  mapRowToExpectedReturn,
  mapRowToProductFromLedger,
  normalizeAmazonReportRowKeys,
} from "../../../../../lib/import-sync-mappers";
import {
  AMAZON_LEDGER_UPLOAD_SOURCE,
  mergeUploadMetadata,
  parseRawReportMetadata,
} from "../../../../../lib/raw-report-upload-metadata";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";

/** Large CSV processing can exceed default serverless limits on some hosts. */
export const maxDuration = 300;

const BATCH_SIZE = 1000;
const PROGRESS_UPDATE_EVERY = 1000;

type Body = { upload_id?: string };

function num(v: unknown, fallback = 0): number {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return fallback;
}

/**
 * Pipeline kind from `raw_report_uploads.report_type` only (canonical + legacy slugs).
 */
function resolveImportKind(reportType: string | null | undefined): "FBA_RETURNS" | "REMOVAL_ORDER" | "INVENTORY_LEDGER" | "UNKNOWN" {
  const rt = String(reportType ?? "").trim();
  if (rt === "FBA_RETURNS" || rt === "fba_customer_returns") return "FBA_RETURNS";
  if (rt === "REMOVAL_ORDER") return "REMOVAL_ORDER";
  if (rt === "INVENTORY_LEDGER" || rt === "inventory_ledger") return "INVENTORY_LEDGER";
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
    // Accept "ready" (Record-First model), "uploaded" (alias), and legacy "pending".
    const isSyncable = status === "ready" || status === "uploaded" || status === "pending";
    if (!isSyncable) {
      if (status === "needs_mapping") {
        return NextResponse.json(
          {
            ok: false,
            error:
              'This upload needs column mapping before it can be synced. Click "Map Columns" in the History table to assign fields.',
          },
          { status: 409 },
        );
      }
      return NextResponse.json(
        {
          ok: false,
          error: `Cannot process while status is "${status}". Expected "ready", "uploaded", or "pending".`,
        },
        { status: 409 },
      );
    }

    const extRaw = typeof metaObj.file_extension === "string" ? metaObj.file_extension.trim().toLowerCase() : "";
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

    const kind = resolveImportKind((row as { report_type?: string | null }).report_type);
    if (kind === "UNKNOWN") {
      return NextResponse.json(
        {
          ok: false,
          error:
            "Could not determine import kind (FBA returns, removal order, or inventory ledger). Set the Type in History or re-upload so headers can be classified.",
        },
        { status: 422 },
      );
    }

    const { data: locked, error: lockErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "processing",
        metadata: mergeUploadMetadata(meta, {
          process_progress: 0,
          error_message: "",
        }),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId)
      .in("status", ["ready", "uploaded", "pending"])   // accept all syncable states
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
        upload_pct: 100,
        process_pct: 0,
        total_rows: estimatedRows ?? null,
        processed_rows: 0,
        error_message: null,
      },
      { onConflict: "upload_id" },
    );

    const source = createConcatenatedPartsReadable(supabaseServer, storagePrefix, totalParts);
    const processSeparator: string = ext === "txt" ? "\t" : ",";
    const parser = csv({
      mapHeaders: ({ header }) => String(header).replace(/^\uFEFF/, "").trim(),
      separator: processSeparator,
    });

    let processed = 0;
    let batch: Record<string, unknown>[] = [];
    let lastProgressWrite = 0;

    const flushProgress = async (force: boolean) => {
      if (!force && processed - lastProgressWrite < PROGRESS_UPDATE_EVERY) return;
      lastProgressWrite = processed;
      const pct =
        estimatedRows && estimatedRows > 0
          ? Math.min(99, Math.round((processed / estimatedRows) * 100))
          : 0;
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
            row_count: processed,
            process_progress: pct,
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
          upload_pct: 100,
          process_pct: pct,
          processed_rows: processed,
          ...(estimatedRows != null ? { total_rows: estimatedRows } : {}),
        },
        { onConflict: "upload_id" },
      );
    };

    const flushBatch = async (rows: Record<string, unknown>[]) => {
      if (rows.length === 0) return;

      if (kind === "FBA_RETURNS") {
        const { error: upErr } = await supabaseServer.from("expected_returns").upsert(rows, {
          onConflict: "organization_id,lpn",
        });
        if (upErr) throw new Error(upErr.message);
        return;
      }

      if (kind === "REMOVAL_ORDER") {
        const { error: upErr } = await supabaseServer.from("expected_removals").upsert(rows, {
          onConflict: "organization_id,order_id,sku",
        });
        if (upErr) throw new Error(upErr.message);
        return;
      }

      if (kind === "INVENTORY_LEDGER") {
        const { error: upErr } = await supabaseServer.from("products").upsert(rows, {
          onConflict: "organization_id,barcode",
        });
        if (upErr) throw new Error(upErr.message);
      }
    };

    await new Promise<void>((resolve, reject) => {
      source.on("error", reject);
      parser.on("error", reject);

      parser.on("data", (csvRow: Record<string, string>) => {
        // Apply the saved column_mapping so user-verified header names resolve correctly
        // even when the CSV uses non-standard column names.
        const mappedRow = applyColumnMappingToRow(normalizeAmazonReportRowKeys(csvRow), columnMapping);

        let insertRow: Record<string, unknown> | null = null;

        if (kind === "FBA_RETURNS") {
          insertRow = mapRowToExpectedReturn(mappedRow, orgId, uploadId) as unknown as Record<string, unknown> | null;
        } else if (kind === "REMOVAL_ORDER") {
          insertRow = mapRowToExpectedRemoval(mappedRow, orgId, uploadId) as unknown as Record<string, unknown> | null;
        } else if (kind === "INVENTORY_LEDGER") {
          const p = mapRowToProductFromLedger(mappedRow, orgId);
          insertRow = p ? { ...p } : null;
        }

        if (!insertRow) {
          return;
        }

        batch.push(insertRow);
        processed += 1;

        if (batch.length >= BATCH_SIZE) {
          parser.pause();
          const chunk = batch;
          batch = [];
          void flushBatch(chunk)
            .then(() => flushProgress(false))
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
              .eq("organization_id", orgId)
              .maybeSingle();

            await supabaseServer
              .from("raw_report_uploads")
              .update({
                status: "synced",
                metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
                  row_count: processed,
                  process_progress: 100,
                  error_message: undefined,
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
                processed_rows: processed,
                ...(estimatedRows != null ? { total_rows: estimatedRows } : {}),
                error_message: null,
              },
              { onConflict: "upload_id" },
            );

            await audit(orgId, null, "import.process_completed", uploadId, {
              rowsInserted: processed,
              kind,
            });
            resolve();
          } catch (e) {
            reject(e instanceof Error ? e : new Error(String(e)));
          }
        })();
      });

      source.pipe(parser);
    });

    return NextResponse.json({ ok: true, rowsProcessed: processed, kind });
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
