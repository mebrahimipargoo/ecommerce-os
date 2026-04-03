import csv from "csv-parser";
import { NextResponse } from "next/server";

import { createConcatenatedPartsReadable } from "../../../../../lib/import-raw-report-stream";
import {
  deriveImportStatus,
  mapCsvRowToReturnFields,
} from "../../../../../lib/import-returns-csv-map";
import { mergeUploadMetadata, parseRawReportMetadata } from "../../../../../lib/raw-report-upload-metadata";
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

    if (metaObj.source === "amazon_ledger_uploader") {
      return NextResponse.json(
        {
          ok: false,
          error:
            "This session is for the Amazon Inventory Ledger uploader. Use “Process to Database” on the Ledger card, not History.",
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
    if (status !== "pending") {
      return NextResponse.json(
        { ok: false, error: `Cannot process while status is "${status}". Expected "pending".` },
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
      .eq("status", "pending")
      .select("id");

    if (lockErr) {
      return NextResponse.json({ ok: false, error: lockErr.message }, { status: 500 });
    }
    if (!locked || locked.length === 0) {
      return NextResponse.json(
        { ok: false, error: "Upload is not pending (already processing or completed)." },
        { status: 409 },
      );
    }

    await audit(orgId, null, "import.process_started", uploadId, {
      fileName: (row as { file_name?: string }).file_name,
      totalParts,
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
    const parser = csv({
      mapHeaders: ({ header }) => String(header).replace(/^\uFEFF/, "").trim(),
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

      // Keep the slim real-time table in sync so Realtime payloads stay tiny.
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
      const withLpn = rows.filter((r) => typeof r.lpn === "string" && String(r.lpn).trim().length > 0);
      const withoutLpn = rows.filter((r) => !(typeof r.lpn === "string" && String(r.lpn).trim().length > 0));

      if (withLpn.length > 0) {
        const { error: upErr } = await supabaseServer.from("returns").upsert(withLpn, {
          onConflict: "organization_id,lpn",
        });
        if (upErr) throw new Error(upErr.message);
      }
      if (withoutLpn.length > 0) {
        const { error: insErr } = await supabaseServer.from("returns").insert(withoutLpn);
        if (insErr) throw new Error(insErr.message);
      }
    };

    await new Promise<void>((resolve, reject) => {
      source.on("error", reject);
      parser.on("error", reject);

      parser.on("data", (row: Record<string, string>) => {
        const mapped = mapCsvRowToReturnFields(row, columnMapping);
        if (!mapped) return;

        const statusDerived = deriveImportStatus(mapped.conditions);
        const insertRow: Record<string, unknown> = {
          organization_id: orgId,
          marketplace: "amazon",
          item_name: mapped.item_name,
          order_id: mapped.order_id,
          lpn: mapped.lpn,
          sku: mapped.sku ?? null,
          conditions: mapped.conditions,
          notes: mapped.notes,
          photo_evidence: null,
          status: statusDerived,
        };

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
                status: "complete",
                metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
                  row_count: processed,
                  process_progress: 100,
                  error_message: undefined,
                }),
                updated_at: new Date().toISOString(),
              })
              .eq("id", uploadId)
              .eq("organization_id", orgId);

            // Mark real-time progress row as complete.
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
            });
            resolve();
          } catch (e) {
            reject(e instanceof Error ? e : new Error(String(e)));
          }
        })();
      });

      source.pipe(parser);
    });

    return NextResponse.json({ ok: true, rowsProcessed: processed });
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
