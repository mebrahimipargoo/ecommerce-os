import { NextResponse } from "next/server";

import { executeAmazonPhase2Staging } from "../../../../../lib/pipeline/amazon-phase2-staging";
import { resolveAmazonImportSyncKind } from "../../../../../lib/pipeline/amazon-report-registry";
import {
  AMAZON_LEDGER_UPLOAD_SOURCE,
  parseRawReportMetadata,
} from "../../../../../lib/raw-report-upload-metadata";
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
      .select("id, organization_id, metadata, status, report_type")
      .eq("id", uploadId)
      .maybeSingle();

    if (fetchErr || !row) {
      return NextResponse.json({ ok: false, error: "Upload session not found." }, { status: 404 });
    }

    const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
    if (!isUuidString(orgId)) {
      return NextResponse.json({ ok: false, error: "Invalid upload row (organization_id)." }, { status: 500 });
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

    return executeAmazonPhase2Staging(body);
  } catch (e) {
    const message = e instanceof Error ? e.message : "Processing failed.";
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
