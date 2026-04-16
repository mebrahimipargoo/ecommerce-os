import { NextResponse } from "next/server";

import { executeAmazonPhase2Staging } from "../../../../../lib/pipeline/amazon-phase2-staging";
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
 * Pipeline kind from `raw_report_uploads.report_type` only (canonical + legacy slugs).
 * Used only to reject UNKNOWN before Phase 2 (staging) runs.
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

/**
 * POST /api/settings/imports/process
 *
 * Phase 2 only: parse file and stage rows into `amazon_staging` (all Amazon file imports
 * including listing exports). Canonical/raw domain tables and generic actions run in
 * later endpoints.
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

    const reportTypeRaw = String((row as { report_type?: string | null }).report_type ?? "").trim();
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

    return executeAmazonPhase2Staging(body);
  } catch (e) {
    const message = e instanceof Error ? e.message : "Processing failed.";
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
