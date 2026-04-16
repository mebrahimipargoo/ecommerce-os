/**
 * POST /api/settings/imports/stage
 *
 * Backward-compatible alias for Phase 2 (raw file → `amazon_staging`).
 * Prefer POST /api/settings/imports/process for the unified pipeline entry.
 */

import { NextResponse } from "next/server";

import { executeAmazonPhase2Staging, type StageRequestBody } from "../../../../../lib/pipeline/amazon-phase2-staging";
import { isListingReportType } from "../../../../../lib/raw-report-types";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";
export const maxDuration = 300;

export async function POST(req: Request): Promise<Response> {
  const body = (await req.json()) as StageRequestBody;
  const uploadId = typeof body.upload_id === "string" ? body.upload_id.trim() : "";
  if (isUuidString(uploadId)) {
    const { data: uploadRow } = await supabaseServer
      .from("raw_report_uploads")
      .select("report_type")
      .eq("id", uploadId)
      .maybeSingle();
    const rt = String((uploadRow as { report_type?: string } | null)?.report_type ?? "").trim();
    if (isListingReportType(rt)) {
      return NextResponse.json(
        {
          ok: false,
          error: "Listing imports use Process only (one step: raw archive + catalog). Stage is not used for listings.",
        },
        { status: 409 },
      );
    }
  }
  return executeAmazonPhase2Staging(body);
}
