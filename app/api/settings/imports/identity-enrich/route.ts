/**
 * POST /api/settings/imports/identity-enrich
 *
 * Wave 5 — explicit, opt-in identifier-map enrichment trigger for the four
 * Wave-4 inventory families:
 *   • MANAGE_FBA_INVENTORY
 *   • FBA_INVENTORY
 *   • INBOUND_PERFORMANCE
 *   • AMAZON_FULFILLED_INVENTORY
 *
 * IMPORTANT — what this route is NOT:
 *   • NOT part of the Process / Sync / Generic pipeline.
 *   • Does NOT change `raw_report_uploads.status`, `file_processing_status`,
 *     or any progress UI.
 *   • Does NOT delete or re-stage any imported data.
 *
 * What it does:
 *   • Reads the uploaded inventory rows for one upload_id.
 *   • Calls `enrichIdentifierMapFromInventoryFamilyUpload` (shared lib).
 *   • Returns the enrichment metrics so the caller can confirm coverage gain.
 *
 * Idempotent: re-running on the same upload_id never weakens existing
 * `product_identifier_map.confidence_score` and never overwrites a non-null
 * trusted FNSKU with a different value.
 *
 * Body: { upload_id: string, dry_run?: boolean }
 * Returns: { ok: true, kind, source_table, dry_run, write_disabled, metrics }
 *
 * dry_run = true:
 *   • No Postgres writes are issued. Counters (`existing_rows_inserted`,
 *     `existing_rows_updated`, `safe_candidates_to_write`,
 *     `ambiguous_candidates_skipped`, `stronger_rows_preserved`) reflect what
 *     a real run would do.
 *
 * INBOUND_PERFORMANCE is currently on the write-blocklist; it always returns
 * dry_run=true and write_disabled=true regardless of the body flag.
 */

import { NextResponse } from "next/server";

import {
  enrichIdentifierMapFromInventoryFamilyUpload,
  getInventoryFamilySpec,
  isInventoryFamilyWriteDisabled,
  type InventoryFamilyReportType,
} from "../../../../../lib/inventory-family-identifier-enrich";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";
export const maxDuration = 300;

type Body = {
  upload_id?: string;
  /** When true, no Postgres writes are issued. Default false. */
  dry_run?: boolean;
};

function resolveStoreIdFromMeta(meta: unknown): string | null {
  const m =
    meta && typeof meta === "object" && !Array.isArray(meta)
      ? (meta as Record<string, unknown>)
      : {};
  const a = typeof m.import_store_id === "string" ? m.import_store_id.trim() : "";
  if (a && isUuidString(a)) return a;
  const b = typeof m.ledger_store_id === "string" ? m.ledger_store_id.trim() : "";
  if (b && isUuidString(b)) return b;
  return null;
}

export async function POST(req: Request): Promise<Response> {
  try {
    const body = (await req.json()) as Body;
    const uploadId = typeof body.upload_id === "string" ? body.upload_id.trim() : "";
    if (!isUuidString(uploadId)) {
      return NextResponse.json({ ok: false, error: "Invalid upload_id." }, { status: 400 });
    }

    const { data: row, error: fetchErr } = await supabaseServer
      .from("raw_report_uploads")
      .select("id, organization_id, status, report_type, metadata")
      .eq("id", uploadId)
      .maybeSingle();

    if (fetchErr || !row) {
      return NextResponse.json(
        { ok: false, error: "Upload session not found." },
        { status: 404 },
      );
    }

    const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
    if (!isUuidString(orgId)) {
      return NextResponse.json(
        { ok: false, error: "Invalid upload row (organization_id)." },
        { status: 500 },
      );
    }

    const rt = String((row as { report_type?: string }).report_type ?? "").trim();
    const spec = getInventoryFamilySpec(rt);
    if (!spec) {
      return NextResponse.json(
        {
          ok: false,
          error:
            `Identity enrichment is only supported for the four Wave-4 inventory ` +
            `families (MANAGE_FBA_INVENTORY, FBA_INVENTORY, INBOUND_PERFORMANCE, ` +
            `AMAZON_FULFILLED_INVENTORY). Got "${rt || "UNKNOWN"}".`,
        },
        { status: 422 },
      );
    }

    /**
     * Status guard — only run after the operational rows are actually in the
     * domain table. We accept the post-Wave-4 terminal states for the four
     * no-op-Generic families: synced / complete / raw_synced.
     */
    const status = String((row as { status?: unknown }).status ?? "").toLowerCase();
    const okStatuses = new Set(["synced", "complete", "raw_synced"]);
    if (!okStatuses.has(status)) {
      return NextResponse.json(
        {
          ok: false,
          error:
            `Upload is not yet synced (current status "${status}"). Run Sync first, ` +
            `then re-trigger identity enrichment.`,
        },
        { status: 409 },
      );
    }

    const storeId = resolveStoreIdFromMeta((row as { metadata?: unknown }).metadata);
    const requestedDryRun = body.dry_run === true;
    const writeDisabled = isInventoryFamilyWriteDisabled(rt as InventoryFamilyReportType);
    const effectiveDryRun = requestedDryRun || writeDisabled;

    const metrics = await enrichIdentifierMapFromInventoryFamilyUpload({
      supabase: supabaseServer,
      organizationId: orgId,
      uploadId,
      storeId,
      reportType: rt as InventoryFamilyReportType,
      dryRun: effectiveDryRun,
    });

    return NextResponse.json({
      ok: true,
      kind: spec.reportType,
      source_table: spec.table,
      dry_run: metrics.dry_run,
      write_disabled: metrics.write_disabled,
      metrics,
    });
  } catch (e) {
    const message = e instanceof Error ? e.message : "Identity enrichment failed.";
    console.error("[identity-enrich] error:", message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
