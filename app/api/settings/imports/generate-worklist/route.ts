/**
 * POST /api/settings/imports/generate-worklist
 *
 * Phase 4 for REMOVAL_ORDER / REMOVAL_SHIPMENT imports: calls the FastAPI agent
 * POST /etl/generate-worklist → aggregates amazon_removals → expected_packages.
 *
 * Requires env LOGISTICS_AGENT_API_URL (e.g. http://127.0.0.1:8000).
 *
 * Body: { upload_id: string }
 *
 * After FastAPI worklist completes, calls DB RPC `enrich_expected_packages_from_shipment_allocations`
 * to fill shipment-derived fields on expected_packages from removal_item_allocations + shipment tree
 * when exactly one container path exists.
 */

import { NextResponse } from "next/server";

import { mergeUploadMetadata } from "../../../../../lib/raw-report-upload-metadata";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";
export const maxDuration = 300;

type Body = { upload_id?: string };

type EtlTaskState = {
  status?: string;
  progress?: number;
  message?: string;
};

function logisticsBaseUrl(): string | null {
  const raw = process.env.LOGISTICS_AGENT_API_URL?.trim();
  if (!raw) return null;
  return raw.replace(/\/$/, "");
}

/** Imports Target Store on `raw_report_uploads.metadata` (same as sync route). */
function resolveImportStoreId(meta: unknown): string | null {
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

/** DB-side enrichment: expected_packages ← allocation layer (single container path only). */
async function enrichExpectedPackagesFromShipmentAllocations(opts: {
  organizationId: string;
  uploadId: string;
  storeId: string | null;
}): Promise<void> {
  const { organizationId, uploadId, storeId } = opts;
  try {
    const { data, error } = await supabaseServer.rpc("enrich_expected_packages_from_shipment_allocations", {
      p_organization_id: organizationId,
      p_upload_id: uploadId,
      p_store_id: storeId,
    });
    if (error) {
      console.warn("[generate-worklist] enrich_expected_packages_from_shipment_allocations:", error.message);
      return;
    }
    const row = Array.isArray(data) ? data[0] : data;
    if (!row || typeof row !== "object") return;
    const r = row as Record<string, unknown>;
    console.log(
      JSON.stringify({
        phase: "expected_packages_allocation_enrich",
        upload_id: uploadId,
        organization_id: organizationId,
        expected_rows_with_single_allocation_path: r.expected_rows_with_single_allocation_path,
        expected_rows_with_multiple_allocation_paths: r.expected_rows_with_multiple_allocation_paths,
        expected_rows_with_no_allocation_path: r.expected_rows_with_no_allocation_path,
        expected_rows_enriched_from_allocation: r.expected_rows_enriched_from_allocation,
      }),
    );
  } catch (e) {
    console.warn(
      "[generate-worklist] enrich_expected_packages_from_shipment_allocations:",
      e instanceof Error ? e.message : e,
    );
  }
}

/**
 * Direct fallback: when the allocation-layer enricher returns 0 (no demand row,
 * canonical key disagreement, or multi-container path) the typed shipment fields
 * already on amazon_removal_shipments must still propagate to expected_packages.
 * Fill-null only, ambiguity-aware, 4-tier priority match. See migration
 * 20260628_backfill_expected_packages_shipment_meta.sql.
 */
async function backfillExpectedPackagesShipmentMeta(opts: {
  organizationId: string;
  uploadId: string;
}): Promise<void> {
  const { organizationId, uploadId } = opts;
  try {
    const { data, error } = await supabaseServer.rpc("backfill_expected_packages_shipment_meta", {
      p_organization_id: organizationId,
      p_upload_id: uploadId,
    });
    if (error) {
      console.warn("[generate-worklist] backfill_expected_packages_shipment_meta:", error.message);
      return;
    }
    const row = Array.isArray(data) ? data[0] : data;
    if (!row || typeof row !== "object") return;
    const r = row as Record<string, unknown>;
    console.log(
      JSON.stringify({
        phase: "expected_packages_shipment_meta_backfill",
        upload_id: uploadId,
        organization_id: organizationId,
        rows_filled: r.rows_filled,
        rows_skipped_ambiguous: r.rows_skipped_ambiguous,
        rows_no_match: r.rows_no_match,
      }),
    );
  } catch (e) {
    console.warn(
      "[generate-worklist] backfill_expected_packages_shipment_meta:",
      e instanceof Error ? e.message : e,
    );
  }
}

export async function POST(req: Request): Promise<Response> {
  try {
    const base = logisticsBaseUrl();
    if (!base) {
      return NextResponse.json(
        {
          ok: false,
          error:
            "LOGISTICS_AGENT_API_URL is not set. Configure the FastAPI logistics agent base URL (e.g. http://127.0.0.1:8000).",
        },
        { status: 503 },
      );
    }

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
      return NextResponse.json({ ok: false, error: "Upload session not found." }, { status: 404 });
    }

    const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
    if (!isUuidString(orgId)) {
      return NextResponse.json(
        { ok: false, error: "Invalid upload row (organization_id)." },
        { status: 500 },
      );
    }

    const status = String((row as { status?: unknown }).status ?? "");
    if (status !== "synced") {
      return NextResponse.json(
        {
          ok: false,
          error: `Generate Worklist requires status "synced" (finish Phase 3 first). Current: "${status}".`,
        },
        { status: 409 },
      );
    }

    const rt = String((row as { report_type?: string }).report_type ?? "").trim();
    if (rt !== "REMOVAL_ORDER" && rt !== "REMOVAL_SHIPMENT") {
      return NextResponse.json(
        {
          ok: false,
          error: `Generate Worklist only applies to removal imports. This upload is "${rt || "UNKNOWN"}".`,
        },
        { status: 422 },
      );
    }

    const meta = (row as { metadata?: unknown }).metadata;

    const startRes = await fetch(`${base}/etl/generate-worklist`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ organization_id: orgId, upload_id: uploadId }),
    });

    const startJson = (await startRes.json()) as { task_id?: string; detail?: unknown };
    if (!startRes.ok) {
      let detail = `FastAPI returned ${startRes.status}`;
      const d = startJson.detail;
      if (typeof d === "string") detail = d;
      else if (Array.isArray(d))
        detail = d.map((x) => (typeof x === "object" && x && "msg" in x ? String((x as { msg?: unknown }).msg) : String(x))).join("; ");
      return NextResponse.json({ ok: false, error: detail }, { status: 502 });
    }

    const taskId = typeof startJson.task_id === "string" ? startJson.task_id : "";
    if (!taskId) {
      return NextResponse.json(
        { ok: false, error: "FastAPI did not return task_id." },
        { status: 502 },
      );
    }

    await supabaseServer
      .from("raw_report_uploads")
      .update({
        metadata: mergeUploadMetadata(meta, {
          worklist_progress: 0,
          etl_phase: "worklist",
          error_message: "",
        }),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId);

    const deadline = Date.now() + 280_000;
    let lastMsg = "";

    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 700));

      const pollRes = await fetch(`${base}/etl/task/${encodeURIComponent(taskId)}`);
      if (!pollRes.ok) {
        await supabaseServer
          .from("raw_report_uploads")
          .update({
            metadata: mergeUploadMetadata(meta, {
              worklist_progress: 0,
              error_message: `Worklist task poll failed: HTTP ${pollRes.status}`,
              etl_phase: "worklist",
            }),
            updated_at: new Date().toISOString(),
          })
          .eq("id", uploadId)
          .eq("organization_id", orgId);

        return NextResponse.json(
          { ok: false, error: `Task poll failed: HTTP ${pollRes.status}` },
          { status: 502 },
        );
      }

      const task = (await pollRes.json()) as EtlTaskState;
      const st = String(task.status ?? "");
      const pct = typeof task.progress === "number" ? task.progress : 0;
      lastMsg = typeof task.message === "string" ? task.message : lastMsg;

      const { data: prevRow } = await supabaseServer
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadId)
        .maybeSingle();

      await supabaseServer
        .from("raw_report_uploads")
        .update({
          metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
            worklist_progress: Math.min(100, Math.max(0, pct)),
            etl_phase: "worklist",
          }),
          updated_at: new Date().toISOString(),
        })
        .eq("id", uploadId)
        .eq("organization_id", orgId);

      if (st === "completed") {
        const { data: prevOk } = await supabaseServer
          .from("raw_report_uploads")
          .select("metadata")
          .eq("id", uploadId)
          .maybeSingle();

        await supabaseServer
          .from("raw_report_uploads")
          .update({
            metadata: mergeUploadMetadata((prevOk as { metadata?: unknown } | null)?.metadata, {
              worklist_progress: 100,
              worklist_completed: true,
              etl_phase: "worklist",
              error_message: "",
            }),
            updated_at: new Date().toISOString(),
          })
          .eq("id", uploadId)
          .eq("organization_id", orgId);

        await enrichExpectedPackagesFromShipmentAllocations({
          organizationId: orgId,
          uploadId,
          storeId: resolveImportStoreId(meta),
        });

        await backfillExpectedPackagesShipmentMeta({
          organizationId: orgId,
          uploadId,
        });

        return NextResponse.json({ ok: true, task_id: taskId, message: lastMsg });
      }

      if (st === "failed") {
        const err = lastMsg || "Worklist generation failed.";
        const { data: prevFail } = await supabaseServer
          .from("raw_report_uploads")
          .select("metadata")
          .eq("id", uploadId)
          .maybeSingle();

        await supabaseServer
          .from("raw_report_uploads")
          .update({
            metadata: mergeUploadMetadata((prevFail as { metadata?: unknown } | null)?.metadata, {
              worklist_progress: 0,
              error_message: err,
              etl_phase: "worklist",
            }),
            updated_at: new Date().toISOString(),
          })
          .eq("id", uploadId)
          .eq("organization_id", orgId);

        return NextResponse.json({ ok: false, error: err }, { status: 500 });
      }
    }

    return NextResponse.json(
      { ok: false, error: "Worklist task timed out waiting for FastAPI." },
      { status: 504 },
    );
  } catch (e) {
    const message = e instanceof Error ? e.message : "Generate worklist failed.";
    console.error("[generate-worklist]", message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
