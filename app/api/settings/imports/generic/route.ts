/**
 * POST /api/settings/imports/generic
 *
 * Phase 4 — post-raw actions (per report type).
 * Listing imports: Phase 4 merges `amazon_listing_report_rows_raw` → `catalog_products` (after Phase 3 raw_synced).
 */

import { NextResponse } from "next/server";

import { syncListingRawRowsToCatalogProducts } from "../../../../../lib/import-listing-canonical-sync";
import {
  isListingAmazonSyncKind,
  resolveAmazonImportSyncKind,
  type AmazonSyncKind,
} from "../../../../../lib/pipeline/amazon-report-registry";
import { mergeUploadMetadata } from "../../../../../lib/raw-report-upload-metadata";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";
export const maxDuration = 800;

type Body = { upload_id?: string };

function normLower(v: unknown): string {
  return String(v ?? "")
    .trim()
    .toLowerCase();
}

function phase3CompleteDb(v: unknown): boolean {
  return normLower(v) === "complete";
}

function phase4CompleteDb(v: unknown): boolean {
  return normLower(v) === "complete";
}

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

/** Same fingerprint as Phase 3 sync (`raw_report_uploads.metadata.content_sha256`). */
function resolveSourceFileSha256(meta: unknown, uploadId: string): string {
  const m = meta && typeof meta === "object" && !Array.isArray(meta) ? (meta as Record<string, unknown>) : {};
  const s = String(m.content_sha256 ?? "").trim().toLowerCase();
  if (s) return s;
  return `legacy-upload-${uploadId}`;
}

async function acquireRemovalPipelineLock(orgId: string, storeId: string, uploadId: string): Promise<void> {
  await supabaseServer.from("import_pipeline_locks").delete().eq("upload_id", uploadId);
  const { error } = await supabaseServer.from("import_pipeline_locks").insert({
    organization_id: orgId,
    store_id: storeId,
    upload_id: uploadId,
  });
  if (!error) return;
  if (error.code === "23505") {
    const { data } = await supabaseServer
      .from("import_pipeline_locks")
      .select("upload_id")
      .eq("organization_id", orgId)
      .eq("store_id", storeId)
      .maybeSingle();
    const existing = data && typeof data === "object" ? String((data as { upload_id?: string }).upload_id ?? "") : "";
    if (existing === uploadId) return;
    throw new Error(
      "Another removal shipment import is running for this store. Wait for it to finish, then retry.",
    );
  }
  throw new Error(`Removal pipeline lock failed: ${error.message}`);
}

async function releaseRemovalPipelineLock(uploadId: string): Promise<void> {
  await supabaseServer.from("import_pipeline_locks").delete().eq("upload_id", uploadId);
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
      console.warn("[generic] enrich_expected_packages_from_shipment_allocations:", error.message);
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
        expected_rows_enriched_from_allocation: r.expected_rows_enriched_from_allocation,
      }),
    );
  } catch (e) {
    console.warn(
      "[generic] enrich_expected_packages_from_shipment_allocations:",
      e instanceof Error ? e.message : e,
    );
  }
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
      .select("id, organization_id, status, report_type, metadata")
      .eq("id", uploadId)
      .maybeSingle();

    if (fetchErr || !row) {
      return NextResponse.json({ ok: false, error: "Upload session not found." }, { status: 404 });
    }

    orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
    if (!isUuidString(orgId)) {
      return NextResponse.json({ ok: false, error: "Invalid upload row (organization_id)." }, { status: 500 });
    }

    const status = String((row as { status?: unknown }).status ?? "");

    const rt = String((row as { report_type?: string }).report_type ?? "").trim();
    const kind = resolveAmazonImportSyncKind(rt);

    const meta = (row as { metadata?: unknown }).metadata;
    const importStoreId = resolveImportStoreId(meta);
    const failedPhaseRaw =
      meta && typeof meta === "object" && !Array.isArray(meta)
        ? normLower((meta as Record<string, unknown>).failed_phase)
        : "";

    const { data: fpsGate } = await supabaseServer
      .from("file_processing_status")
      .select("phase3_status, phase4_status")
      .eq("upload_id", uploadId)
      .maybeSingle();

    const phase3Done = phase3CompleteDb(fpsGate?.phase3_status);
    const phase4Done = phase4CompleteDb(fpsGate?.phase4_status);
    const catalogListingDone =
      meta && typeof meta === "object" && !Array.isArray(meta)
        ? normLower((meta as Record<string, unknown>).catalog_listing_import_phase) === "done"
        : false;

    if (isListingAmazonSyncKind(kind)) {
      if (!phase3Done) {
        return NextResponse.json(
          {
            ok: false,
            error: `Phase 4 requires Phase 3 complete (phase3_status on file_processing_status). Current: "${String(fpsGate?.phase3_status ?? "missing")}". Finish Sync first.`,
          },
          { status: 409 },
        );
      }
      if (phase4Done && catalogListingDone) {
        return NextResponse.json({ ok: true, kind, skipped: true });
      }
    } else if (kind === "REMOVAL_SHIPMENT") {
      if (!phase3Done) {
        return NextResponse.json(
          {
            ok: false,
            error: `Removal shipment Phase 4 requires Phase 3 complete (file_processing_status.phase3_status). Current: "${String(fpsGate?.phase3_status ?? "missing")}". Finish Sync first.`,
          },
          { status: 409 },
        );
      }
      if (phase4Done) {
        return NextResponse.json({ ok: true, kind: "REMOVAL_SHIPMENT", skipped: true });
      }
    } else if (status !== "raw_synced") {
      return NextResponse.json(
        {
          ok: false,
          error: `Phase 4 requires status "raw_synced" (finish Phase 3 first). Current: "${status}".`,
        },
        { status: 409 },
      );
    }

    const lockMetadata = (() => {
      const merged = mergeUploadMetadata(meta, {
        etl_phase: "generic",
        error_message: "",
      }) as Record<string, unknown>;
      delete merged.failed_phase;
      return merged;
    })();

    const lockUpdate = {
      status: "processing" as const,
      metadata: lockMetadata,
      updated_at: new Date().toISOString(),
    };

    let locked: { id?: string }[] | null = null;
    let lockErr: { message: string } | null = null;

    if (isListingAmazonSyncKind(kind)) {
      const listingLockable =
        status === "raw_synced" || (status === "failed" && failedPhaseRaw === "generic");
      if (!listingLockable) {
        return NextResponse.json(
          {
            ok: false,
            error: `Listing Phase 4 cannot start from status "${status}". Expected raw_synced after Sync, or failed after a Generic attempt.`,
          },
          { status: 409 },
        );
      }
      if (status === "raw_synced") {
        const r = await supabaseServer
          .from("raw_report_uploads")
          .update(lockUpdate)
          .eq("id", uploadId)
          .eq("organization_id", orgId)
          .eq("status", "raw_synced")
          .select("id");
        locked = r.data as { id?: string }[] | null;
        if (r.error) lockErr = r.error;
      }
      if ((!locked || locked.length === 0) && status === "failed" && failedPhaseRaw === "generic") {
        const r = await supabaseServer
          .from("raw_report_uploads")
          .update(lockUpdate)
          .eq("id", uploadId)
          .eq("organization_id", orgId)
          .eq("status", "failed")
          .select("id");
        locked = r.data as { id?: string }[] | null;
        if (r.error) lockErr = r.error;
      }
    } else if (kind === "REMOVAL_SHIPMENT") {
      const shipmentLockable =
        status === "raw_synced" || (status === "failed" && failedPhaseRaw === "generic");
      if (!shipmentLockable) {
        return NextResponse.json(
          {
            ok: false,
            error: `Removal shipment Phase 4 cannot start from status "${status}". Expected raw_synced after Sync, or failed after a Generic attempt.`,
          },
          { status: 409 },
        );
      }
      if (status === "raw_synced") {
        const r = await supabaseServer
          .from("raw_report_uploads")
          .update(lockUpdate)
          .eq("id", uploadId)
          .eq("organization_id", orgId)
          .eq("status", "raw_synced")
          .select("id");
        locked = r.data as { id?: string }[] | null;
        if (r.error) lockErr = r.error;
      }
      if ((!locked || locked.length === 0) && status === "failed" && failedPhaseRaw === "generic") {
        const r = await supabaseServer
          .from("raw_report_uploads")
          .update(lockUpdate)
          .eq("id", uploadId)
          .eq("organization_id", orgId)
          .eq("status", "failed")
          .select("id");
        locked = r.data as { id?: string }[] | null;
        if (r.error) lockErr = r.error;
      }
    } else {
      const r = await supabaseServer
        .from("raw_report_uploads")
        .update(lockUpdate)
        .eq("id", uploadId)
        .eq("organization_id", orgId)
        .eq("status", "raw_synced")
        .select("id");
      locked = r.data as { id?: string }[] | null;
      if (r.error) lockErr = r.error;
    }

    if (lockErr) {
      return NextResponse.json({ ok: false, error: lockErr.message }, { status: 500 });
    }
    if (!locked || locked.length === 0) {
      return NextResponse.json(
        { ok: false, error: "Upload is not in a runnable state for Phase 4 (another run may be in progress)." },
        { status: 409 },
      );
    }

    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: uploadId,
        organization_id: orgId,
        status: "processing",
        current_phase: "generic",
        current_phase_label: "Phase 4 — Generic sync / generate",
        phase4_status: "running",
        phase4_started_at: new Date().toISOString(),
        phase4_generic_pct: 0,
      },
      { onConflict: "upload_id" },
    );

    if (isListingAmazonSyncKind(kind)) {
      const sourceFileSha = resolveSourceFileSha256(meta, uploadId);
      const { count: rawCount } = await supabaseServer
        .from("amazon_listing_report_rows_raw")
        .select("*", { count: "exact", head: true })
        .eq("organization_id", orgId)
        .eq("source_file_sha256", sourceFileSha);
      const storedRawRows = typeof rawCount === "number" ? rawCount : 0;

      const m =
        meta && typeof meta === "object" && !Array.isArray(meta) ? (meta as Record<string, unknown>) : {};
      const fileRowsSeen = Math.max(
        storedRawRows,
        typeof m.catalog_listing_file_rows_seen === "number" ? m.catalog_listing_file_rows_seen : 0,
      );

      const canonicalMetrics = await syncListingRawRowsToCatalogProducts({
        supabase: supabaseServer,
        organizationId: orgId,
        storeId: importStoreId,
        sourceUploadId: uploadId,
        sourceFileSha256: sourceFileSha,
        reportTypeRaw: rt,
        fileRowsSeen,
        storedRawRows,
        progressScale: "full",
        onProgress: async (pct, pass2Done) => {
          await supabaseServer.from("file_processing_status").upsert(
            {
              upload_id: uploadId,
              organization_id: orgId,
              status: "processing",
              current_phase: "generic",
              current_phase_label: "Phase 4 — catalog_products",
              current_target_table: "catalog_products",
              phase4_generic_pct: Math.min(100, Math.max(0, Math.round(pct))),
              generic_rows_written: pass2Done,
              process_pct: pct,
            },
            { onConflict: "upload_id" },
          );
        },
      });

      const { data: prevRowFinal } = await supabaseServer
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadId)
        .maybeSingle();

      await supabaseServer
        .from("raw_report_uploads")
        .update({
          status: "synced",
          import_pipeline_completed_at: new Date().toISOString(),
          metadata: mergeUploadMetadata((prevRowFinal as { metadata?: unknown } | null)?.metadata, {
            process_progress: 100,
            catalog_listing_import_phase: "done",
            catalog_listing_canonical_rows_new: canonicalMetrics.canonical_rows_new,
            catalog_listing_canonical_rows_updated: canonicalMetrics.canonical_rows_updated,
            catalog_listing_canonical_rows_unchanged: canonicalMetrics.canonical_rows_unchanged,
            catalog_listing_canonical_rows_invalid_for_merge: canonicalMetrics.canonical_rows_invalid_for_merge,
            catalog_listing_canonical_rows_inserted: canonicalMetrics.canonical_rows_new,
            catalog_listing_canonical_rows_unchanged_or_merged: canonicalMetrics.canonical_rows_unchanged,
            ...(canonicalMetrics.identifier_map_sync_error
              ? { catalog_listing_identifier_map_sync_error: canonicalMetrics.identifier_map_sync_error }
              : { catalog_listing_identifier_map_sync_error: null }),
            import_metrics: { current_phase: "complete" },
            etl_phase: "complete",
            error_message: "",
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
          current_phase: "complete",
          current_phase_label: "Complete",
          current_target_table: "catalog_products",
          upload_pct: 100,
          process_pct: 100,
          sync_pct: 100,
          phase1_upload_pct: 100,
          phase2_stage_pct: 100,
          phase3_raw_sync_pct: 100,
          phase4_generic_pct: 100,
          phase4_status: "complete",
          phase4_completed_at: new Date().toISOString(),
          generic_rows_written: storedRawRows,
        },
        { onConflict: "upload_id" },
      );

      return NextResponse.json({ ok: true, kind, listing: canonicalMetrics });
    }

    if (kind === "REMOVAL_SHIPMENT") {
      if (!importStoreId) {
        return NextResponse.json(
          { ok: false, error: "Target store is required for removal shipment Phase 4." },
          { status: 422 },
        );
      }

      await acquireRemovalPipelineLock(orgId, importStoreId, uploadId);
      try {
        const { count: eligibleShipCount } = await supabaseServer
          .from("amazon_removal_shipments")
          .select("*", { count: "exact", head: true })
          .eq("upload_id", uploadId)
          .eq("organization_id", orgId);
        const genericEligibleRows = typeof eligibleShipCount === "number" ? eligibleShipCount : 0;

        console.log(
          `[generic][REMOVAL_SHIPMENT] Phase 4 only — shipment tree + expected_packages enrich (not raw sync). ` +
            `upload_id=${uploadId} shipment_lines_eligible=${genericEligibleRows}`,
        );

        await supabaseServer.from("file_processing_status").upsert(
          {
            upload_id: uploadId,
            organization_id: orgId,
            status: "processing",
            current_phase: "generic",
            current_phase_label: "Phase 4 — Shipment tree / expected_packages enrich",
            phase4_generic_pct: 15,
            generic_rows_written: 0,
          },
          { onConflict: "upload_id" },
        );

        const pStoreIdForRpc = isUuidString(importStoreId) ? importStoreId : null;
        const { error: treeErr } = await supabaseServer.rpc("rebuild_shipment_tree_from_removal_shipments", {
          p_organization_id: orgId,
          p_store_id: pStoreIdForRpc,
        });
        if (treeErr) {
          throw new Error(
            `[REMOVAL_SHIPMENT] rebuild_shipment_tree_from_removal_shipments failed: ${treeErr.message}`,
          );
        }
        const { error: allocErr } = await supabaseServer.rpc("rebuild_removal_item_allocations", {
          p_organization_id: orgId,
          p_store_id: pStoreIdForRpc,
        });
        if (allocErr) {
          throw new Error(`[REMOVAL_SHIPMENT] rebuild_removal_item_allocations failed: ${allocErr.message}`);
        }

        await enrichExpectedPackagesFromShipmentAllocations({
          organizationId: orgId,
          uploadId,
          storeId: importStoreId,
        });

        const { data: prevRow } = await supabaseServer
          .from("raw_report_uploads")
          .select("metadata")
          .eq("id", uploadId)
          .maybeSingle();

        const mergedMeta = mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
          import_metrics: { current_phase: "complete" },
          etl_phase: "complete",
          error_message: "",
          removal_shipment_phase4_generic_rows_written: genericEligibleRows,
        }) as Record<string, unknown>;
        delete mergedMeta.failed_phase;

        await supabaseServer
          .from("raw_report_uploads")
          .update({
            status: "synced",
            import_pipeline_completed_at: new Date().toISOString(),
            metadata: mergedMeta,
            updated_at: new Date().toISOString(),
          })
          .eq("id", uploadId)
          .eq("organization_id", orgId);

        await supabaseServer.from("file_processing_status").upsert(
          {
            upload_id: uploadId,
            organization_id: orgId,
            status: "complete",
            current_phase: "complete",
            current_phase_label: "Phase 4 complete — shipment tree / expected_packages enrich",
            current_target_table: "shipment_tree",
            upload_pct: 100,
            process_pct: 100,
            sync_pct: 100,
            phase1_upload_pct: 100,
            phase2_stage_pct: 100,
            phase3_raw_sync_pct: 100,
            phase4_generic_pct: 100,
            phase4_status: "complete",
            phase4_completed_at: new Date().toISOString(),
            generic_rows_written: genericEligibleRows,
            error_message: null,
          },
          { onConflict: "upload_id" },
        );

        return NextResponse.json({ ok: true, kind: "REMOVAL_SHIPMENT" });
      } finally {
        await releaseRemovalPipelineLock(uploadId);
      }
    }

    return NextResponse.json(
      { ok: false, error: `No Phase 4 action is defined for report type "${rt || "UNKNOWN"}".` },
      { status: 422 },
    );
  } catch (e) {
    const message = e instanceof Error ? e.message : "Generic phase failed.";
    console.error("[generic] Phase 4 error:", { upload_id: uploadIdForFail, organization_id: orgId, message });
    if (uploadIdForFail && isUuidString(uploadIdForFail) && isUuidString(orgId)) {
      const { data: prevRow } = await supabaseServer
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadIdForFail)
        .maybeSingle();
      await supabaseServer
        .from("raw_report_uploads")
        .update({
          status: "failed",
          metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
            error_message: message,
            failed_phase: "generic",
          }),
          updated_at: new Date().toISOString(),
        })
        .eq("id", uploadIdForFail)
        .eq("organization_id", orgId);

      await supabaseServer.from("file_processing_status").upsert(
        {
          upload_id: uploadIdForFail,
          organization_id: orgId,
          status: "failed",
          current_phase: "failed",
          error_message: message,
          phase4_status: "failed",
        },
        { onConflict: "upload_id" },
      );
    }
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
