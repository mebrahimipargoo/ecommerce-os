/**
 * POST /api/settings/imports/generic
 *
 * Phase 4 — registry-driven post-sync actions only (never raw landing).
 */

import { NextResponse } from "next/server";

import { syncFinancialReferenceResolverForUpload } from "../../../../../lib/financial-reference-resolver-sync";
import { completeInventoryLedgerProductIdentifierMapPhase } from "../../../../../lib/inventory-ledger-generic-completion";
import { logAmazonImportEngineEvent } from "../../../../../lib/pipeline/amazon-import-engine-log";
import { FPS_KEY_GENERIC, fpsLabelGeneric } from "../../../../../lib/pipeline/file-processing-status-contract";
import {
  DOMAIN_TABLE,
  isListingAmazonSyncKind,
  resolveAmazonImportEngineConfig,
  resolveAmazonImportSyncKind,
} from "../../../../../lib/pipeline/amazon-report-registry";
import { runListingCatalogGenericPhase } from "../../../../../lib/pipeline/listing-import-complete-from-staging";
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
    const engine = resolveAmazonImportEngineConfig(kind);

    if (!engine.supports_generic) {
      return NextResponse.json(
        {
          ok: false,
          error: `Report type "${rt || "UNKNOWN"}" has no Generic phase (registry supports_generic=false).`,
        },
        { status: 422 },
      );
    }

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

    const canRetryGeneric = status === "failed" && failedPhaseRaw === "generic";
    const stuckProcessing = status === "processing" && phase3Done;
    const entryOk = status === "raw_synced" || canRetryGeneric || stuckProcessing;

    if (phase4Done) {
      return NextResponse.json({ ok: true, kind, skipped: true });
    }

    if (!entryOk) {
      return NextResponse.json(
        {
          ok: false,
          error: `Generic phase requires status "raw_synced" (after Sync) or "failed" after a Generic error. Current: "${status}".`,
        },
        { status: 409 },
      );
    }

    if (!phase3Done) {
      return NextResponse.json(
        {
          ok: false,
          error: `Generic requires Phase 3 complete (file_processing_status.phase3_status). Current: "${String(fpsGate?.phase3_status ?? "missing")}". Finish Sync first.`,
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
    } else if (canRetryGeneric) {
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

    if (lockErr) {
      return NextResponse.json({ ok: false, error: lockErr.message }, { status: 500 });
    }
    if (!locked || locked.length === 0) {
      return NextResponse.json(
        { ok: false, error: "Upload is not in a runnable state for Phase 4 (another run may be in progress)." },
        { status: 409 },
      );
    }

    const genericLabel = fpsLabelGeneric(engine.generic_target_table);
    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: uploadId,
        organization_id: orgId,
        status: "processing",
        current_phase: "generic",
        phase_key: FPS_KEY_GENERIC,
        phase_label: genericLabel,
        current_phase_label: genericLabel,
        current_target_table: engine.generic_target_table,
        generic_target_table: engine.generic_target_table,
        phase4_status: "running",
        phase4_started_at: new Date().toISOString(),
        phase4_generic_pct: 0,
      },
      { onConflict: "upload_id" },
    );

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
            phase_key: FPS_KEY_GENERIC,
            phase_label: genericLabel,
            current_phase_label: genericLabel,
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
            phase_key: "complete",
            phase_label: "Complete",
            next_action_key: null,
            next_action_label: null,
            current_phase_label: genericLabel,
            current_target_table: engine.generic_target_table,
            generic_target_table: engine.generic_target_table,
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

        logAmazonImportEngineEvent({
          report_type: rt,
          upload_id: uploadId,
          phase: "complete",
          target_table: engine.generic_target_table,
          generic_rows_written: genericEligibleRows,
        });

        return NextResponse.json({ ok: true, kind: "REMOVAL_SHIPMENT" });
      } finally {
        await releaseRemovalPipelineLock(uploadId);
      }
    }

    if (isListingAmazonSyncKind(kind)) {
      await runListingCatalogGenericPhase({ uploadId, orgId });
      logAmazonImportEngineEvent({
        report_type: rt,
        upload_id: uploadId,
        phase: "complete",
        target_table: "catalog_products",
      });
      return NextResponse.json({ ok: true, kind });
    }

    if (kind === "INVENTORY_LEDGER") {
      const { enriched, mapUpserts } = await completeInventoryLedgerProductIdentifierMapPhase({
        supabase: supabaseServer,
        organizationId: orgId,
        uploadId,
        storeId: importStoreId,
        reportTypeRaw: rt,
        engine,
      });

      return NextResponse.json({
        ok: true,
        kind: "INVENTORY_LEDGER",
        enriched: {
          ...enriched,
          map_upserts: mapUpserts,
          catalog_hits: enriched.ledger_bridge_rows_enriched,
        },
      });
    }

    if (kind === "SETTLEMENT" || kind === "TRANSACTIONS" || kind === "REIMBURSEMENTS") {
      await syncFinancialReferenceResolverForUpload(supabaseServer, orgId, uploadId, kind);

      const domainTable = DOMAIN_TABLE[kind];
      let domainRowCount = 0;
      if (domainTable) {
        const { count } = await supabaseServer
          .from(domainTable)
          .select("*", { count: "exact", head: true })
          .eq("organization_id", orgId)
          .eq("upload_id", uploadId);
        domainRowCount = typeof count === "number" ? count : 0;
      }

      const { data: prevFin } = await supabaseServer
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadId)
        .maybeSingle();

      const mergedFin = mergeUploadMetadata((prevFin as { metadata?: unknown } | null)?.metadata, {
        import_metrics: { current_phase: "complete" },
        etl_phase: "complete",
        error_message: "",
      }) as Record<string, unknown>;
      delete mergedFin.failed_phase;

      await supabaseServer
        .from("raw_report_uploads")
        .update({
          status: "synced",
          import_pipeline_completed_at: new Date().toISOString(),
          metadata: mergedFin,
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
          phase_key: "complete",
          phase_label: "Complete",
          current_phase_label: "Phase 4 complete — financial_reference_resolver",
          current_target_table: "financial_reference_resolver",
          generic_target_table: engine.generic_target_table,
          upload_pct: 100,
          process_pct: 100,
          sync_pct: 100,
          phase1_upload_pct: 100,
          phase2_stage_pct: 100,
          phase3_raw_sync_pct: 100,
          phase4_generic_pct: 100,
          phase4_status: "complete",
          phase4_completed_at: new Date().toISOString(),
          generic_rows_written: domainRowCount,
          error_message: null,
        },
        { onConflict: "upload_id" },
      );

      logAmazonImportEngineEvent({
        report_type: rt,
        upload_id: uploadId,
        phase: "complete",
        target_table: "financial_reference_resolver",
        rows_processed: domainRowCount,
      });

      return NextResponse.json({ ok: true, kind });
    }

    return NextResponse.json(
      { ok: false, error: `No Phase 4 handler for report type "${rt || "UNKNOWN"}" (registry mismatch).` },
      { status: 500 },
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
