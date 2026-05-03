/**
 * One-off: run Phase 4 (Generic) for a SETTLEMENT upload — same work as
 * POST /api/settings/imports/generic for financial types (resolver + status).
 *
 *   npx tsx scripts/run-settlement-generic-once.ts
 *   npx tsx scripts/run-settlement-generic-once.ts --upload-id=<uuid>
 *   npx tsx scripts/run-settlement-generic-once.ts --upload-id=<uuid> --fps-only
 *   npx tsx scripts/run-settlement-generic-once.ts --upload-id=<uuid> --full-frr-verify
 *
 * Loads `.env.local` (service role). Does not touch settlement sync logic.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { createClient } from "@supabase/supabase-js";

import { syncFinancialReferenceResolverForUpload } from "@/lib/financial-reference-resolver-sync";
import { logAmazonImportEngineEvent } from "@/lib/pipeline/amazon-import-engine-log";
import {
  FPS_KEY_COMPLETE,
  FPS_KEY_SYNC,
  FPS_LABEL_COMPLETE,
  FPS_NEXT_ACTION_LABEL_GENERIC,
  fpsLabelSync,
  fpsNextAfterSync,
  fpsPctPhase3,
} from "@/lib/pipeline/file-processing-status-contract";
import { DOMAIN_TABLE, resolveAmazonImportEngineConfig, resolveAmazonImportSyncKind } from "@/lib/pipeline/amazon-report-registry";
import { mergeUploadMetadata } from "@/lib/raw-report-upload-metadata";

function loadEnvLocal(): void {
  const p = path.join(process.cwd(), ".env.local");
  if (!fs.existsSync(p)) return;
  const text = fs.readFileSync(p, "utf8");
  for (const line of text.split(/\r?\n/)) {
    const m = line.match(/^\s*([^#=]+)=(.*)$/);
    if (!m) continue;
    const key = m[1].trim();
    const val = m[2].trim();
    if (key) process.env[key] = val;
  }
}

function parseArg(name: string): string | null {
  const pre = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(pre));
  return hit ? hit.slice(pre.length).trim() : null;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(`--${name}`);
}

function intOr(v: unknown, fallback: number): number {
  if (typeof v === "number" && Number.isFinite(v)) return Math.floor(v);
  return fallback;
}

async function main(): Promise<void> {
  loadEnvLocal();
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    console.error("Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
    process.exit(1);
  }

  const sb = createClient(url, key, { auth: { persistSession: false } });

  let uploadId = parseArg("upload-id");
  let orgId: string | null = null;

  if (!uploadId) {
    const { data: rows, error } = await sb
      .from("raw_report_uploads")
      .select("id, organization_id, status, updated_at")
      .eq("report_type", "SETTLEMENT")
      .in("status", ["raw_synced", "processing"])
      .order("updated_at", { ascending: false })
      .limit(15);
    if (error) throw new Error(error.message);
    const list = rows ?? [];
    const pick =
      list.find((r) => String((r as { status?: string }).status) === "raw_synced") ??
      list.find((r) => String((r as { status?: string }).status) === "processing");
    if (!pick) {
      console.error("No SETTLEMENT upload in raw_synced/processing found. Pass --upload-id=...");
      process.exit(1);
    }
    uploadId = String((pick as { id: string }).id);
    orgId = String((pick as { organization_id: string }).organization_id);
    console.log("Using latest candidate upload:", uploadId, "org:", orgId, "status:", (pick as { status?: string }).status);
  }

  if (!uploadId) {
    console.error("No upload_id");
    process.exit(1);
  }

  const { data: row, error: fetchErr } = await sb
    .from("raw_report_uploads")
    .select("id, organization_id, status, report_type, metadata")
    .eq("id", uploadId)
    .maybeSingle();
  if (fetchErr || !row) throw new Error(fetchErr?.message ?? "upload not found");

  orgId = String((row as { organization_id: string }).organization_id);
  const status = String((row as { status?: string }).status ?? "");
  const rt = String((row as { report_type?: string }).report_type ?? "").trim();
  const kind = resolveAmazonImportSyncKind(rt);
  const engine = resolveAmazonImportEngineConfig(kind);

  if (kind !== "SETTLEMENT") {
    console.error("Upload is not SETTLEMENT:", rt, kind);
    process.exit(1);
  }
  if (!engine.supports_generic) {
    console.error("Registry says supports_generic=false for SETTLEMENT — unexpected.");
    process.exit(1);
  }

  const { data: fps0 } = await sb.from("file_processing_status").select("*").eq("upload_id", uploadId).maybeSingle();
  const fps = (fps0 ?? {}) as Record<string, unknown>;
  const phase3Done = String(fps.phase3_status ?? "").toLowerCase() === "complete";
  const phase4Done = String(fps.phase4_status ?? "").toLowerCase() === "complete";
  const fpsOnly = hasFlag("fps-only");

  if (phase4Done) {
    console.log("Phase 4 already complete — skipping resolver (idempotent).");
  } else if (fpsOnly) {
    if (!phase3Done) {
      console.error("Phase 3 not complete; cannot --fps-only finalize.");
      process.exit(1);
    }
    if (status !== "synced" && status !== "raw_synced") {
      console.error(`--fps-only requires upload status synced or raw_synced (got "${status}").`);
      process.exit(1);
    }

    const domainTable = DOMAIN_TABLE[kind]!;
    const { count: domainRowCount, error: domErr } = await sb
      .from(domainTable)
      .select("id", { count: "exact", head: true })
      .eq("organization_id", orgId)
      .eq("upload_id", uploadId);
    if (domErr) throw new Error(domErr.message);
    const domainRowCountN = typeof domainRowCount === "number" ? domainRowCount : 0;

    const { data: prevFin } = await sb.from("raw_report_uploads").select("metadata").eq("id", uploadId).maybeSingle();
    const mergedFin = mergeUploadMetadata((prevFin as { metadata?: unknown } | null)?.metadata, {
      import_metrics: { current_phase: "complete" },
      etl_phase: "complete",
      error_message: "",
    }) as Record<string, unknown>;
    delete mergedFin.failed_phase;

    const { error: ruErr } = await sb
      .from("raw_report_uploads")
      .update({
        status: "synced",
        import_pipeline_completed_at: new Date().toISOString(),
        metadata: mergedFin,
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId);
    if (ruErr) throw new Error(`raw_report_uploads update: ${ruErr.message}`);

    const priorFpsOnly = (fps0 ?? {}) as Record<string, unknown>;
    const { error: fpsFinErr } = await sb.from("file_processing_status").upsert(
      {
        upload_id: uploadId,
        organization_id: orgId,
        total_rows: intOr(priorFpsOnly.total_rows, domainRowCountN),
        processed_rows: intOr(priorFpsOnly.processed_rows, domainRowCountN),
        status: "complete",
        current_phase: "complete",
        phase_key: FPS_KEY_COMPLETE,
        phase_label: FPS_LABEL_COMPLETE,
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
        generic_rows_written: domainRowCountN,
        error_message: null,
      },
      { onConflict: "upload_id" },
    );
    if (fpsFinErr) throw new Error(`file_processing_status final upsert: ${fpsFinErr.message}`);

    logAmazonImportEngineEvent({
      report_type: rt,
      upload_id: uploadId,
      phase: "complete",
      target_table: "financial_reference_resolver",
      rows_processed: domainRowCountN,
    });
    console.log("--fps-only: metadata + file_processing_status finalized (no FRR re-sync).");
  } else {
    const entryOk =
      status === "raw_synced" ||
      (status === "processing" && phase3Done) ||
      (status === "synced" && !phase4Done);
    if (!phase3Done) {
      console.error("Phase 3 not complete (phase3_status). Finish Sync first. fps:", fps.phase3_status);
      process.exit(1);
    }
    if (!entryOk) {
      console.error(
        `Upload status "${status}" — expected raw_synced, processing (phase3 complete), or synced with Phase 4 incomplete.`,
      );
      process.exit(1);
    }

    // Unstick: FPS left as "syncing" while domain sync is done (upload raw_synced or already marked synced)
    const fpsStatus = String(fps.status ?? "").toLowerCase();
    const fpsPhase = String(fps.current_phase ?? "").toLowerCase();
    if ((status === "raw_synced" || status === "synced") && (fpsStatus === "syncing" || fpsPhase === "sync")) {
      const domainLabel = DOMAIN_TABLE[kind] ?? "none";
      const syncLabelDone = fpsLabelSync(engine.sync_target_table);
      const nextGeneric = fpsNextAfterSync(engine.supports_generic);
      const { count: domCount } = await sb
        .from("amazon_settlements")
        .select("*", { count: "exact", head: true })
        .eq("organization_id", orgId)
        .eq("upload_id", uploadId);
      const rowsEligibleGeneric = typeof domCount === "number" ? domCount : 0;
      const finalRawW =
        typeof fps.raw_rows_written === "number" && Number.isFinite(fps.raw_rows_written)
          ? Math.floor(fps.raw_rows_written)
          : rowsEligibleGeneric;
      const finalRawSkip =
        typeof fps.raw_rows_skipped_existing === "number" && Number.isFinite(fps.raw_rows_skipped_existing)
          ? Math.floor(fps.raw_rows_skipped_existing)
          : 0;
      const tr = typeof fps.total_rows === "number" ? Math.floor(fps.total_rows) : null;
      const denom = tr != null && tr > 0 ? tr : Math.max(1, rowsEligibleGeneric);
      const finalPhase3Pct = fpsPctPhase3(finalRawW, finalRawSkip, denom);

      const { error: stuckFpsErr } = await sb.from("file_processing_status").upsert(
        {
          upload_id: uploadId,
          organization_id: orgId,
          status: "processing",
          current_phase: "raw_synced",
          phase_key: FPS_KEY_SYNC,
          phase_label: syncLabelDone,
          next_action_key: nextGeneric,
          next_action_label: FPS_NEXT_ACTION_LABEL_GENERIC,
          current_phase_label: syncLabelDone,
          stage_target_table: engine.stage_target_table,
          sync_target_table: engine.sync_target_table,
          generic_target_table: engine.generic_target_table,
          current_target_table: domainLabel,
          upload_pct: 100,
          process_pct: 100,
          sync_pct: 100,
          phase1_upload_pct: 100,
          phase2_stage_pct: 100,
          phase3_raw_sync_pct: finalPhase3Pct,
          phase4_generic_pct: 0,
          phase4_status: "pending",
          phase4_completed_at: null,
          phase3_status: "complete",
          raw_rows_written: finalRawW,
          raw_rows_skipped_existing: finalRawSkip,
          rows_eligible_for_generic: rowsEligibleGeneric,
          error_message: null,
        },
        { onConflict: "upload_id" },
      );
      if (stuckFpsErr) throw new Error(`FPS stuck-repair upsert: ${stuckFpsErr.message}`);
      console.log("Repaired stuck FPS (syncing → raw_synced / phase3 complete, generic pending).");
    }

    const { upserted } = await syncFinancialReferenceResolverForUpload(sb, orgId, uploadId, "SETTLEMENT");
    console.log("financial_reference_resolver upserted rows (chunk attempts):", upserted);

    const domainTable = DOMAIN_TABLE[kind]!;
    const { count: domainRowCount, error: domErr2 } = await sb
      .from(domainTable)
      .select("id", { count: "exact", head: true })
      .eq("organization_id", orgId)
      .eq("upload_id", uploadId);
    if (domErr2) throw new Error(domErr2.message);
    const domainRowCountN = typeof domainRowCount === "number" ? domainRowCount : 0;

    const { data: prevFin } = await sb.from("raw_report_uploads").select("metadata").eq("id", uploadId).maybeSingle();
    const mergedFin = mergeUploadMetadata((prevFin as { metadata?: unknown } | null)?.metadata, {
      import_metrics: { current_phase: "complete" },
      etl_phase: "complete",
      error_message: "",
    }) as Record<string, unknown>;
    delete mergedFin.failed_phase;

    const { error: ruErr } = await sb
      .from("raw_report_uploads")
      .update({
        status: "synced",
        import_pipeline_completed_at: new Date().toISOString(),
        metadata: mergedFin,
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId);
    if (ruErr) throw new Error(`raw_report_uploads update: ${ruErr.message}`);

    // Mirror app/api/settings/imports/generic/route.ts SETTLEMENT branch (FPS shape).
    const priorFps2 = (fps0 ?? {}) as Record<string, unknown>;
    const { error: fpsFinErr } = await sb.from("file_processing_status").upsert(
      {
        upload_id: uploadId,
        organization_id: orgId,
        total_rows: intOr(priorFps2.total_rows, domainRowCountN),
        processed_rows: intOr(priorFps2.processed_rows, domainRowCountN),
        status: "complete",
        current_phase: "complete",
        phase_key: FPS_KEY_COMPLETE,
        phase_label: FPS_LABEL_COMPLETE,
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
        generic_rows_written: domainRowCountN,
        error_message: null,
      },
      { onConflict: "upload_id" },
    );
    if (fpsFinErr) throw new Error(`file_processing_status final upsert: ${fpsFinErr.message}`);

    logAmazonImportEngineEvent({
      report_type: rt,
      upload_id: uploadId,
      phase: "complete",
      target_table: "financial_reference_resolver",
      rows_processed: domainRowCountN,
    });
  }

  // Verify
  const { count: st } = await sb
    .from("amazon_staging")
    .select("*", { count: "exact", head: true })
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);
  const { count: se } = await sb
    .from("amazon_settlements")
    .select("id", { count: "exact", head: true })
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);

  const settlementTotal = typeof se === "number" ? se : 0;
  const { data: up2 } = await sb.from("raw_report_uploads").select("status").eq("id", uploadId).single();
  const { data: fps2 } = await sb
    .from("file_processing_status")
    .select("status, current_phase, phase4_status, generic_rows_written")
    .eq("upload_id", uploadId)
    .single();

  const grw = fps2 && typeof (fps2 as { generic_rows_written?: unknown }).generic_rows_written === "number"
    ? Math.floor((fps2 as { generic_rows_written: number }).generic_rows_written)
    : null;

  let frrCount: number | null = null;
  let frrCovers: boolean | null = null;
  if (hasFlag("full-frr-verify")) {
    const SID_PAGE = 1000;
    const IN_CHUNK = 200;
    frrCount = 0;
    let lastSid: string | null = null;
    for (;;) {
      let sidQ = sb
        .from("amazon_settlements")
        .select("id")
        .eq("organization_id", orgId)
        .eq("upload_id", uploadId)
        .order("id", { ascending: true })
        .limit(SID_PAGE);
      if (lastSid) sidQ = sidQ.gt("id", lastSid);
      const { data: sidPage, error: sidErr } = await sidQ;
      if (sidErr) throw new Error(sidErr.message);
      const ids = (sidPage ?? []).map((r) => String((r as { id: string }).id)).filter(Boolean);
      if (ids.length === 0) break;
      lastSid = ids[ids.length - 1] ?? lastSid;
      for (let i = 0; i < ids.length; i += IN_CHUNK) {
        const part = ids.slice(i, i + IN_CHUNK);
        const { count, error } = await sb
          .from("financial_reference_resolver")
          .select("*", { count: "exact", head: true })
          .eq("organization_id", orgId)
          .eq("source_table", "amazon_settlements")
          .in("source_row_id", part);
        if (error) throw new Error(error.message);
        frrCount += typeof count === "number" ? count : 0;
      }
      if (ids.length < SID_PAGE) break;
    }
    frrCovers = frrCount === settlementTotal;
  } else {
    frrCovers = grw != null && grw === settlementTotal && settlementTotal > 0;
  }

  console.log(
    JSON.stringify(
      {
        verify: {
          staging_rows: st ?? 0,
          settlement_rows: settlementTotal,
          frr_rows_for_upload_settlements: frrCount,
          frr_covers_settlements: frrCovers,
          frr_covers_via_fps_generic_rows_written: grw != null && grw === settlementTotal && settlementTotal > 0,
          upload_status: up2?.status,
          fps: fps2,
        },
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
