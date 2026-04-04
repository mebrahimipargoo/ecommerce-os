/**
 * POST /api/settings/imports/sync
 *
 * Phase 3 of the 3-phase ETL pipeline.
 *
 * Reads rows from amazon_staging (keyed by upload_id), routes them into the
 * correct amazon_ domain table based on report_type, then deletes the processed
 * staging rows.  Sets status → "synced" on completion.
 *
 * Domain table routing (amazon_ prefix standard):
 *   FBA_RETURNS      → amazon_returns          (upsert on organization_id, lpn)
 *   REMOVAL_ORDER    → amazon_removals         (upsert on organization_id, order_id, sku)
 *   INVENTORY_LEDGER → amazon_inventory_ledger (upsert on organization_id, fnsku, disposition, location, event_type)
 *   REIMBURSEMENTS   → amazon_reimbursements   (upsert on organization_id, reimbursement_id)
 *   SETTLEMENT       → amazon_settlements      (upsert on organization_id, settlement_id)
 *   SAFET_CLAIMS     → amazon_safet_claims     (upsert on organization_id, safet_claim_id)
 *   TRANSACTIONS     → amazon_transactions     (upsert on organization_id, order_id, transaction_type)
 *
 * JSONB Fallback: any CSV column not matched by the typed mapper is stored in
 * the `raw_data` JSONB column — this permanently prevents schema cache crashes.
 *
 * Staging Preservation Rule:
 *   Staging rows are deleted ONLY after their domain batch is successfully upserted.
 *   If any upsert fails, the remaining staging rows are left intact so the user
 *   can fix the issue and retry Phase 3 without re-running Phase 2.
 *
 * Accepts: { upload_id: string }
 * Returns: { ok: true, rowsSynced: number, kind: string }
 */

import { NextResponse } from "next/server";

import {
  applyColumnMappingToRow,
  mapRowToAmazonInventoryLedger,
  mapRowToAmazonReimbursement,
  mapRowToAmazonRemoval,
  mapRowToAmazonReturn,
  mapRowToAmazonSafetClaim,
  mapRowToAmazonSettlement,
  mapRowToAmazonTransaction,
  packPayloadForSupabase,
  NATIVE_COLUMNS_RETURNS,
  NATIVE_COLUMNS_REMOVALS,
  NATIVE_COLUMNS_LEDGER,
  NATIVE_COLUMNS_REIMBURSEMENTS,
  NATIVE_COLUMNS_SETTLEMENTS,
  NATIVE_COLUMNS_SAFET,
  NATIVE_COLUMNS_TRANSACTIONS,
} from "../../../../../lib/import-sync-mappers";
import { mergeUploadMetadata } from "../../../../../lib/raw-report-upload-metadata";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";
export const maxDuration = 300;

const BATCH_SIZE = 500;
const STAGING_READ_BATCH = 1000;
const STAGING_TABLE = "amazon_staging";

type Body = { upload_id?: string };

type SyncKind =
  | "FBA_RETURNS"
  | "REMOVAL_ORDER"
  | "INVENTORY_LEDGER"
  | "REIMBURSEMENTS"
  | "SETTLEMENT"
  | "SAFET_CLAIMS"
  | "TRANSACTIONS"
  | "UNKNOWN";

/** amazon_ domain table for each report kind (null = no table yet / UNKNOWN). */
const DOMAIN_TABLE: Record<SyncKind, string | null> = {
  FBA_RETURNS:      "amazon_returns",
  REMOVAL_ORDER:    "amazon_removals",
  INVENTORY_LEDGER: "amazon_inventory_ledger",
  REIMBURSEMENTS:   "amazon_reimbursements",
  SETTLEMENT:       "amazon_settlements",
  SAFET_CLAIMS:     "amazon_safet_claims",
  TRANSACTIONS:     "amazon_transactions",
  UNKNOWN:          null,
};

/**
 * Upsert conflict key for each domain table.
 * Each value must exactly match a UNIQUE constraint that exists in Postgres.
 *
 * Supabase translates onConflict: "col_a,col_b" →
 *   INSERT … ON CONFLICT (col_a, col_b) DO UPDATE SET …
 *
 * Constraints as of migration 20260502+:
 *   amazon_returns          → (organization_id, lpn)
 *   amazon_removals         → (organization_id, order_id, sku)
 *   amazon_inventory_ledger → (organization_id, fnsku, disposition, location, event_type)
 *   amazon_reimbursements   → (organization_id, reimbursement_id, sku)
 *   amazon_settlements      → (organization_id, settlement_id)
 *   amazon_safet_claims     → (organization_id, safet_claim_id)
 *   amazon_transactions     → (organization_id, order_id, transaction_type, amount)
 */
const CONFLICT_KEY: Record<SyncKind, string | null> = {
  FBA_RETURNS:      "organization_id,lpn",
  REMOVAL_ORDER:    "organization_id,order_id,sku",
  INVENTORY_LEDGER: "organization_id,fnsku,disposition,location,event_type",
  REIMBURSEMENTS:   "organization_id,reimbursement_id,sku", // matches uq_amazon_reimbursements_org_reimb_sku
  SETTLEMENT:       "organization_id,settlement_id",
  SAFET_CLAIMS:     "organization_id,safet_claim_id",
  TRANSACTIONS:     "organization_id,order_id,transaction_type,amount",
  UNKNOWN:          null,
};

/** NATIVE_COLUMNS set for each sync kind — passed to packPayloadForSupabase(). */
const NATIVE_COLUMNS_MAP: Record<SyncKind, Set<string> | null> = {
  FBA_RETURNS:      NATIVE_COLUMNS_RETURNS,
  REMOVAL_ORDER:    NATIVE_COLUMNS_REMOVALS,
  INVENTORY_LEDGER: NATIVE_COLUMNS_LEDGER,
  REIMBURSEMENTS:   NATIVE_COLUMNS_REIMBURSEMENTS,
  SETTLEMENT:       NATIVE_COLUMNS_SETTLEMENTS,
  SAFET_CLAIMS:     NATIVE_COLUMNS_SAFET,
  TRANSACTIONS:     NATIVE_COLUMNS_TRANSACTIONS,
  UNKNOWN:          null,
};

/** Maps raw_report_uploads.report_type → canonical SyncKind. */
function resolveImportKind(reportType: string | null | undefined): SyncKind {
  const rt = String(reportType ?? "").trim();
  if (rt === "FBA_RETURNS" || rt === "fba_customer_returns")   return "FBA_RETURNS";
  if (rt === "REMOVAL_ORDER")                                   return "REMOVAL_ORDER";
  if (rt === "INVENTORY_LEDGER" || rt === "inventory_ledger")  return "INVENTORY_LEDGER";
  if (rt === "REIMBURSEMENTS" || rt === "reimbursements")      return "REIMBURSEMENTS";
  if (rt === "SETTLEMENT" || rt === "settlement_repository")   return "SETTLEMENT";
  if (rt === "SAFET_CLAIMS" || rt === "safe_t_claims")         return "SAFET_CLAIMS";
  if (rt === "TRANSACTIONS" || rt === "transaction_view")      return "TRANSACTIONS";
  return "UNKNOWN";
}

async function audit(
  orgId: string,
  action: string,
  entityId: string,
  detail?: Record<string, unknown>,
): Promise<void> {
  await supabaseServer.from("raw_report_import_audit").insert({
    organization_id: orgId,
    user_profile_id: null,
    action,
    entity_id: entityId,
    detail: detail ?? null,
  });
}

/** Write a "failed" status back to the upload row (best-effort, never throws). */
async function markFailed(uploadId: string, orgId: string, message: string): Promise<void> {
  try {
    const { data: prevRow } = await supabaseServer
      .from("raw_report_uploads")
      .select("metadata")
      .eq("id", uploadId)
      .maybeSingle();

    await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "failed",
        metadata: mergeUploadMetadata(
          (prevRow as { metadata?: unknown } | null)?.metadata,
          { error_message: message, failed_phase: "sync" },
        ),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId);
  } catch (inner) {
    console.error("[sync] markFailed write error:", inner);
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
      .select("id, organization_id, metadata, status, report_type, column_mapping, file_name")
      .eq("id", uploadId)
      .maybeSingle();

    if (fetchErr || !row) {
      return NextResponse.json({ ok: false, error: "Upload session not found." }, { status: 404 });
    }

    orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
    if (!isUuidString(orgId)) {
      return NextResponse.json(
        { ok: false, error: "Invalid upload row (organization_id)." },
        { status: 500 },
      );
    }

    const status = String((row as { status?: unknown }).status ?? "");
    if (status !== "staged" && status !== "failed") {
      return NextResponse.json(
        {
          ok: false,
          error: `Phase 3 (Sync) requires status "staged" (or "failed" for retry). Current status is "${status}".${
            status === "mapped" || status === "ready"
              ? " Run Phase 2 (Process) first."
              : status === "needs_mapping"
                ? ' Use "Map Columns" first, then Process, then Sync.'
                : ""
          }`,
        },
        { status: 409 },
      );
    }

    const kind = resolveImportKind((row as { report_type?: string }).report_type);
    if (kind === "UNKNOWN") {
      return NextResponse.json(
        {
          ok: false,
          error:
            "Cannot sync: report type is not set. " +
            "Open the History table, set the correct report type from the dropdown, then re-run Process and Sync.",
        },
        { status: 422 },
      );
    }

    const hasDomainTable = DOMAIN_TABLE[kind] !== null;

    const columnMapping =
      (row as { column_mapping?: unknown }).column_mapping &&
      typeof (row as { column_mapping?: unknown }).column_mapping === "object" &&
      !Array.isArray((row as { column_mapping?: unknown }).column_mapping)
        ? ((row as { column_mapping?: unknown }).column_mapping as Record<string, string>)
        : null;

    const meta = (row as { metadata?: unknown }).metadata;

    // ── Optimistic lock — prevents concurrent clicks from double-syncing ───────
    const { data: locked, error: lockErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "processing",
        metadata: mergeUploadMetadata(meta, { process_progress: 0, error_message: "" }),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId)
      .in("status", ["staged", "failed"])
      .select("id");

    if (lockErr) {
      return NextResponse.json({ ok: false, error: lockErr.message }, { status: 500 });
    }
    if (!locked || locked.length === 0) {
      return NextResponse.json(
        {
          ok: false,
          error: "Upload is not in a syncable state (another operation may be running).",
        },
        { status: 409 },
      );
    }

    await audit(orgId, "import.sync_started", uploadId, {
      fileName: (row as { file_name?: string }).file_name,
      kind,
      domainTable: DOMAIN_TABLE[kind] ?? "none",
    });

    // ── Phase 3 core: read → map → upsert → delete (strictly sequenced) ───────
    //
    // STAGING PRESERVATION RULE:
    //   Staging rows are deleted only AFTER their corresponding domain batch is
    //   confirmed written.  If flushDomainBatch() throws at any point, the
    //   remaining staging rows are left untouched so the user can retry.
    //
    // Errors propagate immediately — no swallowing, no silent fallbacks.
    let offset = 0;
    let synced = 0;

    while (true) {
      // ── Read a page of staging rows ─────────────────────────────────────────
      const { data: stagingRows, error: readErr } = await supabaseServer
        .from(STAGING_TABLE)
        .select("id, raw_row")
        .eq("upload_id", uploadId)
        .eq("organization_id", orgId)
        .range(offset, offset + STAGING_READ_BATCH - 1);

      if (readErr) throw new Error(`Staging read failed: ${readErr.message}`);
      if (!stagingRows || stagingRows.length === 0) break;

      const domainBatch: Record<string, unknown>[] = [];
      const batchStagingIds: string[] = [];

      for (const sr of stagingRows as { id: string; raw_row: Record<string, string> }[]) {
        if (hasDomainTable) {
          const rawRow = (sr.raw_row ?? {}) as Record<string, string>;
          const mappedRow = applyColumnMappingToRow(rawRow, columnMapping);

          let insertRow: Record<string, unknown> | null = null;

          if (kind === "FBA_RETURNS") {
            insertRow = mapRowToAmazonReturn(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "REMOVAL_ORDER") {
            insertRow = mapRowToAmazonRemoval(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "INVENTORY_LEDGER") {
            insertRow = mapRowToAmazonInventoryLedger(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "REIMBURSEMENTS") {
            insertRow = mapRowToAmazonReimbursement(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "SETTLEMENT") {
            insertRow = mapRowToAmazonSettlement(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "SAFET_CLAIMS") {
            insertRow = mapRowToAmazonSafetClaim(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          } else if (kind === "TRANSACTIONS") {
            insertRow = mapRowToAmazonTransaction(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
          }

          if (insertRow) {
            domainBatch.push(insertRow);
            batchStagingIds.push(sr.id);
          }

          // ── Flush domain batch at BATCH_SIZE ─────────────────────────────
          // On error flushDomainBatch() throws.  The staging IDs for THIS
          // batch are not yet in batchStagingIds_flushed so they are preserved.
          if (domainBatch.length >= BATCH_SIZE) {
            const flushedCount = await flushDomainBatch(kind, domainBatch.splice(0));
            synced += flushedCount;
            // Delete the corresponding staging rows only after confirmed write
            await deleteFromStaging(batchStagingIds.splice(0, flushedCount));
          }
        } else {
          // Recognized kind but no domain table — acknowledge as synced
          synced += 1;
          batchStagingIds.push(sr.id);
        }
      }

      // ── Flush remainder of this staging page ───────────────────────────────
      if (domainBatch.length > 0) {
        const flushedCount = await flushDomainBatch(kind, domainBatch);
        synced += flushedCount;
        await deleteFromStaging(batchStagingIds.splice(0, flushedCount));
      }

      // Acknowledge no-domain rows
      if (batchStagingIds.length > 0) {
        await deleteFromStaging(batchStagingIds);
      }

      offset += stagingRows.length;
      if (stagingRows.length < STAGING_READ_BATCH) break;
    }

    // ── Final safety cleanup: delete any residual staging rows for this upload ─
    // Rows whose mapper returned null (e.g. missing required fields) are never
    // added to domainBatch/batchStagingIds, so they would survive the loop.
    // This single DELETE by upload_id catches all of them without touching
    // rows belonging to other concurrent uploads.
    {
      const { error: cleanupErr } = await supabaseServer
        .from(STAGING_TABLE)
        .delete()
        .eq("upload_id", uploadId)
        .eq("organization_id", orgId);
      if (cleanupErr) {
        console.warn("[sync] final staging cleanup warning:", cleanupErr.message);
        // Non-fatal: domain data is already written; stale staging rows will be
        // cleaned up on the next Process run for this upload.
      }
    }

    // ── Mark upload as synced ─────────────────────────────────────────────────
    const { data: prevRow } = await supabaseServer
      .from("raw_report_uploads")
      .select("metadata")
      .eq("id", uploadId)
      .maybeSingle();

    const { error: markErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "synced",
        metadata: mergeUploadMetadata(
          (prevRow as { metadata?: unknown } | null)?.metadata,
          { row_count: synced, process_progress: 100, error_message: undefined },
        ),
        updated_at: new Date().toISOString(),
      })
      .eq("id", uploadId)
      .eq("organization_id", orgId);

    if (markErr) throw new Error(`Sync succeeded but failed to save status: ${markErr.message}`);

    await audit(orgId, "import.sync_completed", uploadId, {
      rowsSynced: synced,
      kind,
      domainTable: DOMAIN_TABLE[kind] ?? "none",
    });

    return NextResponse.json({ ok: true, rowsSynced: synced, kind });

  } catch (e) {
    const message = e instanceof Error ? e.message : "Sync failed.";
    console.error("[sync] error:", message);

    // ── Write failed status back — staging rows are NOT touched ───────────────
    // Any staging rows not yet deleted remain intact so the user can retry
    // Phase 3 after fixing the underlying issue.
    if (uploadIdForFail && isUuidString(uploadIdForFail) && isUuidString(orgId)) {
      await markFailed(uploadIdForFail, orgId, message);
    }

    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

// =============================================================================
// ── Helpers ───────────────────────────────────────────────────────────────────
// =============================================================================

/**
 * Deletes staging rows by ID in chunks of 200.
 * Only called AFTER the corresponding domain batch has been confirmed written.
 */
async function deleteFromStaging(ids: string[]): Promise<void> {
  if (ids.length === 0) return;
  const chunk_size = 200;
  for (let i = 0; i < ids.length; i += chunk_size) {
    const chunk = ids.slice(i, i + chunk_size);
    const { error } = await supabaseServer
      .from(STAGING_TABLE)
      .delete()
      .in("id", chunk);
    if (error) throw new Error(`Staging cleanup failed: ${error.message}`);
  }
}

/**
 * Packs and upserts a batch of mapped domain rows into the correct amazon_ table.
 *
 *  Step 1 — packPayloadForSupabase():
 *    Any key NOT in NATIVE_COLUMNS_MAP[kind] is redirected into the raw_data
 *    JSONB column.  This is the permanent guard against schema cache errors.
 *
 *  Step 2 — deduplicateByConflictKey():
 *    Removes same-batch duplicates before the Postgres upsert to avoid
 *    "ON CONFLICT DO UPDATE command cannot affect a row a second time".
 *
 *  Step 3 — supabase.upsert({ onConflict }):
 *    Uses the explicit conflict key so Postgres knows which unique index to use.
 *    If the upsert fails for any reason, this function throws — the caller must
 *    NOT delete staging rows and must propagate the error.
 *
 * @returns Number of rows actually written (after deduplication).
 */
async function flushDomainBatch(
  kind: SyncKind,
  rows: Record<string, unknown>[],
): Promise<number> {
  if (rows.length === 0) return 0;

  const table = DOMAIN_TABLE[kind];
  if (!table) return rows.length; // no-op for UNKNOWN

  // ── Step 1: JSONB packing ──────────────────────────────────────────────────
  const nativeCols = NATIVE_COLUMNS_MAP[kind];
  const packed = nativeCols ? packPayloadForSupabase(rows, nativeCols) : rows;

  // ── Step 2: JS-level deduplication (normalised + quantity-summing for ledger)
  const deduped = deduplicateByConflictKey(kind, packed);
  console.log(`[${kind}] Original batch size: ${packed.length}, Cleaned batch size: ${deduped.length}`);

  // ── Step 3: upsert with explicit onConflict ────────────────────────────────
  const conflictKey = CONFLICT_KEY[kind];

  if (conflictKey) {
    const { error } = await supabaseServer
      .from(table)
      .upsert(deduped, { onConflict: conflictKey, ignoreDuplicates: false });

    if (error) {
      throw new Error(
        `[${kind}] upsert into ${table} failed: ${error.message}` +
        ` (conflict key: ${conflictKey}, batch size: ${deduped.length})`,
      );
    }
  } else {
    // No unique key defined — plain insert
    const { error } = await supabaseServer.from(table).insert(deduped);
    if (error) {
      throw new Error(`[${kind}] insert into ${table} failed: ${error.message}`);
    }
  }

  return deduped.length;
}

/**
 * Removes duplicate rows within a batch using the same composite key that
 * Postgres would use for ON CONFLICT.
 *
 * INVENTORY_LEDGER special behaviour:
 *   • All key fields (fnsku, disposition, location, event_type) are normalised
 *     to trimmed-lowercase before comparison — prevents "Sellable" vs "sellable"
 *     from slipping through as two different keys.
 *   • When a duplicate is found, quantities are SUMMED (additive merge) rather
 *     than last-wins, which matches Amazon ledger semantics where the same
 *     FNSKU can appear multiple times in the same export with partial quantities.
 *
 * All other tables use last-occurrence-wins (standard upsert semantics).
 *
 * This prevents the Postgres error:
 *   "ON CONFLICT DO UPDATE command cannot affect row a second time"
 * which fires when the same conflict-key appears more than once in a single
 * INSERT statement — common with Amazon CSVs that contain duplicates.
 */
function deduplicateByConflictKey(
  kind: SyncKind,
  rows: Record<string, unknown>[],
): Record<string, unknown>[] {
  /** Normalise a scalar field value for safe key comparison. */
  const norm = (v: unknown): string =>
    String(v ?? "").trim().toLowerCase();

  const seen = new Map<string, Record<string, unknown>>();

  for (const row of rows) {
    let key: string;
    switch (kind) {
      case "FBA_RETURNS":
        key = `${norm(row.organization_id)}|${norm(row.lpn)}`;
        break;
      case "REMOVAL_ORDER":
        key = `${norm(row.organization_id)}|${norm(row.order_id)}|${norm(row.sku)}`;
        break;
      case "INVENTORY_LEDGER":
        // Normalise every component of the 5-column unique constraint.
        // Case-insensitive comparison prevents "Sellable" / "SELLABLE" split.
        key = [
          norm(row.organization_id),
          norm(row.fnsku),
          norm(row.disposition),
          norm(row.location),
          norm(row.event_type),
        ].join("|");
        break;
      case "REIMBURSEMENTS":
        key = `${norm(row.organization_id)}|${norm(row.reimbursement_id)}|${norm(row.sku)}`;
        break;
      case "SETTLEMENT":
        key = `${norm(row.organization_id)}|${norm(row.settlement_id)}`;
        break;
      case "SAFET_CLAIMS":
        key = `${norm(row.organization_id)}|${norm(row.safet_claim_id)}`;
        break;
      case "TRANSACTIONS":
        key = `${norm(row.organization_id)}|${norm(row.order_id)}|${norm(row.transaction_type)}|${norm(row.amount)}`;
        break;
      default:
        // UNKNOWN — give every row a unique key so nothing is silently dropped
        key = `${norm(row.organization_id)}|__unknown__|${Math.random()}`;
    }

    if (kind === "INVENTORY_LEDGER" && seen.has(key)) {
      // Additive quantity merge: sum quantities from all duplicate rows so no
      // ledger movement is silently discarded.
      const existing = seen.get(key)!;
      const existingQty =
        typeof existing.quantity === "number" ? existing.quantity : 0;
      const incomingQty =
        typeof row.quantity === "number" ? row.quantity : 0;
      seen.set(key, { ...row, quantity: existingQty + incomingQty });
    } else {
      seen.set(key, row);
    }
  }

  return [...seen.values()];
}
