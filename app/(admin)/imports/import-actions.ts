"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { getSessionUserIdFromCookies } from "../../../lib/supabase-server-auth";
import { updateUploadAfterChunk as updateUploadProgressAfterChunk } from "../../../lib/import-upload-progress";
import {
  AMAZON_LEDGER_UPLOAD_SOURCE,
  mergeUploadMetadata,
  parseRawReportMetadata,
  type RawReportUploadMetadata,
} from "../../../lib/raw-report-upload-metadata";
import type { RawReportUploadRow } from "../../../lib/raw-report-upload-row";
import { resolveOrganizationId } from "../../../lib/organization";
import { resolveWriteOrganizationId } from "../../../lib/server-tenant";
import type { RawReportType } from "../../../lib/raw-report-types";
import { isUuidString } from "../../../lib/uuid";
import { DB_TABLES, RAW_REPORTS_BUCKET, RAW_REPORT_UPLOADS_SELECT } from "../lib/constants";

/**
 * Inserts use **only** snake_case keys that match PostgREST / Postgres.
 * If you see "Could not find column in schema cache", reload PostgREST:
 *   SQL: `NOTIFY pgrst, 'reload schema';`
 * (or restart the Supabase API / project.)
 */

function serializeColumnMappingJson(
  m: Record<string, string> | null | undefined,
): Record<string, string> | null {
  if (!m || Object.keys(m).length === 0) return null;
  return JSON.parse(JSON.stringify(m)) as Record<string, string>;
}

/**
 * Resolves the actor profile id for import operations.
 *
 * Priority order:
 *   1. Explicit UUID passed from the browser (localStorage-stored profile id).
 *   2. Supabase Auth session cookie — covers the common case where localStorage
 *      `current_user_profile_id` is absent but the user IS authenticated.
 *      This is the key fix for the Ghost History bug: without this step, the
 *      fallback returned the first profile in the default org, causing the list
 *      query to use a different org_id than the one used during insert.
 *   3. First profile in the env-var default org (single-tenant seed fallback).
 *   4. Any profile in the system (last resort).
 */
async function resolveActorForImportAction(explicitUserId?: string | null): Promise<string | null> {
  const raw = explicitUserId?.trim();
  if (raw && isUuidString(raw)) {
    const { data } = await supabaseServer
      .from(DB_TABLES.profiles)
      .select("id")
      .eq("id", raw)
      .maybeSingle();
    if (data?.id) return String(data.id);
  }

  // Cookie-based session fallback (step 2 — critical for Ghost History fix).
  try {
    const sessionUid = await getSessionUserIdFromCookies();
    if (sessionUid && isUuidString(sessionUid)) {
      const { data } = await supabaseServer
        .from(DB_TABLES.profiles)
        .select("id")
        .eq("id", sessionUid)
        .maybeSingle();
      if (data?.id) return String(data.id);
    }
  } catch {
    // Cookies unavailable in some edge-runtime or test contexts — continue.
  }

  const orgId = resolveOrganizationId();
  const { data: firstInOrg } = await supabaseServer
    .from(DB_TABLES.profiles)
    .select("id")
    .eq("organization_id", orgId)
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();
  if (firstInOrg?.id) return String(firstInOrg.id);

  const { data: anyProfile } = await supabaseServer
    .from(DB_TABLES.profiles)
    .select("id")
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();
  return anyProfile?.id ? String(anyProfile.id) : null;
}

export async function getImportLedgerActorUserId(): Promise<
  { ok: true; actorUserId: string } | { ok: false }
> {
  const id = await resolveActorForImportAction(null);
  if (!id) return { ok: false };
  return { ok: true, actorUserId: id };
}

export async function updateUploadAfterChunk(
  input: Parameters<typeof updateUploadProgressAfterChunk>[0],
): ReturnType<typeof updateUploadProgressAfterChunk> {
  return updateUploadProgressAfterChunk(input);
}

async function audit(
  orgId: string,
  userId: string | null,
  action: string,
  entityId: string | null,
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

/** True if any prior upload in this org has the same content fingerprint in `metadata.md5_hash`. */
export async function rawUploadExistsWithMd5Hash(
  md5Hash: string,
  actorUserId?: string | null,
): Promise<{ ok: true; exists: boolean } | { ok: false; error: string }> {
  const userId = await resolveActorForImportAction(actorUserId);
  const orgId = await resolveWriteOrganizationId(userId, null);
  const normalized = md5Hash.trim().toLowerCase();
  if (!/^[a-f0-9]{32}$/.test(normalized)) {
    return { ok: false, error: "Invalid MD5 hash." };
  }

  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .select("id")
      .eq("organization_id", orgId)
      .contains("metadata", { md5_hash: normalized })
      .limit(1)
      .maybeSingle();

    if (error) return { ok: false, error: error.message };
    return { ok: true, exists: !!data };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Lookup failed." };
  }
}

export async function listRawReportUploads(input?: {
  /** Effective tenant scope; super_admin may pass the ledger “Target organization” id. */
  organizationId?: string | null;
  actorUserId?: string | null;
}): Promise<{ ok: true; rows: RawReportUploadRow[] } | { ok: false; error: string }> {
  // Always resolve org from the actor's profile (or cookie) — never from storeId.
  // Passing storeId as the requested org would give a different orgId for super_admins
  // than what was used during insert, causing the Ghost History bug.
  const actorId = await resolveActorForImportAction(input?.actorUserId);
  const orgId = await resolveWriteOrganizationId(actorId, null);

  console.info("[listRawReportUploads] org:", orgId, "| actor:", actorId, "| passed actorUserId:", input?.actorUserId ?? "(none)");

  if (!isUuidString(orgId)) {
    return { ok: false, error: "Could not resolve organization — please sign in again." };
  }

  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .select(RAW_REPORT_UPLOADS_SELECT)
      .eq("organization_id", orgId)
      .order("created_at", { ascending: false })
      .limit(100);

    if (error) return { ok: false, error: error.message };

    const base = (data ?? []) as Record<string, unknown>[];

    const rows: RawReportUploadRow[] = base.map((raw) => {
      const r = raw as Record<string, unknown>;
      const meta = parseRawReportMetadata(r.metadata);
      const metaObj =
        r.metadata && typeof r.metadata === "object" && !Array.isArray(r.metadata)
          ? (r.metadata as Record<string, unknown>)
          : null;
      return {
        id: String(r.id),
        organization_id: String(r.organization_id ?? ""),
        file_name: String(r.file_name ?? ""),
        report_type: String(r.report_type ?? ""),
        storage_prefix: meta.storagePrefix,
        status: String(r.status ?? ""),
        upload_progress: meta.uploadProgress,
        process_progress: meta.processProgress,
        uploaded_bytes: meta.uploadedBytes,
        total_bytes: meta.totalBytes,
        row_count: meta.rowCount,
        column_mapping:
          r.column_mapping && typeof r.column_mapping === "object"
            ? (r.column_mapping as Record<string, string>)
            : null,
        errorMessage: meta.errorMessage,
        metadata: metaObj,
        created_by: (r.created_by as string | null) ?? null,
        created_at: String(r.created_at ?? ""),
        updated_at: String(r.updated_at ?? ""),
        created_by_name: null,
      };
    });

    const uploaderIds = [...new Set(rows.map((row) => row.created_by).filter(Boolean))] as string[];
    if (uploaderIds.length > 0) {
      const { data: profRows, error: profErr } = await supabaseServer
        .from(DB_TABLES.profiles)
        .select("id, full_name")
        .in("id", uploaderIds);
      const fullNameById = new Map<string, string>();
      if (!profErr && profRows) {
        for (const p of profRows as { id?: string; full_name?: string | null }[]) {
          if (p.id) fullNameById.set(String(p.id), String(p.full_name ?? "").trim());
        }
      }

      const { data: authData, error: authErr } =
        await supabaseServer.auth.admin.listUsers({ perPage: 1000 });
      const emailById = new Map<string, string>();
      if (!authErr && authData?.users) {
        for (const u of authData.users) {
          emailById.set(u.id, u.email ?? "");
        }
      }
      for (const row of rows) {
        if (row.created_by) {
          const fromAuth = emailById.get(row.created_by)?.trim();
          const fromProfile = fullNameById.get(row.created_by);
          row.created_by_name = fromAuth || fromProfile || null;
        }
      }
    }

    return { ok: true, rows };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load uploads." };
  }
}

export async function createRawReportUploadSession(input: {
  fileName: string;
  totalBytes: number;
  /** Header classification + user selection — persisted as `raw_report_uploads.report_type`. */
  reportType: RawReportType;
  /** Lowercase hex MD5 of full file content */
  md5Hash: string;
  fileExtension: string;
  fileSizeBytes: number;
  uploadChunksCount: number;
  /** Saved on the same row as the session — no separate mapping write required. */
  columnMapping?: Record<string, string> | null;
  /** Original CSV header row — stored in metadata so the mapping modal can offer dropdowns. */
  csvHeaders?: string[] | null;
  /**
   * Record-First strategy: create the DB row with `uploading` BEFORE sending any chunks.
   * This guarantees the file appears in History the moment the upload starts.
   * Defaults to `"uploading"`.
   */
  initialStatus?: "uploading" | "pending";
  actorUserId?: string | null;
  /**
   * Explicit tenant scope (e.g. the "Target store" the user selected in the UI).
   * Super-admins may pass any org UUID; regular admins are scoped to their own org.
   */
  organizationId?: string | null;
}): Promise<
  { ok: true; id: string; storagePrefix: string } | { ok: false; error: string }
> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  const orgId = await resolveWriteOrganizationId(userId, input.organizationId?.trim() ?? null);
  const storagePrefix = `${orgId}/${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

  /** Plain JSON object for `column_mapping` JSONB (PostgREST). */
  const column_mapping =
    input.columnMapping && Object.keys(input.columnMapping).length > 0
      ? (JSON.parse(JSON.stringify(input.columnMapping)) as Record<string, string>)
      : null;

  const md5 = input.md5Hash.trim().toLowerCase();
  if (!/^[a-f0-9]{32}$/.test(md5)) {
    return { ok: false, error: "Invalid MD5 hash." };
  }

  console.info("[createRawReportUploadSession] inserting org:", orgId, "| actor:", userId, "| file:", input.fileName);

  /** Technical tracking lives only in `metadata` JSONB. */
  const metadata = mergeUploadMetadata(null, {
    total_bytes: input.totalBytes,
    storage_prefix: storagePrefix,
    upload_progress: 0,
    uploaded_bytes: 0,
    process_progress: 0,
    md5_hash: md5,
    file_extension: input.fileExtension,
    file_size_bytes: input.fileSizeBytes,
    upload_chunks_count: input.uploadChunksCount,
    ...(input.csvHeaders && input.csvHeaders.length > 0
      ? { csv_headers: input.csvHeaders }
      : {}),
  });

  const insertRow = {
    organization_id: orgId,
    file_name: input.fileName,
    report_type: input.reportType,
    status: (input.initialStatus ?? "uploading") as string,
    column_mapping,
    metadata,
    ...(userId ? { created_by: userId } : {}),
  };

  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .insert(insertRow)
      .select("id")
      .single();

    if (error || !data?.id) {
      return {
        ok: false,
        error:
          error?.message ??
          "Insert failed. Check Supabase migration for raw_report_uploads (organization_id) and organization_settings.",
      };
    }

    await audit(orgId, userId, "import.session_created", data.id as string, {
      fileName: input.fileName,
      totalBytes: input.totalBytes,
      hasMapping: !!column_mapping,
    });

    return { ok: true, id: data.id as string, storagePrefix };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Create failed." };
  }
}

/**
 * Record-First patch: after the DB row is created with `status='uploading'`, call this once
 * the CSV headers have been read and the classify-headers API has returned a result.
 * Updates `report_type`, `column_mapping`, and persists `csv_headers` in metadata so the
 * mapping modal can offer dropdowns even if the upload fails midway.
 */
export async function updateUploadSessionClassification(input: {
  uploadId: string;
  reportType: RawReportType;
  columnMapping?: Record<string, string> | null;
  csvHeaders?: string[] | null;
  actorUserId?: string | null;
  /** Storage path for the single uploaded file (e.g. `{prefix}/original.csv`). */
  rawFilePath?: string | null;
  /** The stores.id selected by the user — stored in metadata for audit/UI (not used as FK). */
  storeId?: string | null;
  /** Total data rows counted from the CSV during Phase 1. */
  totalRows?: number | null;
  /** Whether the user chose to import the full file (ignore date range). */
  importFullFile?: boolean | null;
  /** ISO date strings for the date filter (applied during Phase 2). */
  startDate?: string | null;
  endDate?: string | null;
  /**
   * Zero-based index of the actual header row detected by findHeaderRowIndex.
   * Saved so Phase 2 (stage route) can pass skipLines to csv-parser and skip
   * metadata preamble rows that Amazon's Reports Repository files prepend.
   */
  headerRowIndex?: number | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = await resolveWriteOrganizationId(userId, null);

  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, metadata")
    .eq("id", input.uploadId)
    .eq("organization_id", orgId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };

  const metaPatch: Record<string, unknown> = { upload_progress: 100 };
  if (input.csvHeaders && input.csvHeaders.length > 0) {
    metaPatch.csv_headers = input.csvHeaders;
    // Stable sorted fingerprint — enables Mapping Memory lookup on future uploads
    metaPatch.headers_fingerprint = input.csvHeaders
      .map((h) => h.trim().toLowerCase())
      .sort()
      .join("|");
  }
  if (input.rawFilePath) {
    metaPatch.raw_file_path = input.rawFilePath;
    metaPatch.total_parts = 1;
    metaPatch.upload_chunks_count = 1;
  }
  if (input.storeId && isUuidString(input.storeId)) {
    metaPatch.ledger_store_id = input.storeId;
  }
  if (typeof input.totalRows === "number" && Number.isFinite(input.totalRows)) {
    metaPatch.total_rows = input.totalRows;
  }
  if (input.importFullFile != null) {
    metaPatch.import_full_file = input.importFullFile;
  }
  if (input.startDate) metaPatch.start_date = input.startDate;
  if (input.endDate) metaPatch.end_date = input.endDate;
  if (typeof input.headerRowIndex === "number" && input.headerRowIndex >= 0) {
    metaPatch.header_row_index = input.headerRowIndex;
  }

  const updateRow: Record<string, unknown> = {
    report_type: input.reportType,
    column_mapping: serializeColumnMappingJson(input.columnMapping ?? null),
    metadata: mergeUploadMetadata((row as { metadata?: unknown }).metadata, metaPatch),
    updated_at: new Date().toISOString(),
  };

  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update(updateRow)
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(orgId, userId, "import.classification_updated", input.uploadId, {
    reportType: input.reportType,
    mappingKeys: input.columnMapping ? Object.keys(input.columnMapping) : [],
  });
  return { ok: true };
}

/**
 * Creates a `raw_report_uploads` row for the ledger uploader so progress/history use `organization_id`
 * (via `resolveWriteOrganizationId`) and metadata upload/process percentages.
 */
export async function createAmazonLedgerUploadSession(input: {
  fileName: string;
  fileSizeBytes: number;
  storagePath: string;
  actorUserId?: string | null;
  /** Tenant scope for `raw_report_uploads.organization_id`. */
  organization_id?: string | null;
  requestedOrganizationId?: string | null;
  /** Target store (`stores.id`); stored in metadata for audit/UI. */
  store_id?: string | null;
}): Promise<{ ok: true; id: string } | { ok: false; error: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  const requested =
    (input.organization_id ?? input.requestedOrganizationId)?.trim() ?? null;
  const orgId = await resolveWriteOrganizationId(userId, requested);
  if (!isUuidString(orgId)) return { ok: false, error: "Invalid tenant scope." };

  const sid = input.store_id?.trim() ?? "";
  const metadata = mergeUploadMetadata(null, {
    total_bytes: input.fileSizeBytes,
    upload_progress: 0,
    process_progress: 0,
    uploaded_bytes: 0,
    ledger_storage_path: input.storagePath,
    ...(sid && isUuidString(sid) ? { ledger_store_id: sid } : {}),
    source: AMAZON_LEDGER_UPLOAD_SOURCE,
    file_extension: "csv",
    file_size_bytes: input.fileSizeBytes,
    upload_chunks_count: 1,
    total_parts: 1,
  });

  const insertRow = {
    organization_id: orgId,
    file_name: input.fileName,
    report_type: "inventory_ledger" as const,
    status: "uploading" as const,
    column_mapping: null as Record<string, string> | null,
    metadata,
    ...(userId ? { created_by: userId } : {}),
  };

  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .insert(insertRow)
      .select("id")
      .single();

    if (error || !data?.id) {
      return {
        ok: false,
        error: error?.message ?? "Insert failed (ledger session).",
      };
    }

    await audit(orgId, userId, "import.ledger_session_created", data.id as string, {
      fileName: input.fileName,
      storagePath: input.storagePath,
    });
    return { ok: true, id: data.id as string };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Create failed." };
  }
}

export async function patchAmazonLedgerUploadSession(input: {
  uploadId: string;
  actorUserId?: string | null;
  organization_id?: string | null;
  requestedOrganizationId?: string | null;
  status?: "uploading" | "pending" | "processing" | "complete" | "synced" | "failed";
  metadataPatch: Record<string, unknown>;
}): Promise<
  { ok: true; upload_progress: number; process_progress: number } | { ok: false; error: string }
> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  const requested =
    (input.organization_id ?? input.requestedOrganizationId)?.trim() ?? null;
  const orgId = await resolveWriteOrganizationId(userId, requested);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, metadata")
    .eq("id", input.uploadId)
    .eq("organization_id", orgId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };

  const prev = (row as { metadata?: unknown }).metadata;
  const merged = mergeUploadMetadata(prev, input.metadataPatch as Partial<RawReportUploadMetadata>);

  const updateRow: Record<string, unknown> = {
    metadata: merged,
    updated_at: new Date().toISOString(),
  };
  if (input.status) updateRow.status = input.status;

  const { error: upErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update(updateRow)
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (upErr) return { ok: false, error: upErr.message };

  const parsed = parseRawReportMetadata(merged);
  return { ok: true, upload_progress: parsed.uploadProgress, process_progress: parsed.processProgress };
}

export async function getAmazonLedgerUploadProgress(input: {
  uploadId: string;
  actorUserId?: string | null;
  organization_id?: string | null;
  requestedOrganizationId?: string | null;
}): Promise<
  | { ok: true; upload_progress: number; process_progress: number; status: string }
  | { ok: false; error: string }
> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  const requested =
    (input.organization_id ?? input.requestedOrganizationId)?.trim() ?? null;
  const orgId = await resolveWriteOrganizationId(userId, requested);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const { data: row, error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("metadata, status")
    .eq("id", input.uploadId)
    .eq("organization_id", orgId)
    .maybeSingle();

  if (error || !row) return { ok: false, error: error?.message ?? "Not found." };
  const parsed = parseRawReportMetadata((row as { metadata?: unknown }).metadata);
  return {
    ok: true,
    upload_progress: parsed.uploadProgress,
    process_progress: parsed.processProgress,
    status: String((row as { status?: unknown }).status ?? ""),
  };
}

export async function finalizeRawReportUpload(input: {
  uploadId: string;
  /** Accurate data row count (excluding header; trailing blanks ignored). */
  rowCount?: number | null;
  columnMapping?: Record<string, string> | null;
  actorUserId?: string | null;
  /**
   * Terminal status after upload completes.
   * `mapped`        = AI mapping complete; Phase 2 (Process) button shows in History.
   * `needs_mapping` = AI could not resolve required fields; user must use Map Columns.
   * `ready`/`pending` = legacy aliases accepted for backward compatibility.
   */
  targetStatus?: "mapped" | "ready" | "pending" | "needs_mapping";
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  // Read org from the stored row so we never rely on the env-var default.
  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id, metadata")
    .eq("id", input.uploadId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Upload row has invalid organization_id." };

  const metadata = mergeUploadMetadata((row as { metadata?: unknown }).metadata, {
    upload_progress: 100,
    process_progress: 0,
    ...(input.rowCount != null ? { row_count: input.rowCount } : {}),
  });

  const finalStatus = input.targetStatus ?? "ready";

  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({
      status: finalStatus,
      // Only overwrite column_mapping when explicitly provided — prevents wiping AI-generated
      // mapping that was saved by updateUploadSessionClassification moments earlier.
      ...(input.columnMapping !== undefined
        ? { column_mapping: serializeColumnMappingJson(input.columnMapping) }
        : {}),
      metadata,
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(orgId, userId, "import.upload_finalized", input.uploadId, {
    rowCount: input.rowCount ?? null,
    targetStatus: finalStatus,
  });
  return { ok: true };
}

/**
 * Saves the user-verified column mapping and transitions the upload from `needs_mapping`
 * back to `pending` so the Sync pipeline can run.
 * Optionally updates `report_type` when the user also corrected the report kind.
 */
export async function approveColumnMapping(input: {
  uploadId: string;
  mapping: Record<string, string>;
  reportType?: RawReportType | null;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = await resolveWriteOrganizationId(userId, null);

  const updateRow: Record<string, unknown> = {
    column_mapping: serializeColumnMappingJson(input.mapping),
    status: "ready",
    updated_at: new Date().toISOString(),
  };
  if (input.reportType) updateRow.report_type = input.reportType;

  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update(updateRow)
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(orgId, userId, "import.mapping_approved", input.uploadId, {
    keys: Object.keys(input.mapping),
    reportType: input.reportType ?? null,
  });
  return { ok: true };
}

export async function failRawReportUpload(input: {
  uploadId: string;
  message: string;
  actorUserId?: string | null;
}): Promise<void> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return;

  const { data: row } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id, metadata")
    .eq("id", input.uploadId)
    .maybeSingle();

  const orgId = String((row as { organization_id?: unknown } | null)?.organization_id ?? "").trim();
  const metadata = mergeUploadMetadata((row as { metadata?: unknown } | null)?.metadata, {
    error_message: input.message,
  });

  if (isUuidString(orgId)) {
    await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .update({
        status: "failed",
        metadata,
        updated_at: new Date().toISOString(),
      })
      .eq("id", input.uploadId)
      .eq("organization_id", orgId);

    await audit(orgId, userId, "import.upload_failed", input.uploadId, { message: input.message });
  }
}

/**
 * Resets a row that is stuck in "processing" status back to a retryable state.
 * Safe to call even if the row is not stuck — it only transitions FROM "processing".
 * "staged" → resets to "mapped" (re-run Process then Sync).
 * Otherwise resets to "mapped".
 */
export async function resetStuckUpload(input: {
  uploadId: string;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id, metadata, status")
    .eq("id", input.uploadId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Invalid organization." };

  const currentStatus = String((row as { status?: unknown }).status ?? "");
  if (currentStatus !== "processing") {
    return { ok: false, error: `Row is not stuck — current status is "${currentStatus}".` };
  }

  const metadata = mergeUploadMetadata((row as { metadata?: unknown }).metadata, {
    error_message: "Reset by user after stuck in processing.",
    process_progress: 0,
  });

  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({ status: "mapped", metadata, updated_at: new Date().toISOString() })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId)
    .eq("status", "processing");

  if (error) return { ok: false, error: error.message };

  await audit(orgId, userId, "import.stuck_reset", input.uploadId, { previousStatus: "processing" });
  return { ok: true };
}

export async function updateRawReportType(input: {
  uploadId: string;
  reportType: RawReportType;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = await resolveWriteOrganizationId(userId, null);
  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({
      report_type: input.reportType,
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(orgId, userId, "import.report_type_updated", input.uploadId, {
    reportType: input.reportType,
  });
  return { ok: true };
}

export async function recordColumnMappingDecision(input: {
  uploadId: string;
  mapping: Record<string, string>;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = await resolveWriteOrganizationId(userId, null);
  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({
      column_mapping: serializeColumnMappingJson(input.mapping),
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(orgId, userId, "import.mapping_applied", input.uploadId, { keys: Object.keys(input.mapping) });
  return { ok: true };
}

async function removeObjectsUnderStoragePrefix(prefix: string): Promise<void> {
  const trimmed = prefix.replace(/\/+$/, "");
  if (!trimmed) return;
  const { data: files, error } = await supabaseServer.storage
    .from(RAW_REPORTS_BUCKET)
    .list(trimmed);
  if (error || !files?.length) return;
  const paths = files.map((f) => `${trimmed}/${f.name}`);
  await supabaseServer.storage.from(RAW_REPORTS_BUCKET).remove(paths);
}

/**
 * Full-cleanup delete: removes the upload row, Storage objects (chunk prefix and/or
 * single-file ledger path), and domain rows keyed by `upload_id`.
 */
export async function deleteRawReportUpload(
  uploadId: string,
  actorUserId?: string | null,
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (!isUuidString(uploadId)) return { ok: false, error: "Invalid upload id." };
  void actorUserId;

  // Fetch by ID only — read org from the row to avoid env-var default org drift.
  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id, metadata")
    .eq("id", uploadId)
    .maybeSingle();
  if (fetchErr || !row) {
    return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  }
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Upload row has invalid organization_id." };

  const metaRaw = (row as { metadata?: unknown }).metadata;
  const metaObj =
    metaRaw && typeof metaRaw === "object" && !Array.isArray(metaRaw)
      ? (metaRaw as Record<string, unknown>)
      : {};

  // ── 1. Storage cleanup ────────────────────────────────────────────────────
  const prefix = parseRawReportMetadata(metaRaw).storagePrefix;
  if (prefix) {
    await removeObjectsUnderStoragePrefix(prefix);
  }
  const ledgerPath =
    typeof metaObj.ledger_storage_path === "string" ? metaObj.ledger_storage_path.trim() : "";
  if (ledgerPath) {
    await supabaseServer.storage.from(RAW_REPORTS_BUCKET).remove([ledgerPath]);
  }

  // ── 2. Domain table cleanup (errors are swallowed — missing column is non-fatal) ──
  // expected_returns (legacy FBA Returns pipeline)
  await supabaseServer
    .from("expected_returns")
    .delete()
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);

  // expected_packages (legacy Removal Order pipeline — kept for old data)
  await supabaseServer
    .from("expected_packages")
    .delete()
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);

  // expected_removals (legacy Removal Order pipeline)
  await supabaseServer
    .from("expected_removals")
    .delete()
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);

  // ── amazon_ domain tables (new amazon_ prefix standard) ──────────────────
  const AMAZON_DOMAIN_TABLES = [
    "amazon_returns",
    "amazon_removals",
    "amazon_inventory_ledger",
    "amazon_reimbursements",
    "amazon_settlements",
    "amazon_safet_claims",
    "amazon_transactions",
  ] as const;

  for (const tbl of AMAZON_DOMAIN_TABLES) {
    await supabaseServer
      .from(tbl)
      .delete()
      .eq("organization_id", orgId)
      .eq("upload_id", uploadId);
  }

  // amazon_staging cleanup (renamed from amazon_ledger_staging)
  await supabaseServer
    .from(DB_TABLES.amazonLedgerStaging)
    .delete()
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);

  // ── 3. Delete the upload row itself ──────────────────────────────────────
  const { error: delErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .delete()
    .eq("id", uploadId)
    .eq("organization_id", orgId);
  if (delErr) return { ok: false, error: delErr.message };

  return { ok: true };
}
