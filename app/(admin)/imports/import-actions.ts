"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { resolveActorProfileId } from "../../../lib/import-actor";
import { updateUploadAfterChunk as updateUploadProgressAfterChunk } from "../../../lib/import-upload-progress";
import { mergeUploadMetadata, parseRawReportMetadata } from "../../../lib/raw-report-upload-metadata";
import { resolveOrganizationId } from "../../../lib/organization";
import { resolveWriteOrganizationId } from "../../../lib/server-tenant";
import type { RawReportType } from "../../../lib/raw-report-types";
import { isUuidString } from "../../../lib/uuid";
import { DB_TABLES } from "../lib/constants";

/** Import `RawReportType` from `lib/raw-report-types` in client code — not re-exported here (avoids bundler/runtime issues with `"use server"`). */

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
 * Strict column list for `raw_report_uploads` (technical tracking only in `metadata` JSONB).
 * Uploader is `created_by` → `profiles.id` (not `uploaded_by`).
 */
const RAW_REPORT_UPLOADS_SELECT =
  "id, company_id, file_name, report_type, status, column_mapping, metadata, created_at, updated_at, created_by";

export async function updateUploadAfterChunk(
  input: Parameters<typeof updateUploadProgressAfterChunk>[0],
): ReturnType<typeof updateUploadProgressAfterChunk> {
  return updateUploadProgressAfterChunk(input);
}

export type RawReportUploadRow = {
  id: string;
  company_id: string;
  file_name: string;
  /** Current or legacy slug from `raw_report_uploads.report_type`. */
  report_type: string;
  /** Parsed from `metadata.storage_prefix`. */
  storage_prefix: string | null;
  status: string;
  /** Parsed from `metadata.upload_progress`. */
  upload_progress: number;
  /** Parsed from `metadata.process_progress`. */
  process_progress: number;
  /** Parsed from `metadata.uploaded_bytes`. */
  uploaded_bytes: number;
  /** Parsed from `metadata.total_bytes`. */
  total_bytes: number;
  row_count: number | null;
  column_mapping: Record<string, string> | null;
  /** Parsed from `metadata.error_message` only. */
  errorMessage: string | null;
  /** Raw JSONB for advanced UI. */
  metadata: Record<string, unknown> | null;
  /** FK to `profiles.id` — who created this upload row. */
  created_by: string | null;
  created_at: string;
  updated_at: string;
  /** Optional client-only label — not resolved from DB joins. */
  created_by_name?: string | null;
};

async function audit(
  userId: string | null,
  action: string,
  entityId: string | null,
  detail?: Record<string, unknown>,
): Promise<void> {
  const orgId = resolveOrganizationId();
  await supabaseServer.from("raw_report_import_audit").insert({
    company_id: orgId,
    user_profile_id: userId,
    action,
    entity_id: entityId,
    detail: detail ?? null,
  });
}

/** True if any prior upload in this org has the same content fingerprint in `metadata.md5_hash`. */
export async function rawUploadExistsWithMd5Hash(
  md5Hash: string,
): Promise<{ ok: true; exists: boolean } | { ok: false; error: string }> {
  const orgId = resolveOrganizationId();
  const normalized = md5Hash.trim().toLowerCase();
  if (!/^[a-f0-9]{32}$/.test(normalized)) {
    return { ok: false, error: "Invalid MD5 hash." };
  }

  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .select("id")
      .eq("company_id", orgId)
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
  /** Effective tenant scope; super_admin may pass the ledger “Target company” id. */
  companyId?: string | null;
  actorUserId?: string | null;
}): Promise<{ ok: true; rows: RawReportUploadRow[] } | { ok: false; error: string }> {
  const actorId = await resolveActorProfileId(input?.actorUserId);
  const orgId = await resolveWriteOrganizationId(actorId, input?.companyId?.trim() || null);
  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .select(RAW_REPORT_UPLOADS_SELECT)
      .eq("company_id", orgId)
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
        company_id: String(r.company_id),
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
  reportType: RawReportType;
  /** Lowercase hex MD5 of full file content */
  md5Hash: string;
  fileExtension: string;
  fileSizeBytes: number;
  uploadChunksCount: number;
  /** Saved on the same row as the session — no separate mapping write required. */
  columnMapping?: Record<string, string> | null;
  actorUserId?: string | null;
}): Promise<
  { ok: true; id: string; storagePrefix: string } | { ok: false; error: string }
> {
  const userId = await resolveActorProfileId(input.actorUserId);
  const orgId = resolveOrganizationId();
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
  });

  const insertRow = {
    company_id: orgId,
    file_name: input.fileName,
    report_type: input.reportType,
    status: "pending" as const,
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
          "Insert failed. Check Supabase migration for raw_report_uploads (company_id) and organization_settings.",
      };
    }

    await audit(userId, "import.session_created", data.id as string, {
      fileName: input.fileName,
      totalBytes: input.totalBytes,
      hasMapping: !!column_mapping,
    });

    return { ok: true, id: data.id as string, storagePrefix };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Create failed." };
  }
}

export async function finalizeRawReportUpload(input: {
  uploadId: string;
  /** Accurate data row count (excluding header; trailing blanks ignored). */
  rowCount?: number | null;
  columnMapping?: Record<string, string> | null;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorProfileId(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = resolveOrganizationId();
  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, metadata")
    .eq("id", input.uploadId)
    .eq("company_id", orgId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };

  const metadata = mergeUploadMetadata((row as { metadata?: unknown }).metadata, {
    upload_progress: 100,
    process_progress: 0,
    ...(input.rowCount != null ? { row_count: input.rowCount } : {}),
  });

  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({
      /** Upload finished; Phase 3 processing runs separately via `/api/settings/imports/process`. */
      status: "pending",
      column_mapping: serializeColumnMappingJson(input.columnMapping ?? null),
      metadata,
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("company_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(userId, "import.upload_finalized", input.uploadId, {
    rowCount: input.rowCount ?? null,
  });
  return { ok: true };
}

export async function failRawReportUpload(input: {
  uploadId: string;
  message: string;
  actorUserId?: string | null;
}): Promise<void> {
  const userId = await resolveActorProfileId(input.actorUserId);
  const orgId = resolveOrganizationId();
  if (!isUuidString(input.uploadId)) return;

  const { data: row } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("metadata")
    .eq("id", input.uploadId)
    .eq("company_id", orgId)
    .maybeSingle();

  const metadata = mergeUploadMetadata((row as { metadata?: unknown } | null)?.metadata, {
    error_message: input.message,
  });

  await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({
      status: "failed",
      metadata,
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("company_id", orgId);

  await audit(userId, "import.upload_failed", input.uploadId, { message: input.message });
}

export async function updateRawReportType(input: {
  uploadId: string;
  reportType: RawReportType;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorProfileId(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = resolveOrganizationId();
  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({
      report_type: input.reportType,
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("company_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(userId, "import.report_type_updated", input.uploadId, {
    reportType: input.reportType,
  });
  return { ok: true };
}

export async function recordColumnMappingDecision(input: {
  uploadId: string;
  mapping: Record<string, string>;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorProfileId(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = resolveOrganizationId();
  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({
      column_mapping: serializeColumnMappingJson(input.mapping),
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("company_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(userId, "import.mapping_applied", input.uploadId, { keys: Object.keys(input.mapping) });
  return { ok: true };
}

const RAW_REPORTS_BUCKET = "raw-reports";

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

/** Deletes `raw_report_uploads` row and objects under `metadata.storage_prefix` in Storage. */
export async function deleteRawReportUpload(
  uploadId: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (!isUuidString(uploadId)) return { ok: false, error: "Invalid upload id." };
  const orgId = resolveOrganizationId();
  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("metadata")
    .eq("id", uploadId)
    .eq("company_id", orgId)
    .maybeSingle();
  if (fetchErr || !row) {
    return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  }
  const prefix = parseRawReportMetadata((row as { metadata?: unknown }).metadata).storagePrefix;
  if (prefix) {
    await removeObjectsUnderStoragePrefix(prefix);
  }
  const { error: delErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .delete()
    .eq("id", uploadId)
    .eq("company_id", orgId);
  if (delErr) return { ok: false, error: delErr.message };
  return { ok: true };
}
