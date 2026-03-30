"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { resolveActorUserProfileId } from "../../../lib/import-actor";
import { updateUploadAfterChunk as updateUploadProgressAfterChunk } from "../../../lib/import-upload-progress";
import { resolveOrganizationId } from "../../../lib/organization";
import type { RawReportType } from "../../../lib/raw-report-types";
import { isUuidString } from "../../../lib/uuid";

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

export async function updateUploadAfterChunk(
  input: Parameters<typeof updateUploadProgressAfterChunk>[0],
): ReturnType<typeof updateUploadProgressAfterChunk> {
  return updateUploadProgressAfterChunk(input);
}

export type RawReportUploadRow = {
  id: string;
  organization_id: string;
  file_name: string;
  /** Current or legacy slug from `raw_report_uploads.report_type`. */
  report_type: string;
  storage_prefix: string | null;
  status: string;
  upload_progress: number;
  process_progress: number;
  /** DB column `uploaded_bytes` (cumulative bytes stored). */
  uploaded_bytes: number;
  total_bytes: number;
  row_count: number | null;
  column_mapping: Record<string, string> | null;
  /** DB column `error_log` (import failures). */
  error_log: string | null;
  uploaded_by: string | null;
  created_at: string;
  updated_at: string;
  uploaded_by_name?: string | null;
};

async function audit(
  userId: string | null,
  action: string,
  entityId: string | null,
  detail?: Record<string, unknown>,
): Promise<void> {
  const orgId = resolveOrganizationId();
  await supabaseServer.from("raw_report_import_audit").insert({
    organization_id: orgId,
    user_profile_id: userId,
    action,
    entity_id: entityId,
    detail: detail ?? null,
  });
}

export async function listRawReportUploads(): Promise<
  { ok: true; rows: RawReportUploadRow[] } | { ok: false; error: string }
> {
  const orgId = resolveOrganizationId();
  try {
    const { data, error } = await supabaseServer
      .from("raw_report_uploads")
      .select("*")
      .eq("organization_id", orgId)
      .order("created_at", { ascending: false })
      .limit(100);

    if (error) return { ok: false, error: error.message };

    const base = (data ?? []) as Record<string, unknown>[];
    const ids = [...new Set(base.map((r) => r.uploaded_by).filter(Boolean))] as string[];
    let nameById = new Map<string, string>();
    if (ids.length > 0) {
      const { data: profs } = await supabaseServer
        .from("user_profiles")
        .select("id, full_name")
        .in("id", ids);
      nameById = new Map((profs ?? []).map((p) => [p.id as string, p.full_name as string]));
    }

    const rows: RawReportUploadRow[] = base.map((raw) => {
      const r = raw as Record<string, unknown>;
      const uploadedBytes = Number(r.uploaded_bytes ?? r.bytes_uploaded ?? 0);
      return {
        id: String(r.id),
        organization_id: String(r.organization_id),
        file_name: String(r.file_name ?? ""),
        report_type: String(r.report_type ?? ""),
        storage_prefix: (r.storage_prefix as string | null) ?? null,
        status: String(r.status ?? ""),
        upload_progress: Number(r.upload_progress ?? 0),
        process_progress: Number(r.process_progress ?? 0),
        uploaded_bytes: uploadedBytes,
        total_bytes: Number(r.total_bytes ?? 0),
        row_count: r.row_count != null ? Number(r.row_count) : null,
        column_mapping:
          r.column_mapping && typeof r.column_mapping === "object"
            ? (r.column_mapping as Record<string, string>)
            : null,
        error_log:
          (r.error_log as string | null) ??
          (r.error_message as string | null) ??
          null,
        uploaded_by: (r.uploaded_by as string | null) ?? null,
        created_at: String(r.created_at ?? ""),
        updated_at: String(r.updated_at ?? ""),
        uploaded_by_name: r.uploaded_by
          ? nameById.get(String(r.uploaded_by)) ?? null
          : null,
      };
    });

    return { ok: true, rows };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load uploads." };
  }
}

export async function createRawReportUploadSession(input: {
  fileName: string;
  totalBytes: number;
  reportType: RawReportType;
  /** Saved on the same row as the session — no separate mapping write required. */
  columnMapping?: Record<string, string> | null;
  actorUserId?: string | null;
}): Promise<
  { ok: true; id: string; storagePrefix: string } | { ok: false; error: string }
> {
  const userId = await resolveActorUserProfileId(input.actorUserId);
  const orgId = resolveOrganizationId();
  const storagePrefix = `${orgId}/${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

  /** Plain JSON object for `column_mapping` JSONB (PostgREST). */
  const column_mapping =
    input.columnMapping && Object.keys(input.columnMapping).length > 0
      ? (JSON.parse(JSON.stringify(input.columnMapping)) as Record<string, string>)
      : null;

  /**
   * Strict payload: keys are DB column names (snake_case). JS `totalBytes` → `total_bytes`.
   * Only columns verified in Supabase + required row identity fields.
   */
  const insertRow = {
    organization_id: orgId,
    file_name: input.fileName,
    report_type: input.reportType,
    storage_prefix: storagePrefix,
    status: "pending" as const,
    total_bytes: input.totalBytes,
    upload_progress: 0,
    column_mapping,
  };

  try {
    const { data, error } = await supabaseServer
      .from("raw_report_uploads")
      .insert(insertRow)
      .select("id")
      .single();

    if (error || !data?.id) {
      return {
        ok: false,
        error:
          error?.message ??
          "Insert failed. Check Supabase migration for raw_report_uploads and organization_settings.",
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
  rowEstimate?: number | null;
  columnMapping?: Record<string, string> | null;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorUserProfileId(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = resolveOrganizationId();
  const { data: row, error: fetchErr } = await supabaseServer
    .from("raw_report_uploads")
    .select("id")
    .eq("id", input.uploadId)
    .eq("organization_id", orgId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };

  const { error } = await supabaseServer
    .from("raw_report_uploads")
    .update({
      status: "complete",
      upload_progress: 100,
      column_mapping: serializeColumnMappingJson(input.columnMapping ?? null),
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(userId, "import.upload_finalized", input.uploadId, {
    rowEstimate: input.rowEstimate ?? null,
  });
  return { ok: true };
}

export async function failRawReportUpload(input: {
  uploadId: string;
  message: string;
  actorUserId?: string | null;
}): Promise<void> {
  const userId = await resolveActorUserProfileId(input.actorUserId);
  const orgId = resolveOrganizationId();
  if (!isUuidString(input.uploadId)) return;

  await supabaseServer
    .from("raw_report_uploads")
    .update({
      status: "failed",
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  await audit(userId, "import.upload_failed", input.uploadId, { message: input.message });
}

export async function updateRawReportType(input: {
  uploadId: string;
  reportType: RawReportType;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorUserProfileId(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = resolveOrganizationId();
  const { error } = await supabaseServer
    .from("raw_report_uploads")
    .update({
      report_type: input.reportType,
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

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
  const userId = await resolveActorUserProfileId(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = resolveOrganizationId();
  const { error } = await supabaseServer
    .from("raw_report_uploads")
    .update({
      column_mapping: serializeColumnMappingJson(input.mapping),
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(userId, "import.mapping_applied", input.uploadId, { keys: Object.keys(input.mapping) });
  return { ok: true };
}
