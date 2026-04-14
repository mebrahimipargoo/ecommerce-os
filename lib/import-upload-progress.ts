import "server-only";

import { mergeUploadMetadata } from "./raw-report-upload-metadata";
import { supabaseServer } from "./supabase-server";
import { isUuidString } from "./uuid";

/** Upload progress APIs must not overwrite a finalized `raw_report_uploads.status` (e.g. `mapped`) with `uploading`. */
function mustNotDowngradeLifecycleToUploading(statusRaw: unknown): boolean {
  const s = String(statusRaw ?? "")
    .trim()
    .toLowerCase();
  return (
    s === "mapped" ||
    s === "needs_mapping" ||
    s === "ready" ||
    s === "uploaded" ||
    s === "staged" ||
    s === "processing" ||
    s === "synced" ||
    s === "complete" ||
    s === "failed"
  );
}

/**
 * Chunk progress: merge into `metadata` JSONB (upload_progress, uploaded_bytes, total_bytes).
 *
 * Organization resolution: read `organization_id` from the stored row itself instead of
 * relying on the env-var default.  This is safe because the service-role client bypasses
 * RLS, so a plain `eq("id", uploadId)` fetch is sufficient.  Using the row's own org
 * for the subsequent UPDATE ensures the WHERE clause always matches, regardless of which
 * tenant created the session.
 */
export async function updateUploadAfterChunk(input: {
  uploadId: string;
  partIndex: number;
  totalParts: number;
  bytesUploadedDelta: number;
  totalBytes: number;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  // Fetch by primary-key only — service-role bypasses RLS, org filter not needed here.
  const { data: row, error: fetchErr } = await supabaseServer
    .from("raw_report_uploads")
    .select("id, organization_id, metadata, status")
    .eq("id", input.uploadId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };

  // Use the organization_id that was set when the session was created.
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Upload row has invalid organization_id." };

  const lockedLifecycle = mustNotDowngradeLifecycleToUploading((row as { status?: unknown }).status);

  const prev = (row as { metadata?: unknown }).metadata;
  const prevUploaded =
    prev && typeof prev === "object" && "uploaded_bytes" in (prev as object)
      ? Number((prev as { uploaded_bytes?: unknown }).uploaded_bytes ?? 0)
      : 0;
  const uploadedBytes = prevUploaded + input.bytesUploadedDelta;

  const totalBytes =
    Number.isFinite(input.totalBytes) && input.totalBytes > 0
      ? input.totalBytes
      : Math.max(
          (prev && typeof prev === "object" && "total_bytes" in (prev as object)
            ? Number((prev as { total_bytes?: unknown }).total_bytes)
            : 0) || 0,
          uploadedBytes,
        );

  const pctFromParts = Math.min(
    100,
    Math.max(0, Math.round(((input.partIndex + 1) / Math.max(1, input.totalParts)) * 100)),
  );
  const pctFromBytes =
    totalBytes > 0
      ? Math.min(100, Math.max(0, Math.round((uploadedBytes / totalBytes) * 100)))
      : pctFromParts;
  const pct = Math.max(pctFromParts, pctFromBytes);

  const metadata = mergeUploadMetadata(prev, {
    upload_progress: pct,
    uploaded_bytes: uploadedBytes,
    total_bytes: totalBytes,
    last_part_index: input.partIndex,
    total_parts: input.totalParts,
  });

  const rawUpdate: Record<string, unknown> = {
    metadata,
    updated_at: new Date().toISOString(),
  };
  if (!lockedLifecycle) rawUpdate.status = "uploading";

  const { error } = await supabaseServer
    .from("raw_report_uploads")
    .update(rawUpdate)
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  // Mirror progress into the dedicated real-time table so the UI progress bars
  // move in sync without polling raw_report_uploads metadata.
  if (!lockedLifecycle) {
    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: input.uploadId,
        organization_id: orgId,
        status: "uploading",
        current_phase: "upload",
        upload_pct: pct,
        process_pct: 0,
        sync_pct: 0,
        processed_rows: 0,
        file_size_bytes: totalBytes,
        uploaded_bytes: uploadedBytes,
      },
      { onConflict: "upload_id" },
    );
  } else {
    await supabaseServer
      .from("file_processing_status")
      .update({
        upload_pct: pct,
        uploaded_bytes: uploadedBytes,
        file_size_bytes: totalBytes,
      })
      .eq("upload_id", input.uploadId)
      .eq("organization_id", orgId);
  }

  return { ok: true };
}

/**
 * Single-request uploads (XHR PUT to a signed URL): absolute byte counts for real progress bars.
 */
export async function setUploadByteProgress(input: {
  uploadId: string;
  uploadedBytes: number;
  totalBytes: number;
}): Promise<{ ok: boolean; error?: string }> {
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const { data: row, error: fetchErr } = await supabaseServer
    .from("raw_report_uploads")
    .select("id, organization_id, metadata, status")
    .eq("id", input.uploadId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };

  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Upload row has invalid organization_id." };

  const lockedLifecycle = mustNotDowngradeLifecycleToUploading((row as { status?: unknown }).status);

  const prev = (row as { metadata?: unknown }).metadata;
  const totalBytes =
    Number.isFinite(input.totalBytes) && input.totalBytes > 0
      ? Math.floor(input.totalBytes)
      : Math.max(
          (prev && typeof prev === "object" && "total_bytes" in (prev as object)
            ? Number((prev as { total_bytes?: unknown }).total_bytes)
            : 0) || 0,
          Math.floor(input.uploadedBytes),
        );

  const uploadedBytes = Math.min(totalBytes, Math.max(0, Math.floor(input.uploadedBytes)));
  const pct =
    totalBytes > 0 ? Math.min(100, Math.max(0, Math.round((uploadedBytes / totalBytes) * 100))) : 0;

  const metadata = mergeUploadMetadata(prev, {
    upload_progress: pct,
    uploaded_bytes: uploadedBytes,
    total_bytes: totalBytes,
  });

  const rawUpdate: Record<string, unknown> = {
    metadata,
    updated_at: new Date().toISOString(),
  };
  if (!lockedLifecycle) rawUpdate.status = "uploading";

  const { error } = await supabaseServer
    .from("raw_report_uploads")
    .update(rawUpdate)
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  if (!lockedLifecycle) {
    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: input.uploadId,
        organization_id: orgId,
        status: "uploading",
        current_phase: "upload",
        upload_pct: pct,
        process_pct: 0,
        sync_pct: 0,
        processed_rows: 0,
        file_size_bytes: totalBytes,
        uploaded_bytes: uploadedBytes,
      },
      { onConflict: "upload_id" },
    );
  } else {
    await supabaseServer
      .from("file_processing_status")
      .update({
        upload_pct: pct,
        uploaded_bytes: uploadedBytes,
        file_size_bytes: totalBytes,
      })
      .eq("upload_id", input.uploadId)
      .eq("organization_id", orgId);
  }

  return { ok: true };
}
