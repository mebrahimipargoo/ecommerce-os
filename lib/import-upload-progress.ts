import "server-only";

import { mergeUploadMetadata } from "./raw-report-upload-metadata";
import { supabaseServer } from "./supabase-server";
import { isUuidString } from "./uuid";

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
    .select("id, organization_id, metadata")
    .eq("id", input.uploadId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };

  // Use the organization_id that was set when the session was created.
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Upload row has invalid organization_id." };

  const pct = Math.min(
    100,
    Math.max(0, Math.round(((input.partIndex + 1) / Math.max(1, input.totalParts)) * 100)),
  );

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

  const metadata = mergeUploadMetadata(prev, {
    upload_progress: pct,
    uploaded_bytes: uploadedBytes,
    total_bytes: totalBytes,
    last_part_index: input.partIndex,
    total_parts: input.totalParts,
  });

  const { error } = await supabaseServer
    .from("raw_report_uploads")
    .update({
      metadata,
      updated_at: new Date().toISOString(),
      status: "uploading",
    })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  // Mirror progress into the dedicated real-time table so the UI progress bars
  // move in sync without polling raw_report_uploads metadata.
  await supabaseServer.from("file_processing_status").upsert(
    {
      upload_id: input.uploadId,
      organization_id: orgId,
      status: "uploading",
      upload_pct: pct,
      process_pct: 0,
      processed_rows: 0,
    },
    { onConflict: "upload_id" },
  );

  return { ok: true };
}
