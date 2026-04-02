import "server-only";

import { mergeUploadMetadata } from "./raw-report-upload-metadata";
import { resolveOrganizationId } from "./organization";
import { supabaseServer } from "./supabase-server";
import { isUuidString } from "./uuid";

/**
 * Chunk progress: merge into `metadata` JSONB (upload_progress, uploaded_bytes, total_bytes).
 */
export async function updateUploadAfterChunk(input: {
  uploadId: string;
  partIndex: number;
  totalParts: number;
  bytesUploadedDelta: number;
  totalBytes: number;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  void input.actorUserId;

  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = resolveOrganizationId();
  const { data: row, error: fetchErr } = await supabaseServer
    .from("raw_report_uploads")
    .select("id, metadata")
    .eq("id", input.uploadId)
    .eq("company_id", orgId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };

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
    .eq("company_id", orgId);

  if (error) return { ok: false, error: error.message };
  return { ok: true };
}
