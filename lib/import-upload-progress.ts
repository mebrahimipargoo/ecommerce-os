import "server-only";

import { resolveOrganizationId } from "./organization";
import { supabaseServer } from "./supabase-server";
import { isUuidString } from "./uuid";

/**
 * Chunk progress: only columns safe for minimal schemas (upload_progress, status, updated_at).
 * Progress % from part index — does not require uploaded_bytes / process_progress columns.
 */
export async function updateUploadAfterChunk(input: {
  uploadId: string;
  partIndex: number;
  totalParts: number;
  bytesUploadedDelta: number;
  totalBytes: number;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  void input.bytesUploadedDelta;
  void input.totalBytes;
  void input.actorUserId;

  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = resolveOrganizationId();
  const { data: row, error: fetchErr } = await supabaseServer
    .from("raw_report_uploads")
    .select("id")
    .eq("id", input.uploadId)
    .eq("organization_id", orgId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };

  const pct = Math.min(
    100,
    Math.max(0, Math.round(((input.partIndex + 1) / Math.max(1, input.totalParts)) * 100)),
  );

  const patch: Record<string, unknown> = {
    upload_progress: pct,
    updated_at: new Date().toISOString(),
    status: "uploading",
  };

  const { error } = await supabaseServer
    .from("raw_report_uploads")
    .update(patch)
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };
  return { ok: true };
}
