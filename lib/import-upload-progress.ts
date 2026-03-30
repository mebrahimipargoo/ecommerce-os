import "server-only";

import { resolveOrganizationId } from "./organization";
import { supabaseServer } from "./supabase-server";
import { isUuidString } from "./uuid";

export async function updateUploadAfterChunk(input: {
  uploadId: string;
  partIndex: number;
  totalParts: number;
  bytesUploadedDelta: number;
  totalBytes: number;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const orgId = resolveOrganizationId();
  const { data: row, error: fetchErr } = await supabaseServer
    .from("raw_report_uploads")
    .select("id, uploaded_bytes, uploaded_by")
    .eq("id", input.uploadId)
    .eq("organization_id", orgId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  // Do not require actor === uploaded_by (service-role uploads; avoids silent finalize/chunk failures).

  const prevUploaded =
    Number((row as { uploaded_bytes?: unknown }).uploaded_bytes ?? 0) || 0;
  const nextBytes = prevUploaded + input.bytesUploadedDelta;
  const byIndex = Math.min(
    100,
    Math.round(((input.partIndex + 1) / Math.max(1, input.totalParts)) * 100),
  );
  const byBytes = Math.min(100, Math.round((nextBytes / Math.max(1, input.totalBytes)) * 100));
  const pctRaw = Math.max(byIndex, byBytes);
  const pct = Math.min(100, Math.max(0, Math.round(pctRaw)));

  const patch: Record<string, unknown> = {
    uploaded_bytes: nextBytes,
    upload_progress: pct,
    process_progress: Math.min(pct, 99),
    updated_at: new Date().toISOString(),
    status: "uploading",
  };

  const actor = input.actorUserId?.trim() ?? "";
  if (!row.uploaded_by && actor && isUuidString(actor)) {
    patch.uploaded_by = actor;
  }

  const { error } = await supabaseServer
    .from("raw_report_uploads")
    .update(patch)
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };
  return { ok: true };
}
