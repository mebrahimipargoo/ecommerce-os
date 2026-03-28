"use server";

import { supabaseServer } from "./supabase-server";
import { isUuidString } from "./uuid";

const BUCKET = "media";

/** Storage prefixes under the `media` bucket (service-role upload bypasses Storage RLS). */
export type MediaUploadFolder =
  | "packages"
  | "packages/claim_closed"
  | "packages/claim_opened"
  | "packages/claim_return_label"
  | "packages/manifest"
  | "pallets"
  | "pallets/manifest"
  | "pallets/bol"
  | "evidence/wizard";

/**
 * Upload a file from the browser via FormData using the Supabase service role.
 * Use this instead of the anon client so Storage RLS policies do not block inserts.
 */
export async function uploadMediaFileAction(
  formData: FormData,
): Promise<{ ok: true; publicUrl: string } | { ok: false; error: string }> {
  const file = formData.get("file");
  if (!(file instanceof File) || file.size === 0) {
    return { ok: false, error: "No file provided." };
  }
  const folderRaw = formData.get("folder");
  const folder =
    typeof folderRaw === "string" && folderRaw.length > 0
      ? (folderRaw as MediaUploadFolder)
      : "packages";

  const orgRaw = formData.get("organization_id");
  const organizationId =
    typeof orgRaw === "string" && orgRaw.trim().length > 0 && isUuidString(orgRaw.trim())
      ? orgRaw.trim()
      : null;

  const ext = file.name.split(".").pop()?.toLowerCase() ?? "jpg";
  const unique = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  /** Prefix with tenant UUID so Storage RLS policies can scope objects by org. */
  const path = organizationId
    ? `${organizationId}/${folder}/${unique}.${ext}`
    : `${folder}/${unique}.${ext}`;

  const buf = Buffer.from(await file.arrayBuffer());
  const { error } = await supabaseServer.storage
    .from(BUCKET)
    .upload(path, buf, {
      upsert: true,
      contentType: file.type || undefined,
    });

  if (error) return { ok: false, error: error.message };

  const { data } = supabaseServer.storage.from(BUCKET).getPublicUrl(path);
  return { ok: true, publicUrl: data.publicUrl };
}
