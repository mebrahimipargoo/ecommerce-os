"use server";

import { supabaseServer } from "./supabase-server";
import { isUuidString } from "./uuid";
import {
  STORAGE_BUCKETS,
  type MediaUploadFolder,
  type StorageBucketName,
} from "./media-upload-types";

const DEFAULT_BUCKET = "media";

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

  const bucketRaw = formData.get("bucket");
  const bucket: StorageBucketName =
    typeof bucketRaw === "string" && (STORAGE_BUCKETS as readonly string[]).includes(bucketRaw)
      ? (bucketRaw as StorageBucketName)
      : DEFAULT_BUCKET;

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
    .from(bucket)
    .upload(path, buf, {
      upsert: true,
      contentType: file.type || undefined,
    });

  if (error) return { ok: false, error: error.message };

  const { data } = supabaseServer.storage.from(bucket).getPublicUrl(path);
  return { ok: true, publicUrl: data.publicUrl };
}
