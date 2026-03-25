import { supabase } from "../../src/lib/supabase";

const BUCKET = "media";

export type UploadFolder =
  | "packages"
  | "packages/claim_closed"
  | "packages/claim_opened"
  | "packages/claim_return_label"
  | "pallets";

/**
 * Uploads a File to Supabase Storage and returns the public URL.
 * Files are stored under `{folder}/{timestamp}-{random}.{ext}`.
 */
export async function uploadToStorage(
  file: File,
  folder: UploadFolder = "packages",
): Promise<string> {
  const ext = file.name.split(".").pop()?.toLowerCase() ?? "jpg";
  const unique = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const path = `${folder}/${unique}.${ext}`;

  const { error } = await supabase.storage
    .from(BUCKET)
    .upload(path, file, { upsert: true });

  if (error) throw new Error(error.message);

  const { data } = supabase.storage.from(BUCKET).getPublicUrl(path);
  return data.publicUrl;
}
