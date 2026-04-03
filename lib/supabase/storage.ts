import type { MediaUploadFolder, StorageBucketName } from "../media-upload-types";
import { uploadMediaFileAction } from "../media-upload-actions";
import { resolveOrganizationId } from "../organization";

export type UploadFolder = MediaUploadFolder;

function bucketForFolder(folder: MediaUploadFolder): StorageBucketName {
  if (folder === "packages/manifest" || folder === "pallets/manifest") return "manifests";
  return "media";
}

/**
 * Uploads a File via a Server Action (service role) and returns the public URL.
 * Passes `organization_id` so Storage paths and RLS policies can scope by tenant.
 * Packing slips / manifest scans use the `manifests` bucket; all other paths use `media`.
 */
export async function uploadToStorage(
  file: File,
  folder: UploadFolder = "packages",
  organizationId?: string,
): Promise<string> {
  const fd = new FormData();
  fd.append("file", file);
  fd.append("folder", folder);
  fd.append("bucket", bucketForFolder(folder));
  fd.append("organization_id", organizationId ?? resolveOrganizationId());
  const res = await uploadMediaFileAction(fd);
  if (!res.ok) throw new Error(res.error);
  return res.publicUrl;
}

/** Upload evidence images to the `media` bucket (public URLs). */
export async function uploadToMedia(
  file: File,
  folder: UploadFolder = "incident",
  organizationId?: string,
): Promise<string> {
  const fd = new FormData();
  fd.append("file", file);
  fd.append("folder", folder);
  fd.append("bucket", "media");
  fd.append("organization_id", organizationId ?? resolveOrganizationId());
  const res = await uploadMediaFileAction(fd);
  if (!res.ok) throw new Error(res.error);
  return res.publicUrl;
}

/** @deprecated Use {@link uploadToMedia} */
export const uploadToIncidentPhotos = uploadToMedia;
