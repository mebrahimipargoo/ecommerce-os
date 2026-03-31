import type { MediaUploadFolder } from "../media-upload-types";
import { uploadMediaFileAction } from "../media-upload-actions";
import { resolveOrganizationId } from "../organization";

export type UploadFolder = MediaUploadFolder;

/**
 * Uploads a File via a Server Action (service role) and returns the public URL.
 * Passes `organization_id` so Storage paths and RLS policies can scope by tenant.
 */
export async function uploadToStorage(
  file: File,
  folder: UploadFolder = "packages",
  organizationId?: string,
): Promise<string> {
  const fd = new FormData();
  fd.append("file", file);
  fd.append("folder", folder);
  fd.append("organization_id", organizationId ?? resolveOrganizationId());
  const res = await uploadMediaFileAction(fd);
  if (!res.ok) throw new Error(res.error);
  return res.publicUrl;
}

/** Upload to the `incident-photos` bucket (public URLs). */
export async function uploadToIncidentPhotos(
  file: File,
  folder: UploadFolder = "incident",
  organizationId?: string,
): Promise<string> {
  const fd = new FormData();
  fd.append("file", file);
  fd.append("folder", folder);
  fd.append("bucket", "incident-photos");
  fd.append("organization_id", organizationId ?? resolveOrganizationId());
  const res = await uploadMediaFileAction(fd);
  if (!res.ok) throw new Error(res.error);
  return res.publicUrl;
}
