"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { upsertOrganizationLogoUrl } from "../../lib/organization-logo";
import { resolveOrganizationId } from "../../lib/organization";
import { isUuidString } from "../../lib/uuid";
import { saveCoreSettings } from "./workspace-settings-actions";

/** Primary bucket (create as public in Supabase → Storage). */
const PRIMARY_LOGO_BUCKET = "logos";
/** Fallback when `logos` is not provisioned yet (project already ships `media`). */
const FALLBACK_BUCKET = "media";

function isBucketMissingError(message: string): boolean {
  const m = message.toLowerCase();
  return (
    m.includes("bucket not found") ||
    m.includes("no such bucket") ||
    (m.includes("not found") && m.includes("bucket")) ||
    m.includes("does not exist")
  );
}

const ALLOWED_TYPES = new Set([
  "image/png",
  "image/jpeg",
  "image/jpg",
  "image/webp",
  "image/gif",
  "image/svg+xml",
]);

/**
 * Uploads a logo via service role (bypasses Storage RLS), stores public URL in
 * organization_settings.logo_url and syncs core_settings.company_logo_url.
 */
export async function uploadOrganizationLogoAction(
  formData: FormData,
): Promise<{ ok: true; publicUrl: string } | { ok: false; error: string }> {
  const file = formData.get("file");
  if (!(file instanceof File) || file.size === 0) {
    return { ok: false, error: "No file provided." };
  }
  if (file.size > 5 * 1024 * 1024) {
    return { ok: false, error: "File too large (max 5 MB)." };
  }
  const ct = file.type || "";
  if (ct && !ALLOWED_TYPES.has(ct)) {
    return { ok: false, error: "Unsupported image type. Use PNG, JPEG, WebP, GIF, or SVG." };
  }

  const organizationId = resolveOrganizationId();
  if (!isUuidString(organizationId)) {
    return { ok: false, error: "Invalid organization context." };
  }

  const ext = file.name.split(".").pop()?.toLowerCase() ?? "png";
  const unique = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const pathPrimary = `${organizationId}/${unique}.${ext}`;
  const pathFallback = `logos/${organizationId}/${unique}.${ext}`;

  const buf = Buffer.from(await file.arrayBuffer());

  let bucket = PRIMARY_LOGO_BUCKET;
  let objectPath = pathPrimary;
  let upErr = (
    await supabaseServer.storage.from(PRIMARY_LOGO_BUCKET).upload(pathPrimary, buf, {
      upsert: true,
      contentType: file.type || undefined,
    })
  ).error;

  if (upErr && isBucketMissingError(upErr.message)) {
    bucket = FALLBACK_BUCKET;
    objectPath = pathFallback;
    upErr = (
      await supabaseServer.storage.from(FALLBACK_BUCKET).upload(pathFallback, buf, {
        upsert: true,
        contentType: file.type || undefined,
      })
    ).error;
  }

  if (upErr) {
    const hint =
      bucket === PRIMARY_LOGO_BUCKET && isBucketMissingError(upErr.message)
        ? ` Create a public Storage bucket named "${PRIMARY_LOGO_BUCKET}" in Supabase, or ensure the "${FALLBACK_BUCKET}" bucket exists.`
        : "";
    return { ok: false, error: `${upErr.message}${hint}` };
  }

  const { data: urlData } = supabaseServer.storage.from(bucket).getPublicUrl(objectPath);
  const publicUrl = urlData.publicUrl;

  const orgRes = await upsertOrganizationLogoUrl(publicUrl);
  if (!orgRes.ok) {
    return { ok: false, error: orgRes.error ?? "Failed to save logo_url on organization_settings." };
  }

  const coreRes = await saveCoreSettings({ company_logo_url: publicUrl, logo_url: publicUrl });
  if (!coreRes.ok) {
    return { ok: false, error: coreRes.error ?? "Failed to sync workspace branding." };
  }

  return { ok: true, publicUrl };
}
