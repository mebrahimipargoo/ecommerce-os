"use server";

import {
  canEditTenantOrganizationBranding,
  loadTenantProfile,
  resolveTenantOrganizationId,
  type TenantWriteContext,
} from "../../lib/server-tenant";
import { supabaseServer } from "../../lib/supabase-server";
import { upsertOrganizationLogoUrl } from "../../lib/organization-logo";
import { isUuidString } from "../../lib/uuid";

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
 * `organization_settings.logo_url` only (tenant branding — not `platform_settings` or `core_settings`).
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

  const actorProfileId = String(formData.get("actor_profile_id") ?? "").trim() || null;
  const companyIdRaw = String(formData.get("organization_id") ?? "").trim() || null;
  const tenant: TenantWriteContext = {
    actorProfileId,
    organizationId: companyIdRaw && isUuidString(companyIdRaw) ? companyIdRaw : null,
  };

  if (actorProfileId && isUuidString(actorProfileId)) {
    const profile = await loadTenantProfile(actorProfileId);
    if (!canEditTenantOrganizationBranding(profile)) {
      return { ok: false, error: "You do not have permission to upload a company logo." };
    }
  } else {
    return { ok: false, error: "Missing or invalid session profile." };
  }

  const orgId = await resolveTenantOrganizationId(tenant);
  if (!isUuidString(orgId)) {
    return {
      ok: false,
      error:
        "You must be assigned to an organization to upload a logo. Contact your administrator.",
    };
  }

  // Guard: verify the resolved organization exists in the `organizations` table
  // before any storage or DB writes. This prevents FK constraint violations on
  // `organization_settings.organization_id → organizations(id)`.
  const { data: orgRow } = await supabaseServer
    .from("organizations")
    .select("id")
    .eq("id", orgId)
    .maybeSingle();
  if (!orgRow) {
    return {
      ok: false,
      error:
        "Your account is not linked to a valid organization. Contact your administrator to be assigned to an organization.",
    };
  }

  const ext = file.name.split(".").pop()?.toLowerCase() ?? "png";
  const unique = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const pathPrimary = `${orgId}/${unique}.${ext}`;
  const pathFallback = `logos/${orgId}/${unique}.${ext}`;

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

  const orgRes = await upsertOrganizationLogoUrl(publicUrl, tenant);
  if (!orgRes.ok) {
    return { ok: false, error: orgRes.error ?? "Failed to save logo_url on organization_settings." };
  }

  return { ok: true, publicUrl };
}
