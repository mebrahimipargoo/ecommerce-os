"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import {
  canManagePlatformSettings,
  loadTenantProfile,
} from "../../../lib/server-tenant";
import { isUuidString } from "../../../lib/uuid";

const PRIMARY_LOGO_BUCKET = "logos";
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
 * Uploads a platform logo (service role). Updates only `public.platform_settings.logo_url`.
 */
export async function uploadPlatformLogoAction(
  formData: FormData,
): Promise<{ ok: true; publicUrl: string } | { ok: false; error: string }> {
  const actorProfileId = String(formData.get("actor_profile_id") ?? "").trim();
  if (!actorProfileId || !isUuidString(actorProfileId)) {
    if (process.env.NEXT_PUBLIC_BRANDING_DEBUG === "1" || process.env.BRANDING_DEBUG === "1") {
      console.log("[platform-settings-server] upload → missing/invalid actor profile id", { actorProfileId });
    }
    return { ok: false, error: "Missing or invalid session profile." };
  }
  const profile = await loadTenantProfile(actorProfileId);
  const allowed = canManagePlatformSettings(profile);
  if (process.env.NEXT_PUBLIC_BRANDING_DEBUG === "1" || process.env.BRANDING_DEBUG === "1") {
    console.log("[platform-settings-server] upload → permission check", {
      actorProfileId,
      role: profile?.role ?? null,
      role_scope: profile?.role_scope ?? null,
      canManagePlatformSettings: allowed,
    });
  }
  if (!allowed) {
    return { ok: false, error: "You do not have permission to upload a platform logo." };
  }

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

  const ext = file.name.split(".").pop()?.toLowerCase() ?? "png";
  const unique = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const pathPrimary = `_platform/${unique}.${ext}`;
  const pathFallback = `logos/_platform/${unique}.${ext}`;

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

  const { error: dbErr } = await supabaseServer
    .from("platform_settings")
    .update({ logo_url: publicUrl })
    .eq("id", true);
  if (dbErr) return { ok: false, error: dbErr.message };

  return { ok: true, publicUrl };
}
