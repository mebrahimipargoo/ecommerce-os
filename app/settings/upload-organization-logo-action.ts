"use server";

import { supabaseServer } from "../../lib/supabase-server";
import {
  canEditTenantOrganizationBrandingByRoleKey,
  normalizeRoleKeyForBranding,
} from "../../lib/tenant-branding-permissions";
import { resolveEffectiveCompanyOrganizationId } from "../../lib/resolve-effective-company-organization";
import { getAuthenticatedCompanyActor } from "./company/company-settings-actions";

const BRANDING_DEBUG =
  process.env.BRANDING_DEBUG === "1" ||
  process.env.NEXT_PUBLIC_BRANDING_DEBUG === "1";
function brandingDlog(...args: unknown[]) {
  if (BRANDING_DEBUG) console.log("[upload-organization-logo]", ...args);
}

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

  const actor = await getAuthenticatedCompanyActor();
  if (!actor || !actor.roleKey) {
    brandingDlog("permission: no actor or roleKey", { hasActor: Boolean(actor) });
    return { ok: false, error: "You must be signed in." };
  }
  if (!canEditTenantOrganizationBrandingByRoleKey(actor.roleKey)) {
    brandingDlog("permission: role denied branding upload", {
      role_raw: actor.roleKey,
      role_normalized: normalizeRoleKeyForBranding(actor.roleKey),
    });
    return { ok: false, error: "You do not have permission to upload a company logo." };
  }

  const requestedOrgRaw = formData.get("organization_id");
  const requestedOrg =
    typeof requestedOrgRaw === "string" ? requestedOrgRaw : null;
  const orgId = resolveEffectiveCompanyOrganizationId(actor, requestedOrg);
  if (!orgId) {
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
    brandingDlog("storage: upload failed", { bucket, message: upErr.message });
    const hint =
      bucket === PRIMARY_LOGO_BUCKET && isBucketMissingError(upErr.message)
        ? ` Create a public Storage bucket named "${PRIMARY_LOGO_BUCKET}" in Supabase, or ensure the "${FALLBACK_BUCKET}" bucket exists.`
        : "";
    return { ok: false, error: `${upErr.message}${hint}` };
  }

  const { data: urlData } = supabaseServer.storage.from(bucket).getPublicUrl(objectPath);
  const publicUrl = urlData.publicUrl;

  const { data: existing } = await supabaseServer
    .from("organization_settings")
    .select(
      "is_ai_label_ocr_enabled, is_ai_packing_slip_ocr_enabled, default_claim_evidence, company_display_name",
    )
    .eq("organization_id", orgId)
    .maybeSingle();

  const ex = existing as
    | {
        is_ai_label_ocr_enabled?: boolean | null;
        is_ai_packing_slip_ocr_enabled?: boolean | null;
        default_claim_evidence?: Record<string, unknown> | null;
        company_display_name?: string | null;
      }
    | null;

  const { error: dbErr } = await supabaseServer.from("organization_settings").upsert(
    {
      organization_id: orgId,
      is_ai_label_ocr_enabled: ex?.is_ai_label_ocr_enabled ?? false,
      is_ai_packing_slip_ocr_enabled: ex?.is_ai_packing_slip_ocr_enabled ?? false,
      default_claim_evidence: ex?.default_claim_evidence ?? {},
      company_display_name: ex?.company_display_name ?? null,
      logo_url: publicUrl,
    },
    { onConflict: "organization_id" },
  );
  if (dbErr) {
    brandingDlog("db: organization_settings upsert failed", dbErr.message);
    return { ok: false, error: dbErr.message };
  }

  brandingDlog("ok", { orgId, publicUrl });
  return { ok: true, publicUrl };
}
