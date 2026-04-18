"use server";

import { supabaseServer } from "../../lib/supabase-server";
import {
  canEditTenantOrganizationBranding,
  loadTenantProfile,
  resolveTenantOrganizationId,
  resolveWriteOrganizationId,
  type TenantWriteContext,
} from "../../lib/server-tenant";
import { getOrganizationLogoUrlFromDb } from "../../lib/organization-logo";
import { normalizeTenantLogoUrl } from "../../lib/tenant-logo-url";
import { isUuidString } from "../../lib/uuid";

/** Runtime diagnostics — enable with `BRANDING_DEBUG=1` or `NEXT_PUBLIC_BRANDING_DEBUG=1` */
const DEBUG =
  process.env.BRANDING_DEBUG === "1" ||
  process.env.NEXT_PUBLIC_BRANDING_DEBUG === "1";
function dlog(...args: unknown[]) {
  if (DEBUG) console.log("[tenant-branding-server]", ...args);
}

export type TenantOrganizationBranding = {
  company_display_name: string;
  logo_url: string;
};

/**
 * Tenant / company branding for the effective organization — sourced only from
 * `organization_settings` (and `organizations.name` as a read fallback when no display name).
 */
export async function getTenantBrandingFromOrganization(
  organizationId?: string | null,
): Promise<TenantOrganizationBranding> {
  const raw = organizationId?.trim() ?? "";
  const oid = raw && isUuidString(raw) ? raw : null;
  dlog("getTenantBrandingFromOrganization", { input: organizationId, resolved: oid });
  if (!oid) {
    return { company_display_name: "", logo_url: "" };
  }

  let company_display_name = "";
  try {
    const { data: osRow } = await supabaseServer
      .from("organization_settings")
      .select("company_display_name")
      .eq("organization_id", oid)
      .maybeSingle();
    const dn = (osRow as { company_display_name?: string | null } | null)?.company_display_name;
    company_display_name = typeof dn === "string" ? dn.trim() : "";
  } catch {
    company_display_name = "";
  }

  if (!company_display_name) {
    try {
      const { data: orgRow } = await supabaseServer
        .from("organizations")
        .select("name")
        .eq("id", oid)
        .maybeSingle();
      const n = (orgRow as { name?: string | null } | null)?.name;
      company_display_name = typeof n === "string" ? n.trim() : "";
    } catch {
      /* keep empty */
    }
  }

  const logoRaw = await getOrganizationLogoUrlFromDb(oid);
  const result = {
    company_display_name,
    logo_url: normalizeTenantLogoUrl(logoRaw),
  };
  dlog("getTenantBrandingFromOrganization → result", result);
  return result;
}

/**
 * Resolves the effective organization from the signed-in profile (and optional client org hint
 * for super_admin), then loads tenant branding. Use this from the client so branding appears
 * without relying on client-only `organizationId` timing.
 */
export async function getTenantBrandingForActor(
  actorProfileId: string | null | undefined,
  organizationIdHint?: string | null,
): Promise<TenantOrganizationBranding> {
  const aid = String(actorProfileId ?? "").trim();
  dlog("getTenantBrandingForActor", { actorProfileId: aid || null, organizationIdHint });
  if (!aid || !isUuidString(aid)) {
    dlog("getTenantBrandingForActor → no actor profile id, returning empty");
    return { company_display_name: "", logo_url: "" };
  }
  const profile = await loadTenantProfile(aid);
  dlog("getTenantBrandingForActor → profile", profile ? {
    id: profile.id,
    role: profile.role,
    role_scope: profile.role_scope,
    organization_id: profile.organization_id,
  } : null);
  const orgId = await resolveWriteOrganizationId(aid, organizationIdHint ?? null);
  dlog("getTenantBrandingForActor → resolved orgId", orgId);
  if (!isUuidString(orgId)) {
    return { company_display_name: "", logo_url: "" };
  }
  return getTenantBrandingFromOrganization(orgId);
}

export type SaveTenantOrganizationBrandingInput = {
  company_display_name: string;
  /** Public URL or empty string to clear the stored logo. */
  logo_url: string | null;
};

/**
 * Persists tenant company name and logo on `organization_settings` only.
 * Allowed for `tenant_admin`, legacy `admin`, `programmer`, and `super_admin`
 * (see {@link canEditTenantOrganizationBranding}).
 *
 * Implemented as a single upsert that preserves unrelated columns when the row
 * already exists, so name + logo always land atomically.
 */
export async function saveTenantOrganizationBranding(
  input: SaveTenantOrganizationBrandingInput,
  tenant: TenantWriteContext | null | undefined,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const aid = String(tenant?.actorProfileId ?? "").trim();
  if (!aid || !isUuidString(aid)) {
    dlog("save → missing/invalid actor profile id", { aid });
    return { ok: false, error: "Missing or invalid session profile." };
  }
  const profile = await loadTenantProfile(aid);
  const allowed = canEditTenantOrganizationBranding(profile);
  dlog("save → permission check", {
    aid,
    role: profile?.role ?? null,
    role_scope: profile?.role_scope ?? null,
    profile_organization_id: profile?.organization_id ?? null,
    canEditTenantOrganizationBranding: allowed,
  });
  if (!allowed) {
    return { ok: false, error: "You do not have permission to edit company branding." };
  }

  const orgId = await resolveTenantOrganizationId(tenant ?? null);
  dlog("save → resolved orgId", { tenantHint: tenant?.organizationId ?? null, orgId });
  if (!isUuidString(orgId)) {
    return { ok: false, error: "No organization context for this save." };
  }

  // Guard: verify organization actually exists (FK constraint on organization_settings).
  const { data: orgRow, error: orgLookupErr } = await supabaseServer
    .from("organizations")
    .select("id")
    .eq("id", orgId)
    .maybeSingle();
  if (orgLookupErr || !orgRow) {
    dlog("save → org row not found", { orgId, orgLookupErr });
    return {
      ok: false,
      error:
        "Your account is not linked to a valid organization. Contact your administrator to be assigned to an organization.",
    };
  }

  const name = String(input.company_display_name ?? "").trim();
  const logoRaw = input.logo_url != null ? String(input.logo_url).trim() : "";
  const logoForDb = logoRaw ? normalizeTenantLogoUrl(logoRaw) : null;

  // Load existing row so we preserve non-branding columns on upsert.
  const { data: existing } = await supabaseServer
    .from("organization_settings")
    .select(
      "is_ai_label_ocr_enabled, is_ai_packing_slip_ocr_enabled, default_claim_evidence",
    )
    .eq("organization_id", orgId)
    .maybeSingle();

  const ex = existing as
    | {
        is_ai_label_ocr_enabled?: boolean | null;
        is_ai_packing_slip_ocr_enabled?: boolean | null;
        default_claim_evidence?: Record<string, unknown> | null;
      }
    | null;

  const row = {
    organization_id: orgId,
    is_ai_label_ocr_enabled: ex?.is_ai_label_ocr_enabled ?? false,
    is_ai_packing_slip_ocr_enabled: ex?.is_ai_packing_slip_ocr_enabled ?? false,
    default_claim_evidence: ex?.default_claim_evidence ?? {},
    logo_url: logoForDb,
    company_display_name: name || null,
  };

  const { error: upsertErr } = await supabaseServer
    .from("organization_settings")
    .upsert(row, { onConflict: "organization_id" });

  if (upsertErr) {
    dlog("save → upsert failed", upsertErr);
    return { ok: false, error: upsertErr.message };
  }

  dlog("save → ok", { orgId, company_display_name: row.company_display_name, logo_url: row.logo_url });
  return { ok: true };
}
