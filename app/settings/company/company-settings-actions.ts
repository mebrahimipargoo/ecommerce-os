"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { getSessionUserIdFromCookies } from "../../../lib/supabase-server-auth";
import { resolveEffectiveCompanyOrganizationId } from "../../../lib/resolve-effective-company-organization";
import {
  canEditTenantOrganizationBrandingByRoleKey,
  normalizeRoleKeyForBranding,
} from "../../../lib/tenant-branding-permissions";
import { isUuidString } from "../../../lib/uuid";

type AccessDenied = "not_authenticated" | "forbidden";

export type CompanySettingsView = {
  accessDenied: AccessDenied | null;
  organizationId: string | null;
  companyDisplayName: string;
  fallbackOrganizationName: string;
  logoUrl: string;
};

type SaveCompanySettingsInput = {
  company_display_name: string;
  logo_url: string | null;
  /** Effective org for workspace-picker roles (super_admin, programmer, system_admin). */
  organization_id?: string | null;
};

export async function getAuthenticatedCompanyActor() {
  const sessionUserId = await getSessionUserIdFromCookies();
  if (!sessionUserId) return null;

  const { data: profile } = await supabaseServer
    .from("profiles")
    .select("id, organization_id, role, role_id")
    .eq("id", sessionUserId)
    .maybeSingle();

  if (!profile) return null;

  let roleKey = "";
  const roleId = typeof profile.role_id === "string" ? profile.role_id.trim() : "";
  if (roleId) {
    const { data: roleRow } = await supabaseServer
      .from("roles")
      .select("key")
      .eq("id", roleId)
      .maybeSingle();
    roleKey = typeof roleRow?.key === "string" ? roleRow.key.trim() : "";
  }
  if (!roleKey) {
    roleKey = typeof profile.role === "string" ? profile.role.trim() : "";
  }
  roleKey = normalizeRoleKeyForBranding(roleKey);

  return {
    id: String(profile.id),
    organizationId:
      typeof profile.organization_id === "string" && profile.organization_id.trim()
        ? profile.organization_id.trim()
        : null,
    roleKey: roleKey.length > 0 ? roleKey : null,
  };
}

function canEditCompanyBranding(roleKey: string | null): boolean {
  return canEditTenantOrganizationBrandingByRoleKey(roleKey);
}

export async function getCompanySettingsAction(
  organizationIdHint?: string | null,
): Promise<CompanySettingsView> {
  const actor = await getAuthenticatedCompanyActor();
  if (!actor || !actor.roleKey) {
    return {
      accessDenied: "not_authenticated",
      organizationId: null,
      companyDisplayName: "",
      fallbackOrganizationName: "",
      logoUrl: "",
    };
  }

  if (!canEditCompanyBranding(actor.roleKey)) {
    return {
      accessDenied: "forbidden",
      organizationId: resolveEffectiveCompanyOrganizationId(actor, organizationIdHint),
      companyDisplayName: "",
      fallbackOrganizationName: "",
      logoUrl: "",
    };
  }

  const effectiveOrgId = resolveEffectiveCompanyOrganizationId(actor, organizationIdHint);
  if (!effectiveOrgId) {
    return {
      accessDenied: "forbidden",
      organizationId: null,
      companyDisplayName: "",
      fallbackOrganizationName: "",
      logoUrl: "",
    };
  }

  const [{ data: settings }, { data: organization }] = await Promise.all([
    supabaseServer
      .from("organization_settings")
      .select("company_display_name, logo_url")
      .eq("organization_id", effectiveOrgId)
      .maybeSingle(),
    supabaseServer
      .from("organizations")
      .select("name")
      .eq("id", effectiveOrgId)
      .maybeSingle(),
  ]);

  const fromSettings =
    typeof settings?.company_display_name === "string" ? settings.company_display_name.trim() : "";
  const orgName =
    typeof organization?.name === "string" ? organization.name.trim() : "";
  const preloadedDisplayName = fromSettings || orgName;

  return {
    accessDenied: null,
    organizationId: effectiveOrgId,
    companyDisplayName: preloadedDisplayName,
    fallbackOrganizationName: orgName,
    logoUrl: typeof settings?.logo_url === "string" ? settings.logo_url.trim() : "",
  };
}

export async function saveCompanySettingsAction(
  input: SaveCompanySettingsInput,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const actor = await getAuthenticatedCompanyActor();
  if (!actor || !actor.roleKey) return { ok: false, error: "You must be signed in." };
  if (!canEditCompanyBranding(actor.roleKey)) {
    return { ok: false, error: "You do not have permission to edit company settings." };
  }

  const effectiveOrgId = resolveEffectiveCompanyOrganizationId(actor, input.organization_id);
  if (!effectiveOrgId) {
    return { ok: false, error: "No organization context is available for this account." };
  }

  const { data: orgRow, error: orgLookupErr } = await supabaseServer
    .from("organizations")
    .select("id")
    .eq("id", effectiveOrgId)
    .maybeSingle();
  if (orgLookupErr || !orgRow) {
    return {
      ok: false,
      error:
        "Your account is not linked to a valid organization. Contact your administrator to be assigned to an organization.",
    };
  }

  const companyDisplayName = String(input.company_display_name ?? "").trim();
  const logoUrl =
    input.logo_url == null ? null : String(input.logo_url).trim() || null;

  const { data: existing } = await supabaseServer
    .from("organization_settings")
    .select(
      "is_ai_label_ocr_enabled, is_ai_packing_slip_ocr_enabled, default_claim_evidence",
    )
    .eq("organization_id", effectiveOrgId)
    .maybeSingle();

  const ex = existing as
    | {
        is_ai_label_ocr_enabled?: boolean | null;
        is_ai_packing_slip_ocr_enabled?: boolean | null;
        default_claim_evidence?: Record<string, unknown> | null;
      }
    | null;

  const { error } = await supabaseServer.from("organization_settings").upsert(
    {
      organization_id: effectiveOrgId,
      is_ai_label_ocr_enabled: ex?.is_ai_label_ocr_enabled ?? false,
      is_ai_packing_slip_ocr_enabled: ex?.is_ai_packing_slip_ocr_enabled ?? false,
      default_claim_evidence: ex?.default_claim_evidence ?? {},
      company_display_name: companyDisplayName || null,
      logo_url: logoUrl,
    },
    { onConflict: "organization_id" },
  );

  if (error) return { ok: false, error: error.message };
  return { ok: true };
}
