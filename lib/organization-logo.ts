import "server-only";

import { resolveTenantOrganizationId, type TenantWriteContext } from "./server-tenant";
import { supabaseServer } from "./supabase-server";
import { resolveOrganizationId } from "./organization";
import { isUuidString } from "./uuid";

/**
 * Reads tenant logo URL from organization_settings (canonical for UI / PDFs).
 */
export async function getOrganizationLogoUrlFromDb(companyId?: string): Promise<string> {
  const org = companyId?.trim() && companyId.trim().length > 0
    ? companyId.trim()
    : resolveOrganizationId();
  try {
    const { data, error } = await supabaseServer
      .from("organization_settings")
      .select("logo_url")
      .eq("organization_id", org)
      .maybeSingle();
    if (error || !data) return "";
    const u = (data as { logo_url?: string | null }).logo_url;
    return typeof u === "string" ? u.trim() : "";
  } catch {
    return "";
  }
}

/**
 * Persists logo_url on organization_settings, preserving other columns on upsert.
 */
export async function upsertOrganizationLogoUrl(
  logoUrl: string | null,
  tenant?: TenantWriteContext | null,
): Promise<{ ok: boolean; error?: string }> {
  const organizationId = await resolveTenantOrganizationId(tenant);

  if (!isUuidString(organizationId)) {
    return {
      ok: false,
      error: "You must be assigned to an organization to update settings.",
    };
  }

  // Guard: verify the organization actually exists in the `organizations` table
  // before attempting to write to `organization_settings` (which has a FK on it).
  const { data: orgRow, error: orgLookupErr } = await supabaseServer
    .from("organizations")
    .select("id")
    .eq("id", organizationId)
    .maybeSingle();
  if (orgLookupErr || !orgRow) {
    return {
      ok: false,
      error:
        "Your account is not linked to a valid organization. Contact your administrator to be assigned to an organization.",
    };
  }

  try {
    const { data: existing } = await supabaseServer
      .from("organization_settings")
      .select("is_ai_label_ocr_enabled, is_ai_packing_slip_ocr_enabled, default_claim_evidence")
      .eq("organization_id", organizationId)
      .maybeSingle();

    const ex = existing as {
      is_ai_label_ocr_enabled?: boolean;
      is_ai_packing_slip_ocr_enabled?: boolean;
      default_claim_evidence?: Record<string, unknown>;
    } | null;

    const row = {
      organization_id: organizationId,
      is_ai_label_ocr_enabled: ex?.is_ai_label_ocr_enabled ?? false,
      is_ai_packing_slip_ocr_enabled: ex?.is_ai_packing_slip_ocr_enabled ?? false,
      default_claim_evidence: ex?.default_claim_evidence ?? {},
      logo_url: logoUrl?.trim() || null,
    };

    const { error: upsertErr } = await supabaseServer.from("organization_settings").upsert(row, {
      onConflict: "organization_id",
    });
    if (upsertErr) return { ok: false, error: upsertErr.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Unknown error" };
  }
}
