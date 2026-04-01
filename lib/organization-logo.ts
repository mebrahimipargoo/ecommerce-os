import "server-only";

import { supabaseServer } from "./supabase-server";
import { resolveOrganizationId } from "./organization";

/**
 * Reads tenant logo URL from organization_settings (canonical for UI / PDFs).
 */
export async function getOrganizationLogoUrlFromDb(organizationId?: string): Promise<string> {
  const org = organizationId?.trim() && organizationId.trim().length > 0
    ? organizationId.trim()
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
): Promise<{ ok: boolean; error?: string }> {
  const organizationId = resolveOrganizationId();
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
