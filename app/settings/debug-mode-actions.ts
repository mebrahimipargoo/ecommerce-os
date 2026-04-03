"use server";

import { resolveTenantOrganizationId, type TenantWriteContext } from "../../lib/server-tenant";
import { supabaseServer } from "../../lib/supabase-server";

export async function getOrganizationDebugMode(
  tenant?: TenantWriteContext | null,
): Promise<boolean> {
  const companyId = await resolveTenantOrganizationId(tenant);
  try {
    const { data, error } = await supabaseServer
      .from("organization_settings")
      .select("debug_mode")
      .eq("organization_id", companyId)
      .maybeSingle();
    if (error || !data) return false;
    return Boolean((data as { debug_mode?: boolean }).debug_mode);
  } catch {
    return false;
  }
}

export async function saveOrganizationDebugMode(
  enabled: boolean,
  tenant?: TenantWriteContext | null,
): Promise<{ ok: boolean; error?: string }> {
  const companyId = await resolveTenantOrganizationId(tenant);
  try {
    const { data: existing, error: selErr } = await supabaseServer
      .from("organization_settings")
      .select("organization_id")
      .eq("organization_id", companyId)
      .maybeSingle();

    if (selErr) return { ok: false, error: selErr.message };

    if (existing) {
      const { error: updErr } = await supabaseServer
        .from("organization_settings")
        .update({ debug_mode: enabled })
        .eq("organization_id", companyId);
      if (updErr) return { ok: false, error: updErr.message };
    } else {
      const { error: insErr } = await supabaseServer.from("organization_settings").insert({
        organization_id: companyId,
        is_ai_label_ocr_enabled: false,
        is_ai_packing_slip_ocr_enabled: false,
        default_claim_evidence: {},
        credentials: {},
        debug_mode: enabled,
      });
      if (insErr) return { ok: false, error: insErr.message };
    }

    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Save failed." };
  }
}
