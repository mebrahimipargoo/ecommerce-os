"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { resolveOrganizationId } from "../../lib/organization";

export async function getOrganizationDebugMode(): Promise<boolean> {
  const organizationId = resolveOrganizationId();
  try {
    const { data, error } = await supabaseServer
      .from("organization_settings")
      .select("debug_mode")
      .eq("organization_id", organizationId)
      .maybeSingle();
    if (error || !data) return false;
    return Boolean((data as { debug_mode?: boolean }).debug_mode);
  } catch {
    return false;
  }
}

export async function saveOrganizationDebugMode(
  enabled: boolean,
): Promise<{ ok: boolean; error?: string }> {
  const organizationId = resolveOrganizationId();
  try {
    const { data: existing, error: selErr } = await supabaseServer
      .from("organization_settings")
      .select("organization_id")
      .eq("organization_id", organizationId)
      .maybeSingle();

    if (selErr) return { ok: false, error: selErr.message };

    if (existing) {
      const { error: updErr } = await supabaseServer
        .from("organization_settings")
        .update({ debug_mode: enabled })
        .eq("organization_id", organizationId);
      if (updErr) return { ok: false, error: updErr.message };
    } else {
      const { error: insErr } = await supabaseServer.from("organization_settings").insert({
        organization_id: organizationId,
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
