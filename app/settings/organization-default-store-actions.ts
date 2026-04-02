"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { resolveOrganizationId } from "../../lib/organization";
import { isUuidString } from "../../lib/uuid";

export async function getOrganizationDefaultStoreId(): Promise<string | null> {
  const organizationId = resolveOrganizationId();
  try {
    const { data, error } = await supabaseServer
      .from("organization_settings")
      .select("default_store_id")
      .eq("company_id", organizationId)
      .maybeSingle();
    if (error || !data) return null;
    const id = (data as { default_store_id?: string | null }).default_store_id;
    return typeof id === "string" && isUuidString(id) ? id : null;
  } catch {
    return null;
  }
}

export async function saveOrganizationDefaultStoreId(
  storeId: string | null,
): Promise<{ ok: boolean; error?: string }> {
  const organizationId = resolveOrganizationId();
  try {
    const trimmed = storeId?.trim() ?? "";
    if (trimmed && !isUuidString(trimmed)) {
      return { ok: false, error: "Invalid store id." };
    }
    const finalId = trimmed || null;

    if (finalId) {
      const { data: store, error: storeErr } = await supabaseServer
        .from("stores")
        .select("id")
        .eq("id", finalId)
        .eq("company_id", organizationId)
        .maybeSingle();
      if (storeErr || !store) {
        return { ok: false, error: "Store not found in this workspace." };
      }
    }

    const { data: existing, error: selErr } = await supabaseServer
      .from("organization_settings")
      .select("company_id")
      .eq("company_id", organizationId)
      .maybeSingle();

    if (selErr) return { ok: false, error: selErr.message };

    if (existing) {
      const { error: updErr } = await supabaseServer
        .from("organization_settings")
        .update({ default_store_id: finalId })
        .eq("company_id", organizationId);
      if (updErr) return { ok: false, error: updErr.message };
    } else {
      const { error: insErr } = await supabaseServer.from("organization_settings").insert({
        company_id: organizationId,
        is_ai_label_ocr_enabled: false,
        is_ai_packing_slip_ocr_enabled: false,
        default_claim_evidence: {},
        credentials: {},
        default_store_id: finalId,
      });
      if (insErr) return { ok: false, error: insErr.message };
    }

    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Save failed." };
  }
}
