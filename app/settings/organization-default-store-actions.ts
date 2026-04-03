"use server";

import { resolveTenantOrganizationId, type TenantWriteContext } from "../../lib/server-tenant";
import { supabaseServer } from "../../lib/supabase-server";
import { isUuidString } from "../../lib/uuid";

export async function getOrganizationDefaultStoreId(
  tenant?: TenantWriteContext | null,
): Promise<string | null> {
  const companyId = await resolveTenantOrganizationId(tenant);
  try {
    const { data, error } = await supabaseServer
      .from("organization_settings")
      .select("default_store_id")
      .eq("organization_id", companyId)
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
  tenant?: TenantWriteContext | null,
): Promise<{ ok: boolean; error?: string }> {
  const companyId = await resolveTenantOrganizationId(tenant);
  try {
    const trimmed = storeId?.trim() ?? "";
    if (trimmed && !isUuidString(trimmed)) {
      return { ok: false, error: "Invalid store id." };
    }
    const finalId = trimmed || null;

    if (finalId) {
      const { data: store, error: storeErr } = await supabaseServer
        .from("stores")
        .select("id, organization_id")
        .eq("id", finalId)
        .maybeSingle();
      if (storeErr || !store) {
        return { ok: false, error: "Store not found." };
      }
    }

    const { data: existing, error: selErr } = await supabaseServer
      .from("organization_settings")
      .select("organization_id")
      .eq("organization_id", companyId)
      .maybeSingle();

    if (selErr) return { ok: false, error: selErr.message };

    if (existing) {
      const { error: updErr } = await supabaseServer
        .from("organization_settings")
        .update({ default_store_id: finalId })
        .eq("organization_id", companyId);
      if (updErr) return { ok: false, error: updErr.message };
    } else {
      const { error: insErr } = await supabaseServer.from("organization_settings").insert({
        organization_id: companyId,
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
