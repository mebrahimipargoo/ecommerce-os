"use server";

import { resolveTenantOrganizationId, type TenantWriteContext } from "../../lib/server-tenant";
import { supabaseServer } from "../../lib/supabase-server";
import {
  mergeDefaultClaimEvidence,
  type ClaimEvidenceKey,
  type DefaultClaimEvidence,
} from "../claim-engine/claim-evidence-settings";

export async function getOrganizationClaimEvidenceDefaults(
  tenant?: TenantWriteContext | null,
): Promise<Record<ClaimEvidenceKey, boolean>> {
  const companyId = await resolveTenantOrganizationId(tenant);
  try {
    const { data, error } = await supabaseServer
      .from("organization_settings")
      .select("default_claim_evidence")
      .eq("organization_id", companyId)
      .maybeSingle();
    if (error || !data) return mergeDefaultClaimEvidence(null);
    const raw = (data as { default_claim_evidence?: DefaultClaimEvidence | null }).default_claim_evidence;
    return mergeDefaultClaimEvidence(raw ?? null);
  } catch {
    return mergeDefaultClaimEvidence(null);
  }
}

export async function saveOrganizationClaimEvidenceDefaults(
  patch: DefaultClaimEvidence,
  tenant?: TenantWriteContext | null,
): Promise<{ ok: boolean; error?: string }> {
  const companyId = await resolveTenantOrganizationId(tenant);
  try {
    const merged = mergeDefaultClaimEvidence(patch);
    const { data: existing } = await supabaseServer
      .from("organization_settings")
      .select("is_ai_label_ocr_enabled, is_ai_packing_slip_ocr_enabled")
      .eq("organization_id", companyId)
      .maybeSingle();

    const row = {
      organization_id: companyId,
      is_ai_label_ocr_enabled: (existing as { is_ai_label_ocr_enabled?: boolean } | null)?.is_ai_label_ocr_enabled ?? false,
      is_ai_packing_slip_ocr_enabled:
        (existing as { is_ai_packing_slip_ocr_enabled?: boolean } | null)?.is_ai_packing_slip_ocr_enabled ?? false,
      default_claim_evidence: merged as unknown as Record<string, boolean>,
    };

    const { error: upsertErr } = await supabaseServer.from("organization_settings").upsert(row, {
      onConflict: "organization_id",
    });
    if (upsertErr) return { ok: false, error: upsertErr.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Save failed." };
  }
}
