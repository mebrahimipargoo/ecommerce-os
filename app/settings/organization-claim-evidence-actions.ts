"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { resolveOrganizationId } from "../../lib/organization";
import {
  mergeDefaultClaimEvidence,
  type ClaimEvidenceKey,
  type DefaultClaimEvidence,
} from "../claim-engine/claim-evidence-settings";

export async function getOrganizationClaimEvidenceDefaults(
  organizationId: string = resolveOrganizationId(),
): Promise<Record<ClaimEvidenceKey, boolean>> {
  try {
    const { data, error } = await supabaseServer
      .from("organization_settings")
      .select("default_claim_evidence")
      .eq("company_id", organizationId)
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
  organizationId: string = resolveOrganizationId(),
): Promise<{ ok: boolean; error?: string }> {
  try {
    const merged = mergeDefaultClaimEvidence(patch);
    const { data: existing } = await supabaseServer
      .from("organization_settings")
      .select("is_ai_label_ocr_enabled, is_ai_packing_slip_ocr_enabled")
      .eq("company_id", organizationId)
      .maybeSingle();

    const row = {
      company_id: organizationId,
      is_ai_label_ocr_enabled: (existing as { is_ai_label_ocr_enabled?: boolean } | null)?.is_ai_label_ocr_enabled ?? false,
      is_ai_packing_slip_ocr_enabled:
        (existing as { is_ai_packing_slip_ocr_enabled?: boolean } | null)?.is_ai_packing_slip_ocr_enabled ?? false,
      default_claim_evidence: merged as unknown as Record<string, boolean>,
    };

    const { error: upsertErr } = await supabaseServer.from("organization_settings").upsert(row, {
      onConflict: "company_id",
    });
    if (upsertErr) return { ok: false, error: upsertErr.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Save failed." };
  }
}
