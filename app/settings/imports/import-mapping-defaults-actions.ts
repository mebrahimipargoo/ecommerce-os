"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { resolveOrganizationId } from "../../../lib/organization";
import type { RawReportType } from "../../../lib/raw-report-types";

type MappingDefaults = Partial<Record<RawReportType, Record<string, string>>>;

export async function getImportMappingDefaults(): Promise<MappingDefaults> {
  const organizationId = resolveOrganizationId();
  try {
    const { data, error } = await supabaseServer
      .from("organization_settings")
      .select("import_mapping_defaults")
      .eq("organization_id", organizationId)
      .maybeSingle();
    if (error || !data) return {};
    const raw = (data as { import_mapping_defaults?: unknown }).import_mapping_defaults;
    return typeof raw === "object" && raw !== null ? (raw as MappingDefaults) : {};
  } catch {
    return {};
  }
}

export async function mergeImportMappingDefault(
  reportType: RawReportType,
  mapping: Record<string, string>,
): Promise<{ ok: boolean; error?: string }> {
  const organizationId = resolveOrganizationId();
  try {
    const current = await getImportMappingDefaults();
    const next: MappingDefaults = {
      ...current,
      [reportType]: { ...current[reportType], ...mapping },
    };

    const { data: existing, error: selErr } = await supabaseServer
      .from("organization_settings")
      .select("organization_id")
      .eq("organization_id", organizationId)
      .maybeSingle();

    if (selErr) return { ok: false, error: selErr.message };

    if (existing) {
      const { error: updErr } = await supabaseServer
        .from("organization_settings")
        .update({ import_mapping_defaults: next })
        .eq("organization_id", organizationId);
      if (updErr) return { ok: false, error: updErr.message };
    } else {
      const { error: insErr } = await supabaseServer.from("organization_settings").insert({
        organization_id: organizationId,
        is_ai_label_ocr_enabled: false,
        is_ai_packing_slip_ocr_enabled: false,
        default_claim_evidence: {},
        credentials: {},
        import_mapping_defaults: next,
      });
      if (insErr) return { ok: false, error: insErr.message };
    }

    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Save failed." };
  }
}
