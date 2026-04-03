"use server";

import { resolveTenantOrganizationId, type TenantWriteContext } from "../../../lib/server-tenant";
import { supabaseServer } from "../../../lib/supabase-server";
import type { RawReportType } from "../../../lib/raw-report-types";

type MappingDefaults = Partial<Record<RawReportType, Record<string, string>>>;

export async function getImportMappingDefaults(
  tenant?: TenantWriteContext | null,
): Promise<MappingDefaults> {
  const companyId = await resolveTenantOrganizationId(tenant);
  try {
    const { data, error } = await supabaseServer
      .from("organization_settings")
      .select("import_mapping_defaults")
      .eq("organization_id", companyId)
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
  tenant?: TenantWriteContext | null,
): Promise<{ ok: boolean; error?: string }> {
  const companyId = await resolveTenantOrganizationId(tenant);
  try {
    const current = await getImportMappingDefaults(tenant);
    const next: MappingDefaults = {
      ...current,
      [reportType]: { ...current[reportType], ...mapping },
    };

    const { data: existing, error: selErr } = await supabaseServer
      .from("organization_settings")
      .select("organization_id")
      .eq("organization_id", companyId)
      .maybeSingle();

    if (selErr) return { ok: false, error: selErr.message };

    if (existing) {
      const { error: updErr } = await supabaseServer
        .from("organization_settings")
        .update({ import_mapping_defaults: next })
        .eq("organization_id", companyId);
      if (updErr) return { ok: false, error: updErr.message };
    } else {
      const { error: insErr } = await supabaseServer.from("organization_settings").insert({
        organization_id: companyId,
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
