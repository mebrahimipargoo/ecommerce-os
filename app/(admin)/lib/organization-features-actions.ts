"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { isUuidString } from "../../../lib/uuid";
import { DB_TABLES } from "./constants";

export type OrganizationFeatureRow = {
  company_id: string;
  display_name: string;
  debug_mode: boolean;
  is_ai_label_ocr_enabled: boolean;
  is_ai_packing_slip_ocr_enabled: boolean;
};

async function requireSuperAdmin(
  actorProfileId: string | null | undefined,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const id = actorProfileId?.trim();
  if (!id || !isUuidString(id)) {
    return { ok: false, error: "Not authenticated." };
  }
  const { data, error } = await supabaseServer
    .from(DB_TABLES.profiles)
    .select("role")
    .eq("id", id)
    .maybeSingle();
  if (error || !data) return { ok: false, error: "Profile not found." };
  const role = String((data as { role?: string }).role ?? "").trim();
  if (role !== "super_admin") {
    return { ok: false, error: "Super Admin only." };
  }
  return { ok: true };
}

function companyDisplayName(r: Record<string, unknown>, id: string): string {
  const name =
    (typeof r.name === "string" && r.name.trim()) ||
    (typeof r.display_name === "string" && r.display_name.trim()) ||
    (typeof r.company_name === "string" && r.company_name.trim()) ||
    (typeof r.title === "string" && r.title.trim()) ||
    "";
  return name || id;
}

/**
 * Super Admin: all companies with feature flags from `organization_settings` (defaults if missing).
 */
export async function listOrganizationFeatures(
  actorProfileId: string | null | undefined,
): Promise<
  { ok: true; rows: OrganizationFeatureRow[] } | { ok: false; error: string }
> {
  const gate = await requireSuperAdmin(actorProfileId);
  if (!gate.ok) return gate;

  try {
    const { data: cos, error: coErr } = await supabaseServer
      .from(DB_TABLES.companies)
      .select("*")
      .order("id", { ascending: true });
    if (coErr) return { ok: false, error: coErr.message };

    const { data: sets, error: setErr } = await supabaseServer
      .from(DB_TABLES.organizationSettings)
      .select(
        "company_id, debug_mode, is_ai_label_ocr_enabled, is_ai_packing_slip_ocr_enabled",
      );
    if (setErr) return { ok: false, error: setErr.message };

    const byOrg = new Map<string, Record<string, unknown>>();
    for (const s of sets ?? []) {
      const row = s as Record<string, unknown>;
      const oid = String(row.company_id ?? "");
      if (oid) byOrg.set(oid, row);
    }

    const rows: OrganizationFeatureRow[] = (cos ?? []).map((raw) => {
      const r = raw as Record<string, unknown>;
      const id = String(r.id ?? "");
      const s = byOrg.get(id);
      return {
        company_id: id,
        display_name: companyDisplayName(r, id),
        debug_mode: Boolean(s?.debug_mode),
        is_ai_label_ocr_enabled: Boolean(s?.is_ai_label_ocr_enabled),
        is_ai_packing_slip_ocr_enabled: Boolean(s?.is_ai_packing_slip_ocr_enabled),
      };
    });
    rows.sort((a, b) => a.display_name.localeCompare(b.display_name));
    return { ok: true, rows };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to load organization features.",
    };
  }
}

export async function saveOrganizationFeatures(input: {
  actorProfileId: string | null | undefined;
  organizationId: string;
  debug_mode: boolean;
  is_ai_label_ocr_enabled: boolean;
  is_ai_packing_slip_ocr_enabled: boolean;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const gate = await requireSuperAdmin(input.actorProfileId);
  if (!gate.ok) return gate;

  const orgId = input.organizationId.trim();
  if (!isUuidString(orgId)) {
    return { ok: false, error: "Invalid organization id." };
  }

  try {
    const { data: existing, error: selErr } = await supabaseServer
      .from(DB_TABLES.organizationSettings)
      .select("company_id")
      .eq("company_id", orgId)
      .maybeSingle();

    if (selErr) return { ok: false, error: selErr.message };

    if (existing) {
      const { error: updErr } = await supabaseServer
        .from(DB_TABLES.organizationSettings)
        .update({
          debug_mode: input.debug_mode,
          is_ai_label_ocr_enabled: input.is_ai_label_ocr_enabled,
          is_ai_packing_slip_ocr_enabled: input.is_ai_packing_slip_ocr_enabled,
        })
        .eq("company_id", orgId);
      if (updErr) return { ok: false, error: updErr.message };
    } else {
      const { error: insErr } = await supabaseServer
        .from(DB_TABLES.organizationSettings)
        .insert({
          company_id: orgId,
          is_ai_label_ocr_enabled: input.is_ai_label_ocr_enabled,
          is_ai_packing_slip_ocr_enabled: input.is_ai_packing_slip_ocr_enabled,
          default_claim_evidence: {},
          credentials: {},
          debug_mode: input.debug_mode,
        });
      if (insErr) return { ok: false, error: insErr.message };
    }

    return { ok: true };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Save failed.",
    };
  }
}
