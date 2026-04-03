"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { isUuidString } from "../../../lib/uuid";
import { DB_TABLES } from "./constants";

export type OrganizationFeatureRow = {
  organization_id: string;
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

/**
 * Super Admin: all organizations with feature flags sourced entirely from `organization_settings`.
 */
export async function listOrganizationFeatures(
  actorProfileId: string | null | undefined,
): Promise<
  { ok: true; rows: OrganizationFeatureRow[] } | { ok: false; error: string }
> {
  const gate = await requireSuperAdmin(actorProfileId);
  if (!gate.ok) return gate;

  try {
    const { data: sets, error: setErr } = await supabaseServer
      .from(DB_TABLES.organizationSettings)
      .select(
        "organization_id, company_display_name, debug_mode, is_ai_label_ocr_enabled, is_ai_packing_slip_ocr_enabled",
      )
      .order("organization_id", { ascending: true });
    if (setErr) return { ok: false, error: setErr.message };

    const rows: OrganizationFeatureRow[] = (sets ?? []).map((raw) => {
      const r = raw as Record<string, unknown>;
      const id = String(r.organization_id ?? "");
      const displayName =
        (typeof r.company_display_name === "string" && r.company_display_name.trim()) || id;
      return {
        organization_id: id,
        display_name: displayName,
        debug_mode: Boolean(r.debug_mode),
        is_ai_label_ocr_enabled: Boolean(r.is_ai_label_ocr_enabled),
        is_ai_packing_slip_ocr_enabled: Boolean(r.is_ai_packing_slip_ocr_enabled),
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
      .select("organization_id")
      .eq("organization_id", orgId)
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
        .eq("organization_id", orgId);
      if (updErr) return { ok: false, error: updErr.message };
    } else {
      const { error: insErr } = await supabaseServer
        .from(DB_TABLES.organizationSettings)
        .insert({
          organization_id: orgId,
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
