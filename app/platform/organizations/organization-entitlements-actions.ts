"use server";

import type { ModuleFeatureTreeNode, OrgEntitlementsPayload } from "../../../lib/access-entitlements";
import { supabaseServer } from "../../../lib/supabase-server";
import { isUuidString } from "../../../lib/uuid";
import { assertManagePlatformAccess } from "../access/server-gate";
import { getEntitlementsEditorCatalogForOrganizationAction } from "../access/permissions-actions";

export type OrganizationEntitlementMode = "not_configured" | "enabled" | "disabled";

function deniedStr(d: "not_authenticated" | "forbidden"): string {
  return d === "not_authenticated" ? "Not authenticated." : "Forbidden.";
}

export async function getOrganizationEntitlementsEditorAction(organizationId: string): Promise<
  | {
      ok: true;
      organizationName: string;
      moduleFeatureTree: ModuleFeatureTreeNode[];
      orgEntitlements: OrgEntitlementsPayload;
    }
  | { ok: false; error: string }
> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedStr(g.denied) };
  const oid = organizationId.trim();
  if (!isUuidString(oid)) return { ok: false, error: "Invalid organization." };
  const { data: org, error: oe } = await supabaseServer
    .from("organizations")
    .select("id, name")
    .eq("id", oid)
    .maybeSingle();
  if (oe || !org) return { ok: false, error: oe?.message ?? "Organization not found." };
  const { data: settings } = await supabaseServer
    .from("organization_settings")
    .select("company_display_name")
    .eq("organization_id", oid)
    .maybeSingle();
  const displayName = String(
    (settings as { company_display_name?: string | null } | null)?.company_display_name ?? "",
  ).trim();
  const registryName = String((org as { name?: string }).name ?? "").trim();
  const organizationName = displayName || registryName || oid;
  const cat = await getEntitlementsEditorCatalogForOrganizationAction(oid);
  if (!cat.ok) return { ok: false, error: cat.error };
  return {
    ok: true,
    organizationName,
    moduleFeatureTree: cat.moduleFeatureTree,
    orgEntitlements: cat.orgEntitlements,
  };
}

export async function setOrganizationModuleEntitlementModeAction(input: {
  organizationId: string;
  moduleId: string;
  mode: OrganizationEntitlementMode;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedStr(g.denied) };
  const organizationId = input.organizationId.trim();
  const moduleId = input.moduleId.trim();
  if (!isUuidString(organizationId) || !isUuidString(moduleId)) {
    return { ok: false, error: "Invalid organization or module id." };
  }
  const mode = input.mode;
  try {
    if (mode === "not_configured") {
      const { error } = await supabaseServer
        .from("organization_modules")
        .delete()
        .eq("organization_id", organizationId)
        .eq("module_id", moduleId);
      if (error) return { ok: false, error: error.message };
      return { ok: true };
    }
    const isEnabled = mode === "enabled";
    /** `is_purchased` = licensed slot; `is_enabled` = turned on. Both required by schema in many DBs. */
    const row = {
      organization_id: organizationId,
      module_id: moduleId,
      is_purchased: true,
      is_enabled: isEnabled,
      config: {},
      updated_at: new Date().toISOString(),
    };
    const { error } = await supabaseServer.from("organization_modules").upsert(row, {
      onConflict: "organization_id,module_id",
    });
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

export async function setOrganizationModuleFeatureEntitlementModeAction(input: {
  organizationId: string;
  moduleFeatureId: string;
  mode: OrganizationEntitlementMode;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedStr(g.denied) };
  const organizationId = input.organizationId.trim();
  const moduleFeatureId = input.moduleFeatureId.trim();
  if (!isUuidString(organizationId) || !isUuidString(moduleFeatureId)) {
    return { ok: false, error: "Invalid id." };
  }
  const mode = input.mode;
  try {
    if (mode === "not_configured") {
      const { error } = await supabaseServer
        .from("organization_module_features")
        .delete()
        .eq("organization_id", organizationId)
        .eq("module_feature_id", moduleFeatureId);
      if (error) return { ok: false, error: error.message };
      return { ok: true };
    }
    const isEnabled = mode === "enabled";
    const row = {
      organization_id: organizationId,
      module_feature_id: moduleFeatureId,
      is_purchased: true,
      is_enabled: isEnabled,
      config: {},
      updated_at: new Date().toISOString(),
    };
    const { error } = await supabaseServer.from("organization_module_features").upsert(row, {
      onConflict: "organization_id,module_feature_id",
    });
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}

/**
 * Apply the same mode to every `module_features` row under a module (bulk “all features in this branch”).
 */
export async function setAllModuleFeatureEntitlementsForModuleAction(input: {
  organizationId: string;
  moduleId: string;
  mode: OrganizationEntitlementMode;
}): Promise<{ ok: true; count: number } | { ok: false; error: string }> {
  const g = await assertManagePlatformAccess();
  if (!g.ok) return { ok: false, error: deniedStr(g.denied) };
  const organizationId = input.organizationId.trim();
  const moduleId = input.moduleId.trim();
  if (!isUuidString(organizationId) || !isUuidString(moduleId)) {
    return { ok: false, error: "Invalid organization or module id." };
  }
  const mode = input.mode;
  try {
    const { data: mfs, error: mfErr } = await supabaseServer
      .from("module_features")
      .select("id")
      .eq("module_id", moduleId);
    if (mfErr) return { ok: false, error: mfErr.message };
    const featureIds = (mfs ?? [])
      .map((r) => String((r as { id?: string }).id ?? "").trim())
      .filter(isUuidString);
    if (featureIds.length === 0) {
      return { ok: true, count: 0 };
    }
    if (mode === "not_configured") {
      const { error } = await supabaseServer
        .from("organization_module_features")
        .delete()
        .eq("organization_id", organizationId)
        .in("module_feature_id", featureIds);
      if (error) return { ok: false, error: error.message };
      return { ok: true, count: featureIds.length };
    }
    const isEnabled = mode === "enabled";
    const now = new Date().toISOString();
    const rows = featureIds.map((moduleFeatureId) => ({
      organization_id: organizationId,
      module_feature_id: moduleFeatureId,
      is_purchased: true,
      is_enabled: isEnabled,
      config: {},
      updated_at: now,
    }));
    const { error } = await supabaseServer.from("organization_module_features").upsert(rows, {
      onConflict: "organization_id,module_feature_id",
    });
    if (error) return { ok: false, error: error.message };
    return { ok: true, count: featureIds.length };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Update failed." };
  }
}
