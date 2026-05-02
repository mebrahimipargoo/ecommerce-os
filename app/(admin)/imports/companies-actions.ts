"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { isSuperAdminRole, loadTenantProfile } from "../../../lib/server-tenant";
import { isUuidString } from "../../../lib/uuid";
import type { CompanyOption, StoreImportOption } from "../../../lib/imports-types";
import { DB_TABLES } from "../lib/constants";

function mapOrgSettingsRow(r: Record<string, unknown>): CompanyOption {
  const id = String(r.organization_id ?? "");
  // Prefer organization_settings.company_display_name, then fall back to
  // the joined organizations.name, and only then show the raw UUID.
  const orgJoin = r.organizations as { name?: string | null } | null;
  const display_name =
    (typeof r.company_display_name === "string" && r.company_display_name.trim()) ||
    (typeof orgJoin?.name === "string" && orgJoin.name.trim()) ||
    id;
  return { id, display_name };
}

function formatStoreDisplayName(row: Record<string, unknown>): string {
  const name = typeof row.name === "string" ? row.name.trim() : "";
  const plat = typeof row.platform === "string" ? row.platform.trim() : "";
  if (plat && name) return `(${plat}) ${name}`;
  return name || plat || String(row.id ?? "");
}

/**
 * Active stores the actor may target for imports (`stores.organization_id` = tenant).
 * - `super_admin`: stores for the selected workspace organization when provided
 * - others: only stores for `profiles.organization_id`
 */
export async function listStoresForImports(
  actorUserId?: string | null,
  targetOrganizationId?: string | null,
): Promise<{ ok: true; rows: StoreImportOption[] } | { ok: false; error: string }> {
  try {
    const aid = actorUserId?.trim();
    const requestedOrgId = targetOrganizationId?.trim();

    async function fetchStoresForOrganization(
      organizationId: string,
    ): Promise<{ ok: true; rows: StoreImportOption[] } | { ok: false; error: string }> {
      if (!organizationId || !isUuidString(organizationId)) {
        return { ok: true, rows: [] };
      }

      const { data, error } = await supabaseServer
        .from(DB_TABLES.stores)
        .select("id, organization_id, name, platform, is_active")
        .eq("organization_id", organizationId)
        .eq("is_active", true)
        .order("name", { ascending: true });

      if (error) return { ok: false, error: error.message };

      const rows = (data ?? []) as Record<string, unknown>[];
      const out: StoreImportOption[] = rows.map((r) => ({
        id: String(r.id ?? ""),
        organization_id: String(r.organization_id ?? ""),
        display_name: formatStoreDisplayName(r),
      }));
      return { ok: true, rows: out };
    }

    /** Wait for a resolved session actor — avoid listing every store before `actorUserId` exists. */
    if (!aid || !isUuidString(aid)) {
      return { ok: true, rows: [] };
    }

    const profile = await loadTenantProfile(aid);
    if (!profile) {
      return { ok: false, error: "Could not load your profile." };
    }

    if (isSuperAdminRole(profile.role)) {
      return fetchStoresForOrganization(requestedOrgId ?? profile.organization_id.trim());
    }

    const cid = profile.organization_id.trim();
    if (!cid || !isUuidString(cid)) {
      return { ok: true, rows: [] };
    }

    if (requestedOrgId && requestedOrgId !== cid) {
      return { ok: true, rows: [] };
    }

    return fetchStoresForOrganization(cid);
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to load stores.",
    };
  }
}

/**
 * Organizations the actor may target on Imports, derived from `profiles.organization_id` and `profiles.role`.
 * - `super_admin`: all rows in `organization_settings`
 * - others: only the organization matching `profiles.organization_id` (when set)
 *
 * When `actorUserId` is omitted or invalid, falls back to listing all organizations (bootstrap).
 */
export async function listCompaniesForImports(
  actorUserId?: string | null,
): Promise<{ ok: true; rows: CompanyOption[] } | { ok: false; error: string }> {
  try {
    const aid = actorUserId?.trim();

    async function fetchAllOrganizations(): Promise<{ ok: true; rows: CompanyOption[] } | { ok: false; error: string }> {
      const { data, error } = await supabaseServer
        .from(DB_TABLES.organizationSettings)
        .select("organization_id, company_display_name, organizations(name)")
        .order("organization_id", { ascending: true });

      if (error) return { ok: false, error: error.message };

      const rows = (data ?? []) as Record<string, unknown>[];
      const out = rows.map(mapOrgSettingsRow);
      out.sort((a, b) => a.display_name.localeCompare(b.display_name));
      return { ok: true, rows: out };
    }

    if (!aid || !isUuidString(aid)) {
      return fetchAllOrganizations();
    }

    const profile = await loadTenantProfile(aid);
    if (!profile) {
      return { ok: false, error: "Could not load your profile." };
    }

    if (isSuperAdminRole(profile.role)) {
      return fetchAllOrganizations();
    }

    const cid = profile.organization_id.trim();
    if (!cid || !isUuidString(cid)) {
      return { ok: true, rows: [] };
    }

    const { data, error } = await supabaseServer
      .from(DB_TABLES.organizationSettings)
      .select("organization_id, company_display_name, organizations(name)")
      .eq("organization_id", cid)
      .maybeSingle();

    if (error) return { ok: false, error: error.message };
    if (!data) return { ok: true, rows: [] };

    return { ok: true, rows: [mapOrgSettingsRow(data as Record<string, unknown>)] };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to load companies.",
    };
  }
}

/**
 * Persists the selected workspace company on `profiles.organization_id` when it was unset.
 * Called from admin Imports when the user picks a target company.
 */
export async function saveHomeCompanyForProfile(
  profileId: string,
  companyId: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const pid = profileId.trim();
  const cid = companyId.trim();
  if (!isUuidString(pid) || !isUuidString(cid)) {
    return { ok: false, error: "Invalid profile or company id." };
  }
  try {
    const { data: co, error: coErr } = await supabaseServer
      .from(DB_TABLES.organizationSettings)
      .select("organization_id")
      .eq("organization_id", cid)
      .maybeSingle();
    if (coErr) return { ok: false, error: coErr.message };
    if (!co) return { ok: false, error: "Organization not found." };

    const { error } = await supabaseServer
      .from(DB_TABLES.profiles)
      .update({ organization_id: cid })
      .eq("id", pid);

    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Could not save company.",
    };
  }
}
