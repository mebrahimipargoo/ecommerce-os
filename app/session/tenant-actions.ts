"use server";

import { supabaseServer } from "../../lib/supabase-server";
import {
  loadTenantProfile,
  type TenantProfileRow,
} from "../../lib/server-tenant";

/**
 * Loads the signed-in workspace user (`profiles` row) for client session bootstrap.
 */
export async function fetchUserProfileById(
  profileId: string | null | undefined,
): Promise<{ ok: true; profile: TenantProfileRow } | { ok: false; error: string }> {
  const p = await loadTenantProfile(profileId);
  if (!p) {
    return { ok: false, error: "Profile not found." };
  }
  return { ok: true, profile: p };
}

export type WorkspaceOrganizationOption = {
  organization_id: string;
  display_name: string;
};

/**
 * Resolves real display names for a list of organization UUIDs by querying
 * the organizations and organization_settings tables directly.
 *
 * Priority:
 *   1. organization_settings.company_display_name  (admin-set label)
 *   2. organizations.name                          (canonical DB name)
 *   3. raw UUID                                    (last resort)
 *
 * This exists because list_workspace_organizations_for_admin() only checks
 * company_display_name and silently falls back to the raw UUID when that
 * column is NULL — hiding the real name stored in organizations.name.
 */
export async function getOrganizationNames(
  orgIds: string[],
): Promise<{ ok: true; rows: WorkspaceOrganizationOption[] } | { ok: false; error: string }> {
  if (orgIds.length === 0) return { ok: true, rows: [] };
  try {
    const uniqueIds = [...new Set(orgIds.filter(Boolean))];

    const [orgsRes, settingsRes] = await Promise.all([
      supabaseServer
        .from("organizations")
        .select("id, name")
        .in("id", uniqueIds),
      supabaseServer
        .from("organization_settings")
        .select("organization_id, company_display_name")
        .in("organization_id", uniqueIds),
    ]);

    // Build name maps — organization.name as base, company_display_name as override
    const orgNames: Record<string, string> = {};
    for (const o of orgsRes.data ?? []) {
      if (o.id && typeof o.name === "string" && o.name.trim()) {
        orgNames[String(o.id)] = o.name.trim();
      }
    }
    const displayNames: Record<string, string> = {};
    for (const s of settingsRes.data ?? []) {
      if (
        s.organization_id &&
        typeof s.company_display_name === "string" &&
        s.company_display_name.trim()
      ) {
        displayNames[String(s.organization_id)] = s.company_display_name.trim();
      }
    }

    const rows: WorkspaceOrganizationOption[] = uniqueIds.map((id) => ({
      organization_id: id,
      display_name: displayNames[id] ?? orgNames[id] ?? id,
    }));
    return { ok: true, rows };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to fetch organization names.",
    };
  }
}

/**
 * Organizations visible in Super Admin filters (distinct orgs with data or settings).
 */
export async function listWorkspaceOrganizationsForAdmin(): Promise<
  { ok: true; rows: WorkspaceOrganizationOption[] } | { ok: false; error: string }
> {
  try {
    const { data, error } = await supabaseServer.rpc(
      "list_workspace_organizations_for_admin",
    );
    if (error) {
      return { ok: false, error: error.message };
    }
    const rows = (data ?? []) as {
      organization_id?: string;
      display_name?: string;
    }[];
    const out: WorkspaceOrganizationOption[] = rows
      .map((r) => {
        const raw = r as { organization_id?: string; company_id?: string };
        const id = String(raw.organization_id ?? raw.company_id ?? "").trim();
        const dn =
          typeof r.display_name === "string" && r.display_name.trim().length > 0
            ? r.display_name.trim()
            : "";
        return { organization_id: id, display_name: dn || id };
      })
      .filter((row) => row.organization_id.length > 0);
    out.sort((a, b) => a.display_name.localeCompare(b.display_name));
    return { ok: true, rows: out };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to list organizations.",
    };
  }
}
