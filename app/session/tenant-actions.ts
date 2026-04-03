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
        const id = String(r.organization_id ?? "").trim();
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
