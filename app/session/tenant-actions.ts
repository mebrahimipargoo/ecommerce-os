"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { isUuidString } from "../../lib/uuid";
import {
  loadTenantProfile,
  type TenantProfileRow,
} from "../../lib/server-tenant";

/** UI / nav mode from `public.organizations.type` for the effective workspace org. */
export type WorkspaceViewMode = "platform" | "tenant";

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
 * `list_workspace_organizations_for_admin()` applies the same COALESCE server-side;
 * this helper is still used to label org ids resolved outside that RPC (e.g. profile load).
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

function collectRpcOrganizationIds(data: unknown): string[] {
  const rows = (data ?? []) as { organization_id?: string; company_id?: string }[];
  const ids: string[] = [];
  for (const raw of rows) {
    const id = String(raw.organization_id ?? raw.company_id ?? "").trim();
    if (id) ids.push(id);
  }
  return ids;
}

/**
 * Organizations for the header / Settings workspace pickers (internal staff).
 *
 * Merges the legacy RPC with a direct read of `organizations` so new tenants always
 * appear even if the RPC is missing on a remote DB, errors, or returns an older snapshot.
 * Display names use {@link getOrganizationNames} (`company_display_name` → `name` → id).
 */
export async function listWorkspaceOrganizationsForAdmin(): Promise<
  { ok: true; rows: WorkspaceOrganizationOption[] } | { ok: false; error: string }
> {
  try {
    const idSet = new Set<string>();

    const { data: rpcData, error: rpcError } = await supabaseServer.rpc(
      "list_workspace_organizations_for_admin",
    );
    if (!rpcError && rpcData) {
      for (const id of collectRpcOrganizationIds(rpcData)) idSet.add(id);
    }

    const { data: registryRows, error: regError } = await supabaseServer
      .from("organizations")
      .select("id")
      .or("is_active.is.null,is_active.eq.true");

    if (!regError && registryRows) {
      for (const row of registryRows) {
        const id = row.id != null ? String(row.id).trim() : "";
        if (id) idSet.add(id);
      }
    }

    if (idSet.size === 0) {
      if (rpcError && regError) {
        const msg =
          rpcError.message
          + (regError.message ? `; ${regError.message}` : "");
        return { ok: false, error: msg || "Failed to list organizations." };
      }
      return { ok: true, rows: [] };
    }

    const nm = await getOrganizationNames([...idSet]);
    if (!nm.ok) {
      const fallback: WorkspaceOrganizationOption[] = [...idSet]
        .map((organization_id) => ({ organization_id, display_name: organization_id }))
        .sort((a, b) => a.display_name.localeCompare(b.display_name));
      return { ok: true, rows: fallback };
    }
    nm.rows.sort((a, b) => a.display_name.localeCompare(b.display_name));
    return { ok: true, rows: nm.rows };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to list organizations.",
    };
  }
}

/**
 * Resolves whether the app shell should show platform-only nav (`internal` org) or
 * tenant-style nav (any other `organizations.type`).
 */
export async function getWorkspaceViewModeForOrganizationAction(
  organizationId: string,
): Promise<
  { ok: true; viewMode: WorkspaceViewMode } | { ok: false; error: string }
> {
  const id = (organizationId ?? "").trim();
  if (!isUuidString(id)) {
    return { ok: false, error: "Invalid organization id." };
  }
  try {
    const { data, error } = await supabaseServer
      .from("organizations")
      .select("type")
      .eq("id", id)
      .maybeSingle();
    if (error) return { ok: false, error: error.message };
    const t = (data?.type != null ? String(data.type) : "").trim().toLowerCase();
    return { ok: true, viewMode: t === "internal" ? "platform" : "tenant" };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to resolve organization type.",
    };
  }
}
