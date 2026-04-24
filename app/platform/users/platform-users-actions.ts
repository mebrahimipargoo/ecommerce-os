"use server";

import type { CompanyOption } from "../../../lib/imports-types";
import { loadTenantProfile } from "../../../lib/server-tenant";
import { supabaseServer } from "../../../lib/supabase-server";
import { getSessionUserIdFromCookies } from "../../../lib/supabase-server-auth";
import { normalizeRoleKeyForBranding } from "../../../lib/tenant-branding-permissions";
import { isUuidString } from "../../../lib/uuid";
import { DB_TABLES } from "../../(admin)/lib/constants";
import { updateUserProfile } from "../../(admin)/users/users-actions";
import { getAuthenticatedPlatformRoleKey } from "../settings/platform-settings-actions";
import { isOrganizationTypeValue } from "../organizations/provisioning-field-validation";

export type PlatformUsersPageAccess = {
  accessDenied: "not_authenticated" | "forbidden" | null;
};

function mapOrgSettingsRow(r: Record<string, unknown>): CompanyOption {
  const id = String(r.organization_id ?? "");
  const orgJoin = r.organizations as {
    name?: string | null;
    type?: string | null;
  } | null;
  const display_name =
    (typeof r.company_display_name === "string" && r.company_display_name.trim()) ||
    (typeof orgJoin?.name === "string" && orgJoin.name.trim()) ||
    id;
  const typeRaw =
    orgJoin?.type != null ? String(orgJoin.type).trim().toLowerCase() : "";
  const organization_type: "tenant" | "internal" =
    typeRaw === "internal" ? "internal" : "tenant";
  return { id, display_name, organization_type };
}

export async function getPlatformUsersPageAccessAction(): Promise<PlatformUsersPageAccess> {
  const raw = await getAuthenticatedPlatformRoleKey();
  if (!raw) {
    return { accessDenied: "not_authenticated" };
  }
  const k = normalizeRoleKeyForBranding(raw);
  if (k !== "super_admin") {
    return { accessDenied: "forbidden" };
  }
  return { accessDenied: null };
}

export async function listOrganizationsForPlatformUserDirectory(): Promise<
  { ok: true; rows: CompanyOption[] } | { ok: false; error: string }
> {
  const access = await getPlatformUsersPageAccessAction();
  if (access.accessDenied === "not_authenticated") {
    return { ok: false, error: "Not authenticated." };
  }
  if (access.accessDenied === "forbidden") {
    return { ok: false, error: "Forbidden." };
  }
  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.organizationSettings)
      .select("organization_id, company_display_name, organizations(name, type)")
      .order("organization_id", { ascending: true });
    if (error) return { ok: false, error: error.message };
    const rows = (data ?? []) as Record<string, unknown>[];
    const out = rows.map(mapOrgSettingsRow);
    out.sort((a, b) => a.display_name.localeCompare(b.display_name));
    return { ok: true, rows: out };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Failed to load organizations.",
    };
  }
}

export async function updatePlatformUserProfile(
  id: string,
  patch: {
    full_name?: string;
    role?: string;
    organization_id?: string;
    /** Writes `public.organizations.type` for the organization in `organization_id`. */
    organization_type?: string | null;
  },
): Promise<{ ok: boolean; error?: string }> {
  const fromSession = await getSessionUserIdFromCookies();
  const actorId =
    fromSession && isUuidString(fromSession) ? fromSession : null;
  if (!actorId) return { ok: false, error: "Not authenticated." };
  const actor = await loadTenantProfile(actorId);
  if (!actor || normalizeRoleKeyForBranding(actor.role) !== "super_admin") {
    return { ok: false, error: "Forbidden." };
  }
  if (!isUuidString(id)) return { ok: false, error: "Invalid user id." };

  let orgId: string | undefined;
  if (typeof patch.organization_id === "string") {
    const oid = patch.organization_id.trim();
    if (!isUuidString(oid)) return { ok: false, error: "Invalid organization." };
    const { data: org, error: oErr } = await supabaseServer
      .from("organizations")
      .select("id")
      .eq("id", oid)
      .maybeSingle();
    if (oErr) return { ok: false, error: oErr.message };
    if (!org) return { ok: false, error: "Organization not found." };
    orgId = oid;
  }

  if (
    typeof patch.full_name === "string"
    || typeof patch.role === "string"
  ) {
    const res = await updateUserProfile(id, {
      full_name: patch.full_name,
      role: patch.role,
    });
    if (!res.ok) return res;
  }

  if (orgId !== undefined) {
    try {
      const { error } = await supabaseServer
        .from("profiles")
        .update({
          organization_id: orgId,
          updated_at: new Date().toISOString(),
        })
        .eq("id", id);
      if (error) return { ok: false, error: error.message };
    } catch (e) {
      return {
        ok: false,
        error: e instanceof Error ? e.message : "Update failed.",
      };
    }
  }

  const typePatch = patch.organization_type;
  if (orgId !== undefined && typeof typePatch === "string") {
    const t = typePatch.trim().toLowerCase();
    if (!isOrganizationTypeValue(t)) {
      return { ok: false, error: "Organization type must be tenant or internal." };
    }
    const { error: typeErr } = await supabaseServer
      .from("organizations")
      .update({ type: t })
      .eq("id", orgId);
    if (typeErr) return { ok: false, error: typeErr.message };
  }

  return { ok: true };
}
