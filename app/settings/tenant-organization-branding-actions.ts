"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { getSessionUserIdFromCookies } from "../../lib/supabase-server-auth";
import {
  canEditTenantOrganizationBranding,
  loadActorTenantProfile,
  resolveTenantOrganizationId,
  resolveWriteOrganizationId,
  type TenantProfileRow,
  type TenantWriteContext,
} from "../../lib/server-tenant";
import {
  canPickWorkspaceOrganizationForTenantBranding,
  normalizeRoleKeyForBranding,
} from "../../lib/tenant-branding-permissions";
import { getOrganizationLogoUrlFromDb } from "../../lib/organization-logo";
import { normalizeTenantLogoUrl } from "../../lib/tenant-logo-url";
import { isUuidString } from "../../lib/uuid";

/** Runtime diagnostics — enable with `BRANDING_DEBUG=1` or `NEXT_PUBLIC_BRANDING_DEBUG=1` */
const DEBUG =
  process.env.BRANDING_DEBUG === "1" ||
  process.env.NEXT_PUBLIC_BRANDING_DEBUG === "1";
function dlog(...args: unknown[]) {
  if (DEBUG) console.log("[tenant-branding-server]", ...args);
}

/** Same columns as `loadTenantProfile` — used only for precise error strings when resolution fails. */
const PROFILE_SELECT_FOR_DIAGNOSTICS =
  "id, organization_id, full_name, role, role_id, photo_url, team_groups";

/**
 * Explains why `loadActorTenantProfile` returned null (no session, missing profiles row, DB error, or id mismatch).
 * Prefix `[branding]` makes these easy to grep while debugging.
 */
async function buildBrandingProfileResolutionError(clientAid: string): Promise<string> {
  let sessionUid: string | null = null;
  try {
    sessionUid = await getSessionUserIdFromCookies();
  } catch {
    return "[branding] Could not read auth cookies (cookies() failed).";
  }
  if (!sessionUid) {
    return (
      "[branding] No Supabase session user id in cookies. Sign in again, or ensure auth cookies are sent with server actions."
    );
  }

  const s = await supabaseServer
    .from("profiles")
    .select(PROFILE_SELECT_FOR_DIAGNOSTICS)
    .eq("id", sessionUid)
    .maybeSingle();
  if (s.error) {
    return `[branding] profiles query failed for session uid (${sessionUid.slice(0, 8)}…): ${s.error.message}`;
  }
  if (!s.data) {
    return (
      "[branding] No row in public.profiles for the auth session user id. " +
      "This app expects profiles.id = auth.users.id. Create/sync the profile row for this user."
    );
  }

  const c = await supabaseServer
    .from("profiles")
    .select(PROFILE_SELECT_FOR_DIAGNOSTICS)
    .eq("id", clientAid)
    .maybeSingle();
  if (c.error) {
    return `[branding] profiles query failed for client actor id (${clientAid.slice(0, 8)}…): ${c.error.message}`;
  }
  if (!c.data) {
    return (
      "[branding] No public.profiles row for the client actor id. " +
      "Settings must pass profiles.id (same as auth.users.id). Reload the page to refresh actor id."
    );
  }

  if (sessionUid !== clientAid) {
    return (
      `[branding] Session user id (${sessionUid.slice(0, 8)}…) and client actor id (${clientAid.slice(0, 8)}…) differ. ` +
      "Both have profiles rows; resolve the mismatch (sign out/in or hard refresh)."
    );
  }

  return (
    "[branding] Unexpected: profiles rows exist but loadActorTenantProfile returned null. " +
    "Enable BRANDING_DEBUG=1 and check [tenant-branding-server] logs."
  );
}

function buildBrandingOrgResolutionError(
  profile: TenantProfileRow,
  tenantOrgHint: string | null | undefined,
): string {
  const rk = normalizeRoleKeyForBranding(profile.role);
  const hint = (tenantOrgHint ?? "").trim();
  if (canPickWorkspaceOrganizationForTenantBranding(rk)) {
    if (!hint || !isUuidString(hint)) {
      return (
        "[branding] No workspace organization selected. Pick a tenant in the org/workspace switcher, then save again."
      );
    }
    return (
      "[branding] Could not resolve the selected workspace organization. Re-select the organization and try again."
    );
  }
  const home = (profile.organization_id ?? "").trim();
  if (!home) {
    return (
      "[branding] Your profile has no organization_id. Ask an admin to assign you to an organization."
    );
  }
  return "[branding] Resolved organization id is missing or invalid for this save.";
}

export type TenantOrganizationBranding = {
  company_display_name: string;
  logo_url: string;
};

/**
 * Tenant / company branding for the effective organization — sourced only from
 * `organization_settings` (and `organizations.name` as a read fallback when no display name).
 */
export async function getTenantBrandingFromOrganization(
  organizationId?: string | null,
): Promise<TenantOrganizationBranding> {
  const raw = organizationId?.trim() ?? "";
  const oid = raw && isUuidString(raw) ? raw : null;
  dlog("getTenantBrandingFromOrganization", { input: organizationId, resolved: oid });
  if (!oid) {
    return { company_display_name: "", logo_url: "" };
  }

  let company_display_name = "";
  try {
    const { data: osRow } = await supabaseServer
      .from("organization_settings")
      .select("company_display_name")
      .eq("organization_id", oid)
      .maybeSingle();
    const dn = (osRow as { company_display_name?: string | null } | null)?.company_display_name;
    company_display_name = typeof dn === "string" ? dn.trim() : "";
  } catch {
    company_display_name = "";
  }

  if (!company_display_name) {
    try {
      const { data: orgRow } = await supabaseServer
        .from("organizations")
        .select("name")
        .eq("id", oid)
        .maybeSingle();
      const n = (orgRow as { name?: string | null } | null)?.name;
      company_display_name = typeof n === "string" ? n.trim() : "";
    } catch {
      /* keep empty */
    }
  }

  const logoRaw = await getOrganizationLogoUrlFromDb(oid);
  const result = {
    company_display_name,
    logo_url: normalizeTenantLogoUrl(logoRaw),
  };
  dlog("getTenantBrandingFromOrganization → result", result);
  return result;
}

/**
 * Resolves the effective organization from the signed-in profile (and optional client org hint
 * for super_admin), then loads tenant branding. Use this from the client so branding appears
 * without relying on client-only `organizationId` timing.
 */
export async function getTenantBrandingForActor(
  actorProfileId: string | null | undefined,
  organizationIdHint?: string | null,
): Promise<TenantOrganizationBranding> {
  const clientAid = String(actorProfileId ?? "").trim();
  dlog("getTenantBrandingForActor", { actorProfileId: clientAid || null, organizationIdHint });
  if (!clientAid || !isUuidString(clientAid)) {
    dlog("getTenantBrandingForActor → no actor profile id, returning empty");
    return { company_display_name: "", logo_url: "" };
  }
  const profile = await loadActorTenantProfile(clientAid);
  dlog("getTenantBrandingForActor → profile", profile ? {
    id: profile.id,
    role: profile.role,
    role_scope: profile.role_scope,
    organization_id: profile.organization_id,
  } : null);
  if (!profile) {
    dlog("getTenantBrandingForActor → loadActorTenantProfile returned null", { clientAid });
    return { company_display_name: "", logo_url: "" };
  }
  const orgId = await resolveWriteOrganizationId(profile.id, organizationIdHint ?? null);
  dlog("getTenantBrandingForActor → resolved orgId", orgId);
  if (!isUuidString(orgId)) {
    return { company_display_name: "", logo_url: "" };
  }
  return getTenantBrandingFromOrganization(orgId);
}

export type SaveTenantOrganizationBrandingInput = {
  company_display_name: string;
  /** Public URL or empty string to clear the stored logo. */
  logo_url: string | null;
};

/**
 * Persists tenant company name and logo on `organization_settings` only.
 * Allowed roles: see {@link canEditTenantOrganizationBranding} / `tenant-branding-permissions`.
 *
 * Implemented as a single upsert that preserves unrelated columns when the row
 * already exists, so name + logo always land atomically.
 */
export async function saveTenantOrganizationBranding(
  input: SaveTenantOrganizationBrandingInput,
  tenant: TenantWriteContext | null | undefined,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const clientAid = String(tenant?.actorProfileId ?? "").trim();
  if (!clientAid || !isUuidString(clientAid)) {
    dlog("save → missing/invalid actor profile id", { aid: clientAid });
    return { ok: false, error: "Missing or invalid session profile." };
  }

  /**
   * Session-first, then client `actorProfileId` — same chain as `loadActorTenantProfile` in server-tenant.
   * Avoids only querying by session uid when that id has no profiles row yet the client id does.
   */
  const profile = await loadActorTenantProfile(clientAid);
  const allowed = canEditTenantOrganizationBranding(profile);
  dlog("save → permission check", {
    clientAid,
    resolved_profile_id: profile?.id ?? null,
    role_raw: profile?.role ?? null,
    role_normalized: profile ? normalizeRoleKeyForBranding(profile.role) : null,
    role_scope: profile?.role_scope ?? null,
    profile_organization_id: profile?.organization_id ?? null,
    profile_loaded: Boolean(profile),
    canEditTenantOrganizationBranding: allowed,
  });
  if (!profile) {
    dlog("save → loadActorTenantProfile returned null", { clientAid });
    return { ok: false, error: await buildBrandingProfileResolutionError(clientAid) };
  }
  if (!allowed) {
    return { ok: false, error: "You do not have permission to edit company branding." };
  }

  const tenantForWrite: TenantWriteContext = {
    actorProfileId: profile.id,
    organizationId: tenant?.organizationId,
  };
  const orgId = await resolveTenantOrganizationId(tenantForWrite);
  dlog("save → resolved orgId", {
    tenantHint: tenant?.organizationId ?? null,
    orgId,
  });
  if (!isUuidString(orgId)) {
    return { ok: false, error: buildBrandingOrgResolutionError(profile, tenant?.organizationId) };
  }

  // Guard: verify organization actually exists (FK constraint on organization_settings).
  const { data: orgRow, error: orgLookupErr } = await supabaseServer
    .from("organizations")
    .select("id")
    .eq("id", orgId)
    .maybeSingle();
  if (orgLookupErr || !orgRow) {
    dlog("save → org row not found", { orgId, orgLookupErr });
    return {
      ok: false,
      error:
        "Your account is not linked to a valid organization. Contact your administrator to be assigned to an organization.",
    };
  }

  const name = String(input.company_display_name ?? "").trim();
  const logoRaw = input.logo_url != null ? String(input.logo_url).trim() : "";
  const logoForDb = logoRaw ? normalizeTenantLogoUrl(logoRaw) : null;

  // Load existing row so we preserve non-branding columns on upsert.
  const { data: existing } = await supabaseServer
    .from("organization_settings")
    .select(
      "is_ai_label_ocr_enabled, is_ai_packing_slip_ocr_enabled, default_claim_evidence",
    )
    .eq("organization_id", orgId)
    .maybeSingle();

  const ex = existing as
    | {
        is_ai_label_ocr_enabled?: boolean | null;
        is_ai_packing_slip_ocr_enabled?: boolean | null;
        default_claim_evidence?: Record<string, unknown> | null;
      }
    | null;

  const row = {
    organization_id: orgId,
    is_ai_label_ocr_enabled: ex?.is_ai_label_ocr_enabled ?? false,
    is_ai_packing_slip_ocr_enabled: ex?.is_ai_packing_slip_ocr_enabled ?? false,
    default_claim_evidence: ex?.default_claim_evidence ?? {},
    logo_url: logoForDb,
    company_display_name: name || null,
  };

  const { error: upsertErr } = await supabaseServer
    .from("organization_settings")
    .upsert(row, { onConflict: "organization_id" });

  if (upsertErr) {
    dlog("save → upsert failed", upsertErr);
    return { ok: false, error: upsertErr.message };
  }

  dlog("save → ok", { orgId, company_display_name: row.company_display_name, logo_url: row.logo_url });
  return { ok: true };
}
