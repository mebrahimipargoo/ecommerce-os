"use client";

/**
 * useRbacPermissions — Single source of truth for UI-layer access control.
 *
 * Architecture rule: ALL visibility/permission logic lives here.
 * UI components MUST NOT contain role comparisons — they import this hook instead.
 *
 * 5-Tier hierarchy (lowest → highest):
 *   operator  →  employee  →  admin  →  system_employee  →  super_admin
 */

import { useMemo } from "react";
import {
  useUserRole,
  ROLE_HIERARCHY,
  INTERNAL_DEV_BADGE_ROLE_KEYS,
  type UserRole,
} from "../components/UserRoleContext";
import { canManagePlatformAccessCatalog } from "../lib/platform-access-management";
import {
  canEditTenantOrganizationBrandingByRoleKey,
  normalizeRoleKeyForBranding,
} from "../lib/tenant-branding-permissions";

export type RbacPermissions = {
  // ── Operations (visible to everyone except pure operator) ──
  canSeeDashboard:       boolean;
  canSeeReturns:         boolean;
  canSeeClaimEngine:     boolean;
  canSeeReportHistory:   boolean;

  // ── System (admin+) ────────────────────────────────────────
  canSeeSettings:        boolean;
  canSeeUsers:           boolean;

  // ── System Admin panel (admin+) ───────────────────────────
  canSeeImports:         boolean;
  canSeeSystemAdmin:     boolean;

  /**
   * Platform Settings nav (`/platform/*` orgs, branding, platform users) — signed-in `super_admin` only (canonical role key).
   */
  canSeePlatformAdmin:   boolean;

  /**
   * Global user directory (`/platform/users`) — same gate as {@link canSeePlatformAdmin}.
   */
  canSeePlatformUserDirectory: boolean;

  /** Provision new tenant org (`/platform/organizations/new`) — same gate as {@link canSeePlatformAdmin}. */
  canSeeCreateOrganization: boolean;

  /**
   * Platform access management (`/platform/access`) — signed-in catalog roles:
   * `super_admin`, `programmer`, `system_admin` (via {@link canManagePlatformAccessCatalog}).
   */
  canSeePlatformAccess: boolean;

  /** Tenant company name / logo on `organization_settings` — tenant_admin (tier `admin`), super_admin, and catalog `programmer`. */
  canEditTenantBranding: boolean;

  /**
   * Tech Debug slide-over panel (granular debug flags + mock role tier).
   * Internal technical catalog roles; see implementation.
   */
  canSeeTechDebug:       boolean;

  // ── WMS (all roles; ONLY section shown to operator) ───────
  canSeeWmsTools:        boolean;
  /**
   * True when the sidebar should show WMS section ONLY.
   * Operators see no other nav items.
   */
  isWmsOnly:             boolean;

  /**
   * True when the workspace org switcher should be visible
   * (super_admin and system_employee managing multiple orgs).
   */
  canSwitchOrganization: boolean;

  /**
   * When false, the effective org is a tenant company: hide platform-only nav and
   * use tenant-style UI, even for `super_admin` (backend authority unchanged).
   */
  isPlatformShellView: boolean;

  /**
   * Utility: returns true when the current role is at or above `minRole`
   * in the 5-tier hierarchy.
   *
   * @example perms.isAtLeast("admin") // true for admin, system_employee, super_admin
   */
  isAtLeast: (minRole: UserRole) => boolean;
};

export function useRbacPermissions(): RbacPermissions {
  const {
    role,
    canonicalRoleKey,
    actorCanonicalRoleKey,
    workspaceViewMode,
    workspaceViewModeReady,
  } = useUserRole();
  const ck = (canonicalRoleKey ?? "").trim().toLowerCase();
  /** Workspace chrome must follow the signed-in account, not “view as” simulation. */
  const actorCk = (actorCanonicalRoleKey ?? "").trim().toLowerCase();
  const actorNorm = normalizeRoleKeyForBranding(actorCanonicalRoleKey);

  return useMemo((): RbacPermissions => {
    const isAtLeast = (minRole: UserRole): boolean =>
      ROLE_HIERARCHY.indexOf(role) >= ROLE_HIERARCHY.indexOf(minRole);

    const isOperator = role === "operator";
    /** Super_admin (actor) in tenant org context: hide platform nav until type resolves, then follow org type. */
    const isPlatformShellView =
      !workspaceViewModeReady || workspaceViewMode === "platform";

    const canPlatformSettings = actorNorm === "super_admin" && isPlatformShellView;

    return {
      // Ops modules: everyone except pure warehouse operators
      canSeeDashboard:      !isOperator,
      canSeeReturns:        !isOperator,
      canSeeClaimEngine:    !isOperator,
      canSeeReportHistory:  !isOperator,

      // System tab: admin and above
      canSeeSettings:       isAtLeast("admin"),
      canSeeUsers:          isAtLeast("admin"),

      // System Admin panel (Imports, Debug): admin and above
      canSeeImports:        isAtLeast("admin"),
      canSeeSystemAdmin:    isAtLeast("admin"),

      canSeePlatformAdmin: canPlatformSettings,

      canSeePlatformUserDirectory: canPlatformSettings,

      canSeeCreateOrganization: canPlatformSettings,

      canSeePlatformAccess: canManagePlatformAccessCatalog(actorCanonicalRoleKey),

      // Tenant org branding: canonical role keys only (not 5-tier UI labels).
      canEditTenantBranding: canEditTenantOrganizationBrandingByRoleKey(canonicalRoleKey),

      // Tech Debug panel: super_admin tier, or internal staff by catalog role key.
      canSeeTechDebug:
        isPlatformShellView
        && (role === "super_admin" || INTERNAL_DEV_BADGE_ROLE_KEYS.has(ck)),

      // WMS: available to all roles; operators see only this section
      canSeeWmsTools:       true,
      isWmsOnly:            isOperator,

      // Multi-org switcher: platform-level staff (actor only — stays on while viewing as a tenant user)
      canSwitchOrganization:
        actorCk === "super_admin" || actorCk === "programmer" || actorCk === "system_admin",

      isPlatformShellView,

      isAtLeast,
    };
  }, [
    role,
    ck,
    actorCk,
    actorNorm,
    actorCanonicalRoleKey,
    canonicalRoleKey,
    workspaceViewMode,
    workspaceViewModeReady,
  ]);
}
