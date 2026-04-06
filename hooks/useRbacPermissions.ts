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
import { useUserRole, ROLE_HIERARCHY, type UserRole } from "../components/UserRoleContext";

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

  // ── Platform (super_admin only) ───────────────────────────
  canSeePlatformAdmin:   boolean;

  /**
   * Tech Debug slide-over panel (granular debug flags). super_admin only.
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
   * Utility: returns true when the current role is at or above `minRole`
   * in the 5-tier hierarchy.
   *
   * @example perms.isAtLeast("admin") // true for admin, system_employee, super_admin
   */
  isAtLeast: (minRole: UserRole) => boolean;
};

export function useRbacPermissions(): RbacPermissions {
  const { role } = useUserRole();

  return useMemo((): RbacPermissions => {
    const isAtLeast = (minRole: UserRole): boolean =>
      ROLE_HIERARCHY.indexOf(role) >= ROLE_HIERARCHY.indexOf(minRole);

    const isOperator = role === "operator";

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

      // Platform menu: super_admin only
      canSeePlatformAdmin:  role === "super_admin",

      canSeeTechDebug:      role === "super_admin",

      // WMS: available to all roles; operators see only this section
      canSeeWmsTools:       true,
      isWmsOnly:            isOperator,

      // Multi-org switcher: platform-level staff
      canSwitchOrganization: role === "super_admin" || role === "system_employee",

      isAtLeast,
    };
  }, [role]);
}
