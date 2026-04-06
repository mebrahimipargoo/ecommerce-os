"use client";

/**
 * <RoleGuard minRole="admin"> ... </RoleGuard>
 *
 * Renders children only when the authenticated user's role is at or above
 * `minRole` in the 5-tier RBAC hierarchy. When access is denied, renders
 * `fallback` (defaults to null — invisible, no layout impact).
 *
 * 5-tier hierarchy (lowest → highest privilege):
 *   operator → employee → admin → system_employee → super_admin
 *
 * Architecture:
 * — Consumes useRbacPermissions() — the single source of truth for UI RBAC.
 * — Zero role string comparisons here; isAtLeast() is defined in the hook.
 * — Wrap routes, sections, buttons, or individual UI elements — all are valid.
 *
 * @example
 *   // Hide a menu item from operators and employees:
 *   <RoleGuard minRole="admin">
 *     <ImportButton />
 *   </RoleGuard>
 *
 *   // Show an access-denied fallback for non-super-admins:
 *   <RoleGuard minRole="super_admin" fallback={<AccessDeniedBanner />}>
 *     <PlatformSettingsPanel />
 *   </RoleGuard>
 *
 *   // Wrap an entire page section:
 *   <RoleGuard minRole="employee">
 *     <ClaimEngineSection />
 *   </RoleGuard>
 */

import React from "react";
import { useRbacPermissions } from "../hooks/useRbacPermissions";
import type { UserRole } from "./UserRoleContext";

export interface RoleGuardProps {
  /**
   * Minimum role required to render children.
   * Any role at this tier or above in the hierarchy will pass.
   */
  minRole: UserRole;
  /**
   * Element to render when access is denied.
   * Defaults to null (renders nothing, no layout shift).
   */
  fallback?: React.ReactNode;
  children: React.ReactNode;
}

export function RoleGuard({ minRole, fallback = null, children }: RoleGuardProps) {
  const { isAtLeast } = useRbacPermissions();
  if (!isAtLeast(minRole)) return <>{fallback}</>;
  return <>{children}</>;
}
