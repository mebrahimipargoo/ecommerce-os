"use client";

import React, {
  createContext, useCallback, useContext, useEffect, useMemo, useState,
} from "react";
import {
  fetchUserProfileById,
  listWorkspaceOrganizationsForAdmin,
  type WorkspaceOrganizationOption,
} from "../app/session/tenant-actions";
import { isUuidString } from "../lib/uuid";
import { useDebugMode } from "./DebugModeContext";

// ─── 5-Tier Role Hierarchy ───────────────────────────────────────────────────
//  Ordered from lowest privilege → highest privilege.
//  This order is used by useRbacPermissions for isAtLeast() comparisons.

export type UserRole =
  | "operator"         // warehouse worker — WMS tools only
  | "employee"         // office worker — ops modules, no settings/users/imports
  | "admin"            // org-level admin — full org access + settings + users
  | "system_employee"  // internal platform staff — multi-org read + limited write
  | "super_admin";     // platform owner — unrestricted access

export const ROLE_HIERARCHY: UserRole[] = [
  "operator",
  "employee",
  "admin",
  "system_employee",
  "super_admin",
];

const LS_WORKSPACE_ORGANIZATION = "workspace_selected_organization_id";

// ─── Context Shape ────────────────────────────────────────────────────────────

type UserRoleContextValue = {
  role: UserRole;
  actorName: string;
  /** Resolved workspace user profile id (`profiles.id`) for audit trails and tenant bootstrap */
  actorUserId: string | null;
  /** Home organization from profile (`profiles.organization_id`) */
  homeOrganizationId: string | null;
  /** Tenant scope for logistics data — effective `organization_id` (super_admin may override via workspace picker) */
  organizationId: string | null;
  profileLoading: boolean;
  profileError: string | null;
  /** GBAC foundation: group/team slugs from `profiles.team_groups` JSONB */
  teamGroups: string[];
  /** super_admin: organizations for header switcher */
  workspaceOrganizations: WorkspaceOrganizationOption[];
  /** super_admin: persist selected organization (also in localStorage) */
  setWorkspaceOrganizationId: (id: string) => void;
  /** Dev-mode only: directly set a mocked role (null = revert to real profile role) */
  setDebugRole: (role: UserRole | null) => void;
  /** Dev-mode only: cycle through all 5 roles in hierarchy order */
  toggleRole: () => void;
  refreshProfile: () => Promise<void>;
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * True for roles that have organization-admin privileges.
 * Used by AdminWorkspaceGate and admin route guards.
 */
export function isAdminRole(role: UserRole): boolean {
  return role === "admin" || role === "system_employee" || role === "super_admin";
}

function readActorUserId(): string | null {
  if (typeof window === "undefined") return null;
  const fromStore = window.localStorage.getItem("current_user_profile_id")?.trim();
  if (fromStore && isUuidString(fromStore)) return fromStore;
  const env = process.env.NEXT_PUBLIC_CURRENT_USER_PROFILE_ID?.trim();
  return env && isUuidString(env) ? env : null;
}

function readStoredWorkspaceCompanyId(): string | null {
  if (typeof window === "undefined") return null;
  const t = window.localStorage.getItem(LS_WORKSPACE_ORGANIZATION)?.trim();
  return t && isUuidString(t) ? t : null;
}

function normalizeRole(raw: string | null | undefined): UserRole {
  const r = (raw ?? "").trim().toLowerCase();
  if (r === "super_admin")     return "super_admin";
  if (r === "system_employee") return "system_employee";
  if (r === "admin")           return "admin";
  if (r === "employee")        return "employee";
  return "operator";
}

// ─── Provider ─────────────────────────────────────────────────────────────────

const UserRoleContext = createContext<UserRoleContextValue | null>(null);

export function UserRoleProvider({ children }: { children: React.ReactNode }) {
  const { debugMode, setDebugMode } = useDebugMode();

  const [profileRole, setProfileRole]               = useState<UserRole>("operator");
  const [debugRole,   setDebugRoleState]            = useState<UserRole | null>(null);
  const [actorUserId, setActorUserId]               = useState<string | null>(null);
  const [homeOrganizationId, setHomeOrganizationId] = useState<string | null>(null);
  const [teamGroups,  setTeamGroups]                = useState<string[]>([]);
  const [superAdminOrganizationOverride, setSuperAdminOrganizationOverride] = useState<string | null>(null);
  const [workspaceOrganizations, setWorkspaceOrganizations] = useState<WorkspaceOrganizationOption[]>([]);
  const [actorName,       setActorName]       = useState("Operator");
  const [profileLoading,  setProfileLoading]  = useState(true);
  const [profileError,    setProfileError]    = useState<string | null>(null);

  const loadProfile = useCallback(async () => {
    setProfileLoading(true);
    setProfileError(null);
    const id = readActorUserId();
    setActorUserId(id);
    if (!id) {
      setProfileRole("admin");
      setActorName("Operator");
      setTeamGroups([]);
      const fallback =
        process.env.NEXT_PUBLIC_ORGANIZATION_ID?.trim() ||
        "00000000-0000-0000-0000-000000000001";
      setHomeOrganizationId(isUuidString(fallback) ? fallback : "00000000-0000-0000-0000-000000000001");
      setSuperAdminOrganizationOverride(null);
      setProfileLoading(false);
      return;
    }
    const res = await fetchUserProfileById(id);
    if (!res.ok) {
      setProfileError(res.error);
      setProfileLoading(false);
      return;
    }
    const p = res.profile;
    const nr = normalizeRole(p.role);
    setProfileRole(nr);
    setActorName((p.full_name ?? "").trim() || p.email || "User");
    setTeamGroups(p.team_groups ?? []);
    const cid = (p.organization_id ?? "").trim();
    setHomeOrganizationId(cid && isUuidString(cid) ? cid : null);

    if (nr === "super_admin" || nr === "system_employee") {
      const stored = readStoredWorkspaceCompanyId();
      const pick   = stored && isUuidString(stored) ? stored : cid;
      setSuperAdminOrganizationOverride(pick && isUuidString(pick) ? pick : null);
      void listWorkspaceOrganizationsForAdmin().then((orgRes) => {
        if (!orgRes.ok) return;
        const rows = [...orgRes.rows];
        const ids  = new Set(rows.map((r) => r.organization_id));
        if (cid && isUuidString(cid) && !ids.has(cid)) {
          rows.push({ organization_id: cid, display_name: "Your workspace" });
        }
        rows.sort((a, b) => a.display_name.localeCompare(b.display_name));
        setWorkspaceOrganizations(rows);
      });
    } else {
      setSuperAdminOrganizationOverride(null);
      setWorkspaceOrganizations([]);
    }
    setProfileLoading(false);
  }, []);

  useEffect(() => { void loadProfile(); }, [loadProfile]);

  // Reset debug role when debug mode is turned off
  useEffect(() => {
    if (!debugMode) setDebugRoleState(null);
  }, [debugMode]);

  // Effective role: debug override wins when debug mode is active
  const role = useMemo((): UserRole => {
    if (debugMode && debugRole !== null) return debugRole;
    return profileRole;
  }, [debugMode, debugRole, profileRole]);

  const setWorkspaceOrganizationId = useCallback((id: string) => {
    const t = id.trim();
    if (!isUuidString(t)) return;
    setSuperAdminOrganizationOverride(t);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LS_WORKSPACE_ORGANIZATION, t);
    }
  }, []);

  const organizationId = useMemo((): string | null => {
    if (role === "super_admin" || role === "system_employee") {
      return superAdminOrganizationOverride ?? homeOrganizationId;
    }
    return homeOrganizationId;
  }, [role, superAdminOrganizationOverride, homeOrganizationId]);

  /** Dev-mode: directly set any of the 5 tiers (null = revert to real role).
   *  Auto-enables debug mode if it is not already on. */
  const setDebugRole = useCallback((r: UserRole | null) => {
    if (!debugMode) setDebugMode(true);
    setDebugRoleState(r);
  }, [debugMode, setDebugMode]);

  /** Dev-mode: cycle through all 5 roles in hierarchy order. Auto-enables debug mode. */
  const toggleRole = useCallback(() => {
    if (!debugMode) setDebugMode(true);
    setDebugRoleState((prev) => {
      const cur = prev ?? profileRole;
      const idx = ROLE_HIERARCHY.indexOf(cur);
      return ROLE_HIERARCHY[(idx + 1) % ROLE_HIERARCHY.length];
    });
  }, [debugMode, setDebugMode, profileRole]);

  const value = useMemo(
    () => ({
      role,
      actorName,
      actorUserId,
      homeOrganizationId,
      organizationId,
      profileLoading,
      profileError,
      teamGroups,
      workspaceOrganizations,
      setWorkspaceOrganizationId,
      setDebugRole,
      toggleRole,
      refreshProfile: loadProfile,
    }),
    [
      role, actorName, actorUserId, homeOrganizationId, organizationId,
      profileLoading, profileError, teamGroups, workspaceOrganizations,
      setWorkspaceOrganizationId, setDebugRole, toggleRole, loadProfile,
    ],
  );

  return (
    <UserRoleContext.Provider value={value}>{children}</UserRoleContext.Provider>
  );
}

export function useUserRole(): UserRoleContextValue {
  const ctx = useContext(UserRoleContext);
  if (!ctx) {
    return {
      role: "admin",
      actorName: "Operator",
      actorUserId: null,
      homeOrganizationId: null,
      organizationId: null,
      profileLoading: false,
      profileError: null,
      teamGroups: [],
      workspaceOrganizations: [],
      setWorkspaceOrganizationId: () => {},
      setDebugRole: () => {},
      toggleRole: () => {},
      refreshProfile: async () => {},
    };
  }
  return ctx;
}
