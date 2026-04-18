"use client";

import React, {
  createContext, useCallback, useContext, useEffect, useMemo, useState,
} from "react";
import { supabase } from "@/src/lib/supabase";
import {
  getOrganizationNames,
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

function splitJoined<T>(raw: unknown): T | null {
  if (raw == null) return null;
  if (Array.isArray(raw)) return (raw[0] as T | undefined) ?? null;
  return raw as T;
}

function titleCaseRoleKey(key: string | null | undefined): string {
  const k = (key ?? "").trim().toLowerCase();
  if (!k) return "User";
  return k
    .split("_")
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

// ─── Context Shape ────────────────────────────────────────────────────────────

type UserRoleContextValue = {
  /**
   * Effective tier for RBAC / route gates (`normalizeRole` + optional dev override).
   */
  role: UserRole;
  /** Canonical `roles.key` (preferred) or legacy `profiles.role` text. */
  canonicalRoleKey: string | null;
  /** Catalog `roles.name` when the join resolves; otherwise derived from the key. */
  canonicalRoleLabel: string;
  actorName: string;
  /** Resolved workspace user profile id (`profiles.id`) for audit trails and tenant bootstrap */
  actorUserId: string | null;
  /** Home organization from profile (`profiles.organization_id`) */
  homeOrganizationId: string | null;
  /** `organizations.name` for the profile home org (when readable via RLS). */
  homeOrganizationName: string | null;
  /** Tenant scope for logistics data — effective `organization_id` (internal staff may use workspace override). */
  organizationId: string | null;
  /** Human-readable label for `organizationId` (org name, workspace list fallback, or “No Organization”). */
  organizationName: string;
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

function readStoredWorkspaceCompanyId(): string | null {
  if (typeof window === "undefined") return null;
  const t = window.localStorage.getItem(LS_WORKSPACE_ORGANIZATION)?.trim();
  return t && isUuidString(t) ? t : null;
}

/** Role keys that use the multi-org workspace picker (internal staff). */
export const INTERNAL_STAFF_ROLE_KEYS = new Set([
  "super_admin",
  "system_admin",
  "system_employee",
  "programmer",
  "customer_service",
]);

/**
 * Maps catalog / legacy `roles.key` (or `profiles.role`) to the 5-tier RBAC tier
 * used for badge colors and `isAtLeast()` comparisons.
 */
export function canonicalRoleKeyToTier(raw: string | null | undefined): UserRole {
  const r = (raw ?? "").trim().toLowerCase();
  if (r === "super_admin") return "super_admin";
  if (
    r === "system_employee"
    || r === "system_admin"
    || r === "programmer"
    || r === "customer_service"
  ) {
    return "system_employee";
  }
  if (r === "tenant_admin") return "admin";
  if (r === "admin") return "admin";
  if (r === "employee") return "employee";
  if (r === "operator") return "operator";
  return "operator";
}

/** Role keys that may see internal DEV chrome when Technical debug is on. */
export const INTERNAL_DEV_BADGE_ROLE_KEYS = new Set([
  "super_admin",
  "programmer",
  "system_admin",
]);

export function canShowInternalDevBadge(canonicalRoleKey: string | null | undefined): boolean {
  const k = (canonicalRoleKey ?? "").trim().toLowerCase();
  return INTERNAL_DEV_BADGE_ROLE_KEYS.has(k);
}

function normalizeRole(raw: string | null | undefined): UserRole {
  return canonicalRoleKeyToTier(raw);
}

// ─── Provider ─────────────────────────────────────────────────────────────────

const UserRoleContext = createContext<UserRoleContextValue | null>(null);

export function UserRoleProvider({ children }: { children: React.ReactNode }) {
  const { debugMode, setDebugMode } = useDebugMode();

  const [rbacRole, setRbacRole]                     = useState<UserRole>("operator");
  const [canonicalRoleKey, setCanonicalRoleKey]   = useState<string | null>(null);
  const [canonicalRoleLabel, setCanonicalRoleLabel] = useState<string>("User");
  const [debugRole,   setDebugRoleState]            = useState<UserRole | null>(null);
  const [actorUserId, setActorUserId]               = useState<string | null>(null);
  const [homeOrganizationId, setHomeOrganizationId] = useState<string | null>(null);
  const [homeOrganizationName, setHomeOrganizationName] = useState<string | null>(null);
  const [teamGroups,  setTeamGroups]                = useState<string[]>([]);
  const [superAdminOrganizationOverride, setSuperAdminOrganizationOverride] = useState<string | null>(null);
  const [workspaceOrganizations, setWorkspaceOrganizations] = useState<WorkspaceOrganizationOption[]>([]);
  const [actorName,       setActorName]       = useState("Operator");
  const [profileLoading,  setProfileLoading]  = useState(true);
  const [profileError,    setProfileError]    = useState<string | null>(null);
  const [orgLabelsById,   setOrgLabelsById]   = useState<Record<string, string>>({});

  const loadProfile = useCallback(async () => {
    setProfileLoading(true);
    setProfileError(null);
    const {
      data: { user },
    } = await supabase.auth.getUser();

    const authUserId = user?.id ?? null;
    setActorUserId(authUserId);

    if (!authUserId) {
      setRbacRole("admin");
      setCanonicalRoleKey(null);
      setCanonicalRoleLabel("User");
      setActorName("Operator");
      setTeamGroups([]);
      setHomeOrganizationName(null);
      const fallback =
        process.env.NEXT_PUBLIC_ORGANIZATION_ID?.trim() ||
        "00000000-0000-0000-0000-000000000001";
      setHomeOrganizationId(isUuidString(fallback) ? fallback : "00000000-0000-0000-0000-000000000001");
      setSuperAdminOrganizationOverride(null);
      setProfileLoading(false);
      return;
    }

    const { data: profileData, error: profileSelectError } = await supabase
      .from("profiles")
      .select(
        [
          "id, organization_id, full_name, role, team_groups",
          "roles!profiles_role_id_fkey(key, name, scope)",
          "organizations!profiles_organization_id_fkey(name)",
        ].join(", "),
      )
      .eq("id", authUserId)
      .maybeSingle();

    if (profileSelectError) {
      setHomeOrganizationName(null);
      setProfileError(profileSelectError.message);
      setProfileLoading(false);
      return;
    }

    if (!profileData || typeof profileData !== "object") {
      setHomeOrganizationName(null);
      setProfileError(`Authenticated user is missing a profiles row (id=${authUserId}).`);
      setProfileLoading(false);
      return;
    }

    const profileRow = profileData as Record<string, unknown>;

    const joined = splitJoined<{ key?: string | null; name?: string | null; scope?: string | null }>(
      profileRow.roles,
    );
    const joinedKey = joined?.key != null ? String(joined.key).trim().toLowerCase() : null;
    const rawKey = (joinedKey ?? String(profileRow.role ?? "").trim().toLowerCase());
    const nr = normalizeRole(rawKey || String(profileRow.role ?? ""));
    setRbacRole(nr);

    const canonKey = (joinedKey ?? String(profileRow.role ?? "").trim().toLowerCase()) || null;
    setCanonicalRoleKey(canonKey);
    const catalogName =
      joined?.name != null && String(joined.name).trim().length > 0
        ? String(joined.name).trim()
        : titleCaseRoleKey(canonKey);
    setCanonicalRoleLabel(catalogName);

    setActorName(String(profileRow.full_name ?? "").trim() || "User");
    setTeamGroups(Array.isArray(profileRow.team_groups) ? profileRow.team_groups.map(String) : []);
    const cid = String(profileRow.organization_id ?? "").trim();
    const homeId = cid && isUuidString(cid) ? cid : null;
    setHomeOrganizationId(homeId);

    const orgEmbed = splitJoined<{ name?: string | null }>(profileRow.organizations);
    const joinedName = orgEmbed != null && typeof orgEmbed.name === "string" ? orgEmbed.name.trim() : "";
    setHomeOrganizationName(joinedName.length > 0 ? joinedName : null);

    let internalEffectiveId: string | null = null;
    if (INTERNAL_STAFF_ROLE_KEYS.has(rawKey)) {
      const stored = readStoredWorkspaceCompanyId();
      const pick   = stored && isUuidString(stored) ? stored : (homeId ?? "");
      internalEffectiveId = pick && isUuidString(pick) ? pick : null;
      setSuperAdminOrganizationOverride(internalEffectiveId);
      void listWorkspaceOrganizationsForAdmin().then((orgRes) => {
        if (!orgRes.ok) return;
        const rows = [...orgRes.rows];
        const ids  = new Set(rows.map((r) => r.organization_id));
        if (homeId && !ids.has(homeId)) {
          rows.push({ organization_id: homeId, display_name: "Your workspace" });
        }
        rows.sort((a, b) => a.display_name.localeCompare(b.display_name));
        setWorkspaceOrganizations(rows);
      });
    } else {
      setSuperAdminOrganizationOverride(null);
      setWorkspaceOrganizations([]);
    }

    const idsToLabel = [
      ...new Set(
        [homeId, internalEffectiveId].filter((x): x is string => !!x && isUuidString(x)),
      ),
    ];
    if (idsToLabel.length > 0) {
      const nm = await getOrganizationNames(idsToLabel);
      if (nm.ok) {
        setOrgLabelsById((prev) => {
          const next = { ...prev };
          for (const r of nm.rows) {
            next[r.organization_id] = r.display_name;
          }
          return next;
        });
      }
    }

    setProfileLoading(false);
  }, []);

  useEffect(() => { void loadProfile(); }, [loadProfile]);

  // Keep profile/role in sync with auth transitions (login/logout/token refresh).
  useEffect(() => {
    const { data: subscription } = supabase.auth.onAuthStateChange(() => {
      void loadProfile();
    });
    return () => {
      subscription.subscription.unsubscribe();
    };
  }, [loadProfile]);

  // Reset debug role when debug mode is turned off
  useEffect(() => {
    if (!debugMode) setDebugRoleState(null);
  }, [debugMode]);

  const role = useMemo((): UserRole => debugRole ?? rbacRole, [debugRole, rbacRole]);

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

  /**
   * Human-readable label for the effective `organizationId` (logistics / tenant scope).
   * Not app product branding (`BrandingContext`); not necessarily a custom logo asset.
   *
   * Resolution order:
   *   1. `orgLabelsById[id]` — filled by `getOrganizationNames()` in tenant-actions:
   *      prefers `organization_settings.company_display_name`, else `organizations.name`,
   *      else the UUID string.
   *   2. If `id === homeOrganizationId`, the joined `organizations.name` from the profile
   *      load (`homeOrganizationName`).
   *   3. `workspaceOrganizations` row for this id (RPC list; may be UUID fallback).
   *   4. Raw `organizationId` UUID.
   */
  const organizationName = useMemo((): string => {
    const id = organizationId;
    if (!id) return "No Organization";
    const cached = (orgLabelsById[id] ?? "").trim();
    if (cached) return cached;
    if (id === homeOrganizationId) {
      const home = (homeOrganizationName ?? "").trim();
      if (home) return home;
    }
    const fromList = workspaceOrganizations.find((o) => o.organization_id === id)?.display_name?.trim();
    if (fromList) return fromList;
    return id;
  }, [
    organizationId,
    homeOrganizationId,
    homeOrganizationName,
    workspaceOrganizations,
    orgLabelsById,
  ]);

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
      const cur = prev ?? rbacRole;
      const idx = ROLE_HIERARCHY.indexOf(cur);
      return ROLE_HIERARCHY[(idx + 1) % ROLE_HIERARCHY.length];
    });
  }, [debugMode, setDebugMode, rbacRole]);

  const value = useMemo(
    () => ({
      role,
      canonicalRoleKey,
      canonicalRoleLabel,
      actorName,
      actorUserId,
      homeOrganizationId,
      homeOrganizationName,
      organizationId,
      organizationName,
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
      role, canonicalRoleKey, canonicalRoleLabel, actorName, actorUserId,
      homeOrganizationId, homeOrganizationName,
      organizationId, organizationName,
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
      role: "operator",
      canonicalRoleKey: null,
      canonicalRoleLabel: "User",
      actorName: "User",
      actorUserId: null,
      homeOrganizationId: null,
      homeOrganizationName: null,
      organizationId: null,
      organizationName: "No Organization",
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
