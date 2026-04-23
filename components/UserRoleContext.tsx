"use client";

import React, {
  createContext, useCallback, useContext, useEffect, useMemo, useState,
} from "react";
import { supabase } from "@/src/lib/supabase";
import {
  getOrganizationNames,
  getWorkspaceViewModeForOrganizationAction,
  listWorkspaceOrganizationsForAdmin,
  type WorkspaceOrganizationOption,
  type WorkspaceViewMode,
} from "../app/session/tenant-actions";
import {
  getViewAsProfileSnapshot,
  listViewAsProfilesForOrganization,
  type ViewAsProfileRow,
} from "../app/session/view-as-actions";
import { normalizeRoleKeyForBranding } from "../lib/tenant-branding-permissions";
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
const LS_VIEW_AS_PROFILE_ID = "workspace_view_as_profile_id";

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
  /** Canonical `roles.key` (preferred) or legacy `profiles.role` text — effective for UI/RBAC (includes “view as” simulation). */
  canonicalRoleKey: string | null;
  /** Signed-in user’s catalog role key only (never the simulated “view as” role). */
  actorCanonicalRoleKey: string | null;
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
  /**
   * True when signed-in user may use org workspace + “view as” (super_admin / programmer / system_admin).
   * Used for tenant scope — not affected by simulated role.
   */
  sessionCanWorkspaceSwitch: boolean;
  /** Simulated member profile id, or null (your own navigation / RBAC). */
  viewAsProfileId: string | null;
  setViewAsProfileId: (profileId: string | null) => void;
  /** Profiles in the current workspace org for the “View as” control. */
  viewAsProfileOptions: ViewAsProfileRow[];
  viewAsProfileOptionsLoading: boolean;
  /** True when navigation reflects another user’s role (UI simulation only). */
  isViewingAsAnotherUser: boolean;
  /** Display name of the simulated user (banner / switcher). */
  viewAsDisplayName: string | null;
  /** Dev-mode only: directly set a mocked role (null = revert to real profile role) */
  setDebugRole: (role: UserRole | null) => void;
  /** Dev-mode only: cycle through all 5 roles in hierarchy order */
  toggleRole: () => void;
  refreshProfile: () => Promise<void>;
  /**
   * Shell UI mode for the **effective** `organizationId`: `internal` org → platform
   * (full platform nav); any other org → tenant (hide platform-only nav for internal staff).
   */
  workspaceViewMode: WorkspaceViewMode;
  /** Becomes true after `organizations.type` is resolved (or when there is no org to resolve). */
  workspaceViewModeReady: boolean;
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

function readStoredViewAsProfileId(): string | null {
  if (typeof window === "undefined") return null;
  const t = window.localStorage.getItem(LS_VIEW_AS_PROFILE_ID)?.trim();
  return t && isUuidString(t) ? t : null;
}

/** One row per org id (RPC / merges can repeat the same UUID). Prefer a human label over raw UUID text. */
function dedupeWorkspaceOrganizations(
  rows: WorkspaceOrganizationOption[],
): WorkspaceOrganizationOption[] {
  const byId = new Map<string, WorkspaceOrganizationOption>();
  for (const r of rows) {
    const id = (r.organization_id ?? "").trim();
    if (!id) continue;
    const prev = byId.get(id);
    if (!prev) {
      byId.set(id, r);
      continue;
    }
    const prevPlain = prev.display_name.trim() === id;
    const nextPlain = r.display_name.trim() === id;
    if (prevPlain && !nextPlain) byId.set(id, r);
    else if (prevPlain === nextPlain && r.display_name.length > prev.display_name.length) {
      byId.set(id, r);
    }
  }
  return [...byId.values()];
}

/** Role keys that use the multi-org workspace picker (internal staff). */
export const INTERNAL_STAFF_ROLE_KEYS = new Set([
  "super_admin",
  "system_admin",
  "programmer",
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
  const [workspaceViewMode, setWorkspaceViewMode] = useState<WorkspaceViewMode>("platform");
  const [workspaceViewModeReady, setWorkspaceViewModeReady] = useState(false);

  const [sessionCanWorkspaceSwitch, setSessionCanWorkspaceSwitch] = useState(false);
  const [viewAsProfileId, setViewAsProfileIdState] = useState<string | null>(null);
  const [viewAsSnapshot, setViewAsSnapshot] = useState<{
    rbacRole: UserRole;
    canonicalKey: string;
    label: string;
    name: string;
    teamGroups: string[];
  } | null>(null);
  const [viewAsProfileOptions, setViewAsProfileOptions] = useState<ViewAsProfileRow[]>([]);
  const [viewAsProfileOptionsLoading, setViewAsProfileOptionsLoading] = useState(false);

  /** Increments each time `loadProfile` runs; stale async completions ignore their results. */
  const loadProfileGenerationRef = React.useRef(0);

  const loadProfile = useCallback(async () => {
    const gen = ++loadProfileGenerationRef.current;
    setProfileLoading(true);
    setProfileError(null);
    try {
      const {
        data: { user },
      } = await supabase.auth.getUser();

      if (gen !== loadProfileGenerationRef.current) return;

      const authUserId = user?.id ?? null;
      setActorUserId(authUserId);

      if (!authUserId) {
        if (gen !== loadProfileGenerationRef.current) return;
        setRbacRole("admin");
        setCanonicalRoleKey(null);
        setCanonicalRoleLabel("User");
        setActorName("Operator");
        setTeamGroups([]);
        setHomeOrganizationName(null);
        setSessionCanWorkspaceSwitch(false);
        setViewAsProfileIdState(null);
        setViewAsSnapshot(null);
        setViewAsProfileOptions([]);
        if (typeof window !== "undefined") {
          window.localStorage.removeItem(LS_VIEW_AS_PROFILE_ID);
        }
        const fallback =
          process.env.NEXT_PUBLIC_ORGANIZATION_ID?.trim() ||
          "00000000-0000-0000-0000-000000000001";
        setHomeOrganizationId(isUuidString(fallback) ? fallback : "00000000-0000-0000-0000-000000000001");
        setSuperAdminOrganizationOverride(null);
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

      if (gen !== loadProfileGenerationRef.current) return;

      if (profileSelectError) {
        setHomeOrganizationName(null);
        setProfileError(profileSelectError.message);
        setSessionCanWorkspaceSwitch(false);
        return;
      }

      if (!profileData || typeof profileData !== "object") {
        setHomeOrganizationName(null);
        setProfileError(`Authenticated user is missing a profiles row (id=${authUserId}).`);
        setSessionCanWorkspaceSwitch(false);
        return;
      }

      const profileRow = profileData as Record<string, unknown>;

      const joined = splitJoined<{ key?: string | null; name?: string | null; scope?: string | null }>(
        profileRow.roles,
      );
      const joinedKey = joined?.key != null ? String(joined.key).trim().toLowerCase() : null;
      const catalogKeyNorm = normalizeRoleKeyForBranding(
        joinedKey ?? String(profileRow.role ?? ""),
      );
      const nr = normalizeRole(catalogKeyNorm || String(profileRow.role ?? ""));
      setRbacRole(nr);

      const canonKey = catalogKeyNorm.length > 0 ? catalogKeyNorm : null;
      setCanonicalRoleKey(canonKey);

      const canWs =
        INTERNAL_STAFF_ROLE_KEYS.has(catalogKeyNorm)
        || nr === "super_admin"
        || nr === "system_employee";
      setSessionCanWorkspaceSwitch(canWs);
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

      let workspaceSelectionId: string | null = null;
      let mergedWorkspaceList: WorkspaceOrganizationOption[] = [];

      if (canWs) {
        const stored = readStoredWorkspaceCompanyId();
        if (stored && isUuidString(stored)) {
          workspaceSelectionId = stored;
        } else if (homeId) {
          workspaceSelectionId = homeId;
        }
        setSuperAdminOrganizationOverride(workspaceSelectionId);

        const orgRes = await listWorkspaceOrganizationsForAdmin();
        if (gen !== loadProfileGenerationRef.current) return;

        const rows: WorkspaceOrganizationOption[] = orgRes.ok ? [...orgRes.rows] : [];
        const ids = new Set(rows.map((r) => r.organization_id));
        if (homeId && !ids.has(homeId)) {
          rows.push({
            organization_id: homeId,
            display_name: joinedName || "Your organization",
          });
          ids.add(homeId);
        }
        if (workspaceSelectionId && !ids.has(workspaceSelectionId)) {
          rows.push({
            organization_id: workspaceSelectionId,
            display_name:
              workspaceSelectionId === homeId && joinedName
                ? joinedName
                : workspaceSelectionId,
          });
          ids.add(workspaceSelectionId);
        }
        mergedWorkspaceList = dedupeWorkspaceOrganizations(rows);
        mergedWorkspaceList.sort((a, b) => a.display_name.localeCompare(b.display_name));
        setWorkspaceOrganizations(mergedWorkspaceList);
      } else {
        setSuperAdminOrganizationOverride(null);
        setWorkspaceOrganizations([]);
      }

      const labelIdSet = new Set<string>();
      if (homeId && isUuidString(homeId)) labelIdSet.add(homeId);
      if (workspaceSelectionId && isUuidString(workspaceSelectionId)) {
        labelIdSet.add(workspaceSelectionId);
      }
      for (const r of mergedWorkspaceList) {
        if (isUuidString(r.organization_id)) labelIdSet.add(r.organization_id);
      }
      if (labelIdSet.size > 0) {
        const nm = await getOrganizationNames([...labelIdSet]);
        if (nm.ok && gen === loadProfileGenerationRef.current) {
          setOrgLabelsById((prev) => {
            const next = { ...prev };
            for (const r of nm.rows) {
              next[r.organization_id] = r.display_name;
            }
            return next;
          });
        }
      }

      if (gen !== loadProfileGenerationRef.current) return;

      if (canWs) {
        const va = readStoredViewAsProfileId();
        if (va && va !== authUserId) {
          setViewAsProfileIdState(va);
        } else {
          setViewAsProfileIdState(null);
          setViewAsSnapshot(null);
          if (typeof window !== "undefined") {
            window.localStorage.removeItem(LS_VIEW_AS_PROFILE_ID);
          }
        }
      } else {
        setViewAsProfileIdState(null);
        setViewAsSnapshot(null);
        setViewAsProfileOptions([]);
        if (typeof window !== "undefined") {
          window.localStorage.removeItem(LS_VIEW_AS_PROFILE_ID);
        }
      }
    } finally {
      if (gen === loadProfileGenerationRef.current) {
        setProfileLoading(false);
      }
    }
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

  const organizationId = useMemo((): string | null => {
    if (sessionCanWorkspaceSwitch) {
      return superAdminOrganizationOverride ?? homeOrganizationId;
    }
    return homeOrganizationId;
  }, [sessionCanWorkspaceSwitch, superAdminOrganizationOverride, homeOrganizationId]);

  useEffect(() => {
    if (profileLoading) {
      return;
    }
    const oid = (organizationId ?? "").trim();
    if (!oid) {
      setWorkspaceViewMode("tenant");
      setWorkspaceViewModeReady(true);
      return;
    }
    let cancelled = false;
    setWorkspaceViewModeReady(false);
    void getWorkspaceViewModeForOrganizationAction(oid).then((res) => {
      if (cancelled) return;
      if (res.ok) {
        setWorkspaceViewMode(res.viewMode);
      } else {
        setWorkspaceViewMode("platform");
      }
      setWorkspaceViewModeReady(true);
    });
    return () => {
      cancelled = true;
    };
  }, [organizationId, profileLoading]);

  useEffect(() => {
    if (!sessionCanWorkspaceSwitch || profileLoading || !organizationId) {
      setViewAsProfileOptions([]);
      setViewAsProfileOptionsLoading(false);
      return;
    }
    let cancelled = false;
    setViewAsProfileOptionsLoading(true);
    void listViewAsProfilesForOrganization(organizationId).then((res) => {
      if (cancelled) return;
      setViewAsProfileOptionsLoading(false);
      if (res.ok) setViewAsProfileOptions(res.rows);
      else setViewAsProfileOptions([]);
    });
    return () => {
      cancelled = true;
    };
  }, [sessionCanWorkspaceSwitch, profileLoading, organizationId]);

  useEffect(() => {
    if (!viewAsProfileId) {
      setViewAsSnapshot(null);
      return;
    }
    if (!organizationId || !sessionCanWorkspaceSwitch || !actorUserId) {
      return;
    }
    if (viewAsProfileId === actorUserId) {
      setViewAsProfileIdState(null);
      setViewAsSnapshot(null);
      if (typeof window !== "undefined") {
        window.localStorage.removeItem(LS_VIEW_AS_PROFILE_ID);
      }
      return;
    }
    let cancelled = false;
    void getViewAsProfileSnapshot(viewAsProfileId, organizationId).then((res) => {
      if (cancelled) return;
      if (!res.ok) {
        setViewAsProfileIdState(null);
        setViewAsSnapshot(null);
        if (typeof window !== "undefined") {
          window.localStorage.removeItem(LS_VIEW_AS_PROFILE_ID);
        }
        return;
      }
      const s = res.snapshot;
      setViewAsSnapshot({
        rbacRole: canonicalRoleKeyToTier(s.canonical_role_key),
        canonicalKey: s.canonical_role_key,
        label: s.role_label,
        name: s.full_name,
        teamGroups: s.team_groups,
      });
    });
    return () => {
      cancelled = true;
    };
  }, [viewAsProfileId, organizationId, sessionCanWorkspaceSwitch, actorUserId]);

  const role = useMemo((): UserRole => {
    if (viewAsSnapshot) return viewAsSnapshot.rbacRole;
    return debugRole ?? rbacRole;
  }, [viewAsSnapshot, debugRole, rbacRole]);

  const effectiveCanonicalRoleKey = useMemo(() => {
    if (viewAsSnapshot) return viewAsSnapshot.canonicalKey;
    return canonicalRoleKey;
  }, [viewAsSnapshot, canonicalRoleKey]);

  const effectiveCanonicalRoleLabel = useMemo(() => {
    if (viewAsSnapshot) return viewAsSnapshot.label;
    return canonicalRoleLabel;
  }, [viewAsSnapshot, canonicalRoleLabel]);

  const effectiveTeamGroups = useMemo(() => {
    if (viewAsSnapshot) return viewAsSnapshot.teamGroups;
    return teamGroups;
  }, [viewAsSnapshot, teamGroups]);

  const isViewingAsAnotherUser = Boolean(viewAsSnapshot && viewAsProfileId);
  const viewAsDisplayName = viewAsSnapshot?.name ?? null;

  const setViewAsProfileId = useCallback(
    (profileId: string | null) => {
      const t = (profileId ?? "").trim();
      if (!t || !isUuidString(t) || t === actorUserId) {
        setViewAsProfileIdState(null);
        setViewAsSnapshot(null);
        if (typeof window !== "undefined") {
          window.localStorage.removeItem(LS_VIEW_AS_PROFILE_ID);
        }
        return;
      }
      setViewAsProfileIdState(t);
      if (typeof window !== "undefined") {
        window.localStorage.setItem(LS_VIEW_AS_PROFILE_ID, t);
      }
    },
    [actorUserId],
  );

  const setWorkspaceOrganizationId = useCallback((id: string) => {
    const t = id.trim();
    if (!isUuidString(t)) return;
    setSuperAdminOrganizationOverride(t);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LS_WORKSPACE_ORGANIZATION, t);
    }
  }, []);

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
      canonicalRoleKey: effectiveCanonicalRoleKey,
      actorCanonicalRoleKey: canonicalRoleKey,
      canonicalRoleLabel: effectiveCanonicalRoleLabel,
      actorName,
      actorUserId,
      homeOrganizationId,
      homeOrganizationName,
      organizationId,
      organizationName,
      profileLoading,
      profileError,
      teamGroups: effectiveTeamGroups,
      workspaceOrganizations,
      setWorkspaceOrganizationId,
      sessionCanWorkspaceSwitch,
      viewAsProfileId,
      setViewAsProfileId,
      viewAsProfileOptions,
      viewAsProfileOptionsLoading,
      isViewingAsAnotherUser,
      viewAsDisplayName,
      setDebugRole,
      toggleRole,
      refreshProfile: loadProfile,
      workspaceViewMode,
      workspaceViewModeReady,
    }),
    [
      role,
      effectiveCanonicalRoleKey,
      canonicalRoleKey,
      effectiveCanonicalRoleLabel,
      actorName,
      actorUserId,
      homeOrganizationId,
      homeOrganizationName,
      organizationId,
      organizationName,
      profileLoading,
      profileError,
      effectiveTeamGroups,
      workspaceOrganizations,
      setWorkspaceOrganizationId,
      sessionCanWorkspaceSwitch,
      viewAsProfileId,
      setViewAsProfileId,
      viewAsProfileOptions,
      viewAsProfileOptionsLoading,
      isViewingAsAnotherUser,
      viewAsDisplayName,
      setDebugRole,
      toggleRole,
      loadProfile,
      workspaceViewMode,
      workspaceViewModeReady,
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
      actorCanonicalRoleKey: null,
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
      sessionCanWorkspaceSwitch: false,
      viewAsProfileId: null,
      setViewAsProfileId: () => {},
      viewAsProfileOptions: [],
      viewAsProfileOptionsLoading: false,
      isViewingAsAnotherUser: false,
      viewAsDisplayName: null,
      setDebugRole: () => {},
      toggleRole: () => {},
      refreshProfile: async () => {},
      workspaceViewMode: "platform",
      workspaceViewModeReady: true,
    };
  }
  return ctx;
}

export type { WorkspaceViewMode } from "../app/session/tenant-actions";
