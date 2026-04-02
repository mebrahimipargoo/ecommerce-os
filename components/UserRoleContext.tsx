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

export type UserRole = "operator" | "admin" | "super_admin";

const LS_WORKSPACE_COMPANY = "workspace_selected_company_id";

type UserRoleContextValue = {
  role: UserRole;
  actorName: string;
  /** Resolved workspace user profile id (`profiles.id`) for audit trails and tenant bootstrap */
  actorUserId: string | null;
  /** Home company from profile (`profiles.company_id`) */
  homeCompanyId: string | null;
  /** Tenant scope for logistics data — effective company UUID (super_admin may override via workspace picker) */
  organizationId: string | null;
  profileLoading: boolean;
  profileError: string | null;
  /** super_admin: companies for header switcher */
  workspaceCompanies: WorkspaceOrganizationOption[];
  /** super_admin: persist selected company (also in localStorage) */
  setWorkspaceCompanyId: (id: string) => void;
  /** Debug only: cycle operator → admin → super_admin */
  toggleRole: () => void;
  refreshProfile: () => Promise<void>;
};

export function isAdminRole(role: UserRole): boolean {
  return role === "admin" || role === "super_admin";
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
  const t = window.localStorage.getItem(LS_WORKSPACE_COMPANY)?.trim();
  return t && isUuidString(t) ? t : null;
}

function normalizeRole(raw: string | null | undefined): UserRole {
  const r = (raw ?? "").trim().toLowerCase();
  if (r === "super_admin") return "super_admin";
  if (r === "admin") return "admin";
  return "operator";
}

const UserRoleContext = createContext<UserRoleContextValue | null>(null);

export function UserRoleProvider({ children }: { children: React.ReactNode }) {
  const { debugMode } = useDebugMode();
  const [profileRole, setProfileRole] = useState<UserRole>("operator");
  const [debugRole, setDebugRole] = useState<UserRole | null>(null);
  const [actorUserId, setActorUserId] = useState<string | null>(null);
  const [homeCompanyId, setHomeCompanyId] = useState<string | null>(null);
  const [superAdminCompanyOverride, setSuperAdminCompanyOverride] = useState<string | null>(null);
  const [workspaceCompanies, setWorkspaceCompanies] = useState<WorkspaceOrganizationOption[]>([]);
  const [actorName, setActorName] = useState("Operator");
  const [profileLoading, setProfileLoading] = useState(true);
  const [profileError, setProfileError] = useState<string | null>(null);

  const loadProfile = useCallback(async () => {
    setProfileLoading(true);
    setProfileError(null);
    const id = readActorUserId();
    setActorUserId(id);
    if (!id) {
      setProfileRole("admin");
      setActorName("Operator");
      const fallback =
        process.env.NEXT_PUBLIC_ORGANIZATION_ID?.trim() ||
        "00000000-0000-0000-0000-000000000001";
      setHomeCompanyId(isUuidString(fallback) ? fallback : "00000000-0000-0000-0000-000000000001");
      setSuperAdminCompanyOverride(null);
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
    const cid = (p.company_id ?? "").trim();
    setHomeCompanyId(cid && isUuidString(cid) ? cid : null);

    if (nr === "super_admin") {
      const stored = readStoredWorkspaceCompanyId();
      const pick = stored && isUuidString(stored) ? stored : cid;
      setSuperAdminCompanyOverride(pick && isUuidString(pick) ? pick : null);
      void listWorkspaceOrganizationsForAdmin().then((orgRes) => {
        if (!orgRes.ok) return;
        const rows = [...orgRes.rows];
        const ids = new Set(rows.map((r) => r.company_id));
        if (cid && isUuidString(cid) && !ids.has(cid)) {
          rows.push({ company_id: cid, display_name: "Your workspace" });
        }
        rows.sort((a, b) => a.display_name.localeCompare(b.display_name));
        setWorkspaceCompanies(rows);
      });
    } else {
      setSuperAdminCompanyOverride(null);
      setWorkspaceCompanies([]);
    }
    setProfileLoading(false);
  }, []);

  useEffect(() => {
    void loadProfile();
  }, [loadProfile]);

  useEffect(() => {
    if (!debugMode) setDebugRole(null);
  }, [debugMode]);

  const role = useMemo((): UserRole => {
    if (debugMode && debugRole !== null) return debugRole;
    return profileRole;
  }, [debugMode, debugRole, profileRole]);

  const setWorkspaceCompanyId = useCallback((id: string) => {
    const t = id.trim();
    if (!isUuidString(t)) return;
    setSuperAdminCompanyOverride(t);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(LS_WORKSPACE_COMPANY, t);
    }
  }, []);

  const organizationId = useMemo((): string | null => {
    if (role === "super_admin") {
      return superAdminCompanyOverride ?? homeCompanyId;
    }
    return homeCompanyId;
  }, [role, superAdminCompanyOverride, homeCompanyId]);

  const value = useMemo(
    () => ({
      role,
      actorName,
      actorUserId,
      homeCompanyId,
      organizationId,
      profileLoading,
      profileError,
      workspaceCompanies,
      setWorkspaceCompanyId,
      toggleRole: () => {
        if (!debugMode) return;
        setDebugRole((prev) => {
          const cur = prev ?? profileRole;
          return cur === "operator" ? "admin" : cur === "admin" ? "super_admin" : "operator";
        });
      },
      refreshProfile: loadProfile,
    }),
    [
      role,
      actorName,
      actorUserId,
      homeCompanyId,
      organizationId,
      profileLoading,
      profileError,
      workspaceCompanies,
      setWorkspaceCompanyId,
      loadProfile,
      debugMode,
      profileRole,
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
      homeCompanyId: null,
      organizationId: null,
      profileLoading: false,
      profileError: null,
      workspaceCompanies: [],
      setWorkspaceCompanyId: () => {},
      toggleRole: () => {},
      refreshProfile: async () => {},
    };
  }
  return ctx;
}
