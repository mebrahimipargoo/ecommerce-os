"use client";

import React, {
  createContext, useCallback, useContext, useEffect, useMemo, useState,
} from "react";
import { fetchUserProfileById } from "../app/session/tenant-actions";
import { isUuidString } from "../lib/uuid";
import { useDebugMode } from "./DebugModeContext";

export type UserRole = "operator" | "admin" | "super_admin";

type UserRoleContextValue = {
  role: UserRole;
  actorName: string;
  /** Resolved workspace user profile id (`profiles.id`) for audit trails and tenant bootstrap */
  actorUserId: string | null;
  /** Tenant scope for logistics data — `organization_id` in the database */
  organizationId: string | null;
  profileLoading: boolean;
  profileError: string | null;
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
  const [organizationId, setOrganizationId] = useState<string | null>(null);
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
      setOrganizationId(
        process.env.NEXT_PUBLIC_ORGANIZATION_ID?.trim() ||
          "00000000-0000-0000-0000-000000000001",
      );
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
    setProfileRole(normalizeRole(p.role));
    setActorName((p.full_name ?? "").trim() || p.email || "User");
    setOrganizationId(p.organization_id);
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

  const toggleRole = useCallback(() => {
    if (!debugMode) return;
    setDebugRole((prev) => {
      const cur = prev ?? profileRole;
      return cur === "operator" ? "admin" : cur === "admin" ? "super_admin" : "operator";
    });
  }, [debugMode, profileRole]);

  const value = useMemo(
    () => ({
      role,
      actorName,
      actorUserId,
      organizationId,
      profileLoading,
      profileError,
      toggleRole,
      refreshProfile: loadProfile,
    }),
    [role, actorName, actorUserId, organizationId, profileLoading, profileError, toggleRole, loadProfile],
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
      organizationId: null,
      profileLoading: false,
      profileError: null,
      toggleRole: () => {},
      refreshProfile: async () => {},
    };
  }
  return ctx;
}
