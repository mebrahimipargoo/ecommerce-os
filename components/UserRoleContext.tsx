"use client";

import React, {
  createContext, useCallback, useContext, useEffect, useMemo, useState,
} from "react";
import { isUuidString } from "../lib/uuid";

export type UserRole = "operator" | "admin" | "super_admin";

type UserRoleContextValue = {
  role:          UserRole;
  actorName:     string;
  /** Resolved workspace user profile id for audit trails (imports, etc.). */
  actorUserId:   string | null;
  toggleRole:    () => void;
};

export function isAdminRole(role: UserRole): boolean {
  return role === "admin" || role === "super_admin";
}

const UserRoleContext = createContext<UserRoleContextValue | null>(null);

function readActorUserId(): string | null {
  if (typeof window === "undefined") return null;
  const fromStore = window.localStorage.getItem("current_user_profile_id")?.trim();
  if (fromStore && isUuidString(fromStore)) return fromStore;
  const env = process.env.NEXT_PUBLIC_CURRENT_USER_PROFILE_ID?.trim();
  return env && isUuidString(env) ? env : null;
}

export function UserRoleProvider({ children }: { children: React.ReactNode }) {
  const [role, setRole] = useState<UserRole>("admin");
  const [actorUserId, setActorUserId] = useState<string | null>(null);

  useEffect(() => {
    setActorUserId(readActorUserId());
  }, []);

  const toggleRole = useCallback(() => {
    setRole((r) => (r === "operator" ? "admin" : r === "admin" ? "super_admin" : "operator"));
  }, []);

  const value = useMemo(
    () => ({ role, actorName: "Maysam", actorUserId, toggleRole }),
    [role, actorUserId, toggleRole],
  );

  return (
    <UserRoleContext.Provider value={value}>{children}</UserRoleContext.Provider>
  );
}

export function useUserRole(): UserRoleContextValue {
  const ctx = useContext(UserRoleContext);
  if (!ctx) {
    return { role: "admin", actorName: "Maysam", actorUserId: null, toggleRole: () => {} } as UserRoleContextValue;
  }
  return ctx;
}
