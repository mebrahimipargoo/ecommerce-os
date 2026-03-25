"use client";

import React, {
  createContext, useCallback, useContext, useMemo, useState,
} from "react";

export type UserRole = "admin" | "operator";

type UserRoleContextValue = {
  role:       UserRole;
  actorName:  string;
  toggleRole: () => void;
};

const UserRoleContext = createContext<UserRoleContextValue | null>(null);

export function UserRoleProvider({ children }: { children: React.ReactNode }) {
  const [role, setRole] = useState<UserRole>("admin");

  const toggleRole = useCallback(
    () => setRole((r) => (r === "admin" ? "operator" : "admin")),
    [],
  );

  const value = useMemo(
    () => ({ role, actorName: "Maysam", toggleRole }),
    [role, toggleRole],
  );

  return (
    <UserRoleContext.Provider value={value}>{children}</UserRoleContext.Provider>
  );
}

export function useUserRole(): UserRoleContextValue {
  const ctx = useContext(UserRoleContext);
  if (!ctx) return { role: "admin", actorName: "Maysam", toggleRole: () => {} };
  return ctx;
}
