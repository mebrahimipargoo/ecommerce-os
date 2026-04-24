"use client";

/**
 * DevRoleSwitcher — colour-coded role <select> for QA / permission testing.
 *
 * Selecting a role automatically enables Debug Mode so the permission system
 * reacts immediately — no manual toggle required.
 *
 * Intended for the Tech Debug panel (not the production header).
 */

import React from "react";
import { useDebugMode } from "./DebugModeContext";
import { useUserRole, ROLE_HIERARCHY, type UserRole } from "./UserRoleContext";

// ── Role labels + per-role colour scheme ──────────────────────────────────────

const ROLE_LABEL: Record<UserRole, string> = {
  super_admin:     "Super Admin",
  system_employee: "System Employee",
  admin:           "Admin",
  employee:        "Employee",
  operator:        "Operator",
};

const ROLE_CLS: Record<UserRole, string> = {
  super_admin:
    "border-violet-300 bg-violet-50 text-violet-800 " +
    "dark:border-violet-700/60 dark:bg-violet-950/40 dark:text-violet-200",
  system_employee:
    "border-fuchsia-300 bg-fuchsia-50 text-fuchsia-700 " +
    "dark:border-fuchsia-700/60 dark:bg-fuchsia-950/40 dark:text-fuchsia-200",
  admin:
    "border-emerald-300 bg-emerald-50 text-emerald-700 " +
    "dark:border-emerald-700/60 dark:bg-emerald-950/30 dark:text-emerald-300",
  employee:
    "border-sky-300 bg-sky-50 text-sky-700 " +
    "dark:border-sky-700/60 dark:bg-sky-950/40 dark:text-sky-300",
  operator:
    "border-slate-300 bg-slate-50 text-slate-600 " +
    "dark:border-slate-600 dark:bg-slate-800/50 dark:text-slate-300",
};

// ── Component ─────────────────────────────────────────────────────────────────

export function DevRoleSwitcher() {
  const { debugMode, setDebugMode } = useDebugMode();
  const { role, setDebugRole } = useUserRole();

  function handleChange(e: React.ChangeEvent<HTMLSelectElement>) {
    const next = e.target.value as UserRole;
    // Auto-enable debug mode so the permission system reacts immediately.
    if (!debugMode) setDebugMode(true);
    setDebugRole(next);
  }

  return (
    <label
      className="flex min-w-0 flex-1 flex-col gap-1"
      title="Switch mock role — changes sidebar and permissions instantly"
    >
      <span className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Mock role tier</span>
      <select
        value={role}
        onChange={handleChange}
        aria-label="Dev: switch mock role"
        className={[
          "h-9 w-full min-w-0 cursor-pointer rounded-lg border px-2 text-xs font-semibold",
          "outline-none transition hover:opacity-90",
          ROLE_CLS[role],
        ].join(" ")}
      >
        {ROLE_HIERARCHY.map((r) => (
          <option key={r} value={r}>
            {ROLE_LABEL[r]}
          </option>
        ))}
      </select>
    </label>
  );
}
