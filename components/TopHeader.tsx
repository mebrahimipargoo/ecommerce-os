"use client";

/**
 * Global top bar — search, notifications, theme, profile only (no tenant logo or title).
 * Branding lives exclusively in the sidebar on all breakpoints.
 */

import React from "react";
import { Bell, ChevronDown, Menu, Search, ShieldCheck, Sparkles, User } from "lucide-react";
import { ThemeToggle } from "./ThemeToggle";
import { useDebugMode } from "./DebugModeContext";
import { useGlobalSearch } from "./GlobalSearchContext";
import { useUserRole } from "./UserRoleContext";

export function TopHeader({ onMenuClick }: { onMenuClick: () => void }) {
  const { query, setQuery } = useGlobalSearch();
  const { debugMode } = useDebugMode();
  const {
    role,
    toggleRole,
    actorName,
    workspaceCompanies,
    setWorkspaceCompanyId,
    organizationId,
  } = useUserRole();
  const profileInitial = actorName.trim().charAt(0).toUpperCase() || "M";

  const roleLabel =
    role === "super_admin" ? "Super Admin" : role === "admin" ? "Admin" : "Operator";

  return (
    <header
      className="sticky top-0 z-40 flex h-14 w-full min-w-0 shrink-0 items-center justify-between gap-3 border-b border-border bg-card px-3 md:px-4"
      role="banner"
    >
      <button
        type="button"
        onClick={onMenuClick}
        aria-label="Open navigation menu"
        className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full border border-border text-muted-foreground transition hover:bg-accent hover:text-accent-foreground md:hidden"
      >
        <Menu className="h-5 w-5" />
      </button>

      <div className="mx-2 hidden min-w-0 max-w-md flex-1 md:flex">
        <label className="relative flex w-full items-center">
          <Search className="pointer-events-none absolute left-3 h-4 w-4 text-muted-foreground" aria-hidden />
          <input
            type="search"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search ID, tracking, ASIN…"
            className="h-9 w-full rounded-lg border border-border bg-muted py-2 pl-9 pr-3 text-xs text-foreground placeholder:text-muted-foreground shadow-sm outline-none ring-0 transition focus:border-primary focus:ring-1 focus:ring-ring"
            aria-label="Global search"
          />
        </label>
      </div>
      <div className="mx-1 flex min-w-0 flex-1 md:hidden">
        <label className="relative flex w-full items-center">
          <Search className="pointer-events-none absolute left-2.5 h-3.5 w-3.5 text-muted-foreground" aria-hidden />
          <input
            type="search"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search…"
            className="h-8 w-full rounded-lg border border-border bg-muted py-1.5 pl-8 pr-2 text-[11px] text-foreground placeholder:text-muted-foreground"
            aria-label="Global search"
          />
        </label>
      </div>

      <div className="ml-auto flex shrink-0 items-center gap-1.5 sm:gap-2">
        <button
          type="button"
          onClick={debugMode ? toggleRole : undefined}
          disabled={!debugMode}
          title={
            debugMode
              ? `[Debug] Viewing as ${roleLabel} — click to rotate`
              : `Signed in as ${roleLabel}`
          }
          className={[
            "hidden h-9 items-center gap-1.5 rounded-full border px-3 text-xs font-semibold sm:flex",
            debugMode ? "transition hover:opacity-90" : "cursor-default",
            role === "super_admin"
              ? "border-violet-300 bg-violet-50 text-violet-800 dark:border-violet-700/60 dark:bg-violet-950/40 dark:text-violet-200"
              : role === "admin"
                ? "border-emerald-300 bg-emerald-50 text-emerald-700 dark:border-emerald-700/60 dark:bg-emerald-950/30 dark:text-emerald-300"
                : "border-slate-300 bg-slate-50 text-slate-600 dark:border-slate-600 dark:bg-slate-800/50 dark:text-slate-300",
          ].join(" ")}
        >
          {role === "super_admin" ? (
            <><Sparkles className="h-3.5 w-3.5" />Super Admin</>
          ) : role === "admin" ? (
            <><ShieldCheck className="h-3.5 w-3.5" />Admin</>
          ) : (
            <><User className="h-3.5 w-3.5" />Operator</>
          )}
        </button>

        {role === "super_admin" && workspaceCompanies.length > 0 && (
          <label className="hidden max-w-[min(100%,14rem)] items-center gap-1 sm:flex">
            <span className="sr-only">Workspace company</span>
            <select
              value={organizationId ?? ""}
              onChange={(e) => setWorkspaceCompanyId(e.target.value)}
              className="h-9 max-w-full rounded-lg border border-border bg-background px-2 text-xs font-medium text-foreground shadow-sm outline-none ring-offset-background focus-visible:ring-2 focus-visible:ring-ring"
              title="Switch company (Super Admin)"
            >
              {workspaceCompanies.map((o) => (
                <option key={o.company_id} value={o.company_id}>
                  {o.display_name}
                </option>
              ))}
            </select>
          </label>
        )}

        <ThemeToggle />
        <button
          type="button"
          aria-label="Notifications"
          className="relative hidden h-9 w-9 items-center justify-center rounded-full border border-border text-muted-foreground transition hover:bg-accent hover:text-accent-foreground sm:flex"
        >
          <Bell className="h-4 w-4" />
          <span className="absolute right-1 top-1 h-2 w-2 rounded-full bg-destructive ring-2 ring-card" />
        </button>
        <button
          type="button"
          className="flex items-center gap-2 rounded-full border border-border bg-card px-2 py-1.5 text-xs shadow-sm transition hover:bg-accent"
        >
          <div className="flex h-7 w-7 items-center justify-center rounded-full bg-gradient-to-tr from-primary to-sky-600 text-[11px] font-semibold text-primary-foreground">
            {profileInitial}
          </div>
          <div className="hidden flex-col items-start leading-tight sm:flex">
            <span className="text-xs font-medium text-foreground">{actorName}</span>
            <span className="text-[10px] text-muted-foreground">{roleLabel}</span>
          </div>
          <ChevronDown className="hidden h-3 w-3 text-muted-foreground sm:block" />
        </button>
      </div>
    </header>
  );
}
