"use client";

/**
 * Global top bar — single place for Theme toggle and User profile.
 * Mobile LTR: [Hamburger] → [Logo / Title] → [Theme + Profile]
 * Desktop:     [optional left slot] … [Theme + Profile] on the right
 */

import React from "react";
import { Bell, ChevronDown, Menu, Search, ShieldCheck, User } from "lucide-react";
import { ThemeToggle } from "./ThemeToggle";
import { LogoMark } from "./LogoMark";
import { useGlobalSearch } from "./GlobalSearchContext";
import { useUserRole } from "./UserRoleContext";

export function TopHeader({ onMenuClick }: { onMenuClick: () => void }) {
  const { query, setQuery } = useGlobalSearch();
  const { role, toggleRole, actorName } = useUserRole();
  const profileInitial = actorName.trim().charAt(0).toUpperCase() || "M";
  return (
    <header
      className="sticky top-0 z-40 flex h-14 w-full min-w-0 shrink-0 items-center justify-between gap-3 border-b border-border bg-card px-3 md:px-4"
      role="banner"
    >
      {/* ── Mobile: hamburger (left) ── */}
      <button
        type="button"
        onClick={onMenuClick}
        aria-label="Open navigation menu"
        className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full border border-border text-muted-foreground transition hover:bg-accent hover:text-accent-foreground md:hidden"
      >
        <Menu className="h-5 w-5" />
      </button>

      {/* ── Logo + title: compact on mobile so global search can grow ── */}
      <div className="flex min-w-0 shrink-0 items-center gap-2.5 md:min-w-0 md:flex-1">
        <div className="flex max-w-[40%] min-w-0 items-center gap-2 sm:max-w-none md:hidden">
          <LogoMark />
          <div className="min-w-0">
            <p className="truncate text-sm font-bold text-foreground">E-commerce OS</p>
            <p className="truncate text-[10px] text-muted-foreground">Returns ERP</p>
          </div>
        </div>
        <div className="hidden min-w-0 md:block">
          <p className="truncate text-sm font-semibold text-foreground">Workspace</p>
          <p className="truncate text-[10px] text-muted-foreground">Enterprise control</p>
        </div>
      </div>

      {/* Global search — filters the active Returns tab (Items / Packages / Pallets) */}
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

      {/* ── Right cluster: pinned to far right ── */}
      <div className="ml-auto flex shrink-0 items-center gap-1.5 sm:gap-2">
        {/* Role switcher — toggles RBAC across the entire app */}
        <button
          type="button"
          onClick={toggleRole}
          title={`Currently viewing as ${role === "admin" ? "Admin" : "Operator"} — click to switch`}
          className={[
            "hidden h-9 items-center gap-1.5 rounded-full border px-3 text-xs font-semibold transition sm:flex",
            role === "admin"
              ? "border-emerald-300 bg-emerald-50 text-emerald-700 hover:bg-emerald-100 dark:border-emerald-700/60 dark:bg-emerald-950/30 dark:text-emerald-300 dark:hover:bg-emerald-950/50"
              : "border-slate-300 bg-slate-50 text-slate-600 hover:bg-slate-100 dark:border-slate-600 dark:bg-slate-800/50 dark:text-slate-300 dark:hover:bg-slate-800",
          ].join(" ")}
        >
          {role === "admin" ? (
            <><ShieldCheck className="h-3.5 w-3.5" />Admin</>
          ) : (
            <><User className="h-3.5 w-3.5" />Operator</>
          )}
        </button>

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
            <span className="text-[10px] text-muted-foreground">Operations Director</span>
          </div>
          <ChevronDown className="hidden h-3 w-3 text-muted-foreground sm:block" />
        </button>
      </div>
    </header>
  );
}
