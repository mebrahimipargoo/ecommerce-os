"use client";

/**
 * Global top bar — search, notifications, theme, and profile menu.
 *
 * Role / RBAC label is not shown here (avoid duplication); it lives on `/profile` only.
 * DEV badge gating unchanged (`debugMode` + internal catalog keys).
 *
 * Tenant context: the workspace strip uses `UserRoleContext.organizationName` (effective
 * org id + name resolution) and a generic `Building2` icon — not tenant logos from
 * `organization_settings`. Platform product name/logo live only in the sidebar (`LogoMark`
 * + `platform_settings` via `PlatformBrandingContext`).
 *
 * Mock role switching for internal QA lives in the Tech Debug panel, not here.
 */

import React from "react";
import {
  Bell, Building2, ChevronDown, LogOut, Menu, Search,
  UserCircle,
} from "lucide-react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { ThemeToggle } from "./ThemeToggle";
import { useGlobalSearch } from "./GlobalSearchContext";
import { useDebugMode } from "./DebugModeContext";
import { useUserRole, canShowInternalDevBadge } from "./UserRoleContext";
import { supabase } from "@/src/lib/supabase";

// ─── Component ────────────────────────────────────────────────────────────────

export function TopHeader({ onMenuClick }: { onMenuClick: () => void }) {
  const router = useRouter();
  const { query, setQuery } = useGlobalSearch();
  const { debugMode } = useDebugMode();
  const {
    canonicalRoleKey,
    actorName,
    organizationName,
  } = useUserRole();
  const [isSigningOut, setIsSigningOut] = React.useState(false);
  const [profileMenuOpen, setProfileMenuOpen] = React.useState(false);
  const profileMenuRef = React.useRef<HTMLDivElement>(null);

  const profileInitial = actorName.trim().charAt(0).toUpperCase() || "U";
  const showDevBadge = debugMode && canShowInternalDevBadge(canonicalRoleKey);

  React.useEffect(() => {
    if (!profileMenuOpen) return;
    function onDocMouseDown(e: MouseEvent) {
      const el = profileMenuRef.current;
      if (el && !el.contains(e.target as Node)) setProfileMenuOpen(false);
    }
    document.addEventListener("mousedown", onDocMouseDown);
    return () => document.removeEventListener("mousedown", onDocMouseDown);
  }, [profileMenuOpen]);

  async function handleSignOut() {
    setIsSigningOut(true);
    const { error } = await supabase.auth.signOut();
    if (error) {
      setIsSigningOut(false);
      return;
    }
    router.replace("/login");
    router.refresh();
  }

  return (
    <header
      className="sticky top-0 z-40 flex h-14 w-full min-w-0 shrink-0 items-center justify-between gap-2 border-b border-border bg-card px-3 sm:gap-3 md:px-4"
      role="banner"
    >
      {/* Hamburger — mobile only */}
      <button
        type="button"
        onClick={onMenuClick}
        aria-label="Open navigation menu"
        className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full border border-border text-muted-foreground transition hover:bg-accent hover:text-accent-foreground md:hidden"
      >
        <Menu className="h-5 w-5" />
      </button>

      {/* Effective workspace org label (see UserRoleContext `organizationName`); generic icon only. */}
      <div
        className="hidden min-w-0 max-w-[14rem] shrink-0 truncate rounded-lg border border-border bg-muted/50 px-2.5 py-1.5 text-left text-xs font-medium text-foreground shadow-sm md:block md:max-w-[20rem]"
        title={organizationName}
        aria-label={`Current workspace organization: ${organizationName}`}
      >
        <span className="mr-1.5 inline-flex shrink-0 align-middle text-muted-foreground" aria-hidden>
          <Building2 className="h-3.5 w-3.5" aria-hidden />
        </span>
        {organizationName}
      </div>

      {/* Search — desktop (center band; does not use tenant branding) */}
      <div className="mx-1 hidden min-w-0 flex-1 md:mx-2 md:flex md:max-w-none md:justify-center">
        <label className="relative flex w-full max-w-xl items-center md:mx-auto">
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

      {/* Effective workspace org (mobile); same data source as desktop chip. */}
      <div
        className="mx-0 max-w-[min(38vw,11rem)] shrink-0 truncate rounded-md border border-border bg-muted/40 px-2 py-1 text-[11px] font-medium text-foreground md:hidden"
        title={organizationName}
        aria-label={`Current workspace organization: ${organizationName}`}
      >
        <span className="mr-1 inline-flex shrink-0 align-middle text-muted-foreground" aria-hidden>
          <Building2 className="h-3 w-3" />
        </span>
        {organizationName}
      </div>

      {/* Search — mobile */}
      <div className="mx-0 flex min-w-0 flex-1 md:hidden">
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

      {/* Right-side controls */}
      <div className="ml-auto flex shrink-0 items-center gap-1.5 sm:gap-2">

        {showDevBadge ? (
          <span
            className="hidden rounded-full border border-orange-300 bg-orange-50 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider text-orange-700 dark:border-orange-600/60 dark:bg-orange-950/40 dark:text-orange-300 sm:inline"
            title="Technical debug mode is on"
          >
            DEV
          </span>
        ) : null}

        <ThemeToggle />

        <button
          type="button"
          aria-label="Notifications"
          className="relative hidden h-9 w-9 items-center justify-center rounded-full border border-border text-muted-foreground transition hover:bg-accent hover:text-accent-foreground sm:flex"
        >
          <Bell className="h-4 w-4" />
          <span className="absolute right-1 top-1 h-2 w-2 rounded-full bg-destructive ring-2 ring-card" />
        </button>

        {/* Profile menu */}
        <div className="relative" ref={profileMenuRef}>
          <button
            type="button"
            aria-expanded={profileMenuOpen}
            aria-haspopup="menu"
            aria-label={`Account menu for ${actorName}`}
            onClick={() => setProfileMenuOpen((o) => !o)}
            className="flex items-center gap-2 rounded-full border border-border bg-card px-2 py-1.5 text-xs shadow-sm transition hover:bg-accent"
          >
            <div className="flex h-7 w-7 items-center justify-center rounded-full bg-gradient-to-tr from-primary to-sky-600 text-[11px] font-semibold text-primary-foreground">
              {profileInitial}
            </div>
            <span className="hidden max-w-[14rem] truncate text-xs font-medium text-foreground sm:inline">
              {actorName}
            </span>
            <ChevronDown className="hidden h-3 w-3 shrink-0 text-muted-foreground sm:block" />
          </button>

          {profileMenuOpen ? (
            <div
              role="menu"
              className="absolute right-0 top-full z-50 mt-1 min-w-[11rem] rounded-lg border border-border bg-popover py-1 text-popover-foreground shadow-lg"
            >
              <Link
                href="/profile"
                role="menuitem"
                className="flex items-center gap-2 px-3 py-2 text-sm hover:bg-accent"
                onClick={() => setProfileMenuOpen(false)}
              >
                <UserCircle className="h-4 w-4 shrink-0 opacity-70" />
                Profile &amp; account
              </Link>
              <button
                type="button"
                role="menuitem"
                disabled={isSigningOut}
                className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm hover:bg-accent disabled:cursor-not-allowed disabled:opacity-60"
                onClick={() => {
                  setProfileMenuOpen(false);
                  void handleSignOut();
                }}
              >
                <LogOut className="h-4 w-4 shrink-0 opacity-70" />
                {isSigningOut ? "Signing out…" : "Sign out"}
              </button>
            </div>
          ) : null}
        </div>
      </div>
    </header>
  );
}
