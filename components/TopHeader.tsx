"use client";

/**
 * Global top bar — search, notifications, theme, and profile menu.
 *
 * Role / RBAC label is not shown here (avoid duplication); it lives on `/profile` only.
 * DEV badge gating unchanged (`debugMode` + internal catalog keys).
 *
 * Tenant context: the workspace strip uses `UserRoleContext.organizationName` plus
 * `BrandingContext.logoUrl` (tenant mark from `organization_settings.logo_url`) when set;
 * otherwise `Building2`. Platform product name/logo stay in the sidebar (`LogoMark`).
 *
 * Internal staff (super_admin / programmer / system_admin): workspace org switcher and
 * optional “view as” user for the selected org — navigation/RBAC simulate that member;
 * server actions still use the signed-in account (banner explains).
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
import { useBranding } from "./BrandingContext";
import { useRbacPermissions } from "../hooks/useRbacPermissions";
import { supabase } from "@/src/lib/supabase";
import { ViewAsUserPicker } from "./ViewAsUserPicker";
import { WorkspaceOrganizationPicker } from "./WorkspaceOrganizationPicker";

/** Tenant logo beside workspace name, or building icon when missing / broken. */
function TenantMarkBesideName({
  logoUrl,
  compact,
  linkHome,
}: {
  logoUrl: string;
  /** Tighter max size for mobile strip */
  compact?: boolean;
  /** Wrap mark in a link to dashboard (/) — use beside org &lt;select&gt; so only logo navigates. */
  linkHome?: boolean;
}) {
  const [broken, setBroken] = React.useState(false);
  const url = logoUrl.trim();
  React.useEffect(() => {
    setBroken(false);
  }, [url]);
  const mark =
    !url || broken ? (
      <Building2
        className={`shrink-0 text-muted-foreground ${compact ? "h-3 w-3" : "h-3.5 w-3.5"}`}
        aria-hidden
      />
    ) : (
      // eslint-disable-next-line @next/next/no-img-element
      <img
        src={url}
        alt=""
        className={
          compact
            ? "h-5 max-h-5 w-auto max-w-[4.5rem] shrink-0 object-contain"
            : "h-6 max-h-6 w-auto max-w-[6.25rem] shrink-0 object-contain"
        }
        onError={() => setBroken(true)}
      />
    );
  if (linkHome) {
    return (
      <Link
        href="/"
        className="shrink-0 rounded-md p-0.5 outline-none ring-offset-background transition hover:bg-muted/60 focus-visible:ring-2 focus-visible:ring-ring"
        title="Home / Dashboard"
        aria-label="Go to home / dashboard"
      >
        {mark}
      </Link>
    );
  }
  return mark;
}

// ─── Component ────────────────────────────────────────────────────────────────

export function TopHeader({ onMenuClick }: { onMenuClick: () => void }) {
  const router = useRouter();
  const { query, setQuery } = useGlobalSearch();
  const { debugMode } = useDebugMode();
  const {
    canonicalRoleKey,
    canonicalRoleLabel,
    actorName,
    organizationId,
    organizationName,
    workspaceOrganizations,
    setWorkspaceOrganizationId,
    viewAsProfileId,
    setViewAsProfileId,
    viewAsProfileOptions,
    viewAsProfileOptionsLoading,
    isViewingAsAnotherUser,
    viewAsDisplayName,
    actorUserId,
  } = useUserRole();
  const { logoUrl: tenantLogoUrl } = useBranding();
  const perms = useRbacPermissions();
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

  const viewAsOptionsOthers = React.useMemo(
    () => viewAsProfileOptions.filter((p) => p.profile_id !== actorUserId),
    [viewAsProfileOptions, actorUserId],
  );
  const showViewAs =
    perms.canSwitchOrganization &&
    Boolean(organizationId) &&
    (viewAsProfileOptionsLoading || viewAsOptionsOthers.length > 0);

  return (
    /* App chrome: z-50 — above in-page sticky bars; modals usually z-90+ */
    <div className="sticky top-0 z-50 shrink-0">
      {isViewingAsAnotherUser && viewAsDisplayName ? (
        <div className="border-b border-amber-300/80 bg-amber-50 px-3 py-1.5 text-center text-[11px] font-medium text-amber-950 dark:border-amber-700/50 dark:bg-amber-950/50 dark:text-amber-100">
          Viewing as <strong className="font-semibold">{viewAsDisplayName}</strong> — sidebar and pages match
          their role. API and saves still use <strong className="font-semibold">your</strong> account.
        </div>
      ) : null}
      <header
        className="flex h-14 w-full min-w-0 items-center justify-between gap-2 border-b border-border bg-card px-3 sm:gap-3 md:px-4"
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

      {/* Workspace org + view-as (desktop). */}
      <div className="hidden min-w-0 flex-1 items-center gap-2 md:flex md:max-w-[min(52vw,28rem)] lg:max-w-[min(60vw,40rem)]">
        {perms.canSwitchOrganization && workspaceOrganizations.length > 0 ? (
          <div
            className="flex min-w-0 flex-1 items-center gap-2 rounded-lg border border-border bg-muted/50 px-2 py-1 text-xs font-medium text-foreground shadow-sm"
            aria-label={`Workspace: ${organizationName}`}
          >
            <TenantMarkBesideName logoUrl={tenantLogoUrl} linkHome />
            <WorkspaceOrganizationPicker
              options={workspaceOrganizations}
              value={organizationId}
              onChange={setWorkspaceOrganizationId}
              leadingIcon={false}
              triggerClassName="flex w-full min-w-0 flex-1 items-center justify-between gap-0.5 border-0 bg-transparent py-0.5 text-left text-xs font-medium text-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-0 disabled:opacity-50"
            />
          </div>
        ) : (
          <Link
            href="/"
            className="flex min-w-0 max-w-[14rem] shrink-0 items-center gap-2 truncate rounded-lg border border-border bg-muted/50 px-2.5 py-1.5 text-left text-xs font-medium text-foreground shadow-sm transition hover:bg-muted/70 md:max-w-[20rem]"
            title="Home / Dashboard"
            aria-label={`${organizationName} — go to home`}
          >
            <TenantMarkBesideName logoUrl={tenantLogoUrl} />
            <span className="min-w-0 truncate">{organizationName}</span>
          </Link>
        )}
        {showViewAs ? (
          <div className="min-w-0 shrink-0 lg:w-[13rem] xl:w-[15rem]">
            <span className="mb-0.5 hidden text-[10px] font-semibold uppercase tracking-wide text-muted-foreground lg:block">
              View as
            </span>
            <ViewAsUserPicker
              actorName={actorName}
              actorUserId={actorUserId}
              options={viewAsProfileOptions}
              value={viewAsProfileId}
              onChange={setViewAsProfileId}
              disabled={viewAsProfileOptionsLoading}
            />
          </div>
        ) : null}
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
            className="h-9 w-full rounded-lg border border-border bg-muted py-2 pl-9 pr-3 text-xs text-foreground placeholder:text-muted-foreground shadow-sm outline-none ring-0 transition focus:border-sky-500 focus:ring-1 focus:ring-sky-500/40"
            aria-label="Global search"
          />
        </label>
      </div>

      {/* Effective workspace org (mobile). */}
      {perms.canSwitchOrganization && workspaceOrganizations.length > 0 ? (
        <div className="mx-0 flex max-w-[min(42vw,12rem)] shrink-0 items-center gap-1.5 rounded-md border border-border bg-muted/40 px-2 py-1 md:hidden">
          <TenantMarkBesideName logoUrl={tenantLogoUrl} compact linkHome />
          <WorkspaceOrganizationPicker
            options={workspaceOrganizations}
            value={organizationId}
            onChange={setWorkspaceOrganizationId}
            dense
            leadingIcon={false}
            triggerClassName="flex w-full min-w-0 flex-1 items-center justify-between gap-0.5 border-0 bg-transparent py-0 text-left text-[11px] font-medium text-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-0 disabled:opacity-50"
          />
        </div>
      ) : (
        <Link
          href="/"
          className="mx-0 flex max-w-[min(38vw,11rem)] shrink-0 items-center gap-1.5 truncate rounded-md border border-border bg-muted/40 px-2 py-1 text-[11px] font-medium text-foreground transition hover:bg-muted/55 md:hidden"
          title="Home / Dashboard"
          aria-label={`${organizationName} — go to home`}
        >
          <TenantMarkBesideName logoUrl={tenantLogoUrl} compact />
          <span className="min-w-0 truncate">{organizationName}</span>
        </Link>
      )}

      {/* Search — mobile */}
      <div className="mx-0 flex min-w-0 flex-1 md:hidden">
        <label className="relative flex w-full items-center">
          <Search className="pointer-events-none absolute left-2.5 h-3.5 w-3.5 text-muted-foreground" aria-hidden />
          <input
            type="search"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search…"
            className="h-8 w-full rounded-lg border border-border bg-muted py-1.5 pl-8 pr-2 text-[11px] text-foreground placeholder:text-muted-foreground outline-none ring-0 transition focus:border-sky-500 focus:ring-1 focus:ring-sky-500/40"
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
            <div className="flex h-7 w-7 items-center justify-center rounded-full bg-gradient-to-tr from-sky-500 to-sky-600 text-[11px] font-semibold text-white">
              {profileInitial}
            </div>
            <span className="hidden max-w-[14rem] truncate text-xs font-medium text-foreground sm:inline">
              {actorName}
            </span>
            <span className="hidden rounded-md border border-border bg-muted/60 px-1.5 py-0.5 text-[10px] font-semibold text-muted-foreground sm:inline">
              {canonicalRoleLabel}
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
      {showViewAs ? (
        <div className="border-b border-border bg-muted/25 px-2 py-1.5 md:hidden">
          <ViewAsUserPicker
            actorName={actorName}
            actorUserId={actorUserId}
            options={viewAsProfileOptions}
            value={viewAsProfileId}
            onChange={setViewAsProfileId}
            disabled={viewAsProfileOptionsLoading}
            dense
            highZ
          />
        </div>
      ) : null}
    </div>
  );
}
