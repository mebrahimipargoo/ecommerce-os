"use client";

/**
 * AppShell — Global layout wrapper.
 *
 * ─ Desktop  : Persistent collapsible sidebar + TopHeader (theme/profile live ONLY here).
 * ─ Mobile   : TopHeader (hamburger + logo + theme + profile) + drawer for nav.
 *
 * Sidebar visibility is driven entirely by useRbacPermissions (no role comparisons here).
 * Content column: TopHeader (shrink-0) + scrollable main (flex-1 min-h-0 overflow-auto).
 *
 * Shell product row: platform name + `LogoMark` from `public.platform_settings` via
 * `PlatformBrandingContext`. Tenant org label + optional tenant logo live in `TopHeader`
 * (`UserRoleContext` + `BrandingContext`).
 */

import React, {
  createContext, useContext, useCallback, useEffect, useState,
} from "react";
import { createPortal } from "react-dom";
import Link from "next/link";
import { DrawerWorkspaceBar } from "./DrawerWorkspaceBar";
import { usePathname } from "next/navigation";
import { ChevronDown, PanelLeftClose, PanelLeftOpen, Wrench, X } from "lucide-react";
import { TopHeader } from "./TopHeader";
import { BrandingProvider } from "./BrandingContext";
import { PlatformBrandingProvider, usePlatformBranding } from "./PlatformBrandingContext";
import { LogoMark } from "./LogoMark";
import { PLATFORM_TAGLINE } from "../lib/platform-branding";
import { GlobalSearchProvider } from "./GlobalSearchContext";
import { UserRoleProvider } from "./UserRoleContext";
import { TechDebugPanel } from "./TechDebugPanel";
import { useRbacPermissions } from "../hooks/useRbacPermissions";
import { MAIN_SIDEBAR, WMS_ONLY_NAV, isLeafVisibleByRbac, type SidebarGroup } from "../lib/sidebar-config";
import { getSidebarIcon } from "../lib/sidebar-icons";

// ─── Nav (from `lib/sidebar-config.ts`) ─────────────────────────────────────

type NavChild = { label: string; href: string; icon: React.ElementType; disabled?: boolean; badge?: string };
type NavItemDef = {
  label: string;
  icon: React.ElementType;
  href?: string;
  disabled?: boolean;
  badge?: string;
  children?: NavChild[];
};

function navChildrenForGroup(
  g: SidebarGroup,
  perms: ReturnType<typeof useRbacPermissions>,
): NavChild[] {
  return g.children
    .filter((c) => {
      if (c.showInSidebar === false) return false;
      if (!isLeafVisibleByRbac(c, perms)) return false;
      if (g.id === "admin_imports" && !perms.canSeeSystemAdmin) return false;
      return true;
    })
    .map((c) => ({
      label: c.label,
      href: c.path,
      icon: getSidebarIcon(c.icon ?? g.icon),
      badge: c.id === "wms_scan" ? "WMS" : undefined,
    }));
}

// ─── Shell context (mobile menu trigger) ──────────────────────────────────────

const MobileMenuCtx = createContext<{ openMobileMenu: () => void }>({ openMobileMenu: () => {} });
export const useAppShell = () => useContext(MobileMenuCtx);

// ─── Shared CSS classes ───────────────────────────────────────────────────────

const CLS = {
  linkActive: "bg-sky-50 text-sky-700 dark:bg-sky-950/60 dark:text-sky-300",
  linkIdle:   "text-muted-foreground hover:bg-accent hover:text-accent-foreground",
  linkDis:    "pointer-events-none text-muted-foreground/40",
  linkBase:   "group relative flex min-h-[40px] w-full items-center gap-3 rounded-xl px-3 py-2 text-sm font-medium transition-all",
  section:    "mb-1 px-3 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground/60",
};

// ─── AppShell root ────────────────────────────────────────────────────────────

export function AppShell({ children }: { children: React.ReactNode }) {
  return (
    <UserRoleProvider>
      <PlatformBrandingProvider>
        <BrandingProvider>
          <AppShellInner>{children}</AppShellInner>
        </BrandingProvider>
      </PlatformBrandingProvider>
    </UserRoleProvider>
  );
}

// ─── Inner shell (has access to context) ─────────────────────────────────────

function AppShellInner({ children }: { children: React.ReactNode }) {
  const { platformAppName, loading: platformNameLoading } = usePlatformBranding();
  const [collapsed,     setCollapsed]     = useState(false);
  const [mobileOpen,    setMobileOpen]    = useState(false);
  const [techDebugOpen, setTechDebugOpen] = useState(false);
  const [mounted,       setMounted]       = useState(false);
  const [expanded,      setExpanded]      = useState<Record<string, boolean>>({});
  const pathname = usePathname();
  const isAuthRoute = pathname === "/login";

  useEffect(() => {
    setMounted(true);
    if (localStorage.getItem("sidebar_collapsed") === "true") setCollapsed(true);
  }, []);

  const toggleCollapsed = useCallback(() => {
    setCollapsed((c) => {
      localStorage.setItem("sidebar_collapsed", String(!c));
      return !c;
    });
  }, []);

  function toggleAccordion(key: string) {
    setExpanded((p) => ({ ...p, [key]: !p[key] }));
  }

  function isActive(href?: string) {
    if (!href || href === "#") return false;
    const path = pathname.endsWith("/") && pathname.length > 1 ? pathname.slice(0, -1) : pathname;
    if (href === "/") return path === "/";
    if (href === "/settings") return path === "/settings";
    if (href === "/claim-engine") {
      return path === "/claim-engine" || path.startsWith("/claim-engine/investigation");
    }
    if (href === "/claim-engine/report-history") {
      return path === "/claim-engine/report-history" || path.startsWith("/claim-engine/report-history/");
    }
    return path === href || path.startsWith(`${href}/`);
  }

  // Auto-expand accordion groups when a child route becomes active
  useEffect(() => {
    const auto: Record<string, boolean> = {};
    for (const sec of MAIN_SIDEBAR) {
      for (const g of sec.groups) {
        const key = `nav-${sec.id}-${g.label}`;
        if (g.children.some((c) => isActive(c.path))) {
          auto[key] = true;
        }
      }
    }
    if (
      pathname.startsWith("/platform/settings")
      || pathname.startsWith("/platform/organizations")
      || pathname.startsWith("/platform/users")
      || pathname.startsWith("/platform/access")
    ) {
      auto["nav-core-platform"] = true;
    }
    if (Object.keys(auto).length) setExpanded((p) => ({ ...p, ...auto }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pathname]);

  const closeMenu = () => setMobileOpen(false);

  function openTechDebug() {
    closeMenu();
    setTechDebugOpen(true);
  }

  // ── NavLink ────────────────────────────────────────────────────────────────
  function NavLink({ item, child = false, alwaysFull = false }: {
    item:        NavItemDef | NavChild;
    child?:      boolean;
    alwaysFull?: boolean;
  }) {
    const href     = (item as NavItemDef).href ?? "";
    const disabled = !!(item as NavItemDef).disabled;
    const badge    = (item as NavItemDef).badge;
    const active   = isActive(href);
    const Icon     = (item as NavItemDef).icon ?? (item as NavChild).icon;
    const showText = alwaysFull || !collapsed;

    return (
      <Link
        href={disabled ? "#" : href}
        onClick={closeMenu}
        aria-disabled={disabled}
        tabIndex={disabled ? -1 : undefined}
        className={[
          CLS.linkBase,
          child && showText ? "pl-9 pr-3" : "",
          collapsed && !showText ? "justify-center" : "",
          active && !disabled ? CLS.linkActive : disabled ? CLS.linkDis : CLS.linkIdle,
        ].join(" ")}
      >
        {Icon && (
          <Icon className={[
            child ? "h-4 w-4 shrink-0" : "h-5 w-5 shrink-0",
            active && !disabled ? "text-sky-600 dark:text-sky-400" : "",
            disabled             ? "opacity-40"                     : "",
          ].join(" ")} />
        )}

        {showText && (
          <>
            <span className="flex-1 truncate">{item.label}</span>
            {badge && (
              <span className="shrink-0 rounded-full bg-muted px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide text-muted-foreground">
                {badge}
              </span>
            )}
            {active && <span className="ml-1 h-1.5 w-1.5 shrink-0 rounded-full bg-sky-500 dark:bg-sky-400" />}
          </>
        )}

        {!showText && (
          <span
            role="tooltip"
            className="invisible absolute left-full top-1/2 z-50 ml-3 -translate-y-1/2 whitespace-nowrap rounded-xl border border-border bg-popover px-3 py-1.5 text-xs font-semibold text-popover-foreground shadow-lg group-hover:visible"
          >
            {item.label}
          </span>
        )}
      </Link>
    );
  }

  // ── AccordionItem ─────────────────────────────────────────────────────────
  function AccordionItem({
    item,
    groupKey,
    alwaysFull = false,
    trailingExpandedContent,
  }: {
    item:        NavItemDef;
    /** Stable id for expand state, e.g. `nav-core-warehouse` */
    groupKey:   string;
    alwaysFull?: boolean;
    /** Rendered inside the expanded panel after link children (e.g. Tech Debug). */
    trailingExpandedContent?: React.ReactNode;
  }) {
    const key         = groupKey;
    const open        = expanded[key] ?? false;
    const childActive = item.children?.some((c) => isActive(c.href));
    const Icon        = item.icon;
    const showText    = alwaysFull || !collapsed;

    return (
      <div>
        <button
          type="button"
          onClick={() => showText && toggleAccordion(key)}
          aria-expanded={showText ? open : undefined}
          className={[
            CLS.linkBase,
            collapsed && !showText ? "justify-center" : "",
            childActive ? CLS.linkActive : CLS.linkIdle,
          ].join(" ")}
        >
          <Icon className={[
            "h-5 w-5 shrink-0",
            childActive ? "text-sky-600 dark:text-sky-400" : "",
          ].join(" ")} />

          {showText && (
            <>
              <span className="flex-1 truncate text-left">{item.label}</span>
              <ChevronDown
                className={[
                  "h-4 w-4 shrink-0 opacity-50 transition-transform duration-200",
                  open ? "rotate-0" : "-rotate-90",
                ].join(" ")}
              />
            </>
          )}

          {!showText && (
            <span
              role="tooltip"
              className="invisible absolute left-full top-1/2 z-50 ml-3 -translate-y-1/2 whitespace-nowrap rounded-xl border border-border bg-popover px-3 py-1.5 text-xs font-semibold text-popover-foreground shadow-lg group-hover:visible"
            >
              {item.label}
            </span>
          )}
        </button>

        {/* Smooth height reveal via CSS grid rows trick */}
        {showText && (
          <div
            className={[
              "grid transition-[grid-template-rows] duration-200 ease-in-out",
              open ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
            ].join(" ")}
          >
            <div className="overflow-hidden">
              <div className="mt-0.5 space-y-0.5 pb-1">
                {item.children?.map((c) => (
                  <NavLink key={c.href} item={c} child alwaysFull={alwaysFull} />
                ))}
                {trailingExpandedContent}
              </div>
            </div>
          </div>
        )}
      </div>
    );
  }

  // ── SidebarBody ───────────────────────────────────────────────────────────
  function SidebarBody({
    alwaysFull = false,
    collapsed: sidebarCollapsed,
  }: {
    alwaysFull?: boolean;
    collapsed:   boolean;
  }) {
    const perms = useRbacPermissions();
    const showSection = alwaysFull || !sidebarCollapsed;

    if (perms.isWmsOnly) {
      const wmsVisible = WMS_ONLY_NAV.leaves.filter((c) => isLeafVisibleByRbac(c, perms));
      return (
        <div className="mb-4">
          {showSection
            ? <p className={CLS.section}>{WMS_ONLY_NAV.label}</p>
            : <div className="mb-2 mx-3 h-px bg-border" />
          }
          <div className="space-y-0.5">
            {wmsVisible.map((leaf) => (
              <NavLink
                key={leaf.id}
                item={{
                  label: leaf.label,
                  href: leaf.path,
                  icon: getSidebarIcon(leaf.icon ?? "ScanLine"),
                  badge: "WMS",
                }}
                alwaysFull={alwaysFull}
              />
            ))}
          </div>
        </div>
      );
    }

    const core = MAIN_SIDEBAR[0]!;
    const admin = MAIN_SIDEBAR[1];
    const adminVisible =
      admin?.groups
        .map((g) => navChildrenForGroup(g, perms))
        .some((ch) => ch.length > 0) ?? false;

    return (
      <>
        <div className="space-y-1">
          {core.groups.map((g) => {
            const ch = navChildrenForGroup(g, perms);
            if (g.id === "platform") {
              const showTech = perms.canSeeTechDebug;
              if (ch.length === 0 && !showTech) return null;
              return (
                <AccordionItem
                  key={g.id}
                  groupKey={`nav-${core.id}-${g.id}`}
                  item={{ label: g.label, icon: getSidebarIcon(g.icon), children: ch }}
                  alwaysFull={alwaysFull}
                  trailingExpandedContent={
                    showTech ? (
                      <TechDebugNavButton
                        child
                        collapsed={sidebarCollapsed}
                        alwaysFull={alwaysFull}
                        onClick={openTechDebug}
                      />
                    ) : null
                  }
                />
              );
            }
            if (ch.length === 0) return null;
            return (
              <AccordionItem
                key={g.id}
                groupKey={`nav-${core.id}-${g.id}`}
                item={{ label: g.label, icon: getSidebarIcon(g.icon), children: ch }}
                alwaysFull={alwaysFull}
              />
            );
          })}
        </div>

        {admin && adminVisible ? (
          <div className="mt-4">
            {showSection
              ? <p className={CLS.section}>{admin.label}</p>
              : <div className="mb-2 mx-3 h-px bg-border" />
            }
            <div className="space-y-0.5">
              {admin.groups.map((g) => {
                const ch = navChildrenForGroup(g, perms);
                if (ch.length === 0) return null;
                if (g.id === "admin_imports" && ch.length === 1) {
                  return <NavLink key={g.id} item={ch[0]!} alwaysFull={alwaysFull} />;
                }
                return (
                  <AccordionItem
                    key={g.id}
                    groupKey={`nav-${admin.id}-${g.id}`}
                    item={{ label: g.label, icon: getSidebarIcon(g.icon), children: ch }}
                    alwaysFull={alwaysFull}
                  />
                );
              })}
            </div>
          </div>
        ) : null}
      </>
    );
  }

  // ── Mobile drawer ─────────────────────────────────────────────────────────
  const mobileDrawer = (
    <>
      <div
        className="fixed inset-0 z-[200] bg-foreground/20 backdrop-blur-sm"
        onClick={closeMenu}
      />
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Navigation"
        className="fixed left-0 top-0 z-[210] flex h-full w-[280px] max-w-[85vw] flex-col border-r border-sidebar-border bg-sidebar shadow-2xl animate-drawer-slide-in-left"
      >
        <div className="flex h-14 shrink-0 items-center justify-between border-b border-sidebar-border px-4">
          <Link
            href="/"
            onClick={closeMenu}
            className="flex min-w-0 flex-1 items-center gap-2.5 rounded-lg outline-none ring-sidebar-ring transition hover:bg-sidebar-accent/25 focus-visible:ring-2"
            title="Home / Dashboard"
          >
            <LogoMark />
            <div className="min-w-0">
              <p className="truncate text-sm font-bold text-sidebar-foreground">
                {platformNameLoading && !platformAppName ? "…" : platformAppName || "·"}
              </p>
              <p className="truncate text-[10px] text-muted-foreground">{PLATFORM_TAGLINE}</p>
            </div>
          </Link>
          <button
            type="button"
            onClick={closeMenu}
            aria-label="Close menu"
            className="flex h-9 w-9 items-center justify-center rounded-full border border-border text-muted-foreground transition hover:bg-accent hover:text-accent-foreground"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <DrawerWorkspaceBar onClose={closeMenu} />

        <nav className="min-h-0 flex-1 overflow-y-auto overflow-x-hidden px-2 py-4">
          <SidebarBody alwaysFull collapsed={false} />
        </nav>
      </div>
    </>
  );

  // Auth routes render without the main app shell chrome.
  if (isAuthRoute) {
    return (
      <GlobalSearchProvider>
        <MobileMenuCtx.Provider value={{ openMobileMenu: () => setMobileOpen(true) }}>
          <div className="min-h-screen bg-background">{children}</div>
        </MobileMenuCtx.Provider>
      </GlobalSearchProvider>
    );
  }

  // ── Full layout ───────────────────────────────────────────────────────────
  return (
    <GlobalSearchProvider>
      <MobileMenuCtx.Provider value={{ openMobileMenu: () => setMobileOpen(true) }}>
        <div className="flex min-h-screen bg-background">

          {/* Desktop sidebar */}
          <aside
            className={[
              "sticky top-0 hidden h-screen flex-col overflow-hidden",
              "border-r border-sidebar-border bg-sidebar",
              "transition-[width] duration-300 ease-in-out md:flex",
              collapsed ? "w-16" : "w-60",
            ].join(" ")}
          >
            <Link
              href="/"
              className={[
                "flex h-14 shrink-0 items-center border-b border-sidebar-border px-4 min-w-0 overflow-hidden outline-none ring-sidebar-ring transition hover:bg-sidebar-accent/25 focus-visible:ring-2",
                collapsed ? "justify-center" : "gap-2.5",
              ].join(" ")}
              title="Home / Dashboard"
            >
              <LogoMark />
              {!collapsed && (
                <div className="min-w-0">
                  <p className="truncate text-sm font-bold text-sidebar-foreground">
                    {platformNameLoading && !platformAppName ? "…" : platformAppName || "·"}
                  </p>
                  <p className="truncate text-[10px] text-muted-foreground">{PLATFORM_TAGLINE}</p>
                </div>
              )}
            </Link>

            <nav className="min-h-0 flex-1 overflow-y-auto overflow-x-hidden px-2 py-4">
              <SidebarBody collapsed={collapsed} />
            </nav>

            <div className="shrink-0 border-t border-sidebar-border p-2">
              <button
                type="button"
                onClick={toggleCollapsed}
                title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
                className={[
                  CLS.linkBase, CLS.linkIdle,
                  collapsed ? "justify-center" : "",
                ].join(" ")}
              >
                {collapsed
                  ? <PanelLeftOpen  className="h-5 w-5 shrink-0" />
                  : <PanelLeftClose className="h-5 w-5 shrink-0" />}
                {!collapsed && <span>Collapse</span>}
              </button>
            </div>
          </aside>

          {/* Main column */}
          <div className="flex min-h-0 min-w-0 w-full flex-1 flex-col">
            <TopHeader onMenuClick={() => setMobileOpen(true)} />
            <div className="flex min-h-0 w-full min-w-0 flex-1 flex-col overflow-auto">
              {children}
            </div>
          </div>

          {mobileOpen && mounted && createPortal(mobileDrawer, document.body)}

          <TechDebugPanel open={techDebugOpen} onClose={() => setTechDebugOpen(false)} />
        </div>
      </MobileMenuCtx.Provider>
    </GlobalSearchProvider>
  );
}

// ─── TechDebugNavButton — opens TechDebugPanel (super_admin) ─────────────────

function TechDebugNavButton({
  collapsed,
  alwaysFull,
  onClick,
  child = false,
}: {
  collapsed:  boolean;
  alwaysFull: boolean;
  onClick:    () => void;
  /** Indent like accordion sub-links (Platform Settings). */
  child?:     boolean;
}) {
  const showText = alwaysFull || !collapsed;

  return (
    <button
      type="button"
      onClick={onClick}
      aria-label="Open Tech Debug panel"
      className={[
        CLS.linkBase, CLS.linkIdle,
        child && showText ? "pl-9 pr-3" : "",
        collapsed && !showText ? "justify-center" : "",
      ].join(" ")}
    >
      <Wrench className={[child ? "h-4 w-4" : "h-5 w-5", "shrink-0"].join(" ")} />

      {showText && (
        <>
          <span className="flex-1 truncate text-left">Tech Debug</span>
          <span className="shrink-0 rounded-full bg-violet-100 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide text-violet-700 dark:bg-violet-950/50 dark:text-violet-300">
            SA
          </span>
        </>
      )}

      {!showText && (
        <span
          role="tooltip"
          className="invisible absolute left-full top-1/2 z-50 ml-3 -translate-y-1/2 whitespace-nowrap rounded-xl border border-border bg-popover px-3 py-1.5 text-xs font-semibold text-popover-foreground shadow-lg group-hover:visible"
        >
          Tech Debug
        </span>
      )}
    </button>
  );
}
