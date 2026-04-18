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
 * `PlatformBrandingContext` — not tenant `organization_settings`. Tenant org label is only
 * in `TopHeader` via `UserRoleContext`.
 */

import React, {
  createContext, useContext, useCallback, useEffect, useState,
} from "react";
import { createPortal } from "react-dom";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Banknote, Building2, ChevronDown,
  ClipboardList, Database, DollarSign, FileText,
  Package, Palette,
  PanelLeftClose, PanelLeftOpen,
  RotateCcw, ScanLine, Settings, Shield, ShieldAlert, Users, Wrench, X,
} from "lucide-react";
import { TopHeader } from "./TopHeader";
import { BrandingProvider } from "./BrandingContext";
import { PlatformBrandingProvider, usePlatformBranding } from "./PlatformBrandingContext";
import { LogoMark } from "./LogoMark";
import { PLATFORM_TAGLINE } from "../lib/platform-branding";
import { GlobalSearchProvider } from "./GlobalSearchContext";
import { UserRoleProvider } from "./UserRoleContext";
import { TechDebugPanel } from "./TechDebugPanel";
import { useRbacPermissions } from "../hooks/useRbacPermissions";

// ─── Nav Definitions ──────────────────────────────────────────────────────────

type NavChild   = { label: string; href: string; icon?: React.ElementType; disabled?: boolean; badge?: string };
type NavItemDef = {
  label: string; icon: React.ElementType;
  href?: string; disabled?: boolean; badge?: string;
  children?: NavChild[];
};
type NavSection = { id: string; label: string; items: NavItemDef[] };

/** Warehouse Operations — accordion group */
const WAREHOUSE_GROUP: NavItemDef = {
  label: "Warehouse Operations",
  icon:  Package,
  children: [
    { label: "Returns Processing", icon: RotateCcw,     href: "/returns"   },
    { label: "Inventory Ledger",   icon: ClipboardList, href: "/inventory" },
  ],
};

/** Finance & Claims — accordion group */
const FINANCE_GROUP: NavItemDef = {
  label: "Finance & Claims",
  icon:  DollarSign,
  children: [
    { label: "Settlements",    icon: Banknote,    href: "/settlements"             },
    { label: "Claim Engine",   icon: ShieldAlert, href: "/claim-engine"            },
    { label: "Report History", icon: FileText,    href: "/claim-engine/report-history" },
  ],
};

/** System Settings — accordion group (admin+) */
const SYSTEM_GROUP: NavItemDef = {
  label: "System Settings",
  icon:  Settings,
  children: [
    { label: "Stores & Adapters", icon: Building2, href: "/settings" },
    { label: "Users",             icon: Users,     href: "/users"    },
  ],
};

/** WMS section — operators only */
const WMS_NAV: NavSection = {
  id: "wms", label: "WMS",
  items: [
    { label: "Scan Item", icon: ScanLine, href: "/returns", badge: "WMS" },
  ],
};

// All accordion groups — used for auto-expand on navigation
const ACCORDION_GROUPS = [WAREHOUSE_GROUP, FINANCE_GROUP, SYSTEM_GROUP];

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
    ACCORDION_GROUPS.forEach((group) => {
      if (group.children?.some((c) => isActive(c.href))) {
        auto[`nav-${group.label}`] = true;
      }
    });
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
            {active && <span className="ml-1 h-1.5 w-1.5 shrink-0 rounded-full bg-primary" />}
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
  function AccordionItem({ item, sectionId, alwaysFull = false }: {
    item:        NavItemDef;
    sectionId:   string;
    alwaysFull?: boolean;
  }) {
    const key         = `${sectionId}-${item.label}`;
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
              </div>
            </div>
          </div>
        )}
      </div>
    );
  }

  // ── SidebarBody ───────────────────────────────────────────────────────────
  // All visibility logic is delegated to useRbacPermissions — zero role
  // comparisons inside this component.
  function SidebarBody({
    alwaysFull = false,
    collapsed: sidebarCollapsed,
  }: {
    alwaysFull?: boolean;
    collapsed:   boolean;
  }) {
    const perms       = useRbacPermissions();
    const showSection = alwaysFull || !sidebarCollapsed;

    // ── OPERATOR: WMS-only view ──────────────────────────────────────────────
    if (perms.isWmsOnly) {
      return (
        <div className="mb-4">
          {showSection
            ? <p className={CLS.section}>{WMS_NAV.label}</p>
            : <div className="mb-2 mx-3 h-px bg-border" />
          }
          <div className="space-y-0.5">
            {WMS_NAV.items.map((item) => (
              <NavLink key={item.label} item={item} alwaysFull={alwaysFull} />
            ))}
          </div>
        </div>
      );
    }

    // ── Build filtered children per accordion group ──────────────────────────

    const warehouseChildren = [
      perms.canSeeReturns && { label: "Returns Processing", icon: RotateCcw,     href: "/returns"   },
                              { label: "Inventory Ledger",   icon: ClipboardList, href: "/inventory" },
    ].filter(Boolean) as NavChild[];

    const financeChildren = [
                                  { label: "Settlements",    icon: Banknote,    href: "/settlements"             },
      perms.canSeeClaimEngine   && { label: "Claim Engine",   icon: ShieldAlert, href: "/claim-engine"            },
      perms.canSeeReportHistory && { label: "Report History", icon: FileText,    href: "/claim-engine/report-history" },
    ].filter(Boolean) as NavChild[];

    const systemChildren = [
      perms.canSeeSettings && { label: "Stores & Adapters", icon: Building2, href: "/settings" },
      perms.canSeeUsers    && { label: "Users",              icon: Users,     href: "/users"    },
    ].filter(Boolean) as NavChild[];

    return (
      <>
        {/* ── Core accordion groups ─────────────────────────────────────── */}
        <div className="space-y-1">
          {warehouseChildren.length > 0 && (
            <AccordionItem
              item={{ ...WAREHOUSE_GROUP, children: warehouseChildren }}
              sectionId="nav"
              alwaysFull={alwaysFull}
            />
          )}

          {financeChildren.length > 0 && (
            <AccordionItem
              item={{ ...FINANCE_GROUP, children: financeChildren }}
              sectionId="nav"
              alwaysFull={alwaysFull}
            />
          )}

          {systemChildren.length > 0 && (
            <AccordionItem
              item={{ ...SYSTEM_GROUP, children: systemChildren }}
              sectionId="nav"
              alwaysFull={alwaysFull}
            />
          )}
        </div>

        {/* ── Tech Debug (internal technical roles; not platform super-admin menu) ── */}
        {perms.canSeeTechDebug && !perms.canSeePlatformAdmin && (
          <div className="mt-4">
            {showSection
              ? <p className={CLS.section}>Developer</p>
              : <div className="mb-2 mx-3 h-px bg-border" />
            }
            <div className="space-y-0.5">
              <TechDebugNavButton
                collapsed={sidebarCollapsed}
                alwaysFull={alwaysFull}
                onClick={openTechDebug}
              />
            </div>
          </div>
        )}

        {/* ── System Admin — Imports (admin+) ───────────────────────────── */}
        {perms.canSeeSystemAdmin && perms.canSeeImports && (
          <div className="mt-4">
            {showSection
              ? <p className={CLS.section}>System Admin</p>
              : <div className="mb-2 mx-3 h-px bg-border" />
            }
            <div className="space-y-0.5">
              <NavLink
                item={{ label: "Imports", icon: Database, href: "/imports" }}
                alwaysFull={alwaysFull}
              />
            </div>
          </div>
        )}

        {/* ── Platform: super_admin (admin + platform branding); internal staff see section for tech debug only ── */}
        {(perms.canSeePlatformAdmin || perms.isAtLeast("system_employee")) && (
          <div className="mt-4">
            {showSection
              ? <p className={CLS.section}>Platform</p>
              : <div className="mb-2 mx-3 h-px bg-border" />
            }
            <div className="space-y-0.5">
              {perms.canSeePlatformAdmin && (
                <NavLink
                  item={{ label: "Admin Settings", icon: Shield, href: "/admin/settings" }}
                  alwaysFull={alwaysFull}
                />
              )}
              {perms.canSeePlatformAdmin && (
                <NavLink
                  item={{ label: "Platform branding", icon: Palette, href: "/platform/settings" }}
                  alwaysFull={alwaysFull}
                />
              )}
              {perms.canSeeTechDebug && (
                <TechDebugNavButton
                  collapsed={sidebarCollapsed}
                  alwaysFull={alwaysFull}
                  onClick={openTechDebug}
                />
              )}
            </div>
          </div>
        )}
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
          <div className="flex min-w-0 flex-1 items-center gap-2.5">
            <LogoMark />
            <div className="min-w-0">
              <p className="truncate text-sm font-bold text-sidebar-foreground">
                {platformNameLoading && !platformAppName ? "…" : platformAppName || "·"}
              </p>
              <p className="truncate text-[10px] text-muted-foreground">{PLATFORM_TAGLINE}</p>
            </div>
          </div>
          <button
            type="button"
            onClick={closeMenu}
            aria-label="Close menu"
            className="flex h-9 w-9 items-center justify-center rounded-full border border-border text-muted-foreground transition hover:bg-accent hover:text-accent-foreground"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

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
            <div className={[
              "flex h-14 shrink-0 items-center border-b border-sidebar-border px-4 min-w-0 overflow-hidden",
              collapsed ? "justify-center" : "gap-2.5",
            ].join(" ")}>
              <LogoMark />
              {!collapsed && (
                <div className="min-w-0">
                  <p className="truncate text-sm font-bold text-sidebar-foreground">
                    {platformNameLoading && !platformAppName ? "…" : platformAppName || "·"}
                  </p>
                  <p className="truncate text-[10px] text-muted-foreground">{PLATFORM_TAGLINE}</p>
                </div>
              )}
            </div>

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
}: {
  collapsed:  boolean;
  alwaysFull: boolean;
  onClick:    () => void;
}) {
  const showText = alwaysFull || !collapsed;

  return (
    <button
      type="button"
      onClick={onClick}
      aria-label="Open Tech Debug panel"
      className={[
        CLS.linkBase, CLS.linkIdle,
        collapsed && !showText ? "justify-center" : "",
      ].join(" ")}
    >
      <Wrench className="h-5 w-5 shrink-0" />

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
