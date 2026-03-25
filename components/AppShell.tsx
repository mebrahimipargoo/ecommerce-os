"use client";

/**
 * AppShell — Global layout wrapper.
 *
 * ─ Desktop  : Persistent collapsible sidebar + TopHeader (theme/profile live ONLY here).
 * ─ Mobile   : TopHeader (hamburger + logo + theme + profile) + drawer for nav.
 *
 * Content column: TopHeader (shrink-0) + scrollable main (flex-1 min-h-0 overflow-auto).
 */

import React, {
  createContext, useContext, useCallback, useEffect, useState,
} from "react";
import { createPortal } from "react-dom";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  ChevronDown, ChevronRight,
  LayoutDashboard, Menu,
  PanelLeftClose, PanelLeftOpen,
  RotateCcw, Settings, ShieldAlert, X,
} from "lucide-react";
import { TopHeader } from "./TopHeader";
import { LogoMark } from "./LogoMark";
import { GlobalSearchProvider } from "./GlobalSearchContext";
import { UserRoleProvider, useUserRole } from "./UserRoleContext";

// ─── Nav Definition ────────────────────────────────────────────────────────────

type NavChild   = { label: string; href: string; disabled?: boolean; badge?: string };
type NavItemDef = {
  label: string; icon: React.ElementType;
  href?: string; disabled?: boolean; badge?: string;
  children?: NavChild[];
};
type NavSection = { id: string; label: string; items: NavItemDef[] };

const NAV: NavSection[] = [
  {
    id: "ops",
    label: "Operations",
    items: [
      { label: "Dashboard",          icon: LayoutDashboard, href: "/" },
      { label: "Returns Processing", icon: RotateCcw,       href: "/returns" },
      { label: "Claim Engine",       icon: ShieldAlert,     href: "#", disabled: true, badge: "Soon" },
    ],
  },
  {
    id: "sys",
    label: "System",
    items: [
      { label: "Settings", icon: Settings, href: "/settings" },
    ],
  },
];

const MobileMenuCtx = createContext<{ openMobileMenu: () => void }>({ openMobileMenu: () => {} });
export const useAppShell = () => useContext(MobileMenuCtx);

const CLS = {
  linkActive: "bg-sky-50 text-sky-700 dark:bg-sky-950/60 dark:text-sky-300",
  linkIdle:   "text-muted-foreground hover:bg-accent hover:text-accent-foreground",
  linkDis:    "pointer-events-none text-muted-foreground/40",
  linkBase:   "group relative flex min-h-[40px] w-full items-center gap-3 rounded-xl px-3 py-2 text-sm font-medium transition-all",
  section:    "mb-1 px-3 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground/60",
};

export function AppShell({ children }: { children: React.ReactNode }) {
  const [collapsed,  setCollapsed]  = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [mounted,    setMounted]    = useState(false);
  const [expanded,   setExpanded]   = useState<Record<string, boolean>>({});
  const pathname = usePathname();

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
    if (href === "/") return pathname === "/";
    return pathname.startsWith(href);
  }

  useEffect(() => {
    const auto: Record<string, boolean> = {};
    NAV.forEach((sec) => {
      sec.items.forEach((item) => {
        if (item.children?.some((c) => isActive(c.href))) {
          auto[`${sec.id}-${item.label}`] = true;
        }
      });
    });
    if (Object.keys(auto).length) setExpanded((p) => ({ ...p, ...auto }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pathname]);

  const closeMenu = () => setMobileOpen(false);

  function NavLink({ item, child = false, alwaysFull = false }: {
    item: NavItemDef | NavChild;
    child?:      boolean;
    alwaysFull?: boolean;
  }) {
    const href     = (item as NavItemDef).href ?? "";
    const disabled = !!(item as NavItemDef).disabled;
    const badge    = (item as NavItemDef).badge;
    const active   = isActive(href);
    const Icon     = (item as NavItemDef).icon;
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
            "h-5 w-5 shrink-0",
            active   && !disabled ? "text-sky-600 dark:text-sky-400" : "",
            disabled ? "opacity-40" : "",
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

  function AccordionItem({ item, sectionId, alwaysFull = false }: {
    item: NavItemDef; sectionId: string; alwaysFull?: boolean;
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
          className={[
            CLS.linkBase,
            collapsed && !showText ? "justify-center" : "",
            childActive ? CLS.linkActive : CLS.linkIdle,
          ].join(" ")}
        >
          <Icon className={["h-5 w-5 shrink-0", childActive ? "text-sky-600 dark:text-sky-400" : ""].join(" ")} />
          {showText && (
            <>
              <span className="flex-1 truncate text-left">{item.label}</span>
              {open
                ? <ChevronDown  className="h-4 w-4 shrink-0 opacity-50 transition-transform" />
                : <ChevronRight className="h-4 w-4 shrink-0 opacity-50 transition-transform" />
              }
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

        {showText && open && (
          <div className="mt-0.5 space-y-0.5">
            {item.children?.map((c) => (
              <NavLink key={c.href} item={c} child alwaysFull={alwaysFull} />
            ))}
          </div>
        )}
      </div>
    );
  }

  function SidebarBody({ alwaysFull = false }: { alwaysFull?: boolean }) {
    const { role } = useUserRole();
    const showSection = alwaysFull || !collapsed;
    const visibleNav = NAV.filter((sec) => sec.id !== "sys" || role === "admin");
    return (
      <>
        {visibleNav.map((sec) => (
          <div key={sec.id} className="mb-4">
            {showSection
              ? <p className={CLS.section}>{sec.label}</p>
              : <div className="mb-2 mx-3 h-px bg-border" />
            }
            <div className="space-y-0.5">
              {sec.items.map((item) =>
                item.children?.length
                  ? <AccordionItem key={item.label} item={item} sectionId={sec.id} alwaysFull={alwaysFull} />
                  : <NavLink       key={item.label} item={item} alwaysFull={alwaysFull} />
              )}
            </div>
          </div>
        ))}
      </>
    );
  }

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
          <div className="flex items-center gap-2.5">
            <LogoMark />
            <div>
              <p className="text-sm font-bold text-sidebar-foreground">E-commerce OS</p>
              <p className="text-[10px] text-muted-foreground">Returns ERP</p>
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

        <nav className="flex-1 overflow-y-auto px-2 py-4">
          <SidebarBody alwaysFull />
        </nav>
      </div>
    </>
  );

  return (
    <UserRoleProvider>
    <GlobalSearchProvider>
    <MobileMenuCtx.Provider value={{ openMobileMenu: () => setMobileOpen(true) }}>
      <div className="flex min-h-screen bg-background">

        {/* Desktop sidebar — navigation only (no theme / profile) */}
        <aside
          className={[
            "sticky top-0 hidden h-screen flex-col overflow-hidden",
            "border-r border-sidebar-border bg-sidebar",
            "transition-[width] duration-300 ease-in-out md:flex",
            collapsed ? "w-16" : "w-60",
          ].join(" ")}
        >
          <div className={[
            "flex h-14 shrink-0 items-center border-b border-sidebar-border px-4",
            collapsed ? "justify-center" : "gap-2.5",
          ].join(" ")}>
            <LogoMark />
            {!collapsed && (
              <div className="min-w-0">
                <p className="truncate text-sm font-bold text-sidebar-foreground">E-commerce OS</p>
                <p className="text-[10px] text-muted-foreground">Returns ERP</p>
              </div>
            )}
          </div>

          <nav className="flex-1 overflow-y-auto overflow-x-hidden px-2 py-4">
            <SidebarBody />
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

        {/* Main column: global header + scrollable page content */}
        <div className="flex min-h-0 min-w-0 w-full flex-1 flex-col">
          <TopHeader onMenuClick={() => setMobileOpen(true)} />

          <div className="flex min-h-0 w-full min-w-0 flex-1 flex-col overflow-auto">
            {children}
          </div>
        </div>

        {mobileOpen && mounted && createPortal(mobileDrawer, document.body)}
      </div>
    </MobileMenuCtx.Provider>
    </GlobalSearchProvider>
    </UserRoleProvider>
  );
}
