"use client";

import React from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  RotateCcw,
  ShieldAlert,
  Store,
  Settings,
} from "lucide-react";
import { useUserRole } from "./UserRoleContext";

const NAV_SECTIONS = [
  {
    section: "Core",
    items: [
      { label: "Dashboard",           icon: LayoutDashboard, href: "/"                  },
      { label: "Returns Processing",  icon: RotateCcw,       href: "/returns"           },
      { label: "Claim Engine",        icon: ShieldAlert,     href: "/claim-engine"  },
    ],
  },
  {
    section: "Integrations",
    items: [
      { label: "Connected Stores", icon: Store, href: "/settings" },
    ],
  },
];

export function DesktopSidebar() {
  const pathname = usePathname();
  const { role } = useUserRole();

  return (
    <aside className="sticky top-0 hidden h-screen w-60 shrink-0 flex-col border-r border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-950 md:flex">
      {/* Logo */}
      <div className="flex h-[57px] shrink-0 items-center gap-2.5 border-b border-slate-200 px-5 dark:border-slate-800">
        <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-sky-500/10 ring-1 ring-sky-500/40">
          <span className="text-sm font-bold text-sky-500">OS</span>
        </div>
        <div>
          <p className="text-sm font-bold text-slate-900 dark:text-white">E-commerce OS</p>
          <p className="text-[10px] text-slate-400">Returns ERP</p>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto px-3 py-4">
        {NAV_SECTIONS.map((group) => (
          <div key={group.section} className="mb-6 space-y-0.5">
            <p className="mb-1 px-2 text-[10px] font-semibold uppercase tracking-widest text-slate-400">
              {group.section}
            </p>
            {group.items.map((item) => {
              const active  = pathname === item.href;
              const disabled = (item as { disabled?: boolean }).disabled;
              return (
                <Link
                  key={item.label}
                  href={item.href}
                  aria-disabled={disabled}
                  tabIndex={disabled ? -1 : undefined}
                  className={[
                    "flex min-h-[40px] w-full items-center gap-2.5 rounded-xl px-3 py-2 text-sm font-medium transition",
                    active
                      ? "bg-sky-50 text-sky-700 dark:bg-sky-950/60 dark:text-sky-300"
                      : disabled
                      ? "pointer-events-none text-slate-300 dark:text-slate-600"
                      : "text-slate-600 hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-900 dark:hover:text-slate-100",
                  ].join(" ")}
                >
                  <item.icon
                    className={[
                      "h-4 w-4 shrink-0",
                      active   ? "text-sky-600 dark:text-sky-400"  : "",
                      disabled ? "text-slate-300 dark:text-slate-700" : "",
                    ].join(" ")}
                  />
                  {item.label}
                  {active && (
                    <span className="ml-auto h-1.5 w-1.5 rounded-full bg-sky-500" />
                  )}
                  {disabled && (
                    <span className="ml-auto rounded-full bg-slate-100 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide text-slate-400 dark:bg-slate-800 dark:text-slate-500">
                      Soon
                    </span>
                  )}
                </Link>
              );
            })}
          </div>
        ))}
      </nav>

      {/* Footer — Settings link only rendered for admins */}
      {role === "admin" && (
        <div className="shrink-0 border-t border-slate-200 px-3 py-3 dark:border-slate-800">
          <Link
            href="/settings"
            className={[
              "flex min-h-[40px] w-full items-center gap-2.5 rounded-xl px-3 py-2 text-sm font-medium transition",
              pathname === "/settings"
                ? "bg-sky-50 text-sky-700 dark:bg-sky-950/60 dark:text-sky-300"
                : "text-slate-500 hover:bg-slate-100 hover:text-slate-700 dark:text-slate-400 dark:hover:bg-slate-900 dark:hover:text-slate-200",
            ].join(" ")}
          >
            <Settings className="h-4 w-4 shrink-0" />
            Settings
          </Link>
        </div>
      )}
    </aside>
  );
}
