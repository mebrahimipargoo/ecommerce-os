"use client";

import React, { useState, useEffect } from "react";
import { createPortal } from "react-dom";
import Link from "next/link";
import {
  FileText,
  FileUp,
  LayoutDashboard,
  Package,
  RotateCcw,
  ShieldAlert,
  Settings,
  Store,
  Menu,
  X,
} from "lucide-react";
import { isAdminRole, useUserRole } from "./UserRoleContext";

const navLinks = [
  {
    section: "Core",
    items: [
      { label: "Dashboard", icon: LayoutDashboard, href: "/" },
      { label: "Returns Processing", icon: RotateCcw, href: "/returns" },
      { label: "Claim Engine", icon: ShieldAlert, href: "/claim-engine" },
      { label: "Report history", icon: FileText, href: "/claim-engine/report-history" },
      { label: "Product Information Management", icon: Package, href: "/dashboard/products" },
    ],
  },
  {
    section: "Data Management",
    items: [{ label: "Imports", icon: FileUp, href: "/dashboard/file-import" }],
  },
  {
    section: "Integrations",
    items: [
      { label: "Connected Stores", icon: Store, href: "/settings" },
    ],
  },
];

export function MobileNav() {
  const { role } = useUserRole();
  const [isOpen, setIsOpen] = useState(false);
  // Portal requires the DOM to be mounted — avoid SSR mismatch
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    queueMicrotask(() => setMounted(true));
  }, []);

  // Prevent body scroll when drawer is open
  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "";
    }
    return () => {
      document.body.style.overflow = "";
    };
  }, [isOpen]);

  const closeMenu = () => setIsOpen(false);

  const drawer = (
    <>
      {/* Backdrop — portaled to body, immune to parent stacking contexts */}
      <div
        className="fixed inset-0 z-[200] bg-slate-950/70 backdrop-blur-sm"
        onClick={closeMenu}
        aria-hidden="true"
      />

      {/* Drawer panel */}
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Navigation menu"
        className="fixed left-0 top-0 z-[210] flex h-full w-[280px] max-w-[85vw] flex-col border-r border-slate-800 bg-slate-950 shadow-2xl animate-drawer-slide-in-left"
      >
        {/* Header */}
        <div className="flex h-14 shrink-0 items-center justify-between border-b border-slate-800 px-4">
          <div className="flex items-center gap-2">
            <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-sky-500/10 ring-1 ring-sky-500/40">
              <span className="text-lg font-semibold text-sky-400">OS</span>
            </div>
            <span className="text-sm font-semibold text-slate-50">E‑commerce OS</span>
          </div>
          <button
            type="button"
            onClick={closeMenu}
            className="flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full border border-slate-800 text-slate-300 transition hover:border-sky-500/60 hover:text-sky-400"
            aria-label="Close menu"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Nav */}
        <nav className="flex-1 overflow-y-auto px-3 py-4">
          {navLinks.map((group) => (
            <div key={group.section} className="mb-6 space-y-1">
              <p className="px-2 text-xs font-medium uppercase tracking-wide text-slate-500">
                {group.section}
              </p>
              {group.items.map((item) => (
                <Link
                  key={item.label}
                  href={item.href}
                  onClick={closeMenu}
                  className="flex min-h-[44px] w-full items-center gap-2 rounded-lg px-2.5 py-2 text-sm font-medium text-slate-400 transition hover:bg-slate-900/60 hover:text-slate-100"
                >
                  <item.icon className="h-4 w-4 shrink-0 text-slate-500" />
                  {item.label}
                </Link>
              ))}
            </div>
          ))}
        </nav>

        {/* Footer */}
        {isAdminRole(role) && (
          <div className="shrink-0 border-t border-slate-800 px-3 py-3">
            <Link
              href="/settings"
              onClick={closeMenu}
              className="flex min-h-[44px] w-full items-center gap-2 rounded-lg px-2.5 py-2 text-sm font-medium text-slate-400 transition hover:bg-slate-900/80 hover:text-slate-100"
            >
              <Settings className="h-4 w-4 shrink-0 text-slate-500" />
              Settings
            </Link>
          </div>
        )}
      </div>
    </>
  );

  return (
    <>
      <button
        type="button"
        onClick={() => setIsOpen(true)}
        className="flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full border border-slate-700 text-slate-300 transition hover:border-sky-500/60 hover:text-sky-400 md:hidden"
        aria-label="Open menu"
      >
        <Menu className="h-5 w-5" />
      </button>

      {/* Portal renders directly on document.body — escapes all stacking contexts */}
      {isOpen && mounted && createPortal(drawer, document.body)}
    </>
  );
}
