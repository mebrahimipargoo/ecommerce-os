"use client";

import React from "react";
import { Bug, Terminal } from "lucide-react";
import { useDebugMode } from "./DebugModeContext";

/**
 * System Admin sidebar — technical debug (#db: table tags). Bug + terminal motif; green when on.
 */
export function SidebarDebugToggle({ collapsed }: { collapsed: boolean }) {
  const { debugMode, setDebugMode } = useDebugMode();

  return (
    <div
      suppressHydrationWarning
      className={[
        "group mb-1 flex min-h-[40px] items-center gap-2 rounded-xl px-3 py-2",
        collapsed ? "justify-center px-2" : "",
      ].join(" ")}
    >
      {!collapsed && (
        <span className="flex min-w-0 flex-1 items-center gap-2 text-left">
          <span className="flex shrink-0 items-center gap-1 text-muted-foreground" aria-hidden>
            <Bug className="h-4 w-4" />
            <Terminal className="h-3.5 w-3.5 opacity-70" />
          </span>
          <span className="truncate text-xs font-medium text-sidebar-foreground">Technical debug</span>
        </span>
      )}
      {collapsed && (
        <>
          <span className="flex shrink-0 items-center gap-0.5 text-muted-foreground" aria-hidden>
            <Bug className="h-4 w-4" />
          </span>
          <span className="sr-only">Technical debug mode</span>
        </>
      )}
      <button
        type="button"
        role="switch"
        aria-checked={debugMode}
        title={debugMode ? "Debug mode on" : "Debug mode off"}
        onClick={() => setDebugMode(!debugMode)}
        className={[
          "relative inline-flex h-6 w-11 shrink-0 cursor-pointer rounded-full border border-transparent transition-colors",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2",
          debugMode
            ? "bg-emerald-600 dark:bg-emerald-500"
            : "bg-muted",
        ].join(" ")}
      >
        <span
          className={[
            "pointer-events-none inline-block h-5 w-5 translate-x-0.5 rounded-full bg-white shadow ring-0 transition",
            debugMode ? "translate-x-5" : "translate-x-0.5",
          ].join(" ")}
        />
      </button>
    </div>
  );
}
