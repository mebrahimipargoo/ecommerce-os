"use client";

/**
 * <DebugTag tableName="return_items" />
 *
 * Renders a violet #db:table_name annotation pill when BOTH the master
 * debugMode AND the "Show DB Table Names" granular flag are enabled.
 * Returns null in production / when flags are off — zero render cost.
 *
 * Usage:
 *   import { DebugTag } from "@/components/DebugTag";
 *
 *   <h2>Returns <DebugTag tableName="return_items" /></h2>
 *   <Card><DebugTag tableName="packages" className="mb-2" /> ...</Card>
 */

import React from "react";
import { useDebugMode } from "./DebugModeContext";

interface DebugTagProps {
  /** Postgres table (or view) name to annotate — e.g. "return_items". */
  tableName: string;
  /** Extra Tailwind classes on the pill wrapper. */
  className?: string;
}

export function DebugTag({ tableName, className = "" }: DebugTagProps) {
  const { debugMode, showDbTableNames } = useDebugMode();

  if (!debugMode || !showDbTableNames) return null;

  return (
    <span
      suppressHydrationWarning
      title={`PostgreSQL table: ${tableName}`}
      aria-label={`Debug: DB table ${tableName}`}
      className={[
        "inline-flex items-center gap-0.5 rounded px-1.5 py-0.5",
        "font-mono text-[9px] font-semibold uppercase tracking-wide",
        "border border-violet-200 bg-violet-50 text-violet-700",
        "dark:border-violet-800 dark:bg-violet-950/50 dark:text-violet-300",
        "select-none",
        className,
      ].join(" ")}
    >
      #db:{tableName}
    </span>
  );
}
