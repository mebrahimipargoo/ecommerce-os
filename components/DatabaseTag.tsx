"use client";

import { useDebugMode } from "./DebugModeContext";

/**
 * Debug-only annotation: `#db: table_name` in the bottom-right of a `relative` parent.
 * Uses absolute positioning so toggling debug mode never changes layout (no flow space).
 */
export function DatabaseTag({
  table,
  className = "",
}: {
  /** Table or logical store name shown after `#db:` */
  table: string;
  className?: string;
}) {
  const { debugMode } = useDebugMode();
  if (!debugMode) return null;

  return (
    <span
      className={[
        "pointer-events-none absolute bottom-2 right-2 z-[5] select-none",
        "font-mono text-[10px] leading-none tracking-tight text-muted-foreground/75",
        className,
      ].join(" ")}
      aria-hidden
    >
      #db: {table}
    </span>
  );
}
