"use client";

import { Info } from "lucide-react";
import { useState, type ReactNode } from "react";

export type PageHeaderWithInfoProps = {
  title: ReactNode;
  children: ReactNode;
  /** Merged with `text-balance` on the &lt;h1&gt;. */
  titleClassName?: string;
  className?: string;
  helpPanelClassName?: string;
  /** @default "Page information" */
  infoAriaLabel?: string;
};

/**
 * Page title with a compact (i) button that toggles the descriptive copy — keeps headers clean
 * and matches platform Access Management behavior.
 */
export function PageHeaderWithInfo({
  title,
  children,
  titleClassName = "text-2xl font-bold tracking-tight text-foreground",
  className = "mb-6",
  helpPanelClassName = "mt-3 max-w-3xl space-y-2 rounded-lg border border-border bg-muted/20 p-3 text-sm text-muted-foreground",
  infoAriaLabel = "Page information",
}: PageHeaderWithInfoProps) {
  const [open, setOpen] = useState(false);
  return (
    <div className={className}>
      <div className="flex flex-wrap items-center gap-2">
        <h1 className={["text-balance", titleClassName].filter(Boolean).join(" ")}>{title}</h1>
        <button
          type="button"
          onClick={() => setOpen((o) => !o)}
          className="inline-flex h-7 w-7 shrink-0 items-center justify-center rounded-full border border-border bg-background text-muted-foreground transition hover:bg-muted/80 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
          aria-expanded={open}
          aria-label={infoAriaLabel}
        >
          <Info className="h-3.5 w-3.5" aria-hidden />
        </button>
      </div>
      {open ? <div className={helpPanelClassName}>{children}</div> : null}
    </div>
  );
}
