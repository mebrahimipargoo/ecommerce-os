"use client";

import React, {
  useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState,
} from "react";
import { createPortal } from "react-dom";
import { Building2, ChevronDown, Search } from "lucide-react";
import type { WorkspaceOrganizationOption } from "@/app/session/tenant-actions";
import { isUuidString } from "@/lib/uuid";

type Props = {
  options: WorkspaceOrganizationOption[];
  value: string | null | undefined;
  onChange: (organizationId: string) => void;
  disabled?: boolean;
  dense?: boolean;
  highZ?: boolean;
  /** For `<label htmlFor="…">` */
  buttonId?: string;
  ariaLabel?: string;
  /** Full trigger `className` (e.g. form select). If omitted, a compact header style is used. */
  triggerClassName?: string;
  /** Show building icon before the label (off when a logo sits beside the control). */
  leadingIcon?: boolean;
};

const PANEL_Z_NORMAL = "z-[80]";
const PANEL_Z_DRAWER = "z-[320]";

export function WorkspaceOrganizationPicker({
  options,
  value,
  onChange,
  disabled,
  dense,
  highZ,
  buttonId,
  ariaLabel = "Switch workspace organization",
  triggerClassName,
  leadingIcon = true,
}: Props) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const [mounted, setMounted] = useState(false);
  const [panelBox, setPanelBox] = useState<{
    top: number;
    left: number;
    width: number;
    maxHeight: number;
  } | null>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setMounted(true);
  }, []);

  const selectedLabel = useMemo(() => {
    const id = (value ?? "").trim();
    if (!id) return "Select company…";
    const row = options.find((o) => o.organization_id === id);
    return row?.display_name?.trim() || id;
  }, [value, options]);

  const filtered = useMemo(() => {
    const qq = q.trim().toLowerCase();
    if (!qq) return options;
    return options.filter(
      (o) =>
        o.display_name.toLowerCase().includes(qq) ||
        o.organization_id.toLowerCase().includes(qq),
    );
  }, [options, q]);

  const updatePosition = useCallback(() => {
    const el = buttonRef.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    const width = Math.max(r.width, 200);
    const margin = 8;
    const below = r.bottom + margin;
    const maxHeight = Math.min(384, Math.max(120, window.innerHeight - below - margin));
    setPanelBox({
      top: below,
      left: r.left,
      width,
      maxHeight,
    });
  }, []);

  useLayoutEffect(() => {
    if (!open) {
      setPanelBox(null);
      return;
    }
    updatePosition();
    window.addEventListener("resize", updatePosition);
    document.addEventListener("scroll", updatePosition, true);
    return () => {
      window.removeEventListener("resize", updatePosition);
      document.removeEventListener("scroll", updatePosition, true);
    };
  }, [open, updatePosition]);

  useEffect(() => {
    if (!open) return;
    function onDoc(e: MouseEvent) {
      const t = e.target as Node;
      if (buttonRef.current?.contains(t)) return;
      if (panelRef.current?.contains(t)) return;
      setOpen(false);
    }
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  useEffect(() => {
    if (!open) setQ("");
  }, [open]);

  const pick = useCallback(
    (id: string) => {
      const t = id.trim();
      if (!isUuidString(t)) return;
      onChange(t);
      setOpen(false);
      setQ("");
    },
    [onChange],
  );

  const panelZ = highZ ? PANEL_Z_DRAWER : PANEL_Z_NORMAL;

  const builtInTrigger = [
    "flex w-full min-w-0 items-center justify-between gap-1 rounded-md border border-border bg-background px-2 py-1.5 text-left text-xs font-medium shadow-sm outline-none ring-offset-background hover:bg-muted/60 focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50",
    dense ? "py-1" : "",
  ]
    .filter(Boolean)
    .join(" ");

  const panel =
    open && mounted && panelBox
      ? createPortal(
          <div
            ref={panelRef}
            role="listbox"
            aria-label={ariaLabel}
            className={[
              "fixed flex max-h-[min(24rem,72vh)] flex-col overflow-hidden rounded-lg border border-border bg-popover text-popover-foreground shadow-lg",
              panelZ,
            ].join(" ")}
            style={{
              top: panelBox.top,
              left: panelBox.left,
              width: panelBox.width,
              maxHeight: panelBox.maxHeight,
            }}
          >
            <div className="shrink-0 border-b border-border p-2">
              <label className="relative block">
                <Search
                  className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground"
                  aria-hidden
                />
                <input
                  type="search"
                  value={q}
                  onChange={(e) => setQ(e.target.value)}
                  placeholder="Search company name…"
                  className="h-8 w-full rounded-md border border-input bg-background py-1 pl-8 pr-2 text-xs text-foreground placeholder:text-muted-foreground"
                  autoFocus
                />
              </label>
            </div>
            <div className="min-h-0 flex-1 overflow-y-auto py-1">
              {filtered.map((o) => (
                <button
                  key={o.organization_id}
                  type="button"
                  role="option"
                  aria-selected={value === o.organization_id}
                  className="flex w-full flex-col items-start gap-0.5 px-3 py-2 text-left text-xs hover:bg-accent"
                  onClick={() => pick(o.organization_id)}
                >
                  <span className="font-medium text-foreground">{o.display_name}</span>
                  <span className="font-mono text-[10px] text-muted-foreground">
                    {o.organization_id}
                  </span>
                </button>
              ))}
              {filtered.length === 0 ? (
                <p className="px-3 py-4 text-center text-[11px] text-muted-foreground">
                  No matching companies.
                </p>
              ) : null}
            </div>
          </div>,
          document.body,
        )
      : null;

  return (
    <div className={`relative min-w-0 flex-1 ${highZ ? "z-[300]" : ""}`}>
      <button
        id={buttonId}
        ref={buttonRef}
        type="button"
        disabled={disabled}
        onClick={() => setOpen((o) => !o)}
        className={triggerClassName?.trim() ? triggerClassName : builtInTrigger}
        aria-expanded={open}
        aria-haspopup="listbox"
        aria-label={ariaLabel}
      >
        <span className="flex min-w-0 flex-1 items-center gap-1.5">
          {leadingIcon ? (
            <Building2 className="h-3.5 w-3.5 shrink-0 opacity-60" aria-hidden />
          ) : null}
          <span className="min-w-0 truncate">{selectedLabel}</span>
        </span>
        <ChevronDown
          className={`h-3.5 w-3.5 shrink-0 opacity-60 transition ${open ? "rotate-180" : ""}`}
          aria-hidden
        />
      </button>
      {panel}
    </div>
  );
}
