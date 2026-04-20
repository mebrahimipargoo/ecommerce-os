"use client";

import React, {
  useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState,
} from "react";
import { createPortal } from "react-dom";
import { ChevronDown, Search, User } from "lucide-react";
import type { ViewAsProfileRow } from "@/app/session/view-as-actions";
import { isUuidString } from "@/lib/uuid";

type Props = {
  actorName: string;
  actorUserId: string | null;
  options: ViewAsProfileRow[];
  value: string | null;
  onChange: (profileId: string | null) => void;
  disabled?: boolean;
  /** Smaller control (drawer / mobile). */
  dense?: boolean;
  /** Raise dropdown above drawer overlay (z-index). */
  highZ?: boolean;
};

/** Below full-screen modals (often z-90+); above app TopHeader (z-40). */
const PANEL_Z_NORMAL = "z-[80]";
/** Above mobile nav drawer (z-210). */
const PANEL_Z_DRAWER = "z-[320]";

export function ViewAsUserPicker({
  actorName,
  actorUserId,
  options,
  value,
  onChange,
  disabled,
  dense,
  highZ,
}: Props) {
  const others = useMemo(
    () => options.filter((p) => p.profile_id !== actorUserId),
    [options, actorUserId],
  );
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

  const updatePosition = useCallback(() => {
    const el = buttonRef.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    const width = Math.max(r.width, 192);
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

  const filtered = useMemo(() => {
    const qq = q.trim().toLowerCase();
    if (!qq) return others;
    return others.filter(
      (p) =>
        p.full_name.toLowerCase().includes(qq) ||
        p.role_key.toLowerCase().includes(qq) ||
        p.profile_id.toLowerCase().includes(qq),
    );
  }, [others, q]);

  const selectedLabel = useMemo(() => {
    if (!value) return `Yourself (${actorName})`;
    const row = others.find((p) => p.profile_id === value);
    return row ? `${row.full_name} · ${row.role_key.replace(/_/g, " ")}` : "…";
  }, [value, others, actorName]);

  const pick = useCallback(
    (id: string | null) => {
      onChange(id && isUuidString(id) ? id : null);
      setOpen(false);
      setQ("");
    },
    [onChange],
  );

  const panelZ = highZ ? PANEL_Z_DRAWER : PANEL_Z_NORMAL;

  const panel =
    open && mounted && panelBox
      ? createPortal(
          <div
            ref={panelRef}
            role="listbox"
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
                  placeholder="Search name or role…"
                  className="h-8 w-full rounded-md border border-input bg-background py-1 pl-8 pr-2 text-xs text-foreground placeholder:text-muted-foreground"
                  autoFocus
                />
              </label>
            </div>
            <div className="min-h-0 flex-1 overflow-y-auto py-1">
              <button
                type="button"
                role="option"
                aria-selected={!value}
                className="flex w-full items-center px-3 py-2 text-left text-xs hover:bg-accent"
                onClick={() => pick(null)}
              >
                <span className="font-medium">Yourself</span>
                <span className="ml-1 truncate text-muted-foreground">({actorName})</span>
              </button>
              {filtered.map((p) => (
                <button
                  key={p.profile_id}
                  type="button"
                  role="option"
                  aria-selected={value === p.profile_id}
                  className="flex w-full flex-col items-start gap-0.5 px-3 py-2 text-left text-xs hover:bg-accent"
                  onClick={() => pick(p.profile_id)}
                >
                  <span className="font-medium text-foreground">{p.full_name}</span>
                  <span className="text-[10px] text-muted-foreground">
                    {p.role_key.replace(/_/g, " ")}
                  </span>
                </button>
              ))}
              {filtered.length === 0 ? (
                <p className="px-3 py-4 text-center text-[11px] text-muted-foreground">
                  No matching users.
                </p>
              ) : null}
            </div>
          </div>,
          document.body,
        )
      : null;

  return (
    <div className={`relative min-w-0 ${highZ ? "z-[300]" : ""}`}>
      <button
        ref={buttonRef}
        type="button"
        disabled={disabled}
        onClick={() => setOpen((o) => !o)}
        className={[
          "flex w-full min-w-0 items-center justify-between gap-1 rounded-md border border-border bg-background px-2 py-1.5 text-left text-xs font-medium shadow-sm outline-none ring-offset-background hover:bg-muted/60 focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50",
          dense ? "py-1" : "",
        ].join(" ")}
        aria-expanded={open}
        aria-haspopup="listbox"
        aria-label="View application as workspace user"
      >
        <span className="flex min-w-0 items-center gap-1.5">
          <User className="h-3.5 w-3.5 shrink-0 opacity-60" aria-hidden />
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
