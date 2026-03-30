"use client";

import React, { useEffect, useId, useMemo, useRef, useState } from "react";
import { Check, ChevronDown } from "lucide-react";
import { AI_ROLE_TAG_GROUPS, normalizeAIRoleTag } from "../../lib/openai-settings";

type Props = {
  id: string;
  value: string;
  onChange: (value: string) => void;
  disabled?: boolean;
  className?: string;
  placeholder?: string;
  "aria-label"?: string;
};

const PANEL_CLS =
  "absolute left-0 right-0 top-full z-50 mt-1 max-h-[min(22rem,70vh)] overflow-y-auto rounded-lg border border-border bg-popover p-2 text-popover-foreground shadow-lg";

export function RoleTagCombobox({
  id,
  value,
  onChange,
  disabled,
  className = "",
  placeholder = "Select or type a tag…",
  "aria-label": ariaLabel = "Role tag",
}: Props) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const rootRef = useRef<HTMLDivElement>(null);
  const searchId = useId();

  const allPresets = useMemo(
    () => AI_ROLE_TAG_GROUPS.flatMap((g) => g.tags.map((t) => ({ ...t, groupLabel: g.group }))),
    [],
  );

  const q = query.trim().toLowerCase();

  const filteredGroups = useMemo(() => {
    if (!q) return AI_ROLE_TAG_GROUPS;
    return AI_ROLE_TAG_GROUPS.map((g) => ({
      ...g,
      tags: g.tags.filter(
        (t) =>
          t.value.toLowerCase().includes(q) ||
          t.label.toLowerCase().includes(q) ||
          t.description.toLowerCase().includes(q),
      ),
    })).filter((g) => g.tags.length > 0);
  }, [q]);

  const commitCustom = () => {
    const next = normalizeAIRoleTag(query);
    onChange(next);
    setOpen(false);
    setQuery("");
  };

  const pickPreset = (v: string) => {
    onChange(normalizeAIRoleTag(v));
    setOpen(false);
    setQuery("");
  };

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);

  useEffect(() => {
    if (open) setQuery("");
  }, [open]);

  return (
    <div ref={rootRef} className="relative w-full">
      <button
        type="button"
        id={id}
        disabled={disabled}
        aria-label={ariaLabel}
        aria-expanded={open}
        aria-haspopup="listbox"
        onClick={() => !disabled && setOpen((o) => !o)}
        className={[
          "flex h-10 w-full items-center justify-between gap-2 rounded-md border border-input bg-background px-3 py-2 text-left text-sm shadow-sm ring-offset-background",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2",
          "disabled:cursor-not-allowed disabled:opacity-50",
          className,
        ].join(" ")}
      >
        <span className={`min-w-0 truncate ${value ? "text-foreground" : "text-muted-foreground"}`}>
          {value || placeholder}
        </span>
        <ChevronDown className={`h-4 w-4 shrink-0 opacity-60 transition ${open ? "rotate-180" : ""}`} />
      </button>

      {open && (
        <div className={PANEL_CLS} role="listbox" aria-labelledby={searchId}>
          <div className="sticky top-0 z-10 border-b border-border bg-popover pb-2">
            <label htmlFor={searchId} className="sr-only">
              Filter tags
            </label>
            <input
              id={searchId}
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault();
                  const qt = query.trim();
                  if (!qt) return;
                  const exact = allPresets.find((t) => t.value.toLowerCase() === qt.toLowerCase());
                  if (exact) pickPreset(exact.value);
                  else commitCustom();
                }
              }}
              placeholder="Search or type a custom tag…"
              autoComplete="off"
              spellCheck={false}
              className="flex h-9 w-full rounded-md border border-input bg-background px-2.5 py-1 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            />
          </div>

          <div className="space-y-3 pt-2">
            {filteredGroups.map((g) => (
              <div key={g.group}>
                <p className="px-1.5 pb-1 text-[10px] font-bold uppercase tracking-wide text-muted-foreground">
                  {g.group}
                </p>
                <p className="mb-1.5 px-1.5 text-[11px] leading-snug text-muted-foreground/90">
                  {g.description}
                </p>
                <ul className="space-y-0.5">
                  {g.tags.map((t) => {
                    const selected = value === t.value;
                    return (
                      <li key={t.value}>
                        <button
                          type="button"
                          role="option"
                          aria-selected={selected}
                          onClick={() => pickPreset(t.value)}
                          className={[
                            "flex w-full flex-col items-start gap-0.5 rounded-md px-2 py-1.5 text-left text-sm transition",
                            selected
                              ? "bg-sky-100 text-sky-900 dark:bg-sky-950/60 dark:text-sky-100"
                              : "hover:bg-muted/80",
                          ].join(" ")}
                        >
                          <span className="flex w-full items-center gap-2 font-mono text-xs font-semibold">
                            <span className="min-w-0 flex-1 truncate">{t.label}</span>
                            {selected && <Check className="h-3.5 w-3.5 shrink-0 text-sky-600 dark:text-sky-400" />}
                          </span>
                          <span className="text-[11px] font-normal text-muted-foreground">{t.description}</span>
                        </button>
                      </li>
                    );
                  })}
                </ul>
              </div>
            ))}
          </div>

          {q.length > 0 &&
            !allPresets.some((t) => t.value.toLowerCase() === q) && (
            <div className="mt-2 border-t border-border pt-2">
              <button
                type="button"
                onClick={commitCustom}
                className="w-full rounded-md border border-dashed border-sky-300 bg-sky-50/80 px-2 py-2 text-left text-xs font-medium text-sky-900 transition hover:bg-sky-100 dark:border-sky-700 dark:bg-sky-950/40 dark:text-sky-100 dark:hover:bg-sky-950/60"
              >
                Use custom tag{" "}
                <code className="rounded bg-white px-1 font-mono dark:bg-sky-900/80">
                  {normalizeAIRoleTag(query)}
                </code>
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
