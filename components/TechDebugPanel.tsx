"use client";

/**
 * TechDebugPanel — Super-admin granular developer tooling panel.
 *
 * Opened via the "🛠️ Tech Debug" sidebar item (super_admin only, controlled by
 * canSeeTechDebug in useRbacPermissions).
 *
 * Manages AdminDebugContext flags through DebugModeContext:
 *   • Master debug mode (gates everything)
 *   • Show DB Table Names   → powers <DebugTag tableName="..." />
 *   • Show API Logs         → future: log API calls to console
 *   • Show Raw JSON         → future: render raw JSON beneath data cards
 *
 * Rendered via createPortal at document.body so it always overlays the
 * sidebar + topbar correctly on both desktop and mobile.
 */

import React from "react";
import { createPortal } from "react-dom";
import {
  Activity,
  Bug,
  ChevronRight,
  Database,
  FileJson,
  Terminal,
  Wrench,
  X,
} from "lucide-react";
import { useDebugMode } from "./DebugModeContext";
import { useUserRole } from "./UserRoleContext";

// ─── Types ────────────────────────────────────────────────────────────────────

interface TechDebugPanelProps {
  open: boolean;
  onClose: () => void;
}

// ─── ToggleRow sub-component ──────────────────────────────────────────────────

function ToggleRow({
  label,
  description,
  checked,
  onChange,
  icon: Icon,
  disabled = false,
}: {
  label: string;
  description: string;
  checked: boolean;
  onChange: (v: boolean) => void;
  icon: React.ElementType;
  disabled?: boolean;
}) {
  const isOn = checked && !disabled;

  return (
    <div
      className={[
        "flex items-start gap-3 rounded-xl p-3 transition-colors",
        disabled ? "opacity-40" : "hover:bg-accent cursor-pointer",
      ].join(" ")}
      onClick={() => !disabled && onChange(!checked)}
      role="button"
      tabIndex={disabled ? -1 : 0}
      onKeyDown={(e) => { if (!disabled && (e.key === "Enter" || e.key === " ")) onChange(!checked); }}
    >
      {/* Icon badge */}
      <div
        className={[
          "mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border transition-colors",
          isOn
            ? "border-emerald-400 bg-emerald-50 text-emerald-600 dark:border-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-400"
            : "border-border bg-muted text-muted-foreground",
        ].join(" ")}
      >
        <Icon className="h-4 w-4" />
      </div>

      {/* Label + description */}
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium text-foreground leading-snug">{label}</p>
        <p className="text-xs text-muted-foreground mt-0.5 leading-relaxed">{description}</p>
      </div>

      {/* Toggle switch */}
      <button
        type="button"
        role="switch"
        aria-checked={isOn}
        disabled={disabled}
        onClick={(e) => { e.stopPropagation(); if (!disabled) onChange(!checked); }}
        className={[
          "relative mt-0.5 inline-flex h-6 w-11 shrink-0 rounded-full border border-transparent transition-colors",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2",
          isOn ? "bg-emerald-600 dark:bg-emerald-500" : "bg-muted",
          disabled ? "cursor-not-allowed" : "cursor-pointer",
        ].join(" ")}
      >
        <span
          className={[
            "pointer-events-none inline-block h-5 w-5 rounded-full bg-white shadow ring-0 transition-transform duration-200",
            isOn ? "translate-x-5" : "translate-x-0.5",
          ].join(" ")}
        />
      </button>
    </div>
  );
}

// ─── Section divider ──────────────────────────────────────────────────────────

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <p className="mb-1 px-3 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground/60">
      {children}
    </p>
  );
}

// ─── Main panel ───────────────────────────────────────────────────────────────

export function TechDebugPanel({ open, onClose }: TechDebugPanelProps) {
  const {
    debugMode, setDebugMode,
    showDbTableNames, setShowDbTableNames,
    showApiLogs,      setShowApiLogs,
    showRawJson,      setShowRawJson,
    resetAllDebugFlags,
  } = useDebugMode();

  const { role, actorName, actorUserId, homeOrganizationId } = useUserRole();

  if (!open) return null;
  if (typeof document === "undefined") return null;

  const panel = (
    <>
      {/* Backdrop */}
      <div
        aria-hidden
        className="fixed inset-0 z-[300] bg-foreground/20 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Slide-over panel */}
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Tech Debug Panel"
        className="fixed right-0 top-0 z-[310] flex h-full w-full max-w-sm flex-col border-l border-border bg-card shadow-2xl animate-drawer-slide-in-right"
      >
        {/* ── Header ─────────────────────────────────────────────────────────── */}
        <div className="flex h-14 shrink-0 items-center justify-between border-b border-border px-4">
          <div className="flex items-center gap-2.5">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg border border-violet-200 bg-violet-50 dark:border-violet-800 dark:bg-violet-950/50">
              <Bug className="h-4 w-4 text-violet-600 dark:text-violet-400" />
            </div>
            <div className="leading-tight">
              <p className="text-sm font-bold text-foreground">Tech Debug</p>
              <p className="text-[10px] text-muted-foreground">Super Admin Developer Tools</p>
            </div>
          </div>

          <button
            type="button"
            onClick={onClose}
            aria-label="Close debug panel"
            className="flex h-9 w-9 items-center justify-center rounded-full border border-border text-muted-foreground transition hover:bg-accent hover:text-accent-foreground"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        {/* ── Scrollable content ──────────────────────────────────────────────── */}
        <div className="flex-1 overflow-y-auto overflow-x-hidden p-4 space-y-6">

          {/* Master toggle */}
          <div>
            <SectionLabel>Master</SectionLabel>
            <ToggleRow
              label="Debug Mode"
              description="Master switch — enables all debug display flags below"
              checked={debugMode}
              onChange={setDebugMode}
              icon={Terminal}
            />
          </div>

          {/* Display flags */}
          <div>
            <SectionLabel>Display Flags</SectionLabel>
            <div className="space-y-0.5">
              <ToggleRow
                label="Show DB Table Names"
                description={`Attach #db:table_name annotation pills to data sections via <DebugTag />`}
                checked={showDbTableNames}
                onChange={setShowDbTableNames}
                icon={Database}
                disabled={!debugMode}
              />
              <ToggleRow
                label="Show API Logs"
                description="Log all API requests + responses to the browser DevTools console"
                checked={showApiLogs}
                onChange={setShowApiLogs}
                icon={Activity}
                disabled={!debugMode}
              />
              <ToggleRow
                label="Show Raw JSON"
                description="Render the raw data payload JSON beneath each card and table row"
                checked={showRawJson}
                onChange={setShowRawJson}
                icon={FileJson}
                disabled={!debugMode}
              />
            </div>
          </div>

          {/* Session info */}
          <div>
            <SectionLabel>Session Info</SectionLabel>
            <div className="rounded-xl border border-border bg-muted/40 p-3 space-y-2 font-mono text-xs">
              <Row k="role" v={role} highlight />
              <Row k="actor" v={actorName} />
              {actorUserId && <Row k="user_id" v={actorUserId} mono small />}
              {homeOrganizationId && <Row k="org_id" v={homeOrganizationId} mono small />}
            </div>
          </div>

          {/* DebugTag usage snippet */}
          <div>
            <SectionLabel>Developer Usage</SectionLabel>
            <div className="rounded-xl border border-dashed border-border bg-muted/30 p-3">
              <div className="flex items-center gap-1.5 mb-2">
                <Wrench className="h-3.5 w-3.5 text-muted-foreground" />
                <span className="text-[10px] font-semibold uppercase tracking-wide text-muted-foreground">DebugTag component</span>
              </div>
              <pre className="text-[10px] text-muted-foreground overflow-x-auto whitespace-pre-wrap leading-relaxed">{`import { DebugTag } from "@/components/DebugTag";

// In any component:
<h2>
  Returns <DebugTag tableName="return_items" />
</h2>

// Visible only when:
// debugMode === true && showDbTableNames === true`}</pre>
            </div>
          </div>

          {/* RoleGuard snippet */}
          <div>
            <div className="rounded-xl border border-dashed border-border bg-muted/30 p-3">
              <div className="flex items-center gap-1.5 mb-2">
                <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
                <span className="text-[10px] font-semibold uppercase tracking-wide text-muted-foreground">RoleGuard component</span>
              </div>
              <pre className="text-[10px] text-muted-foreground overflow-x-auto whitespace-pre-wrap leading-relaxed">{`import { RoleGuard } from "@/components/RoleGuard";

<RoleGuard minRole="admin">
  <AdminOnlyWidget />
</RoleGuard>

<RoleGuard minRole="super_admin" fallback={<Denied />}>
  <PlatformPanel />
</RoleGuard>`}</pre>
            </div>
          </div>

        </div>

        {/* ── Footer — reset button ───────────────────────────────────────────── */}
        <div className="shrink-0 border-t border-border p-4">
          <button
            type="button"
            onClick={() => { resetAllDebugFlags(); onClose(); }}
            className="w-full rounded-xl border border-destructive/40 bg-destructive/10 py-2.5 text-xs font-semibold text-destructive transition hover:bg-destructive/20 active:scale-[0.98]"
          >
            Reset All Debug Flags &amp; Close
          </button>
        </div>
      </div>
    </>
  );

  return createPortal(panel, document.body);
}

// ─── Helper: session info row ─────────────────────────────────────────────────

function Row({
  k, v, highlight = false, mono = false, small = false,
}: {
  k: string; v: string; highlight?: boolean; mono?: boolean; small?: boolean;
}) {
  return (
    <div className="flex items-start justify-between gap-2">
      <span className="shrink-0 text-muted-foreground">{k}</span>
      <span
        className={[
          "text-right break-all",
          highlight ? "font-bold text-violet-600 dark:text-violet-400 uppercase" : "font-semibold text-foreground",
          mono ? "font-mono" : "",
          small ? "text-[10px]" : "",
        ].join(" ")}
      >
        {v}
      </span>
    </div>
  );
}
