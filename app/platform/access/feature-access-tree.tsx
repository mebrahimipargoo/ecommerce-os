"use client";

import React, { useCallback, useRef, useState } from "react";
import Link from "next/link";
import { ChevronDown, ChevronRight, Info, MoreVertical } from "lucide-react";
import {
  effectiveLevelFromFeatureBucket,
  type UiAccessLevel,
  UI_ACCESS_LEVEL_LABEL,
  UI_ACCESS_LEVELS,
} from "../../../lib/access-level";
import {
  GENERAL_FEATURE_KEY,
  orgEntitlementUiStatus,
  type OrgEntitlementUiStatus,
  totalPermissionsInBucket,
  type ModuleFeatureTreeNode,
  type OrgEntitlementsPayload,
} from "../../../lib/access-entitlements";
import type { UserOverrideChoice } from "../../../lib/user-feature-access";

type Mode = "user" | "role" | "group";

const USER_OVERRIDE_OPTIONS: { value: UserOverrideChoice; label: string }[] = [
  { value: "inherit", label: "Inherit" },
  { value: "none", label: "None" },
  { value: "read", label: "Read" },
  { value: "write", label: "Write" },
];

const OVERRIDE_CHOICE_LABEL: Record<UserOverrideChoice, string> = {
  inherit: "Inherit",
  none: "None",
  read: "Read",
  write: "Write",
};

export type FeatureAccessTreeProps = {
  tree: ModuleFeatureTreeNode[];
  mode: Mode;
  /** Role/group: all assigned permission ids. User tab: role + group for reference (not used to render the user row; see `userFeatureAccessByModuleFeatureId`). */
  effectivePermissionIds: ReadonlySet<string>;
  orgEntitlements: OrgEntitlementsPayload | null;
  /** User tab: per `module_feature_id` from `getUserEffectiveAccessAction`. */
  userFeatureAccessByModuleFeatureId?: Readonly<Record<string, { baseline: UiAccessLevel; override: UserOverrideChoice; effective: UiAccessLevel }>> | null;
  busy?: boolean;
  disabled?: boolean;
  onSetLevel: (args: { moduleId: string; featureKey: string; level: UiAccessLevel; scope: "target" }) => void;
  onSetUserOverride?: (args: { moduleFeatureId: string; level: UserOverrideChoice }) => void;
  /** Role/Group: apply R/W/M to every feature in a module. */
  onSetModuleLevel?: (args: { moduleId: string; level: UiAccessLevel }) => void;
  /** User: bulk override for all `module_feature` rows in a module (inherit, none, read, write, or manage as write). */
  onSetModuleUserBulk?: (args: { moduleId: string; level: UiAccessLevel | "inherit" }) => void;
};

function titleizeModuleKey(moduleKey: string): string {
  return moduleKey
    .split(/[_\s]+/)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ");
}

const ENTITLEMENT_STATUS_TITLE: Record<OrgEntitlementUiStatus, string> = {
  enabled:
    "Enabled in organization entitlements (organization_modules / organization_module_features).",
  disabled:
    "Disabled in organization entitlements. You can still assign access; runtime may block use.",
  not_configured:
    "Not configured: no explicit entitlement row for this organization (or org not in explicit mode). You can still assign access.",
};

function EntitlementStatusPill({ status, className = "" }: { status: OrgEntitlementUiStatus; className?: string }) {
  const title = ENTITLEMENT_STATUS_TITLE[status];
  if (status === "enabled") {
    return (
      <span
        className={["inline-flex shrink-0 items-center justify-center", className].filter(Boolean).join(" ")}
        title={title}
      >
        <span
          className="h-1.5 w-1.5 rounded-full bg-emerald-500 ring-1 ring-emerald-500/30 dark:bg-emerald-400 dark:ring-emerald-400/25"
          aria-label="Enabled in organization entitlements"
        />
      </span>
    );
  }
  if (status === "disabled") {
    return (
      <span
        className={["inline-flex shrink-0 items-center justify-center", className].filter(Boolean).join(" ")}
        title={title}
      >
        <span
          className="h-1.5 w-1.5 rounded-full bg-rose-500 ring-1 ring-rose-500/30 dark:bg-rose-400 dark:ring-rose-400/25"
          aria-label="Disabled in organization entitlements"
        />
      </span>
    );
  }
  return (
    <span
      className={["inline-flex shrink-0 items-center justify-center", className].filter(Boolean).join(" ")}
      title={title}
    >
      <span
        className="h-1.5 w-1.5 rounded-full bg-zinc-400 ring-1 ring-zinc-400/40 dark:bg-zinc-500 dark:ring-zinc-500/35"
        aria-label="Not configured in organization entitlements"
      />
    </span>
  );
}

function moduleBulkActionLabel(level: UiAccessLevel): string {
  const base = UI_ACCESS_LEVEL_LABEL[level];
  return `Set all to ${base}`;
}

/** Compact 3-dot menu; None / Read / Write / Manage for whole module. User “Manage” maps to write override in the server action. */
function ModuleBulkMenu({
  mode,
  moduleId,
  disabled,
  onSetModuleLevel,
  onSetModuleUserBulk,
}: {
  mode: Mode;
  moduleId: string;
  disabled: boolean;
  onSetModuleLevel?: (args: { moduleId: string; level: UiAccessLevel }) => void;
  onSetModuleUserBulk?: (args: { moduleId: string; level: UiAccessLevel | "inherit" }) => void;
}) {
  const detailsRef = useRef<HTMLDetailsElement>(null);
  const close = useCallback(() => {
    if (detailsRef.current) {
      detailsRef.current.open = false;
    }
  }, []);

  const items: readonly UiAccessLevel[] = ["none", "read", "write", "manage"];

  if (mode === "user" && onSetModuleUserBulk) {
    return (
      <details ref={detailsRef} className="relative z-20 shrink-0">
        <summary
          className="flex h-7 w-7 cursor-pointer list-none items-center justify-center rounded-md text-muted-foreground transition hover:bg-muted hover:text-foreground [&::-webkit-details-marker]:hidden"
          onClick={(e) => e.stopPropagation()}
          aria-label="Set access for all features in this module"
        >
          <MoreVertical className="h-4 w-4" aria-hidden />
        </summary>
        <div
          className="absolute right-0 top-full z-30 mt-0.5 min-w-[12.5rem] rounded-md border border-border bg-card py-1 text-sm shadow-md"
          onClick={(e) => e.stopPropagation()}
        >
          {items.map((level) => (
            <button
              key={level}
              type="button"
              disabled={disabled}
              className="flex w-full px-3 py-1.5 text-left text-sm text-foreground hover:bg-muted/80 disabled:cursor-not-allowed disabled:opacity-50"
              onClick={() => {
                onSetModuleUserBulk({ moduleId, level });
                close();
              }}
            >
              {moduleBulkActionLabel(level)}
            </button>
          ))}
        </div>
      </details>
    );
  }

  if (mode !== "user" && onSetModuleLevel) {
    return (
      <details ref={detailsRef} className="relative z-20 shrink-0">
        <summary
          className="flex h-7 w-7 cursor-pointer list-none items-center justify-center rounded-md text-muted-foreground transition hover:bg-muted hover:text-foreground [&::-webkit-details-marker]:hidden"
          onClick={(e) => e.stopPropagation()}
          aria-label="Set access for all features in this module"
        >
          <MoreVertical className="h-4 w-4" aria-hidden />
        </summary>
        <div
          className="absolute right-0 top-full z-30 mt-0.5 min-w-[12.5rem] rounded-md border border-border bg-card py-1 text-sm shadow-md"
          onClick={(e) => e.stopPropagation()}
        >
          {items.map((level) => (
            <button
              key={level}
              type="button"
              disabled={disabled}
              className="flex w-full px-3 py-1.5 text-left text-sm text-foreground hover:bg-muted/80 disabled:cursor-not-allowed disabled:opacity-50"
              onClick={() => {
                onSetModuleLevel({ moduleId, level });
                close();
              }}
            >
              {moduleBulkActionLabel(level)}
            </button>
          ))}
        </div>
      </details>
    );
  }

  return null;
}

function LevelSelect({
  value,
  onChange,
  disabled,
  id,
  className = "",
}: {
  value: UiAccessLevel;
  onChange: (l: UiAccessLevel) => void;
  disabled: boolean;
  id: string;
  className?: string;
}) {
  return (
    <select
      id={id}
      className={[
        "rounded-md border border-input bg-background px-2 text-xs font-medium text-foreground shadow-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 sm:text-sm",
        "h-8 min-w-[5.5rem] sm:min-w-[7rem]",
        className,
      ]
        .filter(Boolean)
        .join(" ")}
      value={value}
      disabled={disabled}
      title="Access: None, Read, Write, or Manage"
      aria-label="Access level"
      onChange={(e) => {
        const v = e.target.value;
        if (v === "none" || v === "read" || v === "write" || v === "manage") {
          onChange(v);
        }
      }}
    >
      {UI_ACCESS_LEVELS.map((l) => (
        <option key={l} value={l}>
          {UI_ACCESS_LEVEL_LABEL[l]}
        </option>
      ))}
    </select>
  );
}

/**
 * One row per feature: optional org dot; role/group use an access level combobox; user mode uses the override combobox.
 */
export function FeatureAccessTree({
  tree,
  mode,
  effectivePermissionIds,
  orgEntitlements,
  userFeatureAccessByModuleFeatureId = null,
  busy = false,
  disabled = false,
  onSetLevel,
  onSetUserOverride,
  onSetModuleLevel,
  onSetModuleUserBulk,
}: FeatureAccessTreeProps) {
  const [expandedMod, setExpandedMod] = useState<Record<string, boolean>>(() => ({}));
  /** User tab: which feature rows have debug details open (baseline / override / key). */
  const [userRowDetailsOpen, setUserRowDetailsOpen] = useState<Record<string, boolean>>({});

  const toggleMod = useCallback((id: string) => {
    setExpandedMod((p) => ({ ...p, [id]: p[id] === false ? true : false }));
  }, []);

  const isModOpen = (id: string) => expandedMod[id] === true;

  if (tree.length === 0) {
    return (
      <p className="rounded-lg border border-dashed border-border bg-muted/20 px-4 py-6 text-sm text-muted-foreground">
        The access catalog is empty, or it could not be loaded. Ensure <code className="rounded bg-muted px-1">modules</code> and{" "}
        <code className="rounded bg-muted px-1">module_features</code> (and <code className="rounded bg-muted px-1">permissions</code>
        ) are populated. Company licensing is managed separately on{" "}
        <Link href="/platform/organizations" className="font-medium text-primary underline">
          Organizations (Modules &amp; entitlements)
        </Link>
        .
      </p>
    );
  }

  return (
    <div className="space-y-2 text-sm">
      {tree.map((node) => {
        const mOpen = isModOpen(node.module.id);
        const label = node.module.name || titleizeModuleKey(node.module.key);
        const moduleEntitlementPill = orgEntitlementUiStatus({
          snapshot: orgEntitlements,
          moduleId: node.module.id,
          moduleFeatureId: null,
          featureKey: GENERAL_FEATURE_KEY,
        });
        return (
          <div key={node.module.id} className="rounded-lg border border-border bg-card shadow-sm">
            <div className="flex w-full min-h-[2.5rem] flex-wrap items-center gap-2 px-2.5 py-1.5 sm:gap-3 sm:px-3 sm:py-2">
              <button
                type="button"
                onClick={() => toggleMod(node.module.id)}
                className="flex min-w-0 flex-1 items-center gap-2 text-left text-sm font-semibold transition hover:bg-muted/50"
              >
                {mOpen ? (
                  <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" />
                ) : (
                  <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />
                )}
                <span className="truncate">{label}</span>
                <span className="shrink-0 text-xs font-normal text-muted-foreground">
                  {node.features.length} feature{node.features.length === 1 ? "" : "s"}
                </span>
              </button>
              <div className="ml-auto flex shrink-0 items-center gap-1.5 sm:gap-2">
                <EntitlementStatusPill status={moduleEntitlementPill} />
                <ModuleBulkMenu
                  mode={mode}
                  moduleId={node.module.id}
                  disabled={disabled || busy}
                  onSetModuleLevel={onSetModuleLevel}
                  onSetModuleUserBulk={onSetModuleUserBulk}
                />
              </div>
            </div>
            {mOpen ? (
              <div className="border-t border-border">
                {node.features.map((bucket, fIdx) => {
                  const n = totalPermissionsInBucket(bucket);
                  const gLabel = bucket.sidebarGroupLabel?.trim();
                  const prevLabel =
                    fIdx > 0 ? String(node.features[fIdx - 1]?.sidebarGroupLabel ?? "").trim() : "";
                  const showGroupHeader = Boolean(
                    gLabel && (fIdx === 0 || gLabel !== prevLabel),
                  );
                  const feat = bucket.feature;
                  const moduleFeatureId =
                    feat && !String(feat.id).startsWith("synthetic:") ? String(feat.id) : null;
                  const licenseStatus = orgEntitlementUiStatus({
                    snapshot: orgEntitlements,
                    moduleId: node.module.id,
                    moduleFeatureId,
                    featureKey: bucket.featureKey,
                  });
                  const eff = effectiveLevelFromFeatureBucket(bucket, effectivePermissionIds);
                  const rowId = `${node.module.id}::${bucket.featureKey}`;
                  const uRow =
                    mode === "user" && moduleFeatureId && userFeatureAccessByModuleFeatureId
                      ? userFeatureAccessByModuleFeatureId[moduleFeatureId]
                      : null;

                  const userDetailsOpen = mode === "user" ? userRowDetailsOpen[rowId] === true : false;
                  const safeRowId = rowId.replace(/[^a-zA-Z0-9_-]/g, "_");
                  return (
                    <div
                      key={rowId}
                      className="border-b border-border/60 last:border-0"
                    >
                      {showGroupHeader && gLabel ? (
                        <p className="border-b border-border/40 bg-muted/30 px-2.5 py-1.5 pl-3 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground sm:px-3">
                          {gLabel}
                        </p>
                      ) : null}
                      {mode === "user" && moduleFeatureId && uRow && onSetUserOverride ? (
                        <>
                          <div className="flex items-center gap-2 px-2.5 py-1.5 sm:pl-3 sm:pr-2">
                            <div className="min-w-0 flex-1">
                              <div className="truncate text-sm font-medium text-foreground">
                                {feat?.name ?? titleizeModuleKey(bucket.featureKey)}
                              </div>
                              {n === 0 ? (
                                <p className="text-[10px] text-amber-700 dark:text-amber-300">No catalog permissions for this feature.</p>
                              ) : null}
                            </div>
                            <label className="sr-only" htmlFor={`ov-${safeRowId}`}>
                              Access override for {feat?.name ?? bucket.featureKey}
                            </label>
                            <select
                              id={`ov-${safeRowId}`}
                              className="h-7 w-[7.5rem] shrink-0 rounded-md border border-input bg-background px-2 text-xs tabular-nums shadow-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 sm:w-28"
                              value={uRow.override}
                              disabled={disabled || busy || n === 0}
                              title={
                                n === 0
                                  ? undefined
                                  : `Override: ${OVERRIDE_CHOICE_LABEL[uRow.override]}. Resulting access: ${UI_ACCESS_LEVEL_LABEL[uRow.effective]} (baseline ${UI_ACCESS_LEVEL_LABEL[uRow.baseline]})`
                              }
                              onChange={(e) => {
                                const v = e.target.value;
                                if (v === "inherit" || v === "none" || v === "read" || v === "write") {
                                  onSetUserOverride({ moduleFeatureId, level: v });
                                }
                              }}
                            >
                              {USER_OVERRIDE_OPTIONS.map(({ value: v, label: lbl }) => (
                                <option key={v} value={v}>
                                  {lbl}
                                </option>
                              ))}
                            </select>
                            <button
                              type="button"
                              className={[
                                "inline-flex h-6 w-6 shrink-0 items-center justify-center rounded text-muted-foreground transition hover:bg-muted/80 hover:text-foreground",
                                userDetailsOpen ? "bg-muted/60 text-foreground" : "",
                              ].join(" ")}
                              aria-expanded={userDetailsOpen}
                              aria-label={userDetailsOpen ? "Hide access details" : "Show access details (baseline, override, key)"}
                              title="Details"
                              onClick={() =>
                                setUserRowDetailsOpen((prev) => ({ ...prev, [rowId]: !prev[rowId] }))
                              }
                            >
                              <Info className="h-3.5 w-3.5" aria-hidden />
                            </button>
                            <EntitlementStatusPill status={licenseStatus} className="sm:ml-0.5" />
                          </div>
                          {userDetailsOpen ? (
                            <div className="border-t border-border/40 bg-muted/15 px-2.5 py-1.5 pl-3 text-[10px] leading-snug text-muted-foreground sm:px-3">
                              <p>
                                Effective: {UI_ACCESS_LEVEL_LABEL[uRow.effective]} · Baseline:{" "}
                                {UI_ACCESS_LEVEL_LABEL[uRow.baseline]} · Override: {OVERRIDE_CHOICE_LABEL[uRow.override]}
                              </p>
                              <p className="mt-0.5 font-mono text-[10px] text-muted-foreground/80">{bucket.featureKey}</p>
                            </div>
                          ) : null}
                        </>
                      ) : mode === "user" ? (
                        <div className="flex flex-col gap-1 px-3 py-2 sm:flex-row sm:items-center sm:gap-3">
                          <div className="min-w-0 flex-1">
                            <div className="font-medium text-foreground">
                              {feat?.name ?? titleizeModuleKey(bucket.featureKey)}
                            </div>
                            {n === 0 ? (
                              <p className="mt-0.5 text-xs text-amber-700 dark:text-amber-300">No permissions in catalog for this feature.</p>
                            ) : null}
                          </div>
                          <div className="text-[11px] text-muted-foreground">
                            {moduleFeatureId && !uRow ? "…" : "—"}
                          </div>
                        </div>
                      ) : (
                        <div className="flex flex-col gap-0.5 px-2.5 py-1.5 sm:flex-row sm:items-center sm:gap-3 sm:pl-3 sm:pr-2">
                          <div className="min-w-0 flex-1">
                            <div className="text-sm font-medium text-foreground">
                              {feat?.name ?? titleizeModuleKey(bucket.featureKey)}
                            </div>
                            {n === 0 ? (
                              <p className="text-[10px] text-amber-700 dark:text-amber-300">No permissions in catalog for this feature.</p>
                            ) : null}
                          </div>
                          <div className="flex flex-wrap items-end gap-2 sm:justify-end">
                            <div className="flex min-w-0 flex-col gap-0.5">
                              <label className="sr-only" htmlFor={`access-lvl-${safeRowId}`}>
                                Access for {feat?.name ?? bucket.featureKey}
                              </label>
                              <LevelSelect
                                id={`access-lvl-${safeRowId}`}
                                value={eff}
                                disabled={disabled || busy || n === 0}
                                onChange={(level) => {
                                  onSetLevel({
                                    moduleId: node.module.id,
                                    featureKey: bucket.featureKey,
                                    level,
                                    scope: "target",
                                  });
                                }}
                                className="max-w-[10rem]"
                              />
                            </div>
                            <EntitlementStatusPill status={licenseStatus} />
                          </div>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            ) : null}
          </div>
        );
      })}
    </div>
  );
}
