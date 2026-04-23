"use client";

import React, { useCallback, useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import { ChevronDown, ChevronRight, Loader2, MoreVertical } from "lucide-react";
import { getPlatformAccessPageAccessAction } from "../../../access/access-actions";
import {
  totalPermissionsInBucket,
  type ModuleFeatureTreeNode,
  type ModuleFeatureBucket,
  type OrgEntitlementsPayload,
} from "../../../../../lib/access-entitlements";
import {
  getOrganizationEntitlementsEditorAction,
  setAllModuleFeatureEntitlementsForModuleAction,
  setOrganizationModuleEntitlementModeAction,
  setOrganizationModuleFeatureEntitlementModeAction,
  type OrganizationEntitlementMode,
} from "../../organization-entitlements-actions";
import { responsivePageInner, responsivePageOuter, responsivePageNarrow } from "../../../../../lib/responsive-page-shell";
import { PageHeaderWithInfo } from "../../../components/page-header-with-info";

const SELECT_INPUT =
  "flex h-9 w-full min-w-[10.5rem] max-w-xs rounded-md border border-input bg-background px-2 py-1.5 text-sm text-foreground shadow-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50";

const MODE_MENU: { value: OrganizationEntitlementMode; label: string }[] = [
  { value: "not_configured", label: "Not configured" },
  { value: "enabled", label: "Enabled" },
  { value: "disabled", label: "Disabled" },
];

/** Horizontal left/right switch: off (left, gray) · on (right, green). */
function ModuleOnOffSwitch({
  enabled,
  disabled,
  onToggle,
  label,
}: {
  enabled: boolean;
  disabled: boolean;
  onToggle: () => void;
  label: string;
}) {
  return (
    <button
      type="button"
      role="switch"
      dir="ltr"
      aria-checked={enabled}
      title={enabled ? "On" : "Off"}
      aria-label={`${label} on/off`}
      disabled={disabled}
      onClick={(e) => {
        e.stopPropagation();
        onToggle();
      }}
      className={[
        "inline-flex h-7 w-12 shrink-0 touch-manipulation items-center rounded-full p-0.5 transition",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2",
        enabled ? "justify-end bg-emerald-500/90 dark:bg-emerald-600" : "justify-start bg-muted-foreground/35 dark:bg-zinc-600",
        disabled ? "cursor-not-allowed opacity-50" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <span className="h-5 w-5 rounded-full bg-white shadow dark:bg-zinc-100" />
    </button>
  );
}

function menuButtonClass(disabled: boolean): string {
  return [
    "flex w-full px-3 py-1.5 text-left text-sm text-foreground",
    "hover:bg-muted/80",
    disabled ? "cursor-not-allowed opacity-50" : "",
  ]
    .filter(Boolean)
    .join(" ");
}

/** Module (always) + all-features bulk (only when `moduleUnlocked` — module must be On). */
function ModuleEntitlementsMenu({
  moduleLabel,
  disabled,
  allFeaturesDisabled,
  onModule,
  onAllFeatures,
}: {
  moduleLabel: string;
  disabled: boolean;
  /** When false, "All features" menu items are disabled (module not On / gray switch left). */
  allFeaturesDisabled: boolean;
  onModule: (mode: OrganizationEntitlementMode) => void;
  onAllFeatures: (mode: OrganizationEntitlementMode) => void;
}) {
  const ref = useRef<HTMLDetailsElement>(null);
  const close = useCallback(() => {
    if (ref.current) ref.current.open = false;
  }, []);
  return (
    <details ref={ref} className="relative z-20 shrink-0">
      <summary
        className="flex h-8 w-8 cursor-pointer list-none items-center justify-center rounded-md text-muted-foreground transition hover:bg-muted hover:text-foreground [&::-webkit-details-marker]:hidden"
        onClick={(e) => e.stopPropagation()}
        aria-label={`Module and bulk: ${moduleLabel}`}
        title="Module and bulk"
      >
        <MoreVertical className="h-4 w-4" aria-hidden />
      </summary>
      <div
        className="absolute right-0 top-full z-30 mt-0.5 min-w-[11rem] rounded-md border border-border bg-card py-1 text-sm shadow-md"
        onClick={(e) => e.stopPropagation()}
      >
        <p className="px-3 py-0.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Module</p>
        {MODE_MENU.map((opt) => (
          <button
            key={opt.value}
            type="button"
            disabled={disabled}
            className={menuButtonClass(disabled)}
            onClick={() => {
              onModule(opt.value);
              close();
            }}
          >
            {opt.label}
          </button>
        ))}
        <div className="my-1 border-t border-border" role="separator" />
        <p className="px-3 py-0.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">All features</p>
        {MODE_MENU.map((opt) => {
          const d = disabled || allFeaturesDisabled;
          return (
          <button
            key={`all-${opt.value}`}
            type="button"
            disabled={d}
            className={menuButtonClass(d)}
            onClick={() => {
              onAllFeatures(opt.value);
              close();
            }}
          >
            {opt.label}
          </button>
        );
        })}
      </div>
    </details>
  );
}

function featureModeFromPayload(
  org: OrgEntitlementsPayload,
  moduleId: string,
  moduleFeatureId: string,
): OrganizationEntitlementMode {
  const m = org.moduleFeatureEntitlementModeById?.[moduleFeatureId];
  if (m === "enabled") return "enabled";
  if (m === "disabled") return "disabled";
  if (m === "not_configured") return "not_configured";
  return "not_configured";
}

function moduleModeFromPayload(org: OrgEntitlementsPayload, moduleId: string): OrganizationEntitlementMode {
  const m = org.moduleEntitlementModeById?.[moduleId];
  if (m === "enabled") return "enabled";
  if (m === "disabled") return "disabled";
  if (m === "not_configured") return "not_configured";
  if (!org.modulesExplicit) return "not_configured";
  return org.moduleEntitledById[moduleId] ? "enabled" : "disabled";
}

export default function OrganizationModulesEntitlementsPage() {
  const params = useParams();
  const rawId = params?.id;
  const organizationId = typeof rawId === "string" ? rawId : Array.isArray(rawId) ? rawId[0] : "";

  const [accessDenied, setAccessDenied] = useState<"not_authenticated" | "forbidden" | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [orgName, setOrgName] = useState("");
  const [tree, setTree] = useState<ModuleFeatureTreeNode[]>([]);
  const [orgPayload, setOrgPayload] = useState<OrgEntitlementsPayload | null>(null);
  const [saving, setSaving] = useState(false);
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);

  const showToast = useCallback((msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 4000);
  }, []);

  const refresh = useCallback(async () => {
    if (!organizationId?.trim()) return;
    const r = await getOrganizationEntitlementsEditorAction(organizationId);
    if (!r.ok) {
      setLoadError(r.error);
      return;
    }
    setLoadError(null);
    setOrgName(r.organizationName);
    setTree(r.moduleFeatureTree);
    setOrgPayload(r.orgEntitlements);
  }, [organizationId]);

  useEffect(() => {
    let c = false;
    void (async () => {
      const a = await getPlatformAccessPageAccessAction();
      if (c) return;
      if (a.accessDenied) {
        setAccessDenied(a.accessDenied);
        setLoading(false);
        return;
      }
      setAccessDenied(null);
      await refresh();
      if (c) return;
      setLoading(false);
    })();
    return () => {
      c = true;
    };
  }, [refresh]);

  const onModuleMode = useCallback(
    async (moduleId: string, mode: OrganizationEntitlementMode) => {
      if (!organizationId) return;
      setSaving(true);
      const res = await setOrganizationModuleEntitlementModeAction({ organizationId, moduleId, mode });
      setSaving(false);
      if (!res.ok) {
        showToast(res.error, false);
        return;
      }
      showToast("Module entitlement updated.", true);
      await refresh();
    },
    [organizationId, refresh, showToast],
  );

  /** Turn switch left (off): clear all feature entitlements for this module, then remove the module row. */
  const onModulePowerOff = useCallback(
    async (moduleId: string) => {
      if (!organizationId) return;
      setSaving(true);
      const clear = await setAllModuleFeatureEntitlementsForModuleAction({
        organizationId,
        moduleId,
        mode: "not_configured",
      });
      if (!clear.ok) {
        showToast(clear.error, false);
        setSaving(false);
        return;
      }
      const res = await setOrganizationModuleEntitlementModeAction({
        organizationId,
        moduleId,
        mode: "not_configured",
      });
      setSaving(false);
      if (!res.ok) {
        showToast(res.error, false);
        return;
      }
      showToast("Saved.", true);
      await refresh();
    },
    [organizationId, refresh, showToast],
  );

  /** If the module has no row yet, create an enabled one so feature/bulk changes always work. */
  const onFeatureEntitlement = useCallback(
    async (moduleId: string, featureId: string, mode: OrganizationEntitlementMode) => {
      if (!organizationId) return;
      setSaving(true);
      const o = orgPayload;
      if (o && (mode === "enabled" || mode === "disabled")) {
        if (moduleModeFromPayload(o, moduleId) === "not_configured" || !o.modulesExplicit) {
          const r0 = await setOrganizationModuleEntitlementModeAction({
            organizationId,
            moduleId,
            mode: "enabled",
          });
          if (!r0.ok) {
            showToast(r0.error, false);
            setSaving(false);
            return;
          }
          await refresh();
        }
      }
      const res = await setOrganizationModuleFeatureEntitlementModeAction({
        organizationId,
        moduleFeatureId: featureId,
        mode,
      });
      setSaving(false);
      if (!res.ok) {
        showToast(res.error, false);
        return;
      }
      showToast("Feature entitlement updated.", true);
      await refresh();
    },
    [organizationId, orgPayload, refresh, showToast],
  );

  const onAllFeaturesInModule = useCallback(
    async (moduleId: string, mode: OrganizationEntitlementMode) => {
      if (!organizationId) return;
      setSaving(true);
      const o = orgPayload;
      if (o && mode !== "not_configured") {
        if (moduleModeFromPayload(o, moduleId) === "not_configured" || !o.modulesExplicit) {
          const r0 = await setOrganizationModuleEntitlementModeAction({
            organizationId,
            moduleId,
            mode: "enabled",
          });
          if (!r0.ok) {
            showToast(r0.error, false);
            setSaving(false);
            return;
          }
          await refresh();
        }
      }
      const res = await setAllModuleFeatureEntitlementsForModuleAction({
        organizationId,
        moduleId,
        mode,
      });
      setSaving(false);
      if (!res.ok) {
        showToast(res.error, false);
        return;
      }
      showToast(
        res.count === 0
          ? "This module has no catalog features to update."
          : `Updated ${res.count} feature entitlements in this module.`,
        true,
      );
      await refresh();
    },
    [organizationId, orgPayload, refresh, showToast],
  );

  if (loading) {
    return (
      <div className={responsivePageOuter}>
        <div className={`${responsivePageInner} flex min-h-[40vh] items-center justify-center`}>
          <div className="flex items-center gap-3 text-sm text-muted-foreground">
            <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
            Loading entitlements…
          </div>
        </div>
      </div>
    );
  }

  if (accessDenied) {
    return (
      <div className={responsivePageOuter}>
        <div className={responsivePageNarrow}>
          <h1 className="text-lg font-semibold">Organization entitlements</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            {accessDenied === "not_authenticated"
              ? "You must be signed in."
              : "You do not have access. Required catalog roles: super_admin, programmer, or system_admin."}
          </p>
        </div>
      </div>
    );
  }

  if (loadError) {
    return (
      <div className={responsivePageOuter}>
        <div className={responsivePageNarrow}>
          <h1 className="text-lg font-semibold">Organization entitlements</h1>
          <p className="mt-2 text-sm text-destructive">{loadError}</p>
        </div>
      </div>
    );
  }

  return (
    <div className={responsivePageOuter}>
      {toast && (
        <div
          role="status"
          className={[
            "fixed bottom-6 right-6 z-[80] max-w-md rounded-lg border px-4 py-3 text-sm font-medium shadow-lg",
            toast.ok
              ? "border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-700/50 dark:bg-emerald-950/90 dark:text-emerald-200"
              : "border-rose-200 bg-rose-50 text-rose-800 dark:border-rose-700/50 dark:bg-rose-950/90 dark:text-rose-200",
          ].join(" ")}
        >
          {toast.msg}
        </div>
      )}
      <div className={`${responsivePageInner} min-w-0 space-y-6`}>
        <header className="space-y-2">
          <div className="flex flex-wrap items-center gap-3">
            <Link
              href={`/platform/organizations/${encodeURIComponent(organizationId)}`}
              className="text-sm font-medium text-muted-foreground transition hover:text-foreground"
            >
              ← Back to organization
            </Link>
            <span className="text-muted-foreground">|</span>
            <Link href="/platform/organizations" className="text-sm font-medium text-muted-foreground transition hover:text-foreground">
              All organizations
            </Link>
          </div>
          <PageHeaderWithInfo
            title="Modules & entitlements"
            titleClassName="text-2xl font-bold tracking-tight"
            helpPanelClassName="mt-3 max-w-2xl space-y-2 rounded-lg border border-border bg-muted/20 p-3 text-sm text-muted-foreground"
            infoAriaLabel="About modules and entitlements"
          >
            <p>
              <span className="font-medium text-foreground">{orgName}</span> — <strong className="font-medium text-foreground">module</strong>: horizontal
              switch (left off, right on); <strong className="font-medium text-foreground">features</strong>: dropdown. More options in{" "}
              <span className="whitespace-nowrap">⋯</span>.
            </p>
          </PageHeaderWithInfo>
        </header>

        {orgName ? (
          <div className="rounded-lg border border-border bg-card px-4 py-3 shadow-sm">
            <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Organization</p>
            <p className="mt-0.5 text-lg font-semibold tracking-tight text-foreground">{orgName}</p>
          </div>
        ) : null}

        <div className="space-y-2 text-sm">
          {orgPayload
            ? tree.map((node) => {
            const modOpen = open[node.module.id] !== false;
            const label = node.module.name || node.module.key;
            const o = orgPayload;
            const modMode = moduleModeFromPayload(o, node.module.id);
            const moduleOn = modMode === "enabled";
            return (
              <div key={node.module.id} className="overflow-hidden rounded-lg border border-border bg-card shadow-sm">
                <div className="flex w-full min-h-[2.5rem] flex-wrap items-center gap-2 border-b border-border/60 bg-muted/20 px-2.5 py-1.5 sm:gap-3 sm:px-3 sm:py-2">
                  <button
                    type="button"
                    onClick={() => setOpen((p) => ({ ...p, [node.module.id]: !modOpen }))}
                    className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md text-muted-foreground transition hover:bg-muted/80"
                    aria-expanded={modOpen}
                    title="Expand or collapse"
                  >
                    {modOpen ? <ChevronDown className="h-4 w-4" aria-hidden /> : <ChevronRight className="h-4 w-4" aria-hidden />}
                  </button>
                  <div className="flex min-w-0 flex-1 items-center gap-2">
                    <span className="truncate text-sm font-semibold text-foreground">{label}</span>
                    <span className="shrink-0 text-xs font-normal text-muted-foreground">
                      {node.features.length} feature{node.features.length === 1 ? "" : "s"}
                    </span>
                  </div>
                  <div className="ml-auto flex shrink-0 items-center gap-2 sm:gap-3">
                    <ModuleOnOffSwitch
                      enabled={moduleOn}
                      disabled={saving}
                      label={label}
                      onToggle={() => {
                        if (moduleOn) {
                          void onModulePowerOff(node.module.id);
                        } else {
                          void onModuleMode(node.module.id, "enabled");
                        }
                      }}
                    />
                    <ModuleEntitlementsMenu
                      moduleLabel={label}
                      disabled={saving}
                      allFeaturesDisabled={!moduleOn}
                      onModule={(m) => void onModuleMode(node.module.id, m)}
                      onAllFeatures={(m) => void onAllFeaturesInModule(node.module.id, m)}
                    />
                  </div>
                </div>
                {modOpen ? (
                  <div
                    className={["border-t border-border", !moduleOn ? "opacity-50" : ""].filter(Boolean).join(" ")}
                  >
                    {node.features.map((bucket: ModuleFeatureBucket) => {
                      const feat = bucket.feature;
                      const id =
                        feat && !String(feat.id).startsWith("synthetic:") ? String(feat.id) : null;
                      const n = totalPermissionsInBucket(bucket);
                      const title = feat?.name ?? bucket.featureKey;
                      return (
                        <div
                          key={`${node.module.id}::${bucket.featureKey}`}
                          className="flex flex-col gap-1 border-b border-border/50 px-2.5 py-2 last:border-0 sm:flex-row sm:items-center sm:justify-between sm:px-3"
                        >
                          <div className="min-w-0">
                            <div className="text-sm font-medium text-foreground">{title}</div>
                            {n === 0 ? (
                              <p className="text-[10px] text-amber-700 dark:text-amber-300">No permissions in catalog for this feature.</p>
                            ) : null}
                          </div>
                          {id ? (
                            <div className="ml-auto flex w-full min-w-0 max-w-xs shrink-0 sm:w-auto">
                              <label className="sr-only" htmlFor={`feat-ent-${node.module.id}-${id}`}>
                                {title}
                              </label>
                              <select
                                id={`feat-ent-${node.module.id}-${id}`}
                                className={SELECT_INPUT}
                                disabled={saving || !moduleOn}
                                value={featureModeFromPayload(o, node.module.id, id)}
                                onChange={(e) => {
                                  const v = e.target.value;
                                  if (v === "not_configured" || v === "enabled" || v === "disabled") {
                                    void onFeatureEntitlement(node.module.id, id, v);
                                  }
                                }}
                              >
                                <option value="not_configured">Not configured</option>
                                <option value="enabled">Enabled</option>
                                <option value="disabled">Disabled</option>
                              </select>
                            </div>
                          ) : (
                            <span className="text-xs text-muted-foreground" title="No catalog feature id (synthetic)">
                              —
                            </span>
                          )}
                        </div>
                      );
                    })}
                  </div>
                ) : null}
              </div>
            );
            })
            : null}
        </div>

        <p className="text-sm text-muted-foreground">
          <Link href="/platform/access" className="font-medium text-primary underline">
            Open Access Management
          </Link>{" "}
          for role and user assignments; entitlement status in that tree is read-only relative to this page.
        </p>
      </div>
    </div>
  );
}
