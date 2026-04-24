"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import { ChevronDown, ChevronRight, Loader2, MoreVertical } from "lucide-react";
import { getPlatformAccessPageAccessAction } from "../../../access/access-actions";
import {
  orgEntitlementUiStatus,
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
import { dispatchOrgEntitlementsUpdated } from "../../../../../lib/org-entitlements-events";

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

/** All features in this module at once. Render only when the module switch is on (see call site). */
function AllFeaturesEntitlementsMenu({
  moduleLabel,
  disabled,
  onAllFeatures,
}: {
  moduleLabel: string;
  disabled: boolean;
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
        aria-label={`All features: ${moduleLabel}`}
        title="Apply the same to every feature in this module"
      >
        <MoreVertical className="h-4 w-4" aria-hidden />
      </summary>
      <div
        className="absolute right-0 top-full z-30 mt-0.5 min-w-[12rem] rounded-md border border-border bg-card py-1 text-sm shadow-md"
        onClick={(e) => e.stopPropagation()}
      >
        <p className="px-3 py-0.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">All features</p>
        {MODE_MENU.map((opt) => (
          <button
            key={opt.value}
            type="button"
            disabled={disabled}
            className={menuButtonClass(disabled)}
            onClick={() => {
              onAllFeatures(opt.value);
              close();
            }}
          >
            {opt.label}
          </button>
        ))}
      </div>
    </details>
  );
}

function moduleModeFromPayload(org: OrgEntitlementsPayload, moduleId: string): OrganizationEntitlementMode {
  const m = org.moduleEntitlementModeById?.[moduleId];
  if (m === "enabled") return "enabled";
  if (m === "disabled") return "disabled";
  if (m === "not_configured") return "not_configured";
  if (!org.modulesExplicit) return "not_configured";
  return org.moduleEntitledById[moduleId] ? "enabled" : "disabled";
}

function cloneOrgPayload(p: OrgEntitlementsPayload): OrgEntitlementsPayload {
  return {
    ...p,
    moduleEntitledById: { ...p.moduleEntitledById },
    featuresExplicitByModuleId: { ...p.featuresExplicitByModuleId },
    featureEntitledById: { ...p.featureEntitledById },
    moduleEntitlementModeById: { ...(p.moduleEntitlementModeById ?? {}) },
    moduleFeatureEntitlementModeById: { ...(p.moduleFeatureEntitlementModeById ?? {}) },
  };
}

function clearModuleFeatureModesInTree(
  payload: OrgEntitlementsPayload,
  node: ModuleFeatureTreeNode,
): void {
  for (const b of node.features) {
    const f = b.feature;
    if (!f || String(f.id).startsWith("synthetic:")) continue;
    const fid = String(f.id);
    payload.moduleFeatureEntitlementModeById = {
      ...payload.moduleFeatureEntitlementModeById,
      [fid]: "not_configured",
    };
  }
}

/**
 * When every real feature under a module is explicitly Disabled, treat it like the module switch
 * off: module not_configured and per-feature rows cleared in draft.
 */
function turnOffModuleIfAllFeaturesAreDisabledInDraft(
  n: OrgEntitlementsPayload,
  node: ModuleFeatureTreeNode,
): void {
  const mfm = n.moduleFeatureEntitlementModeById ?? {};
  const realIds: string[] = [];
  for (const b of node.features) {
    const f = b.feature;
    if (!f || String(f.id).startsWith("synthetic:")) continue;
    realIds.push(String(f.id));
  }
  if (realIds.length === 0) return;
  const allDisabled = realIds.every((id) => mfm[id] === "disabled");
  if (!allDisabled) return;
  n.moduleEntitlementModeById = { ...n.moduleEntitlementModeById, [node.module.id]: "not_configured" };
  clearModuleFeatureModesInTree(n, node);
}

/**
 * Fails if any module is “on” but has no catalog feature, or no real feature in the draft is
 * set to "enabled" (so Save must be blocked).
 */
function canSaveOnModulesWithEnabledFeature(
  draft: OrgEntitlementsPayload,
  tree: ModuleFeatureTreeNode[],
): { ok: true } | { ok: false; moduleLabels: string[] } {
  const moduleLabels: string[] = [];
  for (const node of tree) {
    if (moduleModeFromPayload(draft, node.module.id) !== "enabled") {
      continue;
    }
    const mfm = draft.moduleFeatureEntitlementModeById ?? {};
    const realIds: string[] = [];
    for (const b of node.features) {
      const f = b.feature;
      if (!f || String(f.id).startsWith("synthetic:")) continue;
      realIds.push(String(f.id));
    }
    if (realIds.length === 0) {
      moduleLabels.push(node.module.name || node.module.key);
      continue;
    }
    if (!realIds.some((id) => mfm[id] === "enabled")) {
      moduleLabels.push(node.module.name || node.module.key);
    }
  }
  if (moduleLabels.length > 0) {
    return { ok: false, moduleLabels };
  }
  return { ok: true };
}

function entitlementsUiDirty(
  a: OrgEntitlementsPayload,
  b: OrgEntitlementsPayload,
  t: ModuleFeatureTreeNode[],
): boolean {
  for (const node of t) {
    if (moduleModeFromPayload(a, node.module.id) !== moduleModeFromPayload(b, node.module.id)) {
      return true;
    }
    for (const bucket of node.features) {
      const f = bucket.feature;
      if (!f || String(f.id).startsWith("synthetic:")) continue;
      const id = String(f.id);
      const oa = orgEntitlementUiStatus({
        snapshot: a,
        moduleId: node.module.id,
        moduleFeatureId: id,
        featureKey: bucket.featureKey,
      });
      const ob = orgEntitlementUiStatus({
        snapshot: b,
        moduleId: node.module.id,
        moduleFeatureId: id,
        featureKey: bucket.featureKey,
      });
      if (oa !== ob) return true;
    }
  }
  return false;
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
  const [draftPayload, setDraftPayload] = useState<OrgEntitlementsPayload | null>(null);
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
    setDraftPayload(cloneOrgPayload(r.orgEntitlements));
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

  const entitlementsDirty = useMemo(
    () =>
      orgPayload
      && draftPayload
      && entitlementsUiDirty(orgPayload, draftPayload, tree),
    [orgPayload, draftPayload, tree],
  );

  const onModuleMode = useCallback((moduleId: string, mode: OrganizationEntitlementMode) => {
    setDraftPayload((prev) => {
      if (!prev) return prev;
      const n = cloneOrgPayload(prev);
      n.moduleEntitlementModeById = { ...n.moduleEntitlementModeById, [moduleId]: mode };
      const node = tree.find((t) => t.module.id === moduleId);
      if (mode === "not_configured" && node) {
        clearModuleFeatureModesInTree(n, node);
        n.moduleEntitlementModeById[moduleId] = "not_configured";
      }
      return n;
    });
  }, [tree]);

  const onModulePowerOff = useCallback(
    (moduleId: string) => {
      setDraftPayload((prev) => {
        if (!prev) return prev;
        const node = tree.find((t) => t.module.id === moduleId);
        if (!node) return prev;
        const n = cloneOrgPayload(prev);
        n.moduleEntitlementModeById = { ...n.moduleEntitlementModeById, [moduleId]: "not_configured" };
        clearModuleFeatureModesInTree(n, node);
        return n;
      });
    },
    [tree],
  );

  const onFeatureEntitlement = useCallback(
    (moduleId: string, featureId: string, mode: OrganizationEntitlementMode) => {
      setDraftPayload((prev) => {
        if (!prev) return prev;
        const n = cloneOrgPayload(prev);
        if (mode === "enabled" || mode === "disabled") {
          if (moduleModeFromPayload(n, moduleId) === "not_configured" || !n.modulesExplicit) {
            n.moduleEntitlementModeById = { ...n.moduleEntitlementModeById, [moduleId]: "enabled" };
          }
        }
        n.moduleFeatureEntitlementModeById = {
          ...n.moduleFeatureEntitlementModeById,
          [featureId]: mode,
        };
        const node = tree.find((t) => t.module.id === moduleId);
        if (node) {
          turnOffModuleIfAllFeaturesAreDisabledInDraft(n, node);
        }
        return n;
      });
    },
    [tree],
  );

  const onAllFeaturesInModule = useCallback(
    (moduleId: string, mode: OrganizationEntitlementMode) => {
      setDraftPayload((prev) => {
        if (!prev) return prev;
        const node = tree.find((t) => t.module.id === moduleId);
        if (!node) return prev;
        const n = cloneOrgPayload(prev);
        if (mode === "disabled") {
          n.moduleEntitlementModeById = { ...n.moduleEntitlementModeById, [moduleId]: "not_configured" };
          clearModuleFeatureModesInTree(n, node);
          return n;
        }
        if (mode !== "not_configured") {
          if (moduleModeFromPayload(n, moduleId) === "not_configured" || !n.modulesExplicit) {
            n.moduleEntitlementModeById = { ...n.moduleEntitlementModeById, [moduleId]: "enabled" };
          }
        }
        for (const b of node.features) {
          const f = b.feature;
          if (!f || String(f.id).startsWith("synthetic:")) continue;
          n.moduleFeatureEntitlementModeById = {
            ...n.moduleFeatureEntitlementModeById,
            [String(f.id)]: mode,
          };
        }
        return n;
      });
    },
    [tree],
  );

  const handleEntitlementsCancel = useCallback(() => {
    if (orgPayload) setDraftPayload(cloneOrgPayload(orgPayload));
  }, [orgPayload]);

  const saveEntitlementsToServer = useCallback(async () => {
    if (!organizationId || !orgPayload || !draftPayload) return;
    if (!entitlementsUiDirty(orgPayload, draftPayload, tree)) return;
    const gate = canSaveOnModulesWithEnabledFeature(draftPayload, tree);
    if (!gate.ok) {
      const names = gate.moduleLabels.join(", ");
      showToast(
        `Each sub-module with the switch on must have at least one feature set to "Enabled" (or turn the module off first).${
          names ? ` Affected: ${names}.` : ""
        }`.trim(),
        false,
      );
      return;
    }
    setSaving(true);
    try {
      for (const node of tree) {
        const mid = node.module.id;
        const was = moduleModeFromPayload(orgPayload, mid);
        const will = moduleModeFromPayload(draftPayload, mid);
        if (was === "enabled" && will === "not_configured") {
          const clear = await setAllModuleFeatureEntitlementsForModuleAction({
            organizationId,
            moduleId: mid,
            mode: "not_configured",
          });
          if (!clear.ok) {
            showToast(clear.error, false);
            return;
          }
          const mres = await setOrganizationModuleEntitlementModeAction({
            organizationId,
            moduleId: mid,
            mode: "not_configured",
          });
          if (!mres.ok) {
            showToast(mres.error, false);
            return;
          }
        }
      }

      for (const node of tree) {
        const mid = node.module.id;
        const was = moduleModeFromPayload(orgPayload, mid);
        const will = moduleModeFromPayload(draftPayload, mid);
        if (was === will) continue;
        if (was === "enabled" && will === "not_configured") continue;
        const res = await setOrganizationModuleEntitlementModeAction({
          organizationId,
          moduleId: mid,
          mode: will,
        });
        if (!res.ok) {
          showToast(res.error, false);
          return;
        }
      }

      for (const node of tree) {
        if (moduleModeFromPayload(draftPayload, node.module.id) !== "enabled") {
          continue;
        }
        for (const b of node.features) {
          const f = b.feature;
          if (!f || String(f.id).startsWith("synthetic:")) continue;
          const featId = String(f.id);
          const oa = orgEntitlementUiStatus({
            snapshot: orgPayload,
            moduleId: node.module.id,
            moduleFeatureId: featId,
            featureKey: b.featureKey,
          });
          const ob = orgEntitlementUiStatus({
            snapshot: draftPayload,
            moduleId: node.module.id,
            moduleFeatureId: featId,
            featureKey: b.featureKey,
          });
          if (oa === ob) continue;
          const r = await setOrganizationModuleFeatureEntitlementModeAction({
            organizationId,
            moduleFeatureId: featId,
            mode: ob,
          });
          if (!r.ok) {
            showToast(r.error, false);
            return;
          }
        }
      }

      showToast("Saved.", true);
      await refresh();
      dispatchOrgEntitlementsUpdated(organizationId);
    } finally {
      setSaving(false);
    }
  }, [
    organizationId,
    orgPayload,
    draftPayload,
    tree,
    refresh,
    showToast,
  ]);

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
      <div
        className={[
          `${responsivePageInner} min-w-0 space-y-6`,
          entitlementsDirty ? "pb-28" : "",
        ]
          .filter(Boolean)
          .join(" ")}
      >
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
              switch (left off, right on); <strong className="font-medium text-foreground">features</strong>: dropdown. <span className="whitespace-nowrap">⋯</span> sets{" "}
              <strong className="font-medium text-foreground">all features in that module</strong> at once. <strong className="font-medium text-foreground">Save</strong> at the bottom applies
              your edits (each module you leave <strong className="font-medium text-foreground">on</strong> must have at least one feature <strong className="font-medium text-foreground">Enabled</strong>,
              or turn that module off first); <strong className="font-medium text-foreground">Cancel</strong> reverts to the last saved state.
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
          {orgPayload && draftPayload
            ? tree.map((node) => {
            const modOpen = open[node.module.id] !== false;
            const label = node.module.name || node.module.key;
            const o = draftPayload;
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
                    {moduleOn ? (
                      <AllFeaturesEntitlementsMenu
                        moduleLabel={label}
                        disabled={saving}
                        onAllFeatures={(m) => void onAllFeaturesInModule(node.module.id, m)}
                      />
                    ) : (
                      <span
                        className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md text-muted-foreground/40"
                        title="Turn the module on to use the All features (⋯) menu"
                        aria-label="All features menu: turn the module on first"
                      >
                        <MoreVertical className="h-4 w-4" aria-hidden />
                      </span>
                    )}
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
                                value={orgEntitlementUiStatus({
                                  snapshot: o,
                                  moduleId: node.module.id,
                                  moduleFeatureId: id,
                                  featureKey: bucket.featureKey,
                                })}
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

      {entitlementsDirty ? (
        <div
          className="fixed bottom-0 left-0 right-0 z-50 border-t border-border bg-background/95 py-3 shadow-[0_-4px_24px_rgba(0,0,0,0.06)] backdrop-blur supports-[backdrop-filter]:bg-background/80"
        >
          <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-3 px-4 sm:px-6">
            <p className="min-w-0 text-sm text-muted-foreground">You have unsaved changes. Save to apply, or cancel to revert.</p>
            <div className="ml-auto flex flex-wrap items-center justify-end gap-2">
              <button
                type="button"
                onClick={handleEntitlementsCancel}
                disabled={saving}
                className="inline-flex h-10 min-w-[5.5rem] items-center justify-center rounded-md border border-border bg-background px-4 text-sm font-medium transition hover:bg-muted/60 disabled:pointer-events-none disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => void saveEntitlementsToServer()}
                disabled={saving}
                className="inline-flex h-10 min-w-[5.5rem] items-center justify-center gap-2 rounded-md border border-primary bg-primary px-4 text-sm font-medium text-primary-foreground shadow-sm transition hover:bg-primary/90 disabled:pointer-events-none disabled:opacity-50"
              >
                {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                Save
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
