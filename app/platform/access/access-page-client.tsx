"use client";

import React, { Suspense, useCallback, useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import * as XLSX from "xlsx";
import { ArrowLeft, Check, ChevronDown, Info, Loader2 } from "lucide-react";
import { useUserRole } from "../../../components/UserRoleContext";
import { PageHeaderWithInfo } from "../components/page-header-with-info";
import {
  getPlatformAccessPageAccessAction,
  listGroupsForOrganizationAccessAction,
  listOrganizationsForAccessAction,
  listRolesCatalogAction,
  type GroupCatalogRow,
  type OrganizationOptionRow,
  type RoleCatalogRow,
} from "./access-actions";
import type { ModuleFeatureTreeNode, OrgEntitlementsPayload } from "../../../lib/access-entitlements";
import { ORG_ENTITLEMENTS_UPDATED_EVENT, type OrgEntitlementsUpdatedDetail } from "../../../lib/org-entitlements-events";
import { accessTreeModuleMatchesScope, REPORT_MODULE_GROUP_CHOICES } from "../../../lib/access-report-filters";
import {
  applyFeatureLevelToPermissionSet,
  applyModuleLevelToPermissionSet,
  effectiveLevelFromFeatureBucket,
  findAllFeatureBucketsInModule,
  findFeatureBucket,
  UI_ACCESS_LEVEL_LABEL,
  type UiAccessLevel,
} from "../../../lib/access-level";
import { orgAllowsPermission, totalPermissionsInBucket } from "../../../lib/access-entitlements";
import { effectiveUserFeatureAfterOverride, type UserOverrideChoice, type UserFeatureAccessRow } from "../../../lib/user-feature-access";
import { FeatureAccessTree } from "./feature-access-tree";
import {
  getAccessMatrixReportForOrganizationAction,
  getAssignedPermissionIdsForGroupAction,
  getAssignedPermissionIdsForRoleAction,
  getUserEffectiveAccessAction,
  listAccessCatalogForOrganizationAction,
  listProfilesForAccessInspectorAction,
  setFeatureAccessLevelForEntireModuleForTargetAction,
  setFeatureAccessLevelForTargetAction,
  setUserModuleBulkOverrideAction,
  setUserFeatureOverrideAction,
  type AccessInspectorUserRow,
  type AccessReportRow,
  type UserEffectiveAccessResult,
} from "./permissions-actions";

const INPUT =
  "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50";
const LABEL = "mb-2 block text-sm font-medium leading-none";

function setToKey(s: Set<string>): string {
  return [...s].sort().join("\0");
}
function keyToSet(k: string): Set<string> {
  return k ? new Set(k.split("\0")) : new Set();
}

function organizationTypeIsInternal(type: string | null | undefined): boolean {
  return (type ?? "").trim().toLowerCase() === "internal";
}

type TabKey = "users" | "roles" | "groups" | "report";

const USER_COMBO_PANEL =
  "absolute left-0 right-0 top-full z-50 mt-1 max-h-[min(24rem,70vh)] flex flex-col overflow-hidden rounded-md border border-border bg-popover p-0 text-popover-foreground shadow-lg";

function AccessUserCombobox({
  id,
  labelId,
  users,
  value,
  onChange,
  disabled,
  loading,
}: {
  id: string;
  labelId: string;
  users: AccessInspectorUserRow[];
  value: string;
  onChange: (id: string) => void;
  disabled?: boolean;
  loading?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const rootRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const searchId = useId();

  const q = query.trim().toLowerCase();
  const filtered = useMemo(() => {
    if (!q) return users;
    return users.filter(
      (u) =>
        u.full_name.toLowerCase().includes(q)
        || u.organization_name.toLowerCase().includes(q)
        || u.role_key.toLowerCase().includes(q)
        || (u.role_display_name && u.role_display_name.toLowerCase().includes(q)),
    );
  }, [users, q]);

  const selected = useMemo(() => users.find((u) => u.id === value) ?? null, [users, value]);

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

  useLayoutEffect(() => {
    if (open) {
      setQuery("");
      const t = requestAnimationFrame(() => searchInputRef.current?.focus());
      return () => cancelAnimationFrame(t);
    }
  }, [open]);

  const displayLabel = selected
    ? `${selected.full_name} — ${selected.organization_name} — ${selected.role_key}`
    : "";

  return (
    <div ref={rootRef} className="relative w-full min-w-0">
      <button
        type="button"
        id={id}
        disabled={disabled || loading}
        aria-labelledby={labelId}
        aria-expanded={open}
        aria-haspopup="listbox"
        onClick={() => !disabled && !loading && setOpen((o) => !o)}
        className={[
          "flex h-10 w-full min-w-0 items-center justify-between gap-2 rounded-md border border-input bg-background px-3 py-2 text-left text-sm shadow-sm ring-offset-background",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2",
          "disabled:cursor-not-allowed disabled:opacity-50",
        ].join(" ")}
      >
        <span
          className={[
            "min-w-0 flex-1 truncate",
            !displayLabel && !loading ? "text-muted-foreground" : "text-foreground",
          ].join(" ")}
        >
          {loading ? "Loading…" : displayLabel || "Select a user"}
        </span>
        <ChevronDown className={`h-4 w-4 shrink-0 opacity-60 transition ${open ? "rotate-180" : ""}`} />
      </button>
      {open && (
        <div className={USER_COMBO_PANEL} role="listbox" aria-labelledby={labelId}>
          <div className="shrink-0 border-b border-border p-2">
            <label htmlFor={searchId} className="sr-only">
              Search users
            </label>
            <input
              id={searchId}
              ref={searchInputRef}
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Escape") {
                  e.stopPropagation();
                  setOpen(false);
                }
              }}
              placeholder="Search name, company, or role…"
              className={INPUT}
              autoComplete="off"
            />
          </div>
          <div className="min-h-0 flex-1 overflow-y-auto p-1">
            {filtered.length === 0 ? (
              <p className="px-3 py-4 text-center text-sm text-muted-foreground">No matches</p>
            ) : (
              filtered.map((u) => {
                const isSel = value === u.id;
                return (
                  <button
                    key={u.id}
                    type="button"
                    role="option"
                    aria-selected={isSel}
                    onClick={() => {
                      onChange(u.id);
                      setOpen(false);
                    }}
                    className={[
                      "flex w-full min-w-0 items-start gap-2 rounded-md px-2.5 py-2 text-left text-sm transition",
                      isSel
                        ? "bg-muted font-medium"
                        : "hover:bg-muted/80",
                    ].join(" ")}
                  >
                    <Check
                      className={[
                        "mt-0.5 h-4 w-4 shrink-0",
                        isSel ? "text-primary opacity-100" : "opacity-0",
                      ].join(" ")}
                    />
                    <span className="min-w-0 flex-1">
                      <span className="block truncate font-medium leading-tight">{u.full_name}</span>
                      <span className="mt-0.5 block text-xs text-muted-foreground">
                        {u.organization_name} · {u.role_key}
                      </span>
                    </span>
                  </button>
                );
              })
            )}
          </div>
        </div>
      )}
    </div>
  );
}

type SearchableOption = { value: string; label: string; sublabel?: string };

function AccessSearchableOptionsCombobox({
  id,
  labelId,
  value,
  onChange,
  options,
  disabled,
  loading,
  placeholder,
  searchPlaceholder = "Search…",
  noMatches = "No matches",
  emptyList,
}: {
  id: string;
  labelId: string;
  value: string;
  onChange: (v: string) => void;
  options: readonly SearchableOption[];
  disabled?: boolean;
  loading?: boolean;
  placeholder: string;
  searchPlaceholder?: string;
  noMatches?: string;
  emptyList?: string;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const rootRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const searchId = useId();

  const q = query.trim().toLowerCase();
  const filtered = useMemo(() => {
    if (!q) return options;
    return options.filter(
      (o) =>
        o.label.toLowerCase().includes(q)
        || (o.sublabel && o.sublabel.toLowerCase().includes(q))
        || o.value.toLowerCase().includes(q),
    );
  }, [options, q]);

  const selected = useMemo(
    () => options.find((o) => o.value === value) ?? null,
    [options, value],
  );
  const display = loading
    ? "Loading…"
    : (selected
      ? selected.label
      : placeholder);

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

  useLayoutEffect(() => {
    if (open) {
      setQuery("");
      const t = requestAnimationFrame(() => searchInputRef.current?.focus());
      return () => cancelAnimationFrame(t);
    }
  }, [open]);

  return (
    <div ref={rootRef} className="relative w-full min-w-0">
      <button
        type="button"
        id={id}
        disabled={disabled || loading}
        aria-labelledby={labelId}
        aria-expanded={open}
        aria-haspopup="listbox"
        onClick={() => !disabled && !loading && setOpen((o) => !o)}
        className={[
          "flex h-10 w-full min-w-0 items-center justify-between gap-2 rounded-md border border-input bg-background px-3 py-2 text-left text-sm shadow-sm ring-offset-background",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2",
          "disabled:cursor-not-allowed disabled:opacity-50",
        ].join(" ")}
      >
        <span
          className={[
            "min-w-0 flex-1 truncate",
            !selected && !loading ? "text-muted-foreground" : "text-foreground",
          ].join(" ")}
        >
          {display}
        </span>
        <ChevronDown className={`h-4 w-4 shrink-0 opacity-60 transition ${open ? "rotate-180" : ""}`} />
      </button>
      {open && (
        <div className={USER_COMBO_PANEL} role="listbox" aria-labelledby={labelId}>
          <div className="shrink-0 border-b border-border p-2">
            <label htmlFor={searchId} className="sr-only">
              {searchPlaceholder}
            </label>
            <input
              id={searchId}
              ref={searchInputRef}
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Escape") {
                  e.stopPropagation();
                  setOpen(false);
                }
              }}
              placeholder={searchPlaceholder}
              className={INPUT}
              autoComplete="off"
            />
          </div>
          <div className="min-h-0 flex-1 overflow-y-auto p-1">
            {options.length === 0 && emptyList ? (
              <p className="px-3 py-4 text-center text-sm text-muted-foreground">{emptyList}</p>
            ) : filtered.length === 0 ? (
              <p className="px-3 py-4 text-center text-sm text-muted-foreground">{noMatches}</p>
            ) : (
              filtered.map((o) => {
                const isSel = value === o.value;
                return (
                  <button
                    key={o.value || "__empty__"}
                    type="button"
                    role="option"
                    aria-selected={isSel}
                    onClick={() => {
                      onChange(o.value);
                      setOpen(false);
                    }}
                    className={[
                      "flex w-full min-w-0 items-start gap-2 rounded-md px-2.5 py-2 text-left text-sm transition",
                      isSel
                        ? "bg-muted font-medium"
                        : "hover:bg-muted/80",
                    ].join(" ")}
                  >
                    <Check
                      className={[
                        "mt-0.5 h-4 w-4 shrink-0",
                        isSel ? "text-primary opacity-100" : "opacity-0",
                      ].join(" ")}
                    />
                    <span className="min-w-0 flex-1">
                      <span className="block truncate font-medium leading-tight">{o.label}</span>
                      {o.sublabel ? (
                        <span className="mt-0.5 block text-xs text-muted-foreground">{o.sublabel}</span>
                      ) : null}
                    </span>
                  </button>
                );
              })
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function AccessCompanyFilterCombobox({
  id,
  labelId,
  orgs,
  value,
  onChange,
  disabled,
  includeAllRow = true,
  selectWhenEmptyLabel = "Select a company",
}: {
  id: string;
  labelId: string;
  orgs: OrganizationOptionRow[];
  value: string;
  onChange: (organizationId: string) => void;
  disabled?: boolean;
  /** If false, no “All companies” row; use for Role/Group/Report (must pick a company). */
  includeAllRow?: boolean;
  selectWhenEmptyLabel?: string;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const rootRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const searchId = useId();

  const q = query.trim().toLowerCase();
  const filteredOrgs = useMemo(() => {
    if (!q) return orgs;
    return orgs.filter(
      (o) =>
        o.displayName.toLowerCase().includes(q)
        || o.name.toLowerCase().includes(q)
        || (o.type && String(o.type).toLowerCase().includes(q)),
    );
  }, [orgs, q]);

  const displayLabel = useMemo(() => {
    if (!value.trim()) return includeAllRow ? "All companies" : selectWhenEmptyLabel;
    const o = orgs.find((x) => x.id === value);
    if (o) return o.displayName;
    return "Company";
  }, [value, orgs, includeAllRow, selectWhenEmptyLabel]);

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

  useLayoutEffect(() => {
    if (open) {
      setQuery("");
      const t = requestAnimationFrame(() => searchInputRef.current?.focus());
      return () => cancelAnimationFrame(t);
    }
  }, [open]);

  const allSelected = !value.trim() && includeAllRow;

  return (
    <div ref={rootRef} className="relative w-full min-w-0">
      <button
        type="button"
        id={id}
        disabled={disabled}
        aria-labelledby={labelId}
        aria-expanded={open}
        aria-haspopup="listbox"
        onClick={() => !disabled && setOpen((o) => !o)}
        className={[
          "flex h-10 w-full min-w-0 items-center justify-between gap-2 rounded-md border border-input bg-background px-3 py-2 text-left text-sm shadow-sm ring-offset-background",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2",
          "disabled:cursor-not-allowed disabled:opacity-50",
        ].join(" ")}
      >
        <span
          className={[
            "min-w-0 flex-1 truncate",
            allSelected && includeAllRow ? "text-foreground" : !value.trim() && !includeAllRow ? "text-muted-foreground" : "text-foreground",
          ].join(" ")}
        >
          {displayLabel}
        </span>
        <ChevronDown className={`h-4 w-4 shrink-0 opacity-60 transition ${open ? "rotate-180" : ""}`} />
      </button>
      {open && (
        <div className={USER_COMBO_PANEL} role="listbox" aria-labelledby={labelId}>
          <div className="shrink-0 border-b border-border p-2">
            <label htmlFor={searchId} className="sr-only">
              Search companies
            </label>
            <input
              id={searchId}
              ref={searchInputRef}
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Escape") {
                  e.stopPropagation();
                  setOpen(false);
                }
              }}
              placeholder="Search company name or type…"
              className={INPUT}
              autoComplete="off"
            />
          </div>
          <div className="min-h-0 flex-1 overflow-y-auto p-1">
            {includeAllRow ? (
              <button
                type="button"
                role="option"
                aria-selected={allSelected}
                onClick={() => {
                  onChange("");
                  setOpen(false);
                }}
                className={[
                  "flex w-full min-w-0 items-start gap-2 rounded-md px-2.5 py-2 text-left text-sm transition",
                  allSelected ? "bg-muted font-medium" : "hover:bg-muted/80",
                ].join(" ")}
              >
                <Check
                  className={[
                    "mt-0.5 h-4 w-4 shrink-0",
                    allSelected ? "text-primary opacity-100" : "opacity-0",
                  ].join(" ")}
                />
                <span className="min-w-0 flex-1">
                  <span className="block font-medium leading-tight">All companies</span>
                  <span className="mt-0.5 block text-xs text-muted-foreground">No org filter</span>
                </span>
              </button>
            ) : null}
            {filteredOrgs.length === 0 && orgs.length > 0 ? (
              <p className="px-3 py-3 text-center text-sm text-muted-foreground">No matching companies</p>
            ) : null}
            {filteredOrgs.map((o) => {
              const isSel = value === o.id;
              const sub = [o.name !== o.displayName ? o.name : null, o.type].filter(Boolean).join(" · ");
              return (
                <button
                  key={o.id}
                  type="button"
                  role="option"
                  aria-selected={isSel}
                  onClick={() => {
                    onChange(o.id);
                    setOpen(false);
                  }}
                  className={[
                    "flex w-full min-w-0 items-start gap-2 rounded-md px-2.5 py-2 text-left text-sm transition",
                    isSel
                      ? "bg-muted font-medium"
                      : "hover:bg-muted/80",
                  ].join(" ")}
                >
                  <Check
                    className={[
                      "mt-0.5 h-4 w-4 shrink-0",
                      isSel ? "text-primary opacity-100" : "opacity-0",
                    ].join(" ")}
                  />
                  <span className="min-w-0 flex-1">
                    <span className="block truncate font-medium leading-tight">{o.displayName}</span>
                    {sub ? (
                      <span className="mt-0.5 block truncate text-xs text-muted-foreground">{sub}</span>
                    ) : null}
                  </span>
                </button>
              );
            })}
            {orgs.length === 0 ? (
              <p className="px-3 py-3 text-center text-sm text-muted-foreground">No organizations loaded</p>
            ) : null}
          </div>
        </div>
      )}
    </div>
  );
}

function PlatformAccessPageInner() {
  const searchParams = useSearchParams();
  const {
    organizationId: workspaceOrganizationId,
    actorUserId,
    setWorkspaceOrganizationId,
    sessionCanWorkspaceSwitch,
  } = useUserRole();

  const commitWorkspaceOrgScope = useCallback(
    (id: string) => {
      const t = id.trim();
      if (!t) return;
      if (sessionCanWorkspaceSwitch) setWorkspaceOrganizationId(t);
    },
    [sessionCanWorkspaceSwitch, setWorkspaceOrganizationId],
  );

  const [loadingGate, setLoadingGate] = useState(true);
  const [accessDenied, setAccessDenied] = useState<"not_authenticated" | "forbidden" | null>(null);
  const [tab, setTab] = useState<TabKey>("users");

  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);
  const showToast = useCallback((msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 4200);
  }, []);

  const [moduleFeatureTree, setModuleFeatureTree] = useState<ModuleFeatureTreeNode[]>([]);
  const [catalogLoading, setCatalogLoading] = useState(false);
  const [catalogError, setCatalogError] = useState<string | null>(null);
  const [contextOrgEntitlements, setContextOrgEntitlements] = useState<OrgEntitlementsPayload | null>(null);

  const [users, setUsers] = useState<AccessInspectorUserRow[]>([]);
  const [usersLoading, setUsersLoading] = useState(false);
  /** User Access tab: empty = all companies */
  const [userAccessCompanyFilterId, setUserAccessCompanyFilterId] = useState("");
  const [selectedUserId, setSelectedUserId] = useState("");
  const [userAccessLoading, setUserAccessLoading] = useState(false);
  const [userAccessError, setUserAccessError] = useState<string | null>(null);
  const [userEffective, setUserEffective] = useState<UserEffectiveAccessResult | null>(null);
  /** Unsaved per-feature user overrides; applied on Save */
  const [userOverrideDraft, setUserOverrideDraft] = useState<Record<string, UserOverrideChoice>>({});

  const [roles, setRoles] = useState<RoleCatalogRow[]>([]);
  const [rolesLoading, setRolesLoading] = useState(false);
  const [selectedRoleId, setSelectedRoleId] = useState("");
  const [rolePermLoading, setRolePermLoading] = useState(false);
  const [roleAssigned, setRoleAssigned] = useState<Set<string>>(() => new Set());
  const [rolePermBaselineKey, setRolePermBaselineKey] = useState("");

  const [orgs, setOrgs] = useState<OrganizationOptionRow[]>([]);
  const [orgFilterId, setOrgFilterId] = useState("");
  const [groups, setGroups] = useState<GroupCatalogRow[]>([]);
  const [groupsLoading, setGroupsLoading] = useState(false);
  const [selectedGroupId, setSelectedGroupId] = useState("");
  const [groupPermLoading, setGroupPermLoading] = useState(false);
  const [groupAssigned, setGroupAssigned] = useState<Set<string>>(() => new Set());
  const [groupPermBaselineKey, setGroupPermBaselineKey] = useState("");

  const [accessTreeScope, setAccessTreeScope] = useState("");
  const [reportTabHelpOpen, setReportTabHelpOpen] = useState(false);

  const [reportRows, setReportRows] = useState<AccessReportRow[]>([]);
  const [reportMeta, setReportMeta] = useState<{ userCount: number; truncated: boolean } | null>(null);
  const [reportLoading, setReportLoading] = useState(false);
  const [reportError, setReportError] = useState<string | null>(null);
  const [reportMin, setReportMin] = useState<"" | "all" | "read_or_more" | "write_or_more" | "manage_only">("");
  const [reportSelectedUserId, setReportSelectedUserId] = useState("");
  const [reportFeatureToken, setReportFeatureToken] = useState("");
  const [reportModuleGroup, setReportModuleGroup] = useState("");

  const [accessFooterBusy, setAccessFooterBusy] = useState(false);

  const runAccessReport = useCallback(async (): Promise<boolean> => {
    if (!orgFilterId?.trim()) {
      showToast("Choose a company first.", false);
      return false;
    }
    setReportLoading(true);
    setReportError(null);
    const r = await getAccessMatrixReportForOrganizationAction({
      organizationId: orgFilterId,
      minEffective: reportMin || undefined,
      userSearch: reportSelectedUserId?.trim() || undefined,
      featureSearch: reportFeatureToken?.trim() || undefined,
      reportModuleGroup: reportModuleGroup?.trim() || undefined,
    });
    setReportLoading(false);
    if (!r.ok) {
      setReportError(r.error);
      setReportRows([]);
      setReportMeta(null);
      showToast(r.error, false);
      return false;
    }
    setReportRows(r.rows);
    setReportMeta({ userCount: r.userCount, truncated: r.truncated });
    return true;
  }, [
    orgFilterId,
    reportMin,
    reportModuleGroup,
    reportSelectedUserId,
    reportFeatureToken,
    showToast,
  ]);

  const downloadAccessReportCsv = useCallback(() => {
    if (reportRows.length === 0) return;
    const headers = [
      "User",
      "Role",
      "Module",
      "Feature",
      "Effective",
      "Baseline",
      "Override",
      "module_key",
      "feature_key",
    ];
    const esc = (s: string) => `"${String(s).replace(/"/g, '""')}"`;
    const lines = [headers.join(",")];
    for (const row of reportRows) {
      lines.push(
        [
          row.full_name,
          row.role_key,
          row.module_name,
          row.feature_name,
          row.effective,
          row.baseline,
          row.override,
          row.module_key,
          row.feature_key,
        ]
          .map(esc)
          .join(","),
      );
    }
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `access-report-${orgFilterId?.slice(0, 8) ?? "org"}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
  }, [orgFilterId, reportRows]);

  const downloadAccessReportXlsx = useCallback(() => {
    if (reportRows.length === 0) return;
    const headers = [
      "User",
      "Role",
      "Module",
      "Feature",
      "Effective",
      "Baseline",
      "Override",
      "module_key",
      "feature_key",
    ];
    const aoa: string[][] = [
      headers,
      ...reportRows.map((row) => [
        row.full_name,
        row.role_key,
        row.module_name,
        row.feature_name,
        UI_ACCESS_LEVEL_LABEL[row.effective],
        UI_ACCESS_LEVEL_LABEL[row.baseline],
        String(row.override),
        row.module_key,
        row.feature_key,
      ]),
    ];
    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.aoa_to_sheet(aoa);
    XLSX.utils.book_append_sheet(wb, ws, "Access report");
    XLSX.writeFile(wb, `access-report-${orgFilterId?.slice(0, 8) ?? "org"}.xlsx`);
  }, [orgFilterId, reportRows]);

  useEffect(() => {
    const t = searchParams.get("tab")?.trim().toLowerCase();
    if (t === "roles") setTab("roles");
    else if (t === "groups") setTab("groups");
    else if (t === "report" || t === "reports") setTab("report");
    else if (t === "users") setTab("users");
  }, [searchParams]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const access = await getPlatformAccessPageAccessAction();
      if (cancelled) return;
      if (access.accessDenied) {
        setAccessDenied(access.accessDenied);
        setLoadingGate(false);
        return;
      }
      setAccessDenied(null);
      setLoadingGate(false);
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const loadAccessTreeForOrg = useCallback(
    async (organizationId: string, withContextEntitlements: boolean) => {
      if (!organizationId?.trim()) {
        setModuleFeatureTree([]);
        if (withContextEntitlements) setContextOrgEntitlements(null);
        return;
      }
      setCatalogError(null);
      setCatalogLoading(true);
      const r = await listAccessCatalogForOrganizationAction(organizationId);
      setCatalogLoading(false);
      if (!r.ok) {
        setCatalogError(r.error);
        setModuleFeatureTree([]);
        if (withContextEntitlements) setContextOrgEntitlements(null);
        showToast(r.error, false);
        return;
      }
      setModuleFeatureTree(r.moduleFeatureTree);
      if (withContextEntitlements) {
        setContextOrgEntitlements(r.orgEntitlements);
      } else {
        setContextOrgEntitlements(null);
      }
    },
    [showToast],
  );

  const loadUsers = useCallback(async () => {
    setUsersLoading(true);
    const res = await listProfilesForAccessInspectorAction();
    setUsersLoading(false);
    if (!res.ok) {
      showToast(res.error, false);
      return;
    }
    setUsers(res.rows);
  }, [showToast]);

  const loadRoles = useCallback(async () => {
    setRolesLoading(true);
    const res = await listRolesCatalogAction();
    setRolesLoading(false);
    if (!res.ok) {
      showToast(res.error, false);
      return;
    }
    setRoles(res.rows);
  }, [showToast]);

  const loadOrgs = useCallback(async () => {
    const res = await listOrganizationsForAccessAction();
    if (!res.ok) {
      showToast(res.error, false);
      return;
    }
    setOrgs(res.rows);
    setOrgFilterId((prev) => (prev && res.rows.some((o) => o.id === prev) ? prev : ""));
  }, [showToast]);

  useEffect(() => {
    const w = (workspaceOrganizationId ?? "").trim();
    if (!w || orgs.length === 0) return;
    if (!orgs.some((o) => o.id === w)) return;
    setOrgFilterId((cur) => (cur === w ? cur : w));
    setUserAccessCompanyFilterId((cur) => (cur === w ? cur : w));
  }, [workspaceOrganizationId, orgs]);

  const loadGroups = useCallback(
    async (oid: string) => {
      if (!oid.trim()) {
        setGroups([]);
        return;
      }
      setGroupsLoading(true);
      const res = await listGroupsForOrganizationAccessAction(oid);
      setGroupsLoading(false);
      if (!res.ok) {
        showToast(res.error, false);
        return;
      }
      setGroups(res.rows);
    },
    [showToast],
  );

  useEffect(() => {
    const onOrgEntitlementsUpdated = (ev: Event) => {
      const detail = (ev as CustomEvent<OrgEntitlementsUpdatedDetail>).detail;
      const oid = detail?.organizationId?.trim();
      if (!oid) return;
      void loadOrgs();
      if (orgFilterId === oid) {
        void loadAccessTreeForOrg(oid, true);
      }
      if (tab === "users" && userAccessCompanyFilterId === oid) {
        void loadAccessTreeForOrg(oid, false);
      }
      if (tab === "users" && userAccessCompanyFilterId === oid) {
        const target = (selectedUserId.trim() || actorUserId?.trim() || "");
        if (!target) return;
        void (async () => {
          const res = await getUserEffectiveAccessAction(target, { organizationId: oid });
          if (!res.ok) return;
          setUserEffective(res.data);
        })();
      }
    };
    window.addEventListener(ORG_ENTITLEMENTS_UPDATED_EVENT, onOrgEntitlementsUpdated);
    return () => {
      window.removeEventListener(ORG_ENTITLEMENTS_UPDATED_EVENT, onOrgEntitlementsUpdated);
    };
  }, [
    tab,
    orgFilterId,
    userAccessCompanyFilterId,
    selectedUserId,
    actorUserId,
    loadOrgs,
    loadAccessTreeForOrg,
  ]);

  useEffect(() => {
    if (tab !== "users" && tab !== "report") return;
    void loadUsers();
  }, [tab, loadUsers]);

  useEffect(() => {
    if (tab !== "roles") return;
    void loadRoles();
  }, [tab, loadRoles]);

  useEffect(() => {
    if (tab !== "users" && tab !== "roles" && tab !== "groups" && tab !== "report") return;
    void loadOrgs();
  }, [tab, loadOrgs]);

  useEffect(() => {
    if (tab !== "groups" || !orgFilterId) return;
    void loadGroups(orgFilterId);
  }, [tab, orgFilterId, loadGroups]);

  useEffect(() => {
    if (tab !== "report" || !orgFilterId?.trim()) return;
    void loadAccessTreeForOrg(orgFilterId, true);
  }, [tab, orgFilterId, loadAccessTreeForOrg]);

  const reportOrgKeyRef = useRef<string | null>(null);
  useEffect(() => {
    if (tab !== "report") {
      return;
    }
    const o = orgFilterId?.trim() || null;
    if (reportOrgKeyRef.current != null && reportOrgKeyRef.current !== o) {
      setReportSelectedUserId("");
      setReportFeatureToken("");
      setReportMin("");
      setReportModuleGroup("");
    }
    reportOrgKeyRef.current = o;
  }, [tab, orgFilterId]);

  useEffect(() => {
    if (tab !== "roles" && tab !== "groups") return;
    if (!orgFilterId) {
      setModuleFeatureTree([]);
      setContextOrgEntitlements(null);
      return;
    }
    void loadAccessTreeForOrg(orgFilterId, true);
  }, [tab, orgFilterId, loadAccessTreeForOrg]);

  useEffect(() => {
    if (tab !== "users") return;
    const oid = userAccessCompanyFilterId.trim();
    if (!oid) return;
    void loadAccessTreeForOrg(oid, false);
  }, [tab, userAccessCompanyFilterId, loadAccessTreeForOrg]);

  useEffect(() => {
    setUserOverrideDraft({});
  }, [selectedUserId, userAccessCompanyFilterId]);

  useEffect(() => {
    if (tab !== "users") return;
    const oid = userAccessCompanyFilterId.trim();
    if (!oid) {
      setUserEffective(null);
      setUserAccessError(null);
      setUserAccessLoading(false);
      return;
    }
    const target = (selectedUserId.trim() || actorUserId?.trim() || "");
    if (!target) {
      setUserEffective(null);
      setUserAccessError(null);
      setUserAccessLoading(false);
      return;
    }
    let cancelled = false;
    setUserAccessError(null);
    setUserEffective(null);
    setUserAccessLoading(true);
    void (async () => {
      const res = await getUserEffectiveAccessAction(target, { organizationId: oid });
      if (cancelled) return;
      setUserAccessLoading(false);
      if (!res.ok) {
        setUserAccessError(res.error);
        setUserEffective(null);
        showToast(res.error, false);
        return;
      }
      setUserEffective(res.data);
    })();
    return () => {
      cancelled = true;
    };
  }, [tab, userAccessCompanyFilterId, selectedUserId, actorUserId, showToast]);

  useEffect(() => {
    let cancelled = false;
    if (!selectedRoleId) {
      setRoleAssigned(new Set());
      setRolePermBaselineKey("");
      return;
    }
    void (async () => {
      setRolePermLoading(true);
      const res = await getAssignedPermissionIdsForRoleAction(selectedRoleId);
      if (cancelled) return;
      setRolePermLoading(false);
      if (!res.ok) {
        showToast(res.error, false);
        setRoleAssigned(new Set());
        setRolePermBaselineKey("");
        return;
      }
      const next = new Set(res.permissionIds);
      setRolePermBaselineKey(setToKey(next));
      setRoleAssigned(new Set(res.permissionIds));
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedRoleId, showToast]);

  useEffect(() => {
    let cancelled = false;
    if (!selectedGroupId) {
      setGroupAssigned(new Set());
      setGroupPermBaselineKey("");
      return;
    }
    void (async () => {
      setGroupPermLoading(true);
      const res = await getAssignedPermissionIdsForGroupAction(selectedGroupId);
      if (cancelled) return;
      setGroupPermLoading(false);
      if (!res.ok) {
        showToast(res.error, false);
        setGroupAssigned(new Set());
        setGroupPermBaselineKey("");
        return;
      }
      setGroupPermBaselineKey(setToKey(new Set(res.permissionIds)));
      setGroupAssigned(new Set(res.permissionIds));
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedGroupId, showToast]);

  const usersForUserAccessPicker = useMemo(() => {
    if (!userAccessCompanyFilterId.trim()) return [] as AccessInspectorUserRow[];
    return users.filter((u) => u.organization_id === userAccessCompanyFilterId);
  }, [users, userAccessCompanyFilterId]);

  useEffect(() => {
    if (!userAccessCompanyFilterId.trim()) return;
    const sel = users.find((u) => u.id === selectedUserId);
    if (sel && sel.organization_id === userAccessCompanyFilterId) return;
    if (selectedUserId) setSelectedUserId("");
  }, [userAccessCompanyFilterId, users, selectedUserId]);

  const licensedOrgLabel = useMemo(() => {
    if (!orgFilterId) return "";
    return orgs.find((o) => o.id === orgFilterId)?.displayName ?? "";
  }, [orgFilterId, orgs]);

  /** Role Access: tenant orgs → tenant-scoped roles only; internal org → system roles only. */
  const rolesForSelectedOrg = useMemo(() => {
    if (!orgFilterId) return [] as RoleCatalogRow[];
    const o = orgs.find((x) => x.id === orgFilterId);
    const wantSystem = organizationTypeIsInternal(o?.type);
    return roles.filter((r) => (wantSystem ? r.scope === "system" : r.scope === "tenant"));
  }, [roles, orgs, orgFilterId]);

  useEffect(() => {
    if (tab !== "roles") return;
    if (!selectedRoleId) return;
    if (rolesForSelectedOrg.some((r) => r.id === selectedRoleId)) return;
    setSelectedRoleId("");
  }, [tab, selectedRoleId, rolesForSelectedOrg]);

  const roleSelectOptions = useMemo(
    () =>
      rolesForSelectedOrg.map((r) => ({
        value: r.id,
        label: `${r.name} (${r.key})`,
        sublabel: r.scope,
      })),
    [rolesForSelectedOrg],
  );

  const groupSelectOptions = useMemo(
    () =>
      groups.map((g) => ({
        value: g.id,
        label: `${g.name} (${g.key})`,
        sublabel: g.organization_name ?? undefined,
      })),
    [groups],
  );

  const treeScopeOptions = useMemo(
    () => [
      { value: "", label: "All modules" },
      ...REPORT_MODULE_GROUP_CHOICES.map((m) => ({ value: m.value, label: m.label })),
    ],
    [],
  );

  const reportMinOptions = useMemo(
    () => [
      { value: "all", label: "Any (include none)" },
      { value: "read_or_more", label: "Read or higher" },
      { value: "write_or_more", label: "Write or higher" },
      { value: "manage_only", label: "Manage only" },
    ],
    [],
  );

  const reportModuleOptions = useMemo(
    () => REPORT_MODULE_GROUP_CHOICES.map((m) => ({ value: m.value, label: m.label })),
    [],
  );

  const usersInReportOrg = useMemo(
    () => (orgFilterId ? users.filter((u) => u.organization_id === orgFilterId) : []),
    [users, orgFilterId],
  );

  const reportFeatureOptions = useMemo((): SearchableOption[] => {
    const out: SearchableOption[] = [];
    for (const node of moduleFeatureTree) {
      for (const bucket of node.features) {
        const f = bucket.feature;
        if (!f || String(f.id).startsWith("synthetic:") || !String(f.id).trim()) continue;
        const modKey = node.module.key;
        const fk = bucket.featureKey;
        const name = f.name && String(f.name).trim() ? String(f.name) : fk;
        const token = `${modKey} ${fk}`.trim();
        if (out.some((x) => x.value === token)) continue;
        out.push({
          value: token,
          label: name,
          sublabel: node.module.name?.trim() || modKey,
        });
      }
    }
    return out;
  }, [moduleFeatureTree]);

  useEffect(() => {
    if (!reportFeatureToken) return;
    if (!reportFeatureOptions.some((o) => o.value === reportFeatureToken)) {
      setReportFeatureToken("");
    }
  }, [reportFeatureToken, reportFeatureOptions]);

  useEffect(() => {
    if (!reportSelectedUserId) return;
    if (!usersInReportOrg.some((u) => u.id === reportSelectedUserId)) {
      setReportSelectedUserId("");
    }
  }, [reportSelectedUserId, usersInReportOrg]);

  const selectedUserOrgLabel = useMemo(() => {
    if (!userEffective?.organization_id) return "";
    return (
      orgs.find((o) => o.id === userEffective.organization_id)?.displayName
      ?? userEffective.organization_name
    );
  }, [orgs, userEffective]);

  const effectiveUserPermIds = useMemo(() => {
    if (!userEffective) return new Set<string>();
    return new Set(userEffective.permissions.map((p) => p.id));
  }, [userEffective]);

  const filteredModuleFeatureTree = useMemo(() => {
    if (!accessTreeScope.trim()) return moduleFeatureTree;
    return moduleFeatureTree.filter((n) => accessTreeModuleMatchesScope(accessTreeScope, n.module.key));
  }, [moduleFeatureTree, accessTreeScope]);

  const mergedUserFeatureAccessByModuleFeatureId = useMemo((): Record<string, UserFeatureAccessRow> | null => {
    if (!userEffective?.userFeatureAccessByModuleFeatureId) return null;
    const base = userEffective.userFeatureAccessByModuleFeatureId;
    const org = userEffective.orgEntitlements;
    const out: Record<string, UserFeatureAccessRow> = { ...base };
    for (const node of moduleFeatureTree) {
      for (const bucket of node.features) {
        const feat = bucket.feature;
        const mfId = feat && !String(feat.id).startsWith("synthetic:") ? String(feat.id) : null;
        if (!mfId) continue;
        const orig = base[mfId];
        if (!orig) continue;
        const ovr = userOverrideDraft[mfId] ?? orig.override;
        const entitled = orgAllowsPermission({
          snapshot: org,
          moduleId: node.module.id,
          moduleFeatureId: mfId,
          featureKey: bucket.featureKey,
        }).entitled;
        out[mfId] = {
          ...orig,
          module_feature_id: mfId,
          override: ovr,
          effective: effectiveUserFeatureAfterOverride({
            orgEntitled: entitled,
            baseline: orig.baseline,
            override: ovr,
          }),
        };
      }
    }
    return out;
  }, [userEffective, moduleFeatureTree, userOverrideDraft]);

  const accessFormDirty = useMemo(() => {
    if (tab === "users") return Object.keys(userOverrideDraft).length > 0;
    if (tab === "roles") {
      if (!rolePermBaselineKey) return false;
      return setToKey(roleAssigned) !== rolePermBaselineKey;
    }
    if (tab === "groups") {
      if (!groupPermBaselineKey) return false;
      return setToKey(groupAssigned) !== groupPermBaselineKey;
    }
    return false;
  }, [
    tab,
    userOverrideDraft,
    roleAssigned,
    rolePermBaselineKey,
    groupAssigned,
    groupPermBaselineKey,
  ]);

  const onUserOverride = useCallback(
    (args: { moduleFeatureId: string; level: UserOverrideChoice }) => {
      if (!selectedUserId || tab !== "users" || !userEffective) return;
      const orig = userEffective.userFeatureAccessByModuleFeatureId[args.moduleFeatureId]?.override;
      setUserOverrideDraft((d) => {
        const next = { ...d };
        if (args.level === orig) {
          delete next[args.moduleFeatureId];
        } else {
          next[args.moduleFeatureId] = args.level;
        }
        return next;
      });
    },
    [tab, selectedUserId, userEffective],
  );

  const onUserModuleBulk = useCallback(
    (args: { moduleId: string; level: UiAccessLevel | "inherit" }) => {
      if (!selectedUserId || tab !== "users" || !userEffective) return;
      const choice: UserOverrideChoice =
        args.level === "inherit"
          ? "inherit"
          : args.level === "none"
            ? "none"
            : args.level === "read"
              ? "read"
              : "write";
      setUserOverrideDraft((d) => {
        const next = { ...d };
        for (const b of findAllFeatureBucketsInModule(moduleFeatureTree, args.moduleId)) {
          const f = b.feature;
          if (!f || String(f.id).startsWith("synthetic:") || !String(f.id).trim()) continue;
          const mfId = String(f.id);
          const orig = userEffective.userFeatureAccessByModuleFeatureId[mfId]?.override;
          if (choice === orig) {
            delete next[mfId];
          } else {
            next[mfId] = choice;
          }
        }
        return next;
      });
    },
    [tab, selectedUserId, userEffective, moduleFeatureTree],
  );

  const onFeatureAccessLevel = useCallback(
    (args: { moduleId: string; featureKey: string; level: UiAccessLevel; scope: "target" }) => {
      if (tab === "roles" && selectedRoleId) {
        setRoleAssigned((prev) => {
          const b = findFeatureBucket(moduleFeatureTree, args.moduleId, args.featureKey);
          if (!b) return prev;
          const next = new Set(prev);
          applyFeatureLevelToPermissionSet(next, b, args.level);
          return next;
        });
        return;
      }
      if (tab === "groups" && selectedGroupId) {
        setGroupAssigned((prev) => {
          const b = findFeatureBucket(moduleFeatureTree, args.moduleId, args.featureKey);
          if (!b) return prev;
          const next = new Set(prev);
          applyFeatureLevelToPermissionSet(next, b, args.level);
          return next;
        });
      }
    },
    [tab, selectedRoleId, selectedGroupId, moduleFeatureTree],
  );

  const onModuleTargetLevel = useCallback(
    (args: { moduleId: string; level: UiAccessLevel }) => {
      if (tab === "roles" && selectedRoleId) {
        setRoleAssigned((prev) => {
          const next = new Set(prev);
          applyModuleLevelToPermissionSet(next, moduleFeatureTree, args.moduleId, args.level);
          return next;
        });
        return;
      }
      if (tab === "groups" && selectedGroupId) {
        setGroupAssigned((prev) => {
          const next = new Set(prev);
          applyModuleLevelToPermissionSet(next, moduleFeatureTree, args.moduleId, args.level);
          return next;
        });
      }
    },
    [tab, selectedRoleId, selectedGroupId, moduleFeatureTree],
  );

  const handleAccessTabCancel = useCallback(() => {
    if (tab === "users") {
      setUserOverrideDraft({});
    } else if (tab === "roles") {
      if (rolePermBaselineKey) setRoleAssigned(keyToSet(rolePermBaselineKey));
    } else if (tab === "groups") {
      if (groupPermBaselineKey) setGroupAssigned(keyToSet(groupPermBaselineKey));
    }
  }, [tab, rolePermBaselineKey, groupPermBaselineKey]);

  const handleAccessTabSave = useCallback(async () => {
    setAccessFooterBusy(true);
    try {
      if (tab === "users") {
        if (!selectedUserId) {
          showToast("Select a user first.", false);
          return;
        }
        const toApply = { ...userOverrideDraft };
        if (Object.keys(toApply).length === 0) {
          showToast("No access changes to save.", false);
          return;
        }
        setUserAccessError(null);
        for (const [moduleFeatureId, level] of Object.entries(toApply)) {
          const res = await setUserFeatureOverrideAction({
            profileId: selectedUserId,
            moduleFeatureId,
            level,
          });
          if (!res.ok) {
            showToast(res.error, false);
            return;
          }
        }
        setUserOverrideDraft({});
        const orgCtx = userAccessCompanyFilterId.trim();
        const next = await getUserEffectiveAccessAction(selectedUserId, orgCtx ? { organizationId: orgCtx } : undefined);
        if (next.ok) {
          setUserEffective(next.data);
          if (next.data.organization_id) {
            await loadAccessTreeForOrg(next.data.organization_id, false);
          }
          showToast("Access changes saved.", true);
        } else {
          setUserAccessError(next.error);
          showToast(next.error, false);
        }
        return;
      }
      if (tab === "roles") {
        if (!selectedRoleId) {
          showToast("Select a role first.", false);
          return;
        }
        const baseSet = keyToSet(rolePermBaselineKey);
        for (const node of moduleFeatureTree) {
          for (const bucket of node.features) {
            if (totalPermissionsInBucket(bucket) === 0) continue;
            const bL = effectiveLevelFromFeatureBucket(bucket, baseSet);
            const dL = effectiveLevelFromFeatureBucket(bucket, roleAssigned);
            if (bL === dL) continue;
            const res = await setFeatureAccessLevelForTargetAction({
              target: "role",
              targetId: selectedRoleId,
              moduleId: node.module.id,
              featureKey: bucket.featureKey,
              level: dL,
            });
            if (!res.ok) {
              showToast(res.error, false);
              return;
            }
          }
        }
        const r2 = await getAssignedPermissionIdsForRoleAction(selectedRoleId);
        if (!r2.ok) {
          showToast(r2.error, false);
          return;
        }
        const u = new Set(r2.permissionIds);
        setRoleAssigned(u);
        setRolePermBaselineKey(setToKey(u));
        showToast("Role access saved.", true);
        return;
      }
      if (tab === "groups") {
        if (!selectedGroupId) {
          showToast("Select a group first.", false);
          return;
        }
        const baseSet = keyToSet(groupPermBaselineKey);
        for (const node of moduleFeatureTree) {
          for (const bucket of node.features) {
            if (totalPermissionsInBucket(bucket) === 0) continue;
            const bL = effectiveLevelFromFeatureBucket(bucket, baseSet);
            const dL = effectiveLevelFromFeatureBucket(bucket, groupAssigned);
            if (bL === dL) continue;
            const res = await setFeatureAccessLevelForTargetAction({
              target: "group",
              targetId: selectedGroupId,
              moduleId: node.module.id,
              featureKey: bucket.featureKey,
              level: dL,
            });
            if (!res.ok) {
              showToast(res.error, false);
              return;
            }
          }
        }
        const g2 = await getAssignedPermissionIdsForGroupAction(selectedGroupId);
        if (!g2.ok) {
          showToast(g2.error, false);
          return;
        }
        const u = new Set(g2.permissionIds);
        setGroupAssigned(u);
        setGroupPermBaselineKey(setToKey(u));
        showToast("Group access saved.", true);
        return;
      }
    } finally {
      setAccessFooterBusy(false);
    }
  }, [
    tab,
    selectedUserId,
    userOverrideDraft,
    moduleFeatureTree,
    rolePermBaselineKey,
    roleAssigned,
    selectedRoleId,
    groupPermBaselineKey,
    groupAssigned,
    selectedGroupId,
    showToast,
    loadAccessTreeForOrg,
  ]);

  if (loadingGate) {
    return (
      <div className="mx-auto flex min-h-[40vh] max-w-6xl items-center justify-center px-4 py-16">
        <div className="flex items-center gap-3 text-sm text-muted-foreground">
          <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
          Checking access…
        </div>
      </div>
    );
  }

  if (accessDenied) {
    return (
      <div className="mx-auto max-w-lg px-4 py-16 text-center">
        <h1 className="text-lg font-semibold">Access Management</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          {accessDenied === "not_authenticated"
            ? "You must be signed in."
            : "You do not have access. Required catalog roles: super_admin, programmer, or system_admin."}
        </p>
        <Link href="/" className="mt-6 inline-block text-sm font-medium text-primary underline">
          Back to dashboard
        </Link>
      </div>
    );
  }

  return (
    <div className="mx-auto flex min-h-screen w-full min-w-0 max-w-6xl flex-col px-4 py-8 pb-28 sm:px-6">
      <Link
        href="/platform/settings"
        className="mb-6 inline-flex items-center gap-2 text-sm font-medium text-muted-foreground transition hover:text-foreground"
      >
        <ArrowLeft className="h-4 w-4" />
        Platform
      </Link>

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

      <PageHeaderWithInfo
        title="Access Management"
        titleClassName="text-2xl font-bold tracking-tight"
        infoAriaLabel="How Access Management works"
      >
        <p>
          For each <strong className="font-medium text-foreground">company</strong>, the tree lists only{" "}
          <strong className="font-medium text-foreground">enabled</strong> modules (
          <code className="rounded bg-muted px-1">organization_modules</code>
          ). When that org uses per-feature entitlements in{" "}
          <code className="rounded bg-muted px-1">organization_module_features</code>, only{" "}
          <strong className="font-medium text-foreground">enabled</strong> features appear; unlicensed or off features are
          hidden. Status dots: green = enabled, red = disabled, gray = not configured. Configure licensing on{" "}
          <Link className="font-medium text-primary underline" href="/platform/organizations">
            Organizations
          </Link>{" "}
          → <span className="font-medium text-foreground">Modules &amp; entitlements</span>. Baseline access comes from
          role/groups; user overrides use <code className="rounded bg-muted px-1">user_feature_access_overrides</code> (inherit,
          none, read, or write; bulk &quot;Manage&quot; is stored as write on the user row). Internal orgs (platform) see the
          full catalog.
        </p>
        <p>
          <Link href="/platform/access/catalog" className="font-medium text-primary underline hover:text-primary/90">
            Role &amp; group catalog
          </Link>
          {" — create or edit role and group definitions (keys, names, scopes)."}
        </p>
      </PageHeaderWithInfo>

      <div className="mb-4 flex flex-wrap items-stretch gap-2 border-b border-border">
        {(
          [
            ["users", "User Access"],
            ["roles", "Role Access"],
            ["groups", "Group Access"],
          ] as const
        ).map(([k, label]) => (
          <button
            key={k}
            type="button"
            onClick={() => {
              setTab(k);
              setReportTabHelpOpen(false);
            }}
            className={[
              "border-b-2 px-3 py-2 text-sm font-medium transition",
              tab === k
                ? "border-primary text-foreground"
                : "border-transparent text-muted-foreground hover:text-foreground",
            ].join(" ")}
          >
            {label}
          </button>
        ))}
        <div
          className={[
            "inline-flex min-h-[2.5rem] items-stretch gap-0.5 self-end border-b-2",
            tab === "report" ? "border-primary" : "border-transparent",
          ].join(" ")}
        >
          <button
            type="button"
            onClick={() => setTab("report")}
            className={[
              "px-2.5 py-2 text-sm font-medium transition sm:px-3",
              tab === "report" ? "text-foreground" : "text-muted-foreground hover:text-foreground",
            ].join(" ")}
          >
            Report
          </button>
          <button
            type="button"
            onClick={() => {
              setTab("report");
              setReportTabHelpOpen((o) => !o);
            }}
            className="mb-0.5 inline-flex h-7 w-7 shrink-0 self-center items-center justify-center rounded-full border border-border bg-background text-muted-foreground transition hover:bg-muted/80 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
            aria-expanded={reportTabHelpOpen}
            aria-label="About the access report"
          >
            <Info className="h-3.5 w-3.5" aria-hidden />
          </button>
        </div>
      </div>

      <div className="flex-1 min-w-0">
      {tab === "users" ? (
        <div className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <span className={LABEL} id="access-user-company-label">
                Company
              </span>
              <AccessCompanyFilterCombobox
                id="access-user-company-combobox"
                labelId="access-user-company-label"
                orgs={orgs}
                value={userAccessCompanyFilterId}
                onChange={(id) => {
                  setUserAccessCompanyFilterId(id);
                  commitWorkspaceOrgScope(id);
                }}
                includeAllRow={false}
                selectWhenEmptyLabel="Select a company"
              />
            </div>
            <div>
              <span className={LABEL} id="access-user-select-label">
                User
              </span>
              <AccessUserCombobox
                id="access-user-combobox"
                labelId="access-user-select-label"
                users={usersForUserAccessPicker}
                value={selectedUserId}
                onChange={setSelectedUserId}
                disabled={!userAccessCompanyFilterId || (!users.length && !usersLoading)}
                loading={usersLoading}
              />
            </div>
          </div>

          {!userAccessCompanyFilterId.trim() ? (
            <p className="text-sm text-muted-foreground">
              Select a company. Effective access is resolved for that organization (and for a specific user if you pick one; otherwise your own account in this org).
            </p>
          ) : catalogLoading ? (
            <div className="flex items-center gap-2 rounded-lg border border-border bg-card px-4 py-6 text-sm text-muted-foreground">
              <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
              <span>Loading module catalog for the selected company…</span>
            </div>
          ) : catalogError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-4 text-sm text-rose-900 dark:border-rose-800 dark:bg-rose-950/40 dark:text-rose-100">
              <p className="font-medium">Permission catalog could not be loaded</p>
              <p className="mt-1 text-rose-800/90 dark:text-rose-200/90">{catalogError}</p>
            </div>
          ) : userAccessLoading ? (
            <div className="flex items-center gap-2 rounded-lg border border-border bg-card px-4 py-6 text-sm text-muted-foreground">
              <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
              <span>Resolving effective access (role, groups, entitlements) for the selected org…</span>
            </div>
          ) : userAccessError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-4 text-sm text-rose-900 dark:border-rose-800 dark:bg-rose-950/40 dark:text-rose-100">
              <p className="font-medium">Could not load effective access</p>
              <p className="mt-1 text-rose-800/90 dark:text-rose-200/90">{userAccessError}</p>
            </div>
          ) : userEffective ? (
            <div className="space-y-4">
              {!selectedUserId && userEffective.profile_id === actorUserId ? (
                <p className="rounded-md border border-dashed border-border bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
                  Preview: your account&apos;s role, groups, and overrides in this company — as if you were only scoped to this org. Select a user above to edit someone else.
                </p>
              ) : null}
              <p className="text-sm text-foreground/90">
                <span className="font-medium">{userEffective.full_name}</span>{" "}
                <span className="text-muted-foreground">
                  (
                  {userEffective.role.display_name ?? userEffective.role.key}
                  ) — {selectedUserOrgLabel || userEffective.organization_name}
                </span>
                {userEffective.groups.length > 0 ? (
                  <span className="text-muted-foreground">
                    {" "}
                    · {userEffective.groups.map((g) => g.name).join(", ")}
                  </span>
                ) : null}
              </p>
              <div>
                <div className="mb-3 max-w-md">
                  <span className={LABEL} id="access-tree-scope-label">
                    Area
                  </span>
                  <AccessSearchableOptionsCombobox
                    id="access-tree-scope"
                    labelId="access-tree-scope-label"
                    value={accessTreeScope}
                    onChange={setAccessTreeScope}
                    options={treeScopeOptions}
                    placeholder="All modules"
                    searchPlaceholder="Search areas…"
                    emptyList="No area options"
                  />
                </div>
                <h2 className="mb-2 text-sm font-semibold text-foreground">
                  Access{selectedUserOrgLabel || userEffective.organization_name
                    ? ` (${selectedUserOrgLabel || userEffective.organization_name})`
                    : null}
                </h2>
                <FeatureAccessTree
                  tree={filteredModuleFeatureTree}
                  mode="user"
                  effectivePermissionIds={effectiveUserPermIds}
                  userFeatureAccessByModuleFeatureId={
                    mergedUserFeatureAccessByModuleFeatureId
                    ?? userEffective.userFeatureAccessByModuleFeatureId
                  }
                  orgEntitlements={userEffective.orgEntitlements}
                  busy={accessFooterBusy}
                  disabled={accessFooterBusy || !selectedUserId}
                  onSetLevel={() => {}}
                  onSetUserOverride={(a) => void onUserOverride(a)}
                  onSetModuleUserBulk={(a) => void onUserModuleBulk(a)}
                />
              </div>
            </div>
          ) : null}
        </div>
      ) : null}

      {tab === "roles" ? (
        <div className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <span className={LABEL} id="access-role-org-label">
                Company
              </span>
              <AccessCompanyFilterCombobox
                id="access-role-org-combobox"
                labelId="access-role-org-label"
                orgs={orgs}
                value={orgFilterId}
                onChange={(id) => {
                  setOrgFilterId(id);
                  commitWorkspaceOrgScope(id);
                  setSelectedRoleId("");
                }}
                includeAllRow={false}
                selectWhenEmptyLabel="Select a company"
              />
            </div>
            <div>
              <span className={LABEL} id="access-role-select-label">
                Role
              </span>
              <AccessSearchableOptionsCombobox
                id="access-role-combobox"
                labelId="access-role-select-label"
                value={selectedRoleId}
                onChange={setSelectedRoleId}
                options={roleSelectOptions}
                disabled={!orgFilterId}
                loading={rolesLoading}
                placeholder="Select a role"
                searchPlaceholder="Search by name or key…"
                emptyList="No roles for this company"
              />
            </div>
          </div>
          {licensedOrgLabel && orgFilterId ? (
            <div className="rounded-md border border-border bg-muted/30 px-3 py-2 text-sm">
              <span className="text-muted-foreground">Company </span>
              <span className="font-semibold text-foreground">{licensedOrgLabel}</span>
              <span className="text-muted-foreground"> — only modules enabled for this company; badges show entitlement details.</span>
            </div>
          ) : null}
          {catalogLoading ? (
            <div className="flex items-center gap-2 rounded-lg border border-border bg-card px-4 py-6 text-sm text-muted-foreground">
              <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
              Loading access catalog for this organization…
            </div>
          ) : catalogError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-4 text-sm text-rose-900 dark:border-rose-800 dark:bg-rose-950/40 dark:text-rose-100">
              <p className="font-medium">Permission catalog could not be loaded</p>
              <p className="mt-1 text-rose-800/90 dark:text-rose-200/90">{catalogError}</p>
            </div>
          ) : !orgFilterId || !selectedRoleId ? null : rolePermLoading ? (
            <div className="flex items-center gap-2 py-8 text-sm text-muted-foreground">
              <Loader2 className="h-5 w-5 animate-spin" />
              Loading role permissions…
            </div>
          ) : (
            <FeatureAccessTree
              tree={moduleFeatureTree}
              mode="role"
              effectivePermissionIds={roleAssigned}
              orgEntitlements={contextOrgEntitlements}
              busy={accessFooterBusy}
              disabled={accessFooterBusy}
              onSetLevel={(p) => void onFeatureAccessLevel(p)}
              onSetModuleLevel={(a) => void onModuleTargetLevel(a)}
            />
          )}
        </div>
      ) : null}

      {tab === "groups" ? (
        <div className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <span className={LABEL} id="access-group-org-label">
                Company
              </span>
              <AccessCompanyFilterCombobox
                id="access-group-org-combobox"
                labelId="access-group-org-label"
                orgs={orgs}
                value={orgFilterId}
                onChange={(id) => {
                  setOrgFilterId(id);
                  commitWorkspaceOrgScope(id);
                  setSelectedGroupId("");
                }}
                includeAllRow={false}
                selectWhenEmptyLabel="Select a company"
              />
            </div>
            <div>
              <span className={LABEL} id="access-group-select-label">
                Group
              </span>
              <AccessSearchableOptionsCombobox
                id="access-group-combobox"
                labelId="access-group-select-label"
                value={selectedGroupId}
                onChange={setSelectedGroupId}
                options={groupSelectOptions}
                disabled={!orgFilterId}
                loading={groupsLoading}
                placeholder={
                  !orgFilterId
                    ? "Pick a company first"
                    : "Select a group"
                }
                searchPlaceholder="Search by name or key…"
                emptyList={orgFilterId ? "No groups for this company" : undefined}
              />
            </div>
          </div>
          {licensedOrgLabel && orgFilterId ? (
            <div className="rounded-md border border-border bg-muted/30 px-3 py-2 text-sm">
              <span className="text-muted-foreground">Company </span>
              <span className="font-semibold text-foreground">{licensedOrgLabel}</span>
              <span className="text-muted-foreground"> — only modules enabled for this company; badges show entitlement details.</span>
            </div>
          ) : null}
          {catalogLoading ? (
            <div className="flex items-center gap-2 rounded-lg border border-border bg-card px-4 py-6 text-sm text-muted-foreground">
              <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
              Loading access catalog for this organization…
            </div>
          ) : catalogError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-4 text-sm text-rose-900 dark:border-rose-800 dark:bg-rose-950/40 dark:text-rose-100">
              <p className="font-medium">Permission catalog could not be loaded</p>
              <p className="mt-1 text-rose-800/90 dark:text-rose-200/90">{catalogError}</p>
            </div>
          ) : !orgFilterId || !selectedGroupId ? null : groupPermLoading ? (
            <div className="flex items-center gap-2 py-8 text-sm text-muted-foreground">
              <Loader2 className="h-5 w-5 animate-spin" />
              Loading group permissions…
            </div>
          ) : (
            <FeatureAccessTree
              tree={moduleFeatureTree}
              mode="group"
              effectivePermissionIds={groupAssigned}
              orgEntitlements={contextOrgEntitlements}
              busy={accessFooterBusy}
              disabled={accessFooterBusy}
              onSetLevel={(p) => void onFeatureAccessLevel(p)}
              onSetModuleLevel={(a) => void onModuleTargetLevel(a)}
            />
          )}
        </div>
      ) : null}

      {tab === "report" ? (
        <div className="space-y-4">
          {reportTabHelpOpen ? (
            <div className="space-y-2 rounded-lg border border-border bg-muted/20 p-3 text-sm text-muted-foreground">
              <p>
                Choose a <strong className="font-medium text-foreground">company</strong>, then optional area and feature
                search. The report uses the same high-level area list as the tree scope filter. Each row is one person × one
                feature. Up to <span className="font-medium text-foreground">200</span> users per run. Use the User / Role /
                Group tabs for the same enabled-module access tree; edit company entitlements on{" "}
                <Link className="font-medium text-primary underline" href="/platform/organizations">
                  Organizations
                </Link>{" "}
                → <span className="font-medium text-foreground">Modules &amp; entitlements</span>.
              </p>
            </div>
          ) : null}
          <div className="space-y-3">
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              <div className="sm:col-span-2">
                <span className={LABEL} id="access-report-org-label">
                  Company
                </span>
                <AccessCompanyFilterCombobox
                  id="access-report-org-combobox"
                  labelId="access-report-org-label"
                  orgs={orgs}
                  value={orgFilterId}
                  onChange={(id) => {
                    setOrgFilterId(id);
                    commitWorkspaceOrgScope(id);
                  }}
                  includeAllRow={false}
                  selectWhenEmptyLabel="Select a company"
                />
              </div>
              <div>
                <span className={LABEL} id="access-report-min-label">
                  Minimum access
                </span>
                <AccessSearchableOptionsCombobox
                  id="access-report-min-combobox"
                  labelId="access-report-min-label"
                  value={reportMin}
                  onChange={(v) =>
                    setReportMin(
                      v as "all" | "read_or_more" | "write_or_more" | "manage_only" | "",
                    )}
                  options={reportMinOptions}
                  placeholder="Select minimum access"
                  searchPlaceholder="Search options…"
                />
              </div>
              <div className="flex items-end">
                <button
                  type="button"
                  onClick={() => void runAccessReport()}
                  disabled={reportLoading || !orgFilterId}
                  className="inline-flex h-10 w-full items-center justify-center gap-2 rounded-md border border-primary bg-primary px-3 text-sm font-medium text-primary-foreground shadow-sm transition hover:bg-primary/90 disabled:pointer-events-none disabled:opacity-50"
                >
                  {reportLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                  Run report
                </button>
              </div>
            </div>
            <div>
              <span className={LABEL} id="access-report-module-label">
                Module (report areas)
              </span>
              <AccessSearchableOptionsCombobox
                id="access-report-module-combobox"
                labelId="access-report-module-label"
                value={reportModuleGroup}
                onChange={setReportModuleGroup}
                options={reportModuleOptions}
                placeholder="Select a report area"
                searchPlaceholder="Search report areas…"
                emptyList="No report areas"
              />
            </div>
            <div className="grid gap-3 sm:grid-cols-2">
              <div>
                <span className={LABEL} id="access-report-user-label">
                  User
                </span>
                <AccessUserCombobox
                  id="access-report-user-combobox"
                  labelId="access-report-user-label"
                  users={usersInReportOrg}
                  value={reportSelectedUserId}
                  onChange={setReportSelectedUserId}
                  disabled={!orgFilterId}
                  loading={usersLoading}
                />
              </div>
              <div>
                <span className={LABEL} id="access-report-feature-label">
                  Feature
                </span>
                <AccessSearchableOptionsCombobox
                  id="access-report-feature-combobox"
                  labelId="access-report-feature-label"
                  value={reportFeatureToken}
                  onChange={setReportFeatureToken}
                  options={reportFeatureOptions}
                  disabled={!orgFilterId}
                  loading={catalogLoading}
                  placeholder="Select a feature"
                  searchPlaceholder="Search by feature or module…"
                  emptyList={
                    !orgFilterId
                      ? "Select a company first"
                      : "No features in catalog for this company"
                  }
                />
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                onClick={downloadAccessReportCsv}
                disabled={reportRows.length === 0}
                className="h-10 rounded-md border border-border bg-background px-3 text-sm font-medium transition hover:bg-muted/60 disabled:opacity-50"
              >
                Download CSV
              </button>
              <button
                type="button"
                onClick={downloadAccessReportXlsx}
                disabled={reportRows.length === 0}
                className="h-10 rounded-md border border-border bg-background px-3 text-sm font-medium transition hover:bg-muted/60 disabled:opacity-50"
              >
                Download Excel
              </button>
              {reportMeta ? (
                <span className="text-xs text-muted-foreground">
                  {reportMeta.userCount} user{reportMeta.userCount === 1 ? "" : "s"} scanned
                  {reportMeta.truncated ? " (capped at 200)" : ""} · {reportRows.length} row{reportRows.length === 1 ? "" : "s"}
                </span>
              ) : null}
            </div>
          </div>
          {reportError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-900 dark:border-rose-800 dark:bg-rose-950/40 dark:text-rose-100">
              {reportError}
            </div>
          ) : null}
          {reportLoading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-5 w-5 animate-spin" />
              Building report…
            </div>
          ) : null}
          {!reportLoading && reportRows.length > 0 ? (
            <div className="overflow-x-auto rounded-lg border border-border">
              <table className="w-full min-w-[52rem] border-collapse text-left text-sm">
                <thead>
                  <tr className="border-b border-border bg-muted/50 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                    <th className="px-2 py-2 sm:px-3">User</th>
                    <th className="px-2 py-2 sm:px-3">Role</th>
                    <th className="px-2 py-2 sm:px-3">Module</th>
                    <th className="px-2 py-2 sm:px-3">Feature</th>
                    <th className="px-2 py-2 sm:px-3">Effective</th>
                    <th className="px-2 py-2 sm:px-3">Baseline</th>
                    <th className="px-2 py-2 sm:px-3">Override</th>
                  </tr>
                </thead>
                <tbody>
                  {reportRows.map((row, idx) => (
                    <tr
                      key={`${row.profile_id}-${row.module_feature_id}-${idx}`}
                      className="border-b border-border/60 last:border-0"
                    >
                      <td className="max-w-[10rem] truncate px-2 py-1.5 sm:px-3" title={row.full_name}>
                        {row.full_name}
                      </td>
                      <td className="whitespace-nowrap px-2 py-1.5 sm:px-3 text-muted-foreground">{row.role_key}</td>
                      <td className="max-w-[8rem] truncate px-2 py-1.5 sm:px-3" title={row.module_name}>
                        {row.module_name}
                      </td>
                      <td className="max-w-[12rem] truncate px-2 py-1.5 sm:px-3" title={row.feature_name}>
                        {row.feature_name}
                      </td>
                      <td className="whitespace-nowrap px-2 py-1.5 font-medium sm:px-3">
                        {UI_ACCESS_LEVEL_LABEL[row.effective]}
                      </td>
                      <td className="whitespace-nowrap px-2 py-1.5 text-muted-foreground sm:px-3">
                        {UI_ACCESS_LEVEL_LABEL[row.baseline]}
                      </td>
                      <td className="whitespace-nowrap px-2 py-1.5 text-muted-foreground sm:px-3">
                        {row.override}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : !reportLoading && reportMeta && reportRows.length === 0 ? (
            <p className="text-sm text-muted-foreground">No rows match the filters. Try a lower minimum access or clear filters.</p>
          ) : null}
        </div>
      ) : null}
      </div>

      {accessFormDirty ? (
        <div
          className="fixed bottom-0 left-0 right-0 z-50 border-t border-border bg-background/95 py-3 shadow-[0_-4px_24px_rgba(0,0,0,0.06)] backdrop-blur supports-[backdrop-filter]:bg-background/80"
        >
          <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-3 px-4 sm:px-6">
            <p className="min-w-0 text-sm text-muted-foreground">You have unsaved changes on this page.</p>
            <div className="ml-auto flex flex-wrap items-center justify-end gap-2">
              <button
                type="button"
                onClick={handleAccessTabCancel}
                disabled={accessFooterBusy}
                className="inline-flex h-10 min-w-[5.5rem] items-center justify-center rounded-md border border-border bg-background px-4 text-sm font-medium transition hover:bg-muted/60 disabled:pointer-events-none disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => void handleAccessTabSave()}
                disabled={accessFooterBusy}
                className="inline-flex h-10 min-w-[5.5rem] items-center justify-center gap-2 rounded-md border border-primary bg-primary px-4 text-sm font-medium text-primary-foreground shadow-sm transition hover:bg-primary/90 disabled:pointer-events-none disabled:opacity-50"
              >
                {accessFooterBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                Save
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

export default function PlatformAccessPage() {
  return (
    <Suspense
      fallback={(
        <div className="mx-auto flex min-h-[30vh] max-w-6xl items-center justify-center px-4 py-16 text-sm text-muted-foreground">
          <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
          <span className="ml-2">Loading…</span>
        </div>
      )}
    >
      <PlatformAccessPageInner />
    </Suspense>
  );
}
