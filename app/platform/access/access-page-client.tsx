"use client";

import React, { Suspense, useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import * as XLSX from "xlsx";
import { ArrowLeft, Info, Loader2 } from "lucide-react";
import { PageHeaderWithInfo } from "../components/page-header-with-info";
import { useUserRole } from "../../../components/UserRoleContext";
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
import { accessTreeModuleMatchesScope, REPORT_MODULE_GROUP_CHOICES } from "../../../lib/access-report-filters";
import { UI_ACCESS_LEVEL_LABEL, type UiAccessLevel } from "../../../lib/access-level";
import type { UserOverrideChoice } from "../../../lib/user-feature-access";
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

function organizationTypeIsInternal(type: string | null | undefined): boolean {
  return (type ?? "").trim().toLowerCase() === "internal";
}

type TabKey = "users" | "roles" | "groups" | "report";

function PlatformAccessPageInner() {
  const searchParams = useSearchParams();
  const { organizationId, homeOrganizationId } = useUserRole();

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
  const [userFilter, setUserFilter] = useState("");
  const [selectedUserId, setSelectedUserId] = useState("");
  const [userAccessLoading, setUserAccessLoading] = useState(false);
  const [userAccessError, setUserAccessError] = useState<string | null>(null);
  const [userAccessEditBusy, setUserAccessEditBusy] = useState(false);
  const [userEffective, setUserEffective] = useState<UserEffectiveAccessResult | null>(null);

  const [roles, setRoles] = useState<RoleCatalogRow[]>([]);
  const [rolesLoading, setRolesLoading] = useState(false);
  const [selectedRoleId, setSelectedRoleId] = useState("");
  const [rolePermLoading, setRolePermLoading] = useState(false);
  const [rolePermBusy, setRolePermBusy] = useState(false);
  const [roleAssigned, setRoleAssigned] = useState<Set<string>>(() => new Set());

  const [orgs, setOrgs] = useState<OrganizationOptionRow[]>([]);
  const [orgFilterId, setOrgFilterId] = useState("");
  const [groups, setGroups] = useState<GroupCatalogRow[]>([]);
  const [groupsLoading, setGroupsLoading] = useState(false);
  const [selectedGroupId, setSelectedGroupId] = useState("");
  const [groupPermLoading, setGroupPermLoading] = useState(false);
  const [groupPermBusy, setGroupPermBusy] = useState(false);
  const [groupAssigned, setGroupAssigned] = useState<Set<string>>(() => new Set());

  const [accessTreeScope, setAccessTreeScope] = useState("");
  const [reportTabHelpOpen, setReportTabHelpOpen] = useState(false);

  const [reportRows, setReportRows] = useState<AccessReportRow[]>([]);
  const [reportMeta, setReportMeta] = useState<{ userCount: number; truncated: boolean } | null>(null);
  const [reportLoading, setReportLoading] = useState(false);
  const [reportError, setReportError] = useState<string | null>(null);
  const [reportMin, setReportMin] = useState<"all" | "read_or_more" | "write_or_more" | "manage_only">("all");
  const [reportUserQ, setReportUserQ] = useState("");
  const [reportFeatureQ, setReportFeatureQ] = useState("");
  const [reportModuleGroup, setReportModuleGroup] = useState("");

  const runAccessReport = useCallback(async () => {
    if (!orgFilterId?.trim()) {
      showToast("Choose a company first.", false);
      return;
    }
    setReportLoading(true);
    setReportError(null);
    const r = await getAccessMatrixReportForOrganizationAction({
      organizationId: orgFilterId,
      minEffective: reportMin,
      userSearch: reportUserQ.trim() || undefined,
      featureSearch: reportFeatureQ.trim() || undefined,
      reportModuleGroup: reportModuleGroup.trim() || undefined,
    });
    setReportLoading(false);
    if (!r.ok) {
      setReportError(r.error);
      setReportRows([]);
      setReportMeta(null);
      showToast(r.error, false);
      return;
    }
    setReportRows(r.rows);
    setReportMeta({ userCount: r.userCount, truncated: r.truncated });
  }, [orgFilterId, reportMin, reportUserQ, reportFeatureQ, reportModuleGroup, showToast]);

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
    const preferred =
      (organizationId && res.rows.some((o) => o.id === organizationId) ? organizationId : null)
      ?? (homeOrganizationId && res.rows.some((o) => o.id === homeOrganizationId)
        ? homeOrganizationId
        : null)
      ?? res.rows[0]?.id
      ?? "";
    setOrgFilterId((prev) => {
      if (prev && res.rows.some((o) => o.id === prev)) return prev;
      return preferred;
    });
  }, [organizationId, homeOrganizationId, showToast]);

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
    if (tab !== "users") return;
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
    if (!userEffective?.organization_id) return;
    if (userEffective.profile_id !== selectedUserId) return;
    void loadAccessTreeForOrg(userEffective.organization_id, false);
  }, [tab, userEffective, selectedUserId, loadAccessTreeForOrg]);

  useEffect(() => {
    if (tab !== "users") return;
    if (selectedUserId) {
      setModuleFeatureTree([]);
      setCatalogError(null);
    }
  }, [tab, selectedUserId]);

  useEffect(() => {
    let cancelled = false;
    if (!selectedUserId) {
      setUserEffective(null);
      setUserAccessError(null);
      return;
    }
    void (async () => {
      setUserAccessError(null);
      setUserEffective(null);
      setUserAccessLoading(true);
      const res = await getUserEffectiveAccessAction(selectedUserId);
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
  }, [selectedUserId, showToast]);

  useEffect(() => {
    let cancelled = false;
    if (!selectedRoleId) {
      setRoleAssigned(new Set());
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
        return;
      }
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
        return;
      }
      setGroupAssigned(new Set(res.permissionIds));
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedGroupId, showToast]);

  const filteredUsers = useMemo(() => {
    const q = userFilter.trim().toLowerCase();
    if (!q) return users;
    return users.filter(
      (u) =>
        u.full_name.toLowerCase().includes(q)
        || u.organization_name.toLowerCase().includes(q)
        || u.role_key.toLowerCase().includes(q),
    );
  }, [users, userFilter]);

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

  const onUserOverride = useCallback(
    async (args: { moduleFeatureId: string; level: UserOverrideChoice }) => {
      if (!selectedUserId) return;
      if (tab !== "users") return;
      setUserAccessEditBusy(true);
      const res = await setUserFeatureOverrideAction({
        profileId: selectedUserId,
        moduleFeatureId: args.moduleFeatureId,
        level: args.level,
      });
      setUserAccessEditBusy(false);
      if (!res.ok) {
        showToast(res.error, false);
        return;
      }
      const next = await getUserEffectiveAccessAction(selectedUserId);
      if (next.ok) setUserEffective(next.data);
      showToast("User override updated.", true);
    },
    [tab, selectedUserId, showToast],
  );

  const onUserModuleBulk = useCallback(
    async (args: { moduleId: string; level: UiAccessLevel | "inherit" }) => {
      if (!selectedUserId) return;
      if (tab !== "users") return;
      setUserAccessEditBusy(true);
      const res = await setUserModuleBulkOverrideAction({
        profileId: selectedUserId,
        moduleId: args.moduleId,
        level: args.level,
      });
      setUserAccessEditBusy(false);
      if (!res.ok) {
        showToast(res.error, false);
        return;
      }
      const next = await getUserEffectiveAccessAction(selectedUserId);
      if (next.ok) setUserEffective(next.data);
      showToast("User overrides updated for module.", true);
    },
    [tab, selectedUserId, showToast],
  );

  const onFeatureAccessLevel = useCallback(
    async (args: { moduleId: string; featureKey: string; level: UiAccessLevel; scope: "target" }) => {
      if (tab === "roles" && selectedRoleId) {
        setRolePermBusy(true);
        const res = await setFeatureAccessLevelForTargetAction({
          target: "role",
          targetId: selectedRoleId,
          moduleId: args.moduleId,
          featureKey: args.featureKey,
          level: args.level,
        });
        setRolePermBusy(false);
        if (!res.ok) {
          showToast(res.error, false);
          return;
        }
        const r2 = await getAssignedPermissionIdsForRoleAction(selectedRoleId);
        if (r2.ok) setRoleAssigned(new Set(r2.permissionIds));
        showToast("Role access updated.", true);
        return;
      }
      if (tab === "groups" && selectedGroupId) {
        setGroupPermBusy(true);
        const res = await setFeatureAccessLevelForTargetAction({
          target: "group",
          targetId: selectedGroupId,
          moduleId: args.moduleId,
          featureKey: args.featureKey,
          level: args.level,
        });
        setGroupPermBusy(false);
        if (!res.ok) {
          showToast(res.error, false);
          return;
        }
        const g2 = await getAssignedPermissionIdsForGroupAction(selectedGroupId);
        if (g2.ok) setGroupAssigned(new Set(g2.permissionIds));
        showToast("Group access updated.", true);
      }
    },
    [tab, selectedRoleId, selectedGroupId, showToast],
  );

  const onModuleTargetLevel = useCallback(
    async (args: { moduleId: string; level: UiAccessLevel }) => {
      if (tab === "roles" && selectedRoleId) {
        setRolePermBusy(true);
        const res = await setFeatureAccessLevelForEntireModuleForTargetAction({
          target: "role",
          targetId: selectedRoleId,
          moduleId: args.moduleId,
          level: args.level,
        });
        setRolePermBusy(false);
        if (!res.ok) {
          showToast(res.error, false);
          return;
        }
        const r2 = await getAssignedPermissionIdsForRoleAction(selectedRoleId);
        if (r2.ok) setRoleAssigned(new Set(r2.permissionIds));
        showToast("Role access updated for whole module.", true);
        return;
      }
      if (tab === "groups" && selectedGroupId) {
        setGroupPermBusy(true);
        const res = await setFeatureAccessLevelForEntireModuleForTargetAction({
          target: "group",
          targetId: selectedGroupId,
          moduleId: args.moduleId,
          level: args.level,
        });
        setGroupPermBusy(false);
        if (!res.ok) {
          showToast(res.error, false);
          return;
        }
        const g2 = await getAssignedPermissionIdsForGroupAction(selectedGroupId);
        if (g2.ok) setGroupAssigned(new Set(g2.permissionIds));
        showToast("Group access updated for whole module.", true);
      }
    },
    [tab, selectedRoleId, selectedGroupId, showToast],
  );

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
    <div className="mx-auto w-full min-w-0 max-w-6xl px-4 py-8 sm:px-6">
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

      {tab === "users" ? (
        <div className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <label className={LABEL} htmlFor="access-user-filter">
                Search users
              </label>
              <input
                id="access-user-filter"
                className={INPUT}
                value={userFilter}
                onChange={(e) => setUserFilter(e.target.value)}
                placeholder="Name, organization, or role…"
              />
            </div>
            <div>
              <label className={LABEL} htmlFor="access-user-select">
                User
              </label>
              <select
                id="access-user-select"
                className={INPUT}
                value={selectedUserId}
                onChange={(e) => setSelectedUserId(e.target.value)}
                disabled={usersLoading}
              >
                <option value="">{usersLoading ? "Loading…" : "Select a user"}</option>
                {filteredUsers.map((u) => (
                  <option key={u.id} value={u.id}>
                    {u.full_name} — {u.organization_name} — {u.role_key}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {catalogLoading ? (
            <div className="flex items-center gap-2 rounded-lg border border-border bg-card px-4 py-6 text-sm text-muted-foreground">
              <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
              <span>Loading access catalog for this user&apos;s organization…</span>
            </div>
          ) : catalogError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-4 text-sm text-rose-900 dark:border-rose-800 dark:bg-rose-950/40 dark:text-rose-100">
              <p className="font-medium">Permission catalog could not be loaded</p>
              <p className="mt-1 text-rose-800/90 dark:text-rose-200/90">{catalogError}</p>
            </div>
          ) : !selectedUserId ? null : userAccessLoading ? (
            <div className="flex items-center gap-2 rounded-lg border border-border bg-card px-4 py-6 text-sm text-muted-foreground">
              <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
              Loading this user’s effective access (role, groups, entitlements)…
            </div>
          ) : userAccessError ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-4 text-sm text-rose-900 dark:border-rose-800 dark:bg-rose-950/40 dark:text-rose-100">
              <p className="font-medium">Could not load this user’s access</p>
              <p className="mt-1 text-rose-800/90 dark:text-rose-200/90">{userAccessError}</p>
            </div>
          ) : userEffective ? (
            <div className="space-y-4">
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
                <h2 className="mb-2 text-sm font-semibold text-foreground">Tree scope (filter only)</h2>
                <div className="mb-3 max-w-md">
                  <label className={LABEL} htmlFor="access-tree-scope">
                    Show modules
                  </label>
                  <select
                    id="access-tree-scope"
                    className={INPUT}
                    value={accessTreeScope}
                    onChange={(e) => setAccessTreeScope(e.target.value)}
                  >
                    <option value="">All (full catalog layout)</option>
                    {REPORT_MODULE_GROUP_CHOICES.map((m) => (
                      <option key={m.value} value={m.value}>
                        {m.label}
                      </option>
                    ))}
                  </select>
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
                  userFeatureAccessByModuleFeatureId={userEffective.userFeatureAccessByModuleFeatureId}
                  orgEntitlements={userEffective.orgEntitlements}
                  busy={userAccessEditBusy}
                  disabled={userAccessEditBusy}
                  onSetLevel={(p) => void onFeatureAccessLevel(p)}
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
              <label className={LABEL} htmlFor="access-role-org-select">
                Company
              </label>
              <select
                id="access-role-org-select"
                className={INPUT}
                value={orgFilterId}
                onChange={(e) => {
                  setOrgFilterId(e.target.value);
                  setSelectedRoleId("");
                }}
              >
                {orgs.length === 0 ? (
                  <option value="">No organizations</option>
                ) : (
                  orgs.map((o) => (
                    <option key={o.id} value={o.id}>
                      {o.displayName}
                    </option>
                  ))
                )}
              </select>
            </div>
            <div>
              <label className={LABEL} htmlFor="access-role-select">
                Role
              </label>
              <select
                id="access-role-select"
                className={INPUT}
                value={selectedRoleId}
                onChange={(e) => setSelectedRoleId(e.target.value)}
                disabled={rolesLoading}
              >
                <option value="">{rolesLoading ? "Loading…" : "Select a role"}</option>
                {rolesForSelectedOrg.map((r) => (
                  <option key={r.id} value={r.id}>
                    {r.name} ({r.key})
                  </option>
                ))}
              </select>
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
              busy={rolePermBusy}
              disabled={rolePermBusy}
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
              <label className={LABEL} htmlFor="access-org-filter">
                Company
              </label>
              <select
                id="access-org-filter"
                className={INPUT}
                value={orgFilterId}
                onChange={(e) => {
                  setOrgFilterId(e.target.value);
                  setSelectedGroupId("");
                }}
              >
                {orgs.length === 0 ? (
                  <option value="">No organizations</option>
                ) : (
                  orgs.map((o) => (
                    <option key={o.id} value={o.id}>
                      {o.displayName}
                    </option>
                  ))
                )}
              </select>
            </div>
            <div>
              <label className={LABEL} htmlFor="access-group-select">
                Group
              </label>
              <select
                id="access-group-select"
                className={INPUT}
                value={selectedGroupId}
                onChange={(e) => setSelectedGroupId(e.target.value)}
                disabled={groupsLoading || !orgFilterId}
              >
                <option value="">
                  {groupsLoading ? "Loading…" : orgFilterId ? "Select a group" : "Pick an organization first"}
                </option>
                {groups.map((g) => (
                  <option key={g.id} value={g.id}>
                    {g.name} ({g.key})
                  </option>
                ))}
              </select>
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
              busy={groupPermBusy}
              disabled={groupPermBusy}
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
                <label className={LABEL} htmlFor="access-report-org">
                  Company
                </label>
                <select
                  id="access-report-org"
                  className={INPUT}
                  value={orgFilterId}
                  onChange={(e) => setOrgFilterId(e.target.value)}
                >
                  {orgs.length === 0 ? (
                    <option value="">No companies</option>
                  ) : (
                    orgs.map((o) => (
                      <option key={o.id} value={o.id}>
                        {o.displayName}
                      </option>
                    ))
                  )}
                </select>
              </div>
              <div>
                <label className={LABEL} htmlFor="access-report-min">
                  Minimum access
                </label>
                <select
                  id="access-report-min"
                  className={INPUT}
                  value={reportMin}
                  onChange={(e) =>
                    setReportMin(
                      e.target.value as "all" | "read_or_more" | "write_or_more" | "manage_only",
                    )}
                >
                  <option value="all">Any (include none)</option>
                  <option value="read_or_more">Read or higher</option>
                  <option value="write_or_more">Write or higher</option>
                  <option value="manage_only">Manage only</option>
                </select>
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
              <label className={LABEL} htmlFor="access-report-module-group">
                Module (report areas)
              </label>
              <select
                id="access-report-module-group"
                className={INPUT}
                value={reportModuleGroup}
                onChange={(e) => setReportModuleGroup(e.target.value)}
              >
                <option value="">All areas</option>
                {REPORT_MODULE_GROUP_CHOICES.map((m) => (
                  <option key={m.value} value={m.value}>
                    {m.label}
                  </option>
                ))}
              </select>
            </div>
            <div className="grid gap-3 sm:grid-cols-2">
              <div>
                <label className={LABEL} htmlFor="access-report-user-q">
                  User filter
                </label>
                <input
                  id="access-report-user-q"
                  className={INPUT}
                  value={reportUserQ}
                  onChange={(e) => setReportUserQ(e.target.value)}
                  placeholder="Name, id, or role key…"
                />
              </div>
              <div>
                <label className={LABEL} htmlFor="access-report-feat-q">
                  Feature (search)
                </label>
                <input
                  id="access-report-feat-q"
                  className={INPUT}
                  value={reportFeatureQ}
                  onChange={(e) => setReportFeatureQ(e.target.value)}
                  placeholder="Module / feature name or key"
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
