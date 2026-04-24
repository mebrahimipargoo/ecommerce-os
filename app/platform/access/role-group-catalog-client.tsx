"use client";

import React, { Suspense, useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import {
  ArrowLeft, Loader2, Pencil, Plus, Save, Trash2, X,
} from "lucide-react";
import { PageHeaderWithInfo } from "../components/page-header-with-info";
import { useUserRole } from "../../../components/UserRoleContext";
import {
  createGroupAccessAction,
  createRoleAccessAction,
  deleteGroupAccessAction,
  deleteRoleAccessAction,
  getPlatformAccessPageAccessAction,
  listGroupsForOrganizationAccessAction,
  listOrganizationsForAccessAction,
  listRolesCatalogAction,
  updateGroupAccessAction,
  updateRoleAccessAction,
  type GroupCatalogRow,
  type OrganizationOptionRow,
  type RoleCatalogRow,
} from "./access-actions";

const INPUT =
  "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50";
const LABEL = "mb-2 block text-sm font-medium leading-none";
const BTN_PRIMARY =
  "inline-flex items-center justify-center gap-2 rounded-md bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground shadow transition hover:bg-primary/90 disabled:opacity-50";
const BTN_SECONDARY =
  "inline-flex items-center justify-center gap-2 rounded-md border border-border bg-background px-3 py-2 text-sm font-medium transition hover:bg-accent disabled:opacity-50";

type TabKey = "roles" | "groups";

function formatTs(iso: string): string {
  if (!iso?.trim()) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso.trim();
  return d.toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function RoleGroupCatalogInner() {
  const { organizationId, homeOrganizationId } = useUserRole();
  const searchParams = useSearchParams();

  const [loadingGate, setLoadingGate] = useState(true);
  const [accessDenied, setAccessDenied] = useState<"not_authenticated" | "forbidden" | null>(null);
  const [tab, setTab] = useState<TabKey>("roles");

  const [roles, setRoles] = useState<RoleCatalogRow[]>([]);
  const [rolesLoading, setRolesLoading] = useState(false);
  const [roleScopeFilter, setRoleScopeFilter] = useState<"" | "tenant" | "system">("");
  const [roleDeleteBusy, setRoleDeleteBusy] = useState(false);
  const [groupDeleteBusy, setGroupDeleteBusy] = useState(false);
  const [orgs, setOrgs] = useState<OrganizationOptionRow[]>([]);
  const [orgFilterId, setOrgFilterId] = useState("");
  const [groups, setGroups] = useState<GroupCatalogRow[]>([]);
  const [groupsLoading, setGroupsLoading] = useState(false);

  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);

  useEffect(() => {
    const t = searchParams.get("tab")?.trim().toLowerCase();
    if (t === "groups") setTab("groups");
    else if (t === "roles") setTab("roles");
  }, [searchParams]);

  const showToast = useCallback((msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 4200);
  }, []);

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
      ?? (homeOrganizationId && res.rows.some((o) => o.id === homeOrganizationId) ? homeOrganizationId : null)
      ?? res.rows[0]?.id
      ?? "";
    setOrgFilterId((prev) => {
      if (prev && res.rows.some((o) => o.id === prev)) return prev;
      return preferred;
    });
  }, [organizationId, homeOrganizationId, showToast]);

  const loadGroups = useCallback(async (oid: string) => {
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
  }, [showToast]);

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
      await loadRoles();
      await loadOrgs();
    })();
    return () => {
      cancelled = true;
    };
  }, [loadRoles, loadOrgs]);

  useEffect(() => {
    if (!orgFilterId || tab !== "groups") return;
    void loadGroups(orgFilterId);
  }, [orgFilterId, tab, loadGroups]);

  const visibleRoles = React.useMemo(() => {
    if (!roleScopeFilter) return roles;
    return roles.filter((r) => r.scope === roleScopeFilter);
  }, [roles, roleScopeFilter]);

  const [roleModal, setRoleModal] = useState<"create" | RoleCatalogRow | null>(null);
  const [roleSaving, setRoleSaving] = useState(false);
  const [rName, setRName] = useState("");
  const [rKey, setRKey] = useState("");
  const [rDesc, setRDesc] = useState("");
  const [rScope, setRScope] = useState<"tenant" | "system">("tenant");
  const [rAssignable, setRAssignable] = useState(true);

  const [groupModal, setGroupModal] = useState<"create" | GroupCatalogRow | null>(null);
  const [groupSaving, setGroupSaving] = useState(false);
  const [gOrgId, setGOrgId] = useState("");
  const [gName, setGName] = useState("");
  const [gKey, setGKey] = useState("");
  const [gDesc, setGDesc] = useState("");

  function openCreateRole() {
    setRName("");
    setRKey("");
    setRDesc("");
    setRScope("tenant");
    setRAssignable(true);
    setRoleModal("create");
  }

  function openEditRole(row: RoleCatalogRow) {
    setRName(row.name);
    setRKey(row.key);
    setRDesc(row.description ?? "");
    setRScope(row.scope);
    setRAssignable(row.is_assignable);
    setRoleModal(row);
  }

  function openCreateGroup() {
    setGOrgId(orgFilterId || orgs[0]?.id || "");
    setGName("");
    setGKey("");
    setGDesc("");
    setGroupModal("create");
  }

  function openEditGroup(row: GroupCatalogRow) {
    setGOrgId(row.organization_id);
    setGName(row.name);
    setGKey(row.key);
    setGDesc(row.description ?? "");
    setGroupModal(row);
  }

  async function submitRole(e: React.FormEvent) {
    e.preventDefault();
    setRoleSaving(true);
    try {
      if (roleModal === "create") {
        const res = await createRoleAccessAction({
          name: rName,
          key: rKey,
          description: rDesc,
          scope: rScope,
          is_assignable: rAssignable,
        });
        if (!res.ok) {
          showToast(res.error, false);
          return;
        }
        showToast("Role created.", true);
      } else if (roleModal) {
        const res = await updateRoleAccessAction(roleModal.id, {
          name: rName,
          description: rDesc,
          scope: rScope,
          is_assignable: rAssignable,
        });
        if (!res.ok) {
          showToast(res.error, false);
          return;
        }
        showToast("Role updated.", true);
      }
      setRoleModal(null);
      await loadRoles();
    } finally {
      setRoleSaving(false);
    }
  }

  async function handleDeleteRole() {
    if (roleModal === "create" || !roleModal) return;
    if (roleModal.is_system) return;
    if (
      !window.confirm(
        `Delete role «${roleModal.name}» (${roleModal.key})? This cannot be undone.`,
      )
    ) {
      return;
    }
    setRoleDeleteBusy(true);
    try {
      const res = await deleteRoleAccessAction(roleModal.id);
      if (!res.ok) {
        showToast(res.error, false);
        return;
      }
      showToast("Role deleted.", true);
      setRoleModal(null);
      await loadRoles();
    } finally {
      setRoleDeleteBusy(false);
    }
  }

  async function handleDeleteGroup() {
    if (groupModal === "create" || !groupModal) return;
    if (
      !window.confirm(
        `Delete group «${groupModal.name}» (${groupModal.key})? This cannot be undone.`,
      )
    ) {
      return;
    }
    setGroupDeleteBusy(true);
    try {
      const res = await deleteGroupAccessAction(groupModal.id);
      if (!res.ok) {
        showToast(res.error, false);
        return;
      }
      showToast("Group deleted.", true);
      setGroupModal(null);
      if (orgFilterId) await loadGroups(orgFilterId);
    } finally {
      setGroupDeleteBusy(false);
    }
  }

  async function submitGroup(e: React.FormEvent) {
    e.preventDefault();
    setGroupSaving(true);
    try {
      if (groupModal === "create") {
        const res = await createGroupAccessAction({
          organization_id: gOrgId,
          name: gName,
          key: gKey,
          description: gDesc,
        });
        if (!res.ok) {
          showToast(res.error, false);
          return;
        }
        showToast("Group created.", true);
      } else if (groupModal) {
        const res = await updateGroupAccessAction(groupModal.id, {
          name: gName,
          key: gKey,
          description: gDesc,
        });
        if (!res.ok) {
          showToast(res.error, false);
          return;
        }
        showToast("Group updated.", true);
      }
      setGroupModal(null);
      if (orgFilterId) await loadGroups(orgFilterId);
    } finally {
      setGroupSaving(false);
    }
  }

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
        <h1 className="text-lg font-semibold">Catalog</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          {accessDenied === "not_authenticated"
            ? "You must be signed in."
            : "You do not have access. Required catalog roles: super_admin, programmer, or system_admin."}
        </p>
        <Link href="/platform/access" className="mt-6 inline-block text-sm font-medium text-primary underline">
          Back to access management
        </Link>
      </div>
    );
  }

  return (
    <div className="mx-auto w-full min-w-0 max-w-6xl px-4 py-8 sm:px-6">
      <Link
        href="/platform/access"
        className="mb-6 inline-flex items-center gap-2 text-sm font-medium text-muted-foreground transition hover:text-foreground"
      >
        <ArrowLeft className="h-4 w-4" />
        Access management
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

      <PageHeaderWithInfo title="Role & group catalog" infoAriaLabel="About the role and group catalog">
        <p>
          Create and edit role definitions and organization-scoped groups. System roles in use cannot be deleted.
        </p>
      </PageHeaderWithInfo>

      <div className="mb-4 flex flex-wrap gap-2 border-b border-border">
        <button
          type="button"
          onClick={() => setTab("roles")}
          className={[
            "border-b-2 px-3 py-2 text-sm font-medium transition",
            tab === "roles"
              ? "border-primary text-foreground"
              : "border-transparent text-muted-foreground hover:text-foreground",
          ].join(" ")}
        >
          Roles
        </button>
        <button
          type="button"
          onClick={() => setTab("groups")}
          className={[
            "border-b-2 px-3 py-2 text-sm font-medium transition",
            tab === "groups"
              ? "border-primary text-foreground"
              : "border-transparent text-muted-foreground hover:text-foreground",
          ].join(" ")}
        >
          Groups
        </button>
      </div>

      {tab === "roles" ? (
        <div className="space-y-4">
          <div className="flex flex-wrap items-end justify-between gap-3">
            <div className="min-w-[180px]">
              <label className={LABEL} htmlFor="catalog-role-scope-filter">Scope</label>
              <select
                id="catalog-role-scope-filter"
                className={INPUT}
                value={roleScopeFilter}
                onChange={(e) =>
                  setRoleScopeFilter(
                    e.target.value === "system" ? "system" : e.target.value === "tenant" ? "tenant" : "",
                  )
                }
              >
                <option value="">All scopes</option>
                <option value="tenant">tenant</option>
                <option value="system">system</option>
              </select>
            </div>
            <button type="button" className={BTN_PRIMARY} onClick={openCreateRole}>
              <Plus className="h-4 w-4" />
              New role
            </button>
          </div>
          <div className="rounded-xl border border-border bg-card shadow-sm">
            {rolesLoading ? (
              <div className="flex items-center justify-center gap-2 py-16 text-muted-foreground">
                <Loader2 className="h-5 w-5 animate-spin" />
                Loading roles…
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full min-w-[800px] table-fixed border-collapse text-sm">
                  <thead>
                    <tr className="border-b border-border bg-muted/40 text-left text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                      <th className="px-3 py-2.5">Name</th>
                      <th className="px-3 py-2.5">Key</th>
                      <th className="px-3 py-2.5">Scope</th>
                      <th className="px-3 py-2.5">Assignable</th>
                      <th className="px-3 py-2.5">Description</th>
                      <th className="px-3 py-2.5">Created</th>
                      <th className="w-[100px] px-3 py-2.5 text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visibleRoles.length === 0 ? (
                      <tr>
                        <td colSpan={7} className="px-3 py-12 text-center text-sm text-muted-foreground">
                          {roles.length === 0 ? "No roles loaded." : "No roles match this scope filter."}
                        </td>
                      </tr>
                    ) : (
                      visibleRoles.map((row) => (
                        <tr key={row.id} className="border-b border-border last:border-0">
                          <td className="px-3 py-2 align-middle font-medium">{row.name}</td>
                          <td className="px-3 py-2 align-middle font-mono text-xs text-muted-foreground">{row.key}</td>
                          <td className="px-3 py-2 align-middle text-xs">{row.scope}</td>
                          <td className="px-3 py-2 align-middle text-xs">{row.is_assignable ? "Yes" : "No"}</td>
                          <td className="px-3 py-2 align-middle text-xs text-muted-foreground">
                            <span className="line-clamp-2" title={row.description ?? undefined}>
                              {row.description ?? "—"}
                            </span>
                          </td>
                          <td className="px-3 py-2 align-middle text-xs text-muted-foreground whitespace-nowrap">
                            {formatTs(row.created_at)}
                          </td>
                          <td className="px-3 py-2 align-middle text-right">
                            <button
                              type="button"
                              className="rounded-md p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground"
                              aria-label="Edit role"
                              onClick={() => openEditRole(row)}
                            >
                              <Pencil className="h-4 w-4" />
                            </button>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          <div className="flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-end">
            <div className="min-w-[220px] flex-1">
              <label className={LABEL} htmlFor="catalog-org-filter">Organization</label>
              <select
                id="catalog-org-filter"
                className={INPUT}
                value={orgFilterId}
                onChange={(e) => setOrgFilterId(e.target.value)}
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
            <button type="button" className={BTN_PRIMARY} onClick={openCreateGroup} disabled={!orgFilterId}>
              <Plus className="h-4 w-4" />
              New group
            </button>
          </div>
          <div className="rounded-xl border border-border bg-card shadow-sm">
            {groupsLoading ? (
              <div className="flex items-center justify-center gap-2 py-16 text-muted-foreground">
                <Loader2 className="h-5 w-5 animate-spin" />
                Loading groups…
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full min-w-[720px] table-fixed border-collapse text-sm">
                  <thead>
                    <tr className="border-b border-border bg-muted/40 text-left text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                      <th className="px-3 py-2.5">Name</th>
                      <th className="px-3 py-2.5">Key</th>
                      <th className="px-3 py-2.5">Organization</th>
                      <th className="px-3 py-2.5">Description</th>
                      <th className="w-[100px] px-3 py-2.5 text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {groups.length === 0 ? (
                      <tr>
                        <td colSpan={5} className="px-3 py-12 text-center text-sm text-muted-foreground">
                          No groups for this organization.
                        </td>
                      </tr>
                    ) : (
                      groups.map((row) => (
                        <tr key={row.id} className="border-b border-border last:border-0">
                          <td className="px-3 py-2 align-middle font-medium">{row.name}</td>
                          <td className="px-3 py-2 align-middle font-mono text-xs text-muted-foreground">{row.key}</td>
                          <td className="px-3 py-2 align-middle text-xs text-muted-foreground">
                            {row.organization_name ?? row.organization_id}
                          </td>
                          <td className="px-3 py-2 align-middle text-xs text-muted-foreground">
                            <span className="line-clamp-2" title={row.description ?? undefined}>
                              {row.description ?? "—"}
                            </span>
                          </td>
                          <td className="px-3 py-2 align-middle text-right">
                            <button
                              type="button"
                              className="rounded-md p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground"
                              aria-label="Edit group"
                              onClick={() => openEditGroup(row)}
                            >
                              <Pencil className="h-4 w-4" />
                            </button>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}

      {roleModal ? (
        <div className="fixed inset-0 z-[90] flex items-center justify-center bg-foreground/40 p-4 backdrop-blur-sm">
          <div
            role="dialog"
            aria-modal="true"
            className="max-h-[90vh] w-full max-w-md overflow-y-auto rounded-xl border border-border bg-card p-6 shadow-xl"
          >
            <div className="mb-4 flex items-center justify-between">
              <h2 className="text-lg font-bold">
                {roleModal === "create" ? "New role" : "Edit role"}
              </h2>
              <button type="button" className="rounded-md p-1 text-muted-foreground hover:bg-muted" onClick={() => setRoleModal(null)}>
                <X className="h-5 w-5" />
              </button>
            </div>
            <form onSubmit={(e) => void submitRole(e)} className="space-y-4">
              {roleModal !== "create" ? (
                <div>
                  <label className={LABEL}>Key</label>
                  <input className={INPUT} value={rKey} readOnly disabled />
                  <p className="mt-1 text-[11px] text-muted-foreground">Key cannot be changed after creation.</p>
                </div>
              ) : (
                <div>
                  <label className={LABEL} htmlFor="nr-key">Key <span className="text-destructive">*</span></label>
                  <input
                    id="nr-key"
                    className={INPUT}
                    value={rKey}
                    onChange={(e) => setRKey(e.target.value)}
                    required
                    autoComplete="off"
                  />
                </div>
              )}
              <div>
                <label className={LABEL} htmlFor="nr-name">Name <span className="text-destructive">*</span></label>
                <input
                  id="nr-name"
                  className={INPUT}
                  value={rName}
                  onChange={(e) => setRName(e.target.value)}
                  required
                />
              </div>
              <div>
                <label className={LABEL} htmlFor="nr-desc">Description</label>
                <textarea
                  id="nr-desc"
                  className={`${INPUT} min-h-[80px] py-2`}
                  value={rDesc}
                  onChange={(e) => setRDesc(e.target.value)}
                  maxLength={300}
                />
              </div>
              <div>
                <label className={LABEL} htmlFor="nr-scope">Scope</label>
                <select
                  id="nr-scope"
                  className={INPUT}
                  value={rScope}
                  onChange={(e) => setRScope(e.target.value === "system" ? "system" : "tenant")}
                >
                  <option value="tenant">tenant</option>
                  <option value="system">system</option>
                </select>
              </div>
              <label className="flex cursor-pointer items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={rAssignable}
                  onChange={(e) => setRAssignable(e.target.checked)}
                />
                Assignable (show in user role pickers)
              </label>
              <div className="flex flex-wrap items-center justify-between gap-2 pt-2">
                <div className="min-w-0">
                  {roleModal !== "create" ? (
                    roleModal.is_system ? (
                      <p className="text-xs text-muted-foreground">System catalog roles cannot be deleted.</p>
                    ) : (
                      <button
                        type="button"
                        className="inline-flex items-center gap-2 rounded-md border border-destructive/50 bg-background px-3 py-2 text-sm font-medium text-destructive hover:bg-destructive/10 disabled:opacity-50"
                        disabled={roleDeleteBusy || roleSaving}
                        onClick={() => void handleDeleteRole()}
                      >
                        {roleDeleteBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                        Delete role
                      </button>
                    )
                  ) : null}
                </div>
                <div className="flex flex-wrap justify-end gap-2">
                  <button type="button" className={BTN_SECONDARY} onClick={() => setRoleModal(null)} disabled={roleSaving || roleDeleteBusy}>
                    Cancel
                  </button>
                  <button type="submit" className={BTN_PRIMARY} disabled={roleSaving || roleDeleteBusy}>
                    {roleSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                    Save
                  </button>
                </div>
              </div>
            </form>
          </div>
        </div>
      ) : null}

      {groupModal ? (
        <div className="fixed inset-0 z-[90] flex items-center justify-center bg-foreground/40 p-4 backdrop-blur-sm">
          <div
            role="dialog"
            aria-modal="true"
            className="max-h-[90vh] w-full max-w-md overflow-y-auto rounded-xl border border-border bg-card p-6 shadow-xl"
          >
            <div className="mb-4 flex items-center justify-between">
              <h2 className="text-lg font-bold">
                {groupModal === "create" ? "New group" : "Edit group"}
              </h2>
              <button type="button" className="rounded-md p-1 text-muted-foreground hover:bg-muted" onClick={() => setGroupModal(null)}>
                <X className="h-5 w-5" />
              </button>
            </div>
            <form onSubmit={(e) => void submitGroup(e)} className="space-y-4">
              <div>
                <label className={LABEL} htmlFor="ng-org">Organization <span className="text-destructive">*</span></label>
                <select
                  id="ng-org"
                  className={INPUT}
                  value={gOrgId}
                  onChange={(e) => setGOrgId(e.target.value)}
                  required
                  disabled={groupModal !== "create"}
                >
                  {orgs.map((o) => (
                    <option key={o.id} value={o.id}>
                      {o.displayName}
                    </option>
                  ))}
                </select>
                {groupModal !== "create" ? (
                  <p className="mt-1 text-[11px] text-muted-foreground">Organization cannot be moved in this version.</p>
                ) : null}
              </div>
              <div>
                <label className={LABEL} htmlFor="ng-name">Name <span className="text-destructive">*</span></label>
                <input
                  id="ng-name"
                  className={INPUT}
                  value={gName}
                  onChange={(e) => setGName(e.target.value)}
                  required
                />
              </div>
              <div>
                <label className={LABEL} htmlFor="ng-key">Key <span className="text-destructive">*</span></label>
                <input
                  id="ng-key"
                  className={INPUT}
                  value={gKey}
                  onChange={(e) => setGKey(e.target.value)}
                  required
                  autoComplete="off"
                />
              </div>
              <div>
                <label className={LABEL} htmlFor="ng-desc">Description</label>
                <textarea
                  id="ng-desc"
                  className={`${INPUT} min-h-[80px] py-2`}
                  value={gDesc}
                  onChange={(e) => setGDesc(e.target.value)}
                  maxLength={300}
                />
              </div>
              <div className="flex flex-wrap items-center justify-between gap-2 pt-2">
                <div className="min-w-0">
                  {groupModal !== "create" ? (
                    <button
                      type="button"
                      className="inline-flex items-center gap-2 rounded-md border border-destructive/50 bg-background px-3 py-2 text-sm font-medium text-destructive hover:bg-destructive/10 disabled:opacity-50"
                      disabled={groupDeleteBusy || groupSaving}
                      onClick={() => void handleDeleteGroup()}
                    >
                      {groupDeleteBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                      Delete group
                    </button>
                  ) : null}
                </div>
                <div className="flex flex-wrap justify-end gap-2">
                  <button type="button" className={BTN_SECONDARY} onClick={() => setGroupModal(null)} disabled={groupSaving || groupDeleteBusy}>
                    Cancel
                  </button>
                  <button type="submit" className={BTN_PRIMARY} disabled={groupSaving || groupDeleteBusy}>
                    {groupSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                    Save
                  </button>
                </div>
              </div>
            </form>
          </div>
        </div>
      ) : null}
    </div>
  );
}

export default function RoleGroupCatalogPage() {
  return (
    <Suspense
      fallback={
        <div className="mx-auto flex min-h-[40vh] max-w-6xl items-center justify-center gap-3 px-4 py-16 text-sm text-muted-foreground">
          <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
          Loading…
        </div>
      }
    >
      <RoleGroupCatalogInner />
    </Suspense>
  );
}
