"use client";

import React, { useCallback, useEffect, useRef, useState } from "react";
import Link from "next/link";
import {
  ArrowLeft, Loader2, Pencil, Plus, Save, Tag, Trash2, UserRound, X,
} from "lucide-react";
import { useUserRole } from "../../../components/UserRoleContext";
import { useRbacPermissions } from "../../../hooks/useRbacPermissions";
import { OrganizationTypeBadge } from "../../../components/OrganizationTypeBadge";
import { UserProfileAvatar } from "../../(admin)/users/UserProfileAvatar";
import {
  assignUserGroup,
  createUserProfile,
  deleteUserProfile,
  listAllUserProfilesForPlatformDirectory,
  listAssignableRolesForUsers,
  listGroupsForOrganization,
  listUserGroupAssignmentsForProfiles,
  removeUserGroup,
  type AssignableRoleRow,
} from "../../(admin)/users/users-actions";
import type { OrgGroupRow, ProfileRow, UserGroupAssignment } from "../../(admin)/users/users-types";
import type { CompanyOption } from "../../../lib/imports-types";
import { uploadUserProfilePhotoAction } from "../../(admin)/users/upload-profile-photo-action";
import {
  getPlatformUsersPageAccessAction,
  listOrganizationsForPlatformUserDirectory,
  updatePlatformUserProfile,
} from "./platform-users-actions";
import { PageHeaderWithInfo } from "../components/page-header-with-info";

const INPUT =
  "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50";
const LABEL = "mb-2 block text-sm font-medium leading-none";
const BTN_PRIMARY =
  "inline-flex items-center justify-center gap-2 rounded-md bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground shadow transition hover:bg-primary/90 disabled:opacity-50";
const BTN_SECONDARY =
  "inline-flex items-center justify-center gap-2 rounded-md border border-border bg-background px-3 py-2 text-sm font-medium transition hover:bg-accent";

type Toast = { msg: string; ok: boolean } | null;

function humanizeRoleKey(k: string | null | undefined): string {
  const s = (k ?? "").trim().toLowerCase();
  if (!s) return "—";
  return s
    .split("_")
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

function normalizeRolePickerValue(
  role: string | null | undefined,
  assignableKeys: Set<string>,
): string {
  const value = (role ?? "").trim().toLowerCase();
  if (value === "admin") return assignableKeys.has("tenant_admin") ? "tenant_admin" : value;
  if (assignableKeys.has(value)) return value;
  if (assignableKeys.has("employee")) return "employee";
  return [...assignableKeys][0] ?? "employee";
}

function formatCreatedAt(iso: string): string {
  if (!iso?.trim()) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso.trim();
  return d.toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

type TabKey = "platform" | "all";

export default function PlatformUsersPage() {
  const { actorUserId } = useUserRole();
  const perms = useRbacPermissions();
  const [loadingAccess, setLoadingAccess] = useState(true);
  const [accessDenied, setAccessDenied] = useState<"not_authenticated" | "forbidden" | null>(null);

  const [rows, setRows] = useState<ProfileRow[]>([]);
  const [loadingRows, setLoadingRows] = useState(true);
  const [toast, setToast] = useState<Toast>(null);
  const [tab, setTab] = useState<TabKey>("platform");
  const [searchQuery, setSearchQuery] = useState("");
  const [roleFilter, setRoleFilter] = useState("");

  const [modalOpen, setModalOpen] = useState(false);
  const [editing, setEditing] = useState<ProfileRow | null>(null);
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [pendingPhoto, setPendingPhoto] = useState<File | null>(null);
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [userRole, setUserRole] = useState<string>("employee");
  const [editCompanyId, setEditCompanyId] = useState("");
  const [editOrganizationType, setEditOrganizationType] = useState<"tenant" | "internal">("tenant");
  const [assignableRoles, setAssignableRoles] = useState<AssignableRoleRow[]>([]);
  const [companies, setCompanies] = useState<CompanyOption[]>([]);
  const [photoUploading, setPhotoUploading] = useState(false);
  const [groupsForEdit, setGroupsForEdit] = useState<OrgGroupRow[]>([]);
  const [groupsEditLoading, setGroupsEditLoading] = useState(false);
  const [groupPickId, setGroupPickId] = useState("");
  const [groupBusy, setGroupBusy] = useState(false);
  /** Group memberships to apply after create (same org as {@link editCompanyId}). */
  const [pendingCreateGroups, setPendingCreateGroups] = useState<
    Pick<UserGroupAssignment, "group_id" | "key" | "name">[]
  >([]);
  const editLoadSeq = useRef(0);

  const showToast = useCallback((msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 4200);
  }, []);

  useEffect(() => {
    if (!modalOpen || assignableRoles.length === 0) return;
    const keys = new Set(assignableRoles.map((r) => r.key));
    if (!keys.has(userRole)) {
      setUserRole(normalizeRolePickerValue(userRole, keys));
    }
  }, [modalOpen, assignableRoles, userRole]);

  const loadProfiles = useCallback(async (): Promise<ProfileRow[] | null> => {
    setLoadingRows(true);
    const res = await listAllUserProfilesForPlatformDirectory();
    if (!res.ok) {
      setLoadingRows(false);
      showToast(res.error, false);
      return null;
    }
    const ids = res.rows.map((r) => r.id);
    const assignRes = await listUserGroupAssignmentsForProfiles(ids, {
      actorProfileId: actorUserId,
      platformUserDirectoryBypass: true,
    });
    let merged: ProfileRow[];
    if (assignRes.ok) {
      merged = res.rows.map((r) => ({
        ...r,
        assigned_groups: assignRes.byProfileId[r.id] ?? [],
      }));
    } else {
      showToast(assignRes.error, false);
      merged = res.rows.map((r) => ({ ...r, assigned_groups: [] }));
    }
    setRows(merged);
    setLoadingRows(false);
    return merged;
  }, [actorUserId, showToast]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const access = await getPlatformUsersPageAccessAction();
      if (cancelled) return;
      if (access.accessDenied) {
        setAccessDenied(access.accessDenied);
        setLoadingAccess(false);
        setLoadingRows(false);
        return;
      }
      setAccessDenied(null);
      setLoadingAccess(false);

      const [coRes, rolesRes] = await Promise.all([
        listOrganizationsForPlatformUserDirectory(),
        listAssignableRolesForUsers(),
      ]);
      if (cancelled) return;
      if (rolesRes.ok) setAssignableRoles(rolesRes.rows);
      if (coRes.ok) setCompanies(coRes.rows);

      await loadProfiles();
    })();
    return () => {
      cancelled = true;
    };
  }, [loadProfiles]);

  /** Role filter options match catalog scope: system ↔ internal org tab, tenant ↔ tenant org tab. */
  const roleFilterOptions = React.useMemo(() => {
    const scope = tab === "platform" ? "system" : "tenant";
    return assignableRoles
      .filter((r) => r.scope === scope)
      .slice()
      .sort((a, b) => a.key.localeCompare(b.key))
      .map((r) => ({
        key: r.key,
        label: (r.name ?? "").trim() || humanizeRoleKey(r.key),
      }));
  }, [assignableRoles, tab]);

  useEffect(() => {
    const rf = roleFilter.trim().toLowerCase();
    if (!rf) return;
    if (!roleFilterOptions.some((o) => o.key.toLowerCase() === rf)) {
      setRoleFilter("");
    }
  }, [tab, roleFilter, roleFilterOptions]);

  const visibleRows = React.useMemo(() => {
    const base =
      tab === "platform"
        ? rows.filter((r) => r.organization_type === "internal")
        : rows.filter((r) => r.organization_type === "tenant");
    const q = searchQuery.trim().toLowerCase();
    const rf = roleFilter.trim().toLowerCase();
    return base.filter((r) => {
      if (rf && (r.role ?? "").trim().toLowerCase() !== rf) return false;
      if (!q) return true;
      const name = (r.full_name ?? "").toLowerCase();
      const em = (r.email ?? "").toLowerCase();
      const co = (r.company_name ?? "").toLowerCase();
      return name.includes(q) || em.includes(q) || co.includes(q);
    });
  }, [rows, tab, searchQuery, roleFilter]);

  function companyOptionsForEdit(row: ProfileRow | null): CompanyOption[] {
    if (!row?.organization_id) return companies;
    const has = companies.some((c) => c.id === row.organization_id);
    if (has) return companies;
    return [
      ...companies,
      {
        id: row.organization_id,
        display_name: row.company_name?.trim() || row.organization_id,
        organization_type:
          row.organization_type === "internal" ? "internal" : "tenant",
      },
    ];
  }

  function closeModal() {
    setModalOpen(false);
    setEditing(null);
    setGroupsForEdit([]);
    setGroupPickId("");
    setGroupsEditLoading(false);
    setPendingPhoto(null);
    setPendingCreateGroups([]);
  }

  const companiesForCreateMode = React.useMemo(() => {
    return companies.filter(
      (c) => (c.organization_type ?? "tenant") === editOrganizationType,
    );
  }, [companies, editOrganizationType]);

  /** When creating a user, keep org pick valid for the selected organization type. */
  useEffect(() => {
    if (!modalOpen || editing) return;
    const filtered = companies.filter(
      (c) => (c.organization_type ?? "tenant") === editOrganizationType,
    );
    if (filtered.length === 0) return;
    if (!filtered.some((c) => c.id === editCompanyId)) {
      setEditCompanyId(filtered[0]!.id);
    }
  }, [modalOpen, editing, editOrganizationType, companies, editCompanyId]);

  function openCreate() {
    setEditing(null);
    setFullName("");
    setEmail("");
    const keys = new Set(assignableRoles.map((r) => r.key));
    const preferred =
      tab === "platform"
        ? assignableRoles.find((r) => r.scope === "system")?.key
        : assignableRoles.find((r) => r.scope === "tenant")?.key;
    setUserRole(normalizeRolePickerValue(preferred ?? "employee", keys));
    const orgType = tab === "platform" ? "internal" : "tenant";
    setEditOrganizationType(orgType);
    const filtered = companies.filter(
      (c) => (c.organization_type ?? "tenant") === orgType,
    );
    setEditCompanyId(filtered[0]?.id ?? "");
    setPendingPhoto(null);
    setGroupPickId("");
    setGroupsForEdit([]);
    setPendingCreateGroups([]);
    setModalOpen(true);
  }

  function openEdit(row: ProfileRow) {
    setEditing(row);
    setFullName(row.full_name ?? "");
    setEmail(row.email);
    const keys = new Set(assignableRoles.map((r) => r.key));
    setUserRole(normalizeRolePickerValue(row.role, keys));
    setEditCompanyId(row.organization_id ?? "");
    setEditOrganizationType(
      row.organization_type === "internal" ? "internal" : "tenant",
    );
    setGroupPickId("");
    setGroupsForEdit([]);
    setModalOpen(true);
  }

  function syncOrgTypeFromCompanyPicker(companyId: string, row: ProfileRow | null) {
    if (!companyId.trim()) return;
    const opts = companyOptionsForEdit(row);
    const co = opts.find((c) => c.id === companyId);
    setEditOrganizationType(
      co?.organization_type === "internal" ? "internal" : "tenant",
    );
  }

  const effectiveOrgForGroups = (editCompanyId.trim() || editing?.organization_id || "").trim();
  const isEditMode = Boolean(editing);

  /** Changing company while creating clears staged group picks (groups are org-scoped). */
  useEffect(() => {
    if (!modalOpen || editing) return;
    setPendingCreateGroups([]);
  }, [editCompanyId, modalOpen, editing]);

  useEffect(() => {
    if (!modalOpen) {
      setGroupsEditLoading(false);
      return;
    }
    if (!effectiveOrgForGroups) {
      setGroupsForEdit([]);
      setGroupsEditLoading(false);
      return;
    }
    const seq = ++editLoadSeq.current;
    setGroupsEditLoading(true);
    void (async () => {
      const g = await listGroupsForOrganization(effectiveOrgForGroups, {
        actorProfileId: actorUserId,
        platformUserDirectoryBypass: true,
      });
      if (seq !== editLoadSeq.current) return;
      setGroupsEditLoading(false);
      if (g.ok) {
        setGroupsForEdit(g.rows);
      } else {
        showToast(g.error, false);
      }
    })();
  }, [modalOpen, editing?.id, effectiveOrgForGroups, actorUserId, showToast]);

  const assignedGroupsForModal: UserGroupAssignment[] = React.useMemo(() => {
    if (isEditMode) {
      return editing!.assigned_groups ?? [];
    }
    return pendingCreateGroups.map((p) => ({
      user_group_id: `pending:${p.group_id}`,
      group_id: p.group_id,
      key: p.key,
      name: p.name,
    }));
  }, [isEditMode, editing?.assigned_groups, editing?.id, pendingCreateGroups]);

  async function handlePhotoChange(e: React.ChangeEvent<HTMLInputElement>, profileId: string) {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file) return;
    setPhotoUploading(true);
    try {
      const fd = new FormData();
      fd.append("file", file);
      fd.append("profile_id", profileId);
      const res = await uploadUserProfilePhotoAction(fd);
      if (!res.ok) throw new Error(res.error);
      showToast("Profile photo updated.", true);
      const merged = await loadProfiles();
      if (merged) {
        setEditing((prev) => {
          if (!prev || prev.id !== profileId) return prev;
          return merged.find((r) => r.id === profileId) ?? prev;
        });
      }
    } catch (err) {
      showToast(err instanceof Error ? err.message : "Upload failed.", false);
    } finally {
      setPhotoUploading(false);
    }
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!editCompanyId.trim()) {
      showToast("Select an organization.", false);
      return;
    }
    setSaving(true);
    try {
      if (editing) {
        const res = await updatePlatformUserProfile(editing.id, {
          full_name: fullName,
          role: userRole,
          organization_id: editCompanyId.trim(),
          organization_type: editOrganizationType,
        });
        if (!res.ok) throw new Error(res.error ?? "Save failed.");
        showToast("User updated.", true);
      } else {
        const res = await createUserProfile({
          full_name: fullName,
          email: email.trim().toLowerCase(),
          role: userRole,
          organization_id: editCompanyId.trim(),
        });
        if (!res.ok) throw new Error(res.error ?? "Create failed.");
        const syncOrg = await updatePlatformUserProfile(res.id, {
          organization_id: editCompanyId.trim(),
          organization_type: editOrganizationType,
        });
        if (!syncOrg.ok) throw new Error(syncOrg.error ?? "Could not sync organization type.");
        if (pendingPhoto) {
          setPhotoUploading(true);
          const fd = new FormData();
          fd.append("file", pendingPhoto);
          fd.append("profile_id", res.id);
          const up = await uploadUserProfilePhotoAction(fd);
          setPhotoUploading(false);
          if (!up.ok) throw new Error(up.error);
        }
        for (const pg of pendingCreateGroups) {
          const ag = await assignUserGroup(res.id, pg.group_id, {
            actorProfileId: actorUserId,
            platformUserDirectoryBypass: true,
          });
          if (!ag.ok) throw new Error(ag.error ?? "Failed to assign group.");
        }
        setPendingPhoto(null);
        setPendingCreateGroups([]);
        showToast("User created.", true);
      }
      closeModal();
      await loadProfiles();
    } catch (err) {
      showToast(err instanceof Error ? err.message : "Save failed.", false);
    } finally {
      setSaving(false);
    }
  }

  async function handleDeleteUser() {
    if (!editing) return;
    if (!window.confirm("Remove this user from the directory? This cannot be undone.")) return;
    setDeleting(true);
    try {
      const res = await deleteUserProfile(editing.id);
      if (!res.ok) {
        showToast(res.error ?? "Delete failed.", false);
        return;
      }
      showToast("User removed.", true);
      closeModal();
      await loadProfiles();
    } finally {
      setDeleting(false);
    }
  }

  async function handleAddGroup(pickedId?: string) {
    const gid = (pickedId ?? groupPickId).trim();
    if (!gid) return;
    if (!editing) {
      const row = groupsForEdit.find((g) => g.id === gid);
      if (!row) return;
      setPendingCreateGroups((prev) =>
        prev.some((p) => p.group_id === row.id)
          ? prev
          : [...prev, { group_id: row.id, key: row.key, name: row.name }],
      );
      setGroupPickId("");
      return;
    }
    const profileId = editing.id;
    setGroupBusy(true);
    try {
      const res = await assignUserGroup(profileId, gid, {
        actorProfileId: actorUserId,
        platformUserDirectoryBypass: true,
      });
      if (!res.ok) throw new Error(res.error);
      showToast("Group added.", true);
      const merged = await loadProfiles();
      setGroupPickId("");
      if (merged) {
        setEditing((prev) => {
          if (!prev || prev.id !== profileId) return prev;
          return merged.find((r) => r.id === profileId) ?? prev;
        });
      }
    } catch (err) {
      showToast(err instanceof Error ? err.message : "Add failed.", false);
    } finally {
      setGroupBusy(false);
    }
  }

  async function handleRemoveGroup(groupId: string) {
    if (!editing) {
      setPendingCreateGroups((prev) => prev.filter((p) => p.group_id !== groupId));
      return;
    }
    const profileId = editing.id;
    setGroupBusy(true);
    try {
      const res = await removeUserGroup(profileId, groupId, {
        actorProfileId: actorUserId,
        platformUserDirectoryBypass: true,
      });
      if (!res.ok) throw new Error(res.error);
      showToast("Group removed.", true);
      const merged = await loadProfiles();
      if (merged) {
        setEditing((prev) => {
          if (!prev || prev.id !== profileId) return prev;
          return merged.find((r) => r.id === profileId) ?? prev;
        });
      }
    } catch (err) {
      showToast(err instanceof Error ? err.message : "Remove failed.", false);
    } finally {
      setGroupBusy(false);
    }
  }

  if (loadingAccess) {
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
        <UserRound className="mx-auto mb-4 h-12 w-12 text-muted-foreground" />
        <h1 className="text-lg font-bold">Platform users</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          {accessDenied === "not_authenticated"
            ? "You must be signed in to view this page."
            : "This page is restricted to super_admin only."}
        </p>
        <Link href="/" className="mt-6 inline-block text-sm font-medium text-primary underline">
          Back to dashboard
        </Link>
      </div>
    );
  }

  const orgDirty =
    isEditMode
    && editCompanyId.trim() !== (editing!.organization_id ?? "").trim();

  return (
    <div className="mx-auto w-full min-w-0 max-w-6xl px-4 py-8 sm:px-6">
      <Link
        href="/platform/organizations"
        className="mb-6 inline-flex items-center gap-2 text-sm font-medium text-muted-foreground transition hover:text-foreground"
      >
        <ArrowLeft className="h-4 w-4" />
        Platform
      </Link>

      {toast && (
        <div
          role="status"
          className={[
            "fixed bottom-6 right-6 z-[80] flex max-w-md items-center gap-3 rounded-lg border px-4 py-3 text-sm font-medium shadow-lg",
            toast.ok
              ? "border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-700/50 dark:bg-emerald-950/90 dark:text-emerald-200"
              : "border-rose-200 bg-rose-50 text-rose-800 dark:border-rose-700/50 dark:bg-rose-950/90 dark:text-rose-200",
          ].join(" ")}
        >
          {toast.msg}
        </div>
      )}

      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div className="flex min-w-0 flex-1 items-start gap-3">
          <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-2xl bg-sky-100 dark:bg-sky-950/50">
            <UserRound className="h-6 w-6 text-sky-600 dark:text-sky-400" />
          </div>
          <PageHeaderWithInfo
            className="min-w-0 flex-1 mb-0"
            title="Platform users"
            titleClassName="text-2xl font-bold tracking-tight"
            infoAriaLabel="About platform users"
          >
            <p>
              <strong className="font-medium text-foreground">Platform users</strong> lists people in{" "}
              <code className="rounded bg-muted px-1 font-mono text-[11px]">internal</code>{" "}
              organizations;{" "}
              <strong className="font-medium text-foreground">Tenant Users</strong> lists people in{" "}
              <code className="rounded bg-muted px-1 font-mono text-[11px]">tenant</code>{" "}
              organizations (from <code className="rounded bg-muted px-1 font-mono text-[11px]">organizations.type</code>). Tenant{" "}
              <Link href="/users" className="underline hover:text-foreground">/users</Link> is unchanged.
            </p>
          </PageHeaderWithInfo>
        </div>
        <button type="button" onClick={openCreate} className={BTN_PRIMARY}>
          <Plus className="h-4 w-4" />
          Add user
        </button>
      </div>

      <div className="mb-4 flex flex-wrap gap-2 border-b border-border">
        <button
          type="button"
          onClick={() => setTab("platform")}
          className={[
            "border-b-2 px-3 py-2 text-sm font-medium transition",
            tab === "platform"
              ? "border-primary text-foreground"
              : "border-transparent text-muted-foreground hover:text-foreground",
          ].join(" ")}
        >
          Platform users
        </button>
        <button
          type="button"
          onClick={() => setTab("all")}
          className={[
            "border-b-2 px-3 py-2 text-sm font-medium transition",
            tab === "all"
              ? "border-primary text-foreground"
              : "border-transparent text-muted-foreground hover:text-foreground",
          ].join(" ")}
        >
          Tenant Users
        </button>
      </div>

      <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-end">
        <div className="min-w-[200px] flex-1">
          <label className={LABEL} htmlFor="platform-user-search">Search</label>
          <input
            id="platform-user-search"
            className={INPUT}
            placeholder="Name, email, or organization…"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            autoComplete="off"
          />
        </div>
        <div className="w-full sm:w-48">
          <label className={LABEL} htmlFor="platform-user-role">Role</label>
          <select
            id="platform-user-role"
            className={INPUT}
            value={roleFilter}
            onChange={(e) => setRoleFilter(e.target.value)}
          >
            <option value="">All roles</option>
            {roleFilterOptions.map(({ key, label }) => (
              <option key={key} value={key}>
                {label}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card shadow-sm">
        {loadingRows ? (
          <div className="flex items-center justify-center gap-2 py-16 text-muted-foreground">
            <Loader2 className="h-5 w-5 animate-spin" />
            Loading users…
          </div>
        ) : visibleRows.length === 0 ? (
          <p className="py-16 text-center text-sm text-muted-foreground">
            No users match the current tab and filters.
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full min-w-[720px] table-fixed border-collapse text-sm">
              <thead>
                <tr className="border-b border-border bg-muted/40 text-left text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                  <th className="w-[18%] px-2 py-2.5 sm:px-3">Name</th>
                  <th className="w-[22%] px-2 py-2.5 sm:px-3">Email</th>
                  <th className="w-[20%] px-2 py-2.5 sm:px-3">Organization</th>
                  <th className="w-[18%] px-2 py-2.5 sm:px-3">Role</th>
                  <th className="w-[14%] px-2 py-2.5 sm:px-3">Created</th>
                  <th className="w-[8%] px-2 py-2.5 text-right sm:px-3">Actions</th>
                </tr>
              </thead>
              <tbody>
                {visibleRows.map((row) => (
                  <tr key={row.id} className="border-b border-border last:border-0">
                    <td className="px-2 py-2 align-middle sm:px-3">
                      <div className="flex min-w-0 items-center gap-2">
                        <UserProfileAvatar name={row.full_name || row.email} photoUrl={row.photo_url} />
                        <span className="min-w-0 truncate font-medium" title={row.full_name || undefined}>
                          {row.full_name || "—"}
                        </span>
                      </div>
                    </td>
                    <td className="px-2 py-2 align-middle text-muted-foreground sm:px-3">
                      <span className="block truncate text-xs" title={row.email || undefined}>
                        {row.email || "—"}
                      </span>
                    </td>
                    <td className="px-2 py-2 align-middle text-muted-foreground sm:px-3">
                      <div
                        className="flex min-w-0 items-center gap-1.5"
                        title={
                          row.company_name
                            ? `${row.company_name}${row.organization_type === "internal" ? " (Internal)" : row.organization_type === "tenant" ? " (Tenant)" : ""}`
                            : undefined
                        }
                      >
                        <span className="min-w-0 truncate text-xs">
                          {row.company_name ?? "—"}
                        </span>
                        {row.organization_type === "internal" || row.organization_type === "tenant" ? (
                          <OrganizationTypeBadge type={row.organization_type} />
                        ) : null}
                      </div>
                    </td>
                    <td
                      className="px-2 py-2 align-middle sm:px-3"
                      title={
                        row.role
                          ? `${row.role_display_name?.trim() || humanizeRoleKey(row.role)} (${row.role})`
                          : undefined
                      }
                    >
                      <span className="block truncate font-mono text-[11px] text-foreground">
                        {row.role ?? "—"}
                      </span>
                      {row.role_display_name?.trim() ? (
                        <span className="mt-0.5 block truncate text-[10px] text-muted-foreground">
                          {row.role_display_name.trim()}
                        </span>
                      ) : null}
                    </td>
                    <td className="px-2 py-2 align-middle text-xs text-muted-foreground sm:px-3">
                      {formatCreatedAt(row.created_at)}
                    </td>
                    <td className="px-1 py-2 align-middle text-right sm:px-2">
                      <button
                        type="button"
                        className="rounded-md p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground"
                        onClick={() => openEdit(row)}
                        aria-label="Edit"
                        title="Edit user"
                      >
                        <Pencil className="h-4 w-4" />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {modalOpen && (
        <div className="fixed inset-0 z-[90] flex items-center justify-center bg-foreground/40 p-4 backdrop-blur-sm">
          <div
            role="dialog"
            aria-modal="true"
            aria-labelledby="platform-user-modal-title"
            className="max-h-[90vh] w-full max-w-md overflow-y-auto rounded-xl border border-border bg-card p-6 shadow-xl"
          >
            <div className="mb-4 flex items-center justify-between">
              <h2 id="platform-user-modal-title" className="text-lg font-bold">
                {isEditMode ? "Edit user" : "Add user"}
              </h2>
              <button type="button" onClick={closeModal} className="rounded-md p-1 hover:bg-accent" aria-label="Close">
                <X className="h-5 w-5" />
              </button>
            </div>
            <form onSubmit={(e) => void handleSubmit(e)} className="space-y-4">
              <div className="flex flex-col items-center gap-2">
                <UserProfileAvatar
                  name={fullName || email}
                  photoUrl={isEditMode ? editing!.photo_url : null}
                />
                {isEditMode ? (
                  <label className="cursor-pointer text-xs font-medium text-primary underline">
                    <input
                      type="file"
                      accept="image/*"
                      className="hidden"
                      disabled={photoUploading}
                      onChange={(e) => void handlePhotoChange(e, editing!.id)}
                    />
                    {photoUploading ? "Uploading…" : "Change profile photo"}
                  </label>
                ) : (
                  <label className="cursor-pointer text-xs font-medium text-muted-foreground hover:text-foreground">
                    <input
                      type="file"
                      accept="image/*"
                      className="hidden"
                      disabled={photoUploading}
                      onChange={(e) => {
                        const f = e.target.files?.[0] ?? null;
                        setPendingPhoto(f);
                        e.target.value = "";
                      }}
                    />
                    {pendingPhoto ? pendingPhoto.name : "Optional profile photo (uploads after create)"}
                  </label>
                )}
              </div>
              <div>
                <label className={LABEL} htmlFor="pu-fullName">Full name</label>
                <input
                  id="pu-fullName"
                  className={INPUT}
                  value={fullName}
                  onChange={(e) => setFullName(e.target.value)}
                  required
                  autoComplete="name"
                />
              </div>
              <div>
                <label className={LABEL} htmlFor="pu-email">Email</label>
                <input
                  id="pu-email"
                  type="email"
                  className={INPUT}
                  value={email}
                  readOnly={isEditMode}
                  disabled={isEditMode}
                  onChange={(e) => setEmail(e.target.value)}
                  required={!isEditMode}
                  autoComplete="email"
                />
                <p className="mt-1 text-xs text-muted-foreground">
                  {isEditMode
                    ? "Email is fixed after the user is created."
                    : "Used as the unique login identifier."}
                </p>
              </div>
              <div>
                <label className={LABEL} htmlFor="pu-company">
                  Organization <span className="text-destructive">*</span>
                </label>
                <select
                  id="pu-company"
                  className={INPUT}
                  value={editCompanyId}
                  onChange={(e) => {
                    const v = e.target.value;
                    setEditCompanyId(v);
                    if (isEditMode) syncOrgTypeFromCompanyPicker(v, editing);
                  }}
                  required
                >
                  {(isEditMode ? companyOptionsForEdit(editing) : companiesForCreateMode).length === 0 ? (
                    <option value="">
                      {companies.length === 0 ? "Loading organizations…" : "No organizations for this type"}
                    </option>
                  ) : (
                    (isEditMode ? companyOptionsForEdit(editing) : companiesForCreateMode).map((c) => (
                      <option key={c.id} value={c.id}>
                        {c.display_name}
                      </option>
                    ))
                  )}
                </select>
                <p className="mt-1 text-xs text-muted-foreground">
                  Updates <code className="rounded bg-muted px-1 text-[11px]">profiles.organization_id</code>.
                </p>
              </div>
              <div>
                <label className={LABEL} htmlFor="pu-org-type">
                  Organization Type
                </label>
                <select
                  id="pu-org-type"
                  className={INPUT}
                  value={editOrganizationType}
                  onChange={(e) =>
                    setEditOrganizationType(
                      e.target.value === "internal" ? "internal" : "tenant",
                    )
                  }
                >
                  <option value="tenant">Tenant (Customer company)</option>
                  <option value="internal">Internal (Platform company)</option>
                </select>
                <p className="mt-1 text-xs text-muted-foreground">
                  Saves to <code className="rounded bg-muted px-1 text-[11px]">organizations.type</code> for the
                  selected organization (affects every user in that org).
                </p>
              </div>
              <div>
                <label className={LABEL} htmlFor="pu-role">Role</label>
                <select
                  id="pu-role"
                  className={INPUT}
                  value={userRole}
                  onChange={(e) => setUserRole(e.target.value)}
                  disabled={assignableRoles.length === 0}
                >
                  {assignableRoles.length === 0 ? (
                    <option value="">Loading roles…</option>
                  ) : (
                    <>
                      <optgroup label="Company (tenant)">
                        {assignableRoles
                          .filter((r) => r.scope === "tenant")
                          .map((r) => (
                            <option key={r.id} value={r.key}>
                              {r.name} ({r.key})
                            </option>
                          ))}
                      </optgroup>
                      <optgroup label="Internal (system)">
                        {assignableRoles
                          .filter((r) => r.scope === "system")
                          .map((r) => (
                            <option key={r.id} value={r.key}>
                              {r.name} ({r.key})
                            </option>
                          ))}
                      </optgroup>
                    </>
                  )}
                </select>
                <p className="mt-1 text-xs text-muted-foreground">
                  {isEditMode
                    ? "Pick a role and use Save changes."
                    : "Pick a role for the new user; it is stored when you create the user. Groups below apply after create."}
                </p>
                {perms.canSeePlatformAccess ? (
                  <p className="mt-1 text-xs text-muted-foreground">
                    <Link href="/platform/access" className="font-medium text-primary underline hover:text-primary/90">
                      Access management
                    </Link>
                    {" — permissions, roles, and groups."}
                  </p>
                ) : null}
              </div>
              {effectiveOrgForGroups ? (
                <div className="rounded-lg border border-border bg-muted/15 p-3">
                  <div className="mb-2 flex items-center gap-2 text-sm font-medium">
                    <Tag className="h-3.5 w-3.5 text-muted-foreground" />
                    Groups
                  </div>
                  {isEditMode && orgDirty ? (
                    <p className="text-xs text-muted-foreground">
                      Save organization changes first — group membership is scoped to the user&apos;s saved company.
                    </p>
                  ) : groupsEditLoading ? (
                    <p className="flex items-center gap-2 text-xs text-muted-foreground">
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      Loading groups…
                    </p>
                  ) : (
                    <>
                      <div className="mb-2 flex flex-wrap gap-1">
                        {assignedGroupsForModal.length === 0 ? (
                          <span className="text-xs text-muted-foreground">No groups assigned.</span>
                        ) : (
                          assignedGroupsForModal.map((g) => (
                            <span
                              key={g.user_group_id}
                              className="inline-flex items-center gap-1 rounded-md border border-border bg-background px-2 py-0.5 text-xs"
                            >
                              <span className="max-w-[160px] truncate" title={g.key}>{g.name}</span>
                              <button
                                type="button"
                                className="rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
                                disabled={groupBusy}
                                aria-label={`Remove ${g.name}`}
                                onClick={() => void handleRemoveGroup(g.group_id)}
                              >
                                <X className="h-3 w-3" />
                              </button>
                            </span>
                          ))
                        )}
                      </div>
                      <div className="min-w-0">
                        <label className={LABEL} htmlFor="pu-addGroup">Add group</label>
                        <select
                          id="pu-addGroup"
                          className={INPUT}
                          value={groupPickId}
                          onChange={(e) => {
                            const v = e.target.value;
                            setGroupPickId(v);
                            if (!v.trim()) return;
                            if ((isEditMode && orgDirty) || groupBusy || groupsForEdit.length === 0) {
                              return;
                            }
                            void handleAddGroup(v);
                          }}
                          disabled={
                            (isEditMode && orgDirty)
                            || groupBusy
                            || groupsForEdit.length === 0
                          }
                        >
                          <option value="">
                            {groupsForEdit.length === 0 ? "No groups in this org" : "Select a group to assign…"}
                          </option>
                          {groupsForEdit
                            .filter((g) => !assignedGroupsForModal.some((a) => a.group_id === g.id))
                            .map((g) => (
                              <option key={g.id} value={g.id}>
                                {g.name} ({g.key})
                              </option>
                            ))}
                        </select>
                      </div>
                      {!isEditMode ? (
                        <p className="mt-1 text-[11px] text-muted-foreground">
                          Staged groups are written when you click Create user.
                        </p>
                      ) : null}
                    </>
                  )}
                </div>
              ) : null}
              <div className="flex flex-wrap items-center justify-between gap-2 pt-2">
                <div className="min-w-0">
                  {isEditMode ? (
                    <button
                      type="button"
                      className="inline-flex items-center gap-2 rounded-md border border-destructive/50 bg-background px-3 py-2 text-sm font-medium text-destructive hover:bg-destructive/10 disabled:opacity-50"
                      disabled={saving || deleting}
                      onClick={() => void handleDeleteUser()}
                    >
                      {deleting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                      Delete user
                    </button>
                  ) : null}
                </div>
                <div className="flex flex-wrap justify-end gap-2">
                  <button type="button" className={BTN_SECONDARY} onClick={closeModal}>
                    Cancel
                  </button>
                  <button type="submit" className={BTN_PRIMARY} disabled={saving || (!isEditMode && !editCompanyId.trim())}>
                    {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                    {isEditMode ? "Save changes" : "Create user"}
                  </button>
                </div>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
