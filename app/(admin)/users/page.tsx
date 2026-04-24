"use client";

import React, { useCallback, useEffect, useRef, useState } from "react";
import Link from "next/link";
import {
  ArrowLeft, KeyRound, Loader2, Pencil, Plus, Save, Tag, Trash2, UserRound, X,
} from "lucide-react";
import { isAdminRole, useUserRole } from "../../../components/UserRoleContext";
import { useRbacPermissions } from "../../../hooks/useRbacPermissions";
import { UserProfileAvatar } from "./UserProfileAvatar";
import {
  assignUserGroup,
  createUserProfile,
  deleteUserProfile,
  getTenantCompanyIdForUsersPage,
  listAssignableRolesForUsers,
  listGroupsForOrganization,
  listUserGroupAssignmentsForProfiles,
  listUserProfiles,
  removeUserGroup,
  updateUserProfile,
  type AssignableRoleRow,
} from "./users-actions";
import type { OrgGroupRow, ProfileRow, UserGroupAssignment } from "./users-types";
import type { CompanyOption } from "../../../lib/imports-types";
import { listCompaniesForImports } from "../imports/companies-actions";
import { uploadUserProfilePhotoAction } from "./upload-profile-photo-action";

const INPUT =
  "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50";
const LABEL = "mb-2 block text-sm font-medium leading-none";
const BTN_PRIMARY =
  "inline-flex items-center justify-center gap-2 rounded-md bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground shadow transition hover:bg-primary/90 disabled:opacity-50";
const BTN_SECONDARY =
  "inline-flex items-center justify-center gap-2 rounded-md border border-border bg-background px-3 py-2 text-sm font-medium transition hover:bg-accent";

type Toast = { msg: string; ok: boolean } | null;
const MIN_RESET_PASSWORD_LENGTH = 8;

function humanizeRoleKey(k: string | null | undefined): string {
  const s = (k ?? "").trim().toLowerCase();
  if (!s) return "—";
  return s
    .split("_")
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

/** Map stored / legacy display keys to an assignable catalog key for the role dropdown. */
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

export default function UsersPage() {
  const { role, actorUserId, organizationId } = useUserRole();
  const perms = useRbacPermissions();
  const [rows, setRows] = useState<ProfileRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [toast, setToast] = useState<Toast>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [editing, setEditing] = useState<ProfileRow | null>(null);
  const [saving, setSaving] = useState(false);
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [userRole, setUserRole] = useState<string>("employee");
  const [assignableRoles, setAssignableRoles] = useState<AssignableRoleRow[]>([]);

  useEffect(() => {
    if (!modalOpen || assignableRoles.length === 0) return;
    const keys = new Set(assignableRoles.map((r) => r.key));
    if (!keys.has(userRole)) {
      setUserRole(normalizeRolePickerValue(userRole, keys));
    }
  }, [modalOpen, assignableRoles, userRole]);
  const [photoUploading, setPhotoUploading] = useState(false);
  const [pendingPhoto, setPendingPhoto] = useState<File | null>(null);
  const [companies, setCompanies] = useState<CompanyOption[]>([]);
  const [tenantDefaultCompanyId, setTenantDefaultCompanyId] = useState("");
  const [createCompanyId, setCreateCompanyId] = useState("");
  const [resetTarget, setResetTarget] = useState<ProfileRow | null>(null);
  const [resetOpen, setResetOpen] = useState(false);
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [resettingPassword, setResettingPassword] = useState(false);
  const [groupsForEdit, setGroupsForEdit] = useState<OrgGroupRow[]>([]);
  const [groupsEditLoading, setGroupsEditLoading] = useState(false);
  const [groupPickId, setGroupPickId] = useState("");
  const [groupBusy, setGroupBusy] = useState(false);
  const [roleApplyBusy, setRoleApplyBusy] = useState(false);
  /** Group memberships to apply after create (same org as {@link createCompanyId}). */
  const [pendingCreateGroups, setPendingCreateGroups] = useState<
    Pick<UserGroupAssignment, "group_id" | "key" | "name">[]
  >([]);
  const editLoadSeq = useRef(0);

  const isEditMode = Boolean(editing);
  const effectiveOrgForGroups = (
    editing ? (editing.organization_id ?? "").trim() : createCompanyId.trim()
  );

  const showToast = useCallback((msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 4200);
  }, []);

  const load = useCallback(async (): Promise<ProfileRow[] | null> => {
    setLoading(true);
    const res = await listUserProfiles({
      actorProfileId: actorUserId,
      filterOrganizationId: organizationId,
    });
    if (!res.ok) {
      setLoading(false);
      showToast(res.error, false);
      return null;
    }
    const ids = res.rows.map((r) => r.id);
    const assignRes = await listUserGroupAssignmentsForProfiles(ids, {
      actorProfileId: actorUserId,
      filterOrganizationId: organizationId,
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
    setLoading(false);
    return merged;
  }, [showToast, actorUserId, organizationId]);

  useEffect(() => {
    if (isAdminRole(role)) void load();
  }, [role, organizationId, load]);

  useEffect(() => {
    if (!isAdminRole(role)) return;
    let cancelled = false;
    void (async () => {
      const [tid, coRes, rolesRes] = await Promise.all([
        getTenantCompanyIdForUsersPage(),
        listCompaniesForImports(actorUserId),
        listAssignableRolesForUsers(),
      ]);
      if (cancelled) return;
      setTenantDefaultCompanyId(tid);
      if (rolesRes.ok) {
        setAssignableRoles(rolesRes.rows);
      }
      if (coRes.ok) {
        setCompanies(coRes.rows);
        const pick = coRes.rows.some((c) => c.id === tid) ? tid : coRes.rows[0]?.id ?? "";
        setCreateCompanyId(pick);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [role, actorUserId]);

  function openCreate() {
    setEditing(null);
    setFullName("");
    setEmail("");
    const tenantFirst = assignableRoles.find((r) => r.scope === "tenant");
    setUserRole(tenantFirst?.key ?? assignableRoles[0]?.key ?? "employee");
    setPendingPhoto(null);
    const pick =
      companies.some((c) => c.id === tenantDefaultCompanyId) ? tenantDefaultCompanyId : companies[0]?.id ?? "";
    setCreateCompanyId(pick);
    setPendingCreateGroups([]);
    setModalOpen(true);
  }

  function openEdit(row: ProfileRow) {
    setEditing(row);
    setFullName(row.full_name ?? "");
    setEmail(row.email);
    const keys = new Set(assignableRoles.map((r) => r.key));
    setUserRole(normalizeRolePickerValue(row.role, keys));
    setGroupPickId("");
    setGroupsForEdit([]);
    setModalOpen(true);
  }

  function closeModal() {
    setModalOpen(false);
    setEditing(null);
    setGroupsForEdit([]);
    setGroupPickId("");
    setGroupsEditLoading(false);
    setPendingCreateGroups([]);
  }

  useEffect(() => {
    if (!modalOpen || editing) return;
    setPendingCreateGroups([]);
  }, [createCompanyId, modalOpen, editing]);

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
        filterOrganizationId: organizationId,
      });
      if (seq !== editLoadSeq.current) return;
      setGroupsEditLoading(false);
      if (g.ok) {
        setGroupsForEdit(g.rows);
      } else {
        showToast(g.error, false);
      }
    })();
  }, [modalOpen, editing?.id, effectiveOrgForGroups, actorUserId, organizationId, showToast]);

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

  function openResetPassword(row: ProfileRow) {
    setResetTarget(row);
    setNewPassword("");
    setConfirmPassword("");
    setResetOpen(true);
  }

  function closeResetPassword() {
    setResetOpen(false);
    setResetTarget(null);
    setNewPassword("");
    setConfirmPassword("");
    setResettingPassword(false);
  }

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
      const merged = await load();
      if (merged) {
        setEditing((e) => {
          if (!e || e.id !== profileId) return e;
          return merged.find((r) => r.id === profileId) ?? e;
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
    setSaving(true);
    try {
      if (editing) {
        const res = await updateUserProfile(editing.id, {
          full_name: fullName,
          role: userRole,
        });
        if (!res.ok) throw new Error(res.error ?? "Save failed.");
        showToast("User updated.", true);
      } else {
        if (!createCompanyId.trim()) {
          throw new Error("Select a company.");
        }
        const res = await createUserProfile({
          full_name: fullName,
          email,
          role: userRole,
          organization_id: createCompanyId.trim(),
        });
        if (!res.ok) throw new Error(res.error ?? "Create failed.");
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
            filterOrganizationId: organizationId,
          });
          if (!ag.ok) throw new Error(ag.error ?? "Failed to assign group.");
        }
        setPendingPhoto(null);
        setPendingCreateGroups([]);
        showToast("User created.", true);
      }
      closeModal();
      await load();
    } catch (err) {
      showToast(err instanceof Error ? err.message : "Save failed.", false);
    } finally {
      setSaving(false);
    }
  }

  async function handleDelete(id: string) {
    if (!window.confirm("Remove this user from the directory?")) return;
    const res = await deleteUserProfile(id);
    if (!res.ok) {
      showToast(res.error ?? "Delete failed.", false);
      return;
    }
    showToast("User removed.", true);
    await load();
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
        filterOrganizationId: organizationId,
      });
      if (!res.ok) throw new Error(res.error);
      showToast("Group added.", true);
      const merged = await load();
      setGroupPickId("");
      if (merged) {
        setEditing((e) => {
          if (!e || e.id !== profileId) return e;
          return merged.find((r) => r.id === profileId) ?? e;
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
        filterOrganizationId: organizationId,
      });
      if (!res.ok) throw new Error(res.error);
      showToast("Group removed.", true);
      const merged = await load();
      if (merged) {
        setEditing((e) => {
          if (!e || e.id !== profileId) return e;
          return merged.find((r) => r.id === profileId) ?? e;
        });
      }
    } catch (err) {
      showToast(err instanceof Error ? err.message : "Remove failed.", false);
    } finally {
      setGroupBusy(false);
    }
  }

  async function handleResetPasswordSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!resetTarget) return;

    if (newPassword !== confirmPassword) {
      showToast("New password and confirm password must match.", false);
      return;
    }
    if (newPassword.length < MIN_RESET_PASSWORD_LENGTH) {
      showToast(`Password must be at least ${MIN_RESET_PASSWORD_LENGTH} characters.`, false);
      return;
    }

    setResettingPassword(true);
    try {
      const response = await fetch("/api/admin/reset-password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId: resetTarget.id,
          newPassword,
        }),
      });
      const payload = (await response.json().catch(() => null)) as { error?: string } | null;
      if (!response.ok) {
        throw new Error(payload?.error || "Password reset failed.");
      }
      showToast(`Password reset for ${resetTarget.email || "user"}.`, true);
      closeResetPassword();
    } catch (err) {
      showToast(err instanceof Error ? err.message : "Password reset failed.", false);
    } finally {
      setResettingPassword(false);
    }
  }

  if (!isAdminRole(role)) {
    return (
      <div className="mx-auto max-w-lg px-4 py-16 text-center">
        <UserRound className="mx-auto mb-4 h-12 w-12 text-muted-foreground" />
        <h1 className="text-lg font-bold">Users</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          You need an organization <strong>Admin</strong> role (or internal staff with user management) to open this directory.
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
        href="/"
        className="mb-6 inline-flex items-center gap-2 text-sm font-medium text-muted-foreground transition hover:text-foreground"
      >
        <ArrowLeft className="h-4 w-4" />
        Back
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

      <div className="mb-8 flex flex-wrap items-end justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-violet-100 dark:bg-violet-950/50">
            <UserRound className="h-6 w-6 text-violet-600 dark:text-violet-400" />
          </div>
          <div>
            <h1 className="text-2xl font-bold tracking-tight">Users</h1>
            <p className="text-sm text-muted-foreground">
              Directory: roles, groups, and admin password resets.
            </p>
          </div>
        </div>
        <button type="button" onClick={openCreate} className={BTN_PRIMARY}>
          <Plus className="h-4 w-4" />
          Add user
        </button>
      </div>

      <div className="rounded-xl border border-border bg-card shadow-sm">
        {loading ? (
          <div className="flex items-center justify-center gap-2 py-16 text-muted-foreground">
            <Loader2 className="h-5 w-5 animate-spin" />
            Loading users…
          </div>
        ) : rows.length === 0 ? (
          <p className="py-16 text-center text-sm text-muted-foreground">
            No users yet. Add someone to get started.
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full min-w-[640px] table-fixed border-collapse text-sm">
              <thead>
                <tr className="border-b border-border bg-muted/40 text-left text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                  <th className="w-[22%] px-2 py-2.5 sm:px-3">User</th>
                  <th className="w-[26%] px-2 py-2.5 sm:px-3">Email</th>
                  <th className="w-[14%] px-2 py-2.5 sm:px-3">Company</th>
                  <th className="w-[18%] px-2 py-2.5 sm:px-3">Role</th>
                  <th className="w-[12%] px-2 py-2.5 sm:px-3">Groups</th>
                  <th className="w-[8%] px-2 py-2.5 text-right sm:px-3">Actions</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
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
                      <span className="block truncate text-xs" title={row.company_name ?? undefined}>
                        {row.company_name ?? "—"}
                      </span>
                    </td>
                    <td
                      className="px-2 py-2 align-middle sm:px-3"
                      title={row.role ? `${row.role_display_name?.trim() || humanizeRoleKey(row.role)} (${row.role})` : undefined}
                    >
                      <div className="min-w-0">
                        <span className="block truncate text-xs font-medium text-foreground">
                          {row.role_display_name?.trim() || humanizeRoleKey(row.role)}
                        </span>
                        {row.role_scope ? (
                          <span
                            className={[
                              "mt-0.5 inline-block max-w-full truncate rounded px-1 py-0.5 text-[9px] font-semibold uppercase tracking-wide",
                              row.role_scope === "system"
                                ? "bg-amber-100 text-amber-900 dark:bg-amber-950/60 dark:text-amber-100"
                                : "bg-sky-100 text-sky-900 dark:bg-sky-950/60 dark:text-sky-100",
                            ].join(" ")}
                          >
                            {row.role_scope}
                          </span>
                        ) : null}
                      </div>
                    </td>
                    <td className="px-2 py-2 align-middle sm:px-3">
                      {row.organization_id ? (
                        <div className="flex min-w-0 flex-wrap gap-0.5">
                          {(row.assigned_groups ?? []).length === 0 ? (
                            <span className="text-muted-foreground">—</span>
                          ) : (
                            <>
                              {(row.assigned_groups ?? []).slice(0, 2).map((g) => (
                                <span
                                  key={g.user_group_id}
                                  className="inline-flex max-w-[5.5rem] items-center truncate rounded bg-muted px-1 py-0.5 text-[10px] text-foreground"
                                  title={`${g.name} (${g.key})`}
                                >
                                  {g.name}
                                </span>
                              ))}
                              {(row.assigned_groups ?? []).length > 2 ? (
                                <span className="text-[10px] text-muted-foreground">
                                  +{(row.assigned_groups ?? []).length - 2}
                                </span>
                              ) : null}
                            </>
                          )}
                        </div>
                      ) : (
                        <span className="text-muted-foreground">—</span>
                      )}
                    </td>
                    <td className="px-1 py-2 align-middle text-right sm:px-2">
                      <div className="flex justify-end gap-0">
                        <button
                          type="button"
                          className="rounded-md p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground"
                          onClick={() => openResetPassword(row)}
                          aria-label="Reset password"
                          title="Reset password"
                        >
                          <KeyRound className="h-4 w-4" />
                        </button>
                        <button
                          type="button"
                          className="rounded-md p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground"
                          onClick={() => openEdit(row)}
                          aria-label="Edit"
                          title="Edit user"
                        >
                          <Pencil className="h-4 w-4" />
                        </button>
                        <button
                          type="button"
                          className="rounded-md p-1.5 text-rose-600 hover:bg-rose-50 dark:hover:bg-rose-950/30"
                          onClick={() => void handleDelete(row.id)}
                          aria-label="Delete"
                          title="Remove user"
                        >
                          <Trash2 className="h-4 w-4" />
                        </button>
                      </div>
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
            aria-labelledby="user-modal-title"
            className="w-full max-w-md rounded-xl border border-border bg-card p-6 shadow-xl"
          >
            <div className="mb-4 flex items-center justify-between">
              <h2 id="user-modal-title" className="text-lg font-bold">
                {editing ? "Edit user" : "Add user"}
              </h2>
              <button type="button" onClick={closeModal} className="rounded-md p-1 hover:bg-accent" aria-label="Close">
                <X className="h-5 w-5" />
              </button>
            </div>
            <form onSubmit={(e) => void handleSubmit(e)} className="space-y-4">
              {editing ? (
                <div className="flex flex-col items-center gap-2">
                  <UserProfileAvatar name={fullName || email} photoUrl={editing.photo_url} />
                  <label className="cursor-pointer text-xs font-medium text-primary underline">
                    <input
                      type="file"
                      accept="image/*"
                      className="hidden"
                      disabled={photoUploading}
                      onChange={(e) => void handlePhotoChange(e, editing.id)}
                    />
                    {photoUploading ? "Uploading…" : "Change profile photo"}
                  </label>
                </div>
              ) : (
                <div className="rounded-lg border border-dashed border-border bg-muted/20 px-3 py-2 text-center">
                  <label className="cursor-pointer text-xs font-medium text-muted-foreground hover:text-foreground">
                    <input
                      type="file"
                      accept="image/*"
                      className="hidden"
                      onChange={(e) => {
                        const f = e.target.files?.[0] ?? null;
                        setPendingPhoto(f);
                        e.target.value = "";
                      }}
                    />
                    {pendingPhoto ? pendingPhoto.name : "Optional profile photo (after create, uploads automatically)"}
                  </label>
                </div>
              )}
              <div>
                <label className={LABEL} htmlFor="fullName">Full name</label>
                <input
                  id="fullName"
                  className={INPUT}
                  value={fullName}
                  onChange={(e) => setFullName(e.target.value)}
                  required
                  autoComplete="name"
                />
              </div>
              <div>
                <label className={LABEL} htmlFor="email">Email</label>
                <input
                  id="email"
                  type="email"
                  className={INPUT}
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  disabled={!!editing}
                  required={!editing}
                  readOnly={!!editing}
                  autoComplete="email"
                />
                <p className="mt-1 text-xs text-muted-foreground">
                  {editing ? "Email is fixed after the user is created." : "Used as the unique login identifier for this workspace."}
                </p>
              </div>
              {!editing ? (
                <div>
                  <label className={LABEL} htmlFor="company">
                    Company <span className="text-destructive">*</span>
                  </label>
                  <select
                    id="company"
                    className={INPUT}
                    value={createCompanyId}
                    onChange={(e) => setCreateCompanyId(e.target.value)}
                    required
                  >
                    {companies.length === 0 ? (
                      <option value="">Loading companies…</option>
                    ) : (
                      companies.map((c) => (
                        <option key={c.id} value={c.id}>
                          {c.display_name}
                        </option>
                      ))
                    )}
                  </select>
                  <p className="mt-1 text-xs text-muted-foreground">
                    Saved to <code className="rounded bg-muted px-1 text-[11px]">profiles.organization_id</code>.
                  </p>
                </div>
              ) : null}
              <div>
                <label className={LABEL} htmlFor="role">Role</label>
                <select
                  id="role"
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
                  Tenant roles apply to customer organizations; system roles are for internal staff.
                  {editing
                    ? " Pick a role and use Save changes."
                    : " Pick a role for the new user; it is stored when you create the user. Groups below apply after create."}
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
                  {groupsEditLoading ? (
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
                        <label className={LABEL} htmlFor="addGroup">Add group</label>
                        <select
                          id="addGroup"
                          className={INPUT}
                          value={groupPickId}
                          onChange={(e) => {
                            const v = e.target.value;
                            setGroupPickId(v);
                            if (!v.trim() || groupBusy || groupsForEdit.length === 0) return;
                            void handleAddGroup(v);
                          }}
                          disabled={groupBusy || groupsForEdit.length === 0}
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
                      <p className="mt-1 text-[11px] text-muted-foreground">
                        {editing
                          ? "Groups are scoped to this user's company organization."
                          : "Staged groups are written when you click Create user."}
                      </p>
                    </>
                  )}
                </div>
              ) : editing ? (
                <p className="text-xs text-muted-foreground">
                  Groups require a company on the profile; this user has no organization set.
                </p>
              ) : null}
              <div className="flex justify-end gap-2 pt-2">
                <button type="button" className={BTN_SECONDARY} onClick={closeModal}>
                  Cancel
                </button>
                <button type="submit" className={BTN_PRIMARY} disabled={saving}>
                  {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                  {editing ? "Save changes" : "Create user"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {resetOpen && resetTarget && (
        <div className="fixed inset-0 z-[90] flex items-center justify-center bg-foreground/40 p-4 backdrop-blur-sm">
          <div
            role="dialog"
            aria-modal="true"
            aria-labelledby="reset-password-modal-title"
            className="w-full max-w-md rounded-xl border border-border bg-card p-6 shadow-xl"
          >
            <div className="mb-4 flex items-center justify-between">
              <h2 id="reset-password-modal-title" className="text-lg font-bold">
                Reset password
              </h2>
              <button
                type="button"
                onClick={closeResetPassword}
                className="rounded-md p-1 hover:bg-accent"
                aria-label="Close"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <p className="mb-4 text-sm text-muted-foreground">
              Set a new password for <span className="font-medium text-foreground">{resetTarget.email || resetTarget.id}</span>.
            </p>
            <form onSubmit={(e) => void handleResetPasswordSubmit(e)} className="space-y-4">
              <div>
                <label className={LABEL} htmlFor="newPassword">New password</label>
                <input
                  id="newPassword"
                  type="password"
                  className={INPUT}
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  minLength={MIN_RESET_PASSWORD_LENGTH}
                  required
                  autoComplete="new-password"
                />
              </div>
              <div>
                <label className={LABEL} htmlFor="confirmPassword">Confirm password</label>
                <input
                  id="confirmPassword"
                  type="password"
                  className={INPUT}
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  minLength={MIN_RESET_PASSWORD_LENGTH}
                  required
                  autoComplete="new-password"
                />
              </div>
              <p className="text-xs text-muted-foreground">
                Minimum length: {MIN_RESET_PASSWORD_LENGTH} characters.
              </p>
              <div className="flex justify-end gap-2 pt-2">
                <button type="button" className={BTN_SECONDARY} onClick={closeResetPassword}>
                  Cancel
                </button>
                <button type="submit" className={BTN_PRIMARY} disabled={resettingPassword}>
                  {resettingPassword ? <Loader2 className="h-4 w-4 animate-spin" /> : <KeyRound className="h-4 w-4" />}
                  Reset password
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
