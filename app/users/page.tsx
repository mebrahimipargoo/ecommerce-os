"use client";

import React, { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import {
  ArrowLeft, ImageIcon, Loader2, Pencil, Plus, Save, Trash2, UserRound, X,
} from "lucide-react";
import { isAdminRole, useUserRole } from "../../components/UserRoleContext";
import { UserProfileAvatar } from "./UserProfileAvatar";
import {
  createUserProfile,
  deleteUserProfile,
  listUserProfiles,
  updateUserProfile,
  type ProfileRow,
} from "./users-actions";
import { uploadUserProfilePhotoAction } from "./upload-profile-photo-action";

const INPUT =
  "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50";
const LABEL = "mb-2 block text-sm font-medium leading-none";
const BTN_PRIMARY =
  "inline-flex items-center justify-center gap-2 rounded-md bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground shadow transition hover:bg-primary/90 disabled:opacity-50";
const BTN_SECONDARY =
  "inline-flex items-center justify-center gap-2 rounded-md border border-border bg-background px-3 py-2 text-sm font-medium transition hover:bg-accent";

type Toast = { msg: string; ok: boolean } | null;

export default function UsersPage() {
  const { role } = useUserRole();
  const [rows, setRows] = useState<ProfileRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [toast, setToast] = useState<Toast>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [editing, setEditing] = useState<ProfileRow | null>(null);
  const [saving, setSaving] = useState(false);
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [userRole, setUserRole] = useState<"admin" | "operator">("operator");
  const [photoUploading, setPhotoUploading] = useState(false);
  const [pendingPhoto, setPendingPhoto] = useState<File | null>(null);

  const showToast = useCallback((msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 4200);
  }, []);

  const load = useCallback(async () => {
    setLoading(true);
    const res = await listUserProfiles();
    setLoading(false);
    if (!res.ok) {
      showToast(res.error, false);
      return;
    }
    setRows(res.rows);
  }, [showToast]);

  useEffect(() => {
    if (isAdminRole(role)) void load();
  }, [role, load]);

  function openCreate() {
    setEditing(null);
    setFullName("");
    setEmail("");
    setUserRole("operator");
    setPendingPhoto(null);
    setModalOpen(true);
  }

  function openEdit(row: ProfileRow) {
    setEditing(row);
    setFullName(row.full_name);
    setEmail(row.email);
    setUserRole(row.role === "admin" ? "admin" : "operator");
    setModalOpen(true);
  }

  function closeModal() {
    setModalOpen(false);
    setEditing(null);
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
      await load();
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
        const res = await createUserProfile({
          full_name: fullName,
          email,
          role: userRole,
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
        setPendingPhoto(null);
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

  if (!isAdminRole(role)) {
    return (
      <div className="mx-auto max-w-lg px-4 py-16 text-center">
        <UserRound className="mx-auto mb-4 h-12 w-12 text-muted-foreground" />
        <h1 className="text-lg font-bold">Users</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          Switch to <strong>Admin</strong> or <strong>Super Admin</strong> in the header to manage users.
        </p>
        <Link href="/" className="mt-6 inline-block text-sm font-medium text-primary underline">
          Back to dashboard
        </Link>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-5xl px-4 py-8">
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
              Manage workspace profiles, roles, and photos.
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
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border bg-muted/40 text-left text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  <th className="px-4 py-3">User</th>
                  <th className="px-4 py-3">Email</th>
                  <th className="px-4 py-3">Role</th>
                  <th className="px-4 py-3 text-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr key={row.id} className="border-b border-border last:border-0">
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-3">
                        <UserProfileAvatar name={row.full_name || row.email} photoUrl={row.photo_url} />
                        <span className="font-medium">{row.full_name || "—"}</span>
                      </div>
                    </td>
                    <td className="px-4 py-3 text-muted-foreground">{row.email}</td>
                    <td className="px-4 py-3 capitalize">{row.role}</td>
                    <td className="px-4 py-3 text-right">
                      <div className="flex justify-end gap-1">
                        <label className="cursor-pointer rounded-md p-2 text-muted-foreground hover:bg-accent hover:text-foreground">
                          <input
                            type="file"
                            accept="image/*"
                            className="hidden"
                            disabled={photoUploading}
                            onChange={(e) => void handlePhotoChange(e, row.id)}
                          />
                          <span className="sr-only">Change photo</span>
                          <ImageIcon className="h-4 w-4" />
                        </label>
                        <button
                          type="button"
                          className="rounded-md p-2 text-muted-foreground hover:bg-accent hover:text-foreground"
                          onClick={() => openEdit(row)}
                          aria-label="Edit"
                        >
                          <Pencil className="h-4 w-4" />
                        </button>
                        <button
                          type="button"
                          className="rounded-md p-2 text-rose-600 hover:bg-rose-50 dark:hover:bg-rose-950/30"
                          onClick={() => void handleDelete(row.id)}
                          aria-label="Delete"
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
              <div>
                <label className={LABEL} htmlFor="role">Role</label>
                <select
                  id="role"
                  className={INPUT}
                  value={userRole}
                  onChange={(e) => setUserRole(e.target.value as "admin" | "operator")}
                >
                  <option value="operator">Operator</option>
                  <option value="admin">Admin</option>
                </select>
              </div>
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
    </div>
  );
}
