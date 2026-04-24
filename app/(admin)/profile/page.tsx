"use client";

import React, { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { ArrowLeft, KeyRound, Loader2, Save, UserRound } from "lucide-react";
import { supabase } from "@/src/lib/supabase";
import { useUserRole } from "../../../components/UserRoleContext";
import { UserProfileAvatar } from "../users/UserProfileAvatar";
import { uploadUserProfilePhotoAction } from "../users/upload-profile-photo-action";
import { updateOwnProfileFullName } from "./profile-actions";

const INPUT =
  "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50";
const LABEL = "mb-2 block text-sm font-medium leading-none";
const BTN_PRIMARY =
  "inline-flex items-center justify-center gap-2 rounded-md bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground shadow transition hover:bg-primary/90 disabled:opacity-50";

const MIN_PASSWORD = 8;

export default function ProfilePage() {
  const { actorUserId, actorName, refreshProfile, canonicalRoleLabel } = useUserRole();
  const [email, setEmail] = useState<string | null>(null);
  const [fullName, setFullName] = useState("");
  const [photoUrl, setPhotoUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [savingName, setSavingName] = useState(false);
  const [photoBusy, setPhotoBusy] = useState(false);
  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);

  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [pwBusy, setPwBusy] = useState(false);

  const showToast = useCallback((msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 4000);
  }, []);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const {
        data: { user },
      } = await supabase.auth.getUser();
      setEmail(user?.email ?? null);
      const uid = user?.id;
      if (!uid) {
        setFullName("");
        setPhotoUrl(null);
        return;
      }
      const { data, error } = await supabase
        .from("profiles")
        .select("full_name, photo_url")
        .eq("id", uid)
        .maybeSingle();
      if (error) throw new Error(error.message);
      const row = data as { full_name?: string | null; photo_url?: string | null } | null;
      setFullName(String(row?.full_name ?? "").trim() || actorName || "");
      setPhotoUrl(row?.photo_url ? String(row.photo_url) : null);
    } catch (e) {
      showToast(e instanceof Error ? e.message : "Could not load profile.", false);
    } finally {
      setLoading(false);
    }
  }, [actorName, showToast]);

  useEffect(() => {
    void load();
  }, [load]);

  async function onSaveName(e: React.FormEvent) {
    e.preventDefault();
    setSavingName(true);
    try {
      const res = await updateOwnProfileFullName(fullName);
      if (!res.ok) throw new Error(res.error);
      showToast("Name saved.", true);
      await refreshProfile();
    } catch (e) {
      showToast(e instanceof Error ? e.message : "Save failed.", false);
    } finally {
      setSavingName(false);
    }
  }

  async function onPhotoChange(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file || !actorUserId) return;
    setPhotoBusy(true);
    try {
      const fd = new FormData();
      fd.append("file", file);
      fd.append("profile_id", actorUserId);
      const res = await uploadUserProfilePhotoAction(fd);
      if (!res.ok) throw new Error(res.error);
      setPhotoUrl(res.publicUrl);
      showToast("Photo updated.", true);
      await refreshProfile();
    } catch (e) {
      showToast(e instanceof Error ? e.message : "Upload failed.", false);
    } finally {
      setPhotoBusy(false);
    }
  }

  async function onChangePassword(e: React.FormEvent) {
    e.preventDefault();
    if (newPassword !== confirmPassword) {
      showToast("Passwords do not match.", false);
      return;
    }
    if (newPassword.length < MIN_PASSWORD) {
      showToast(`Use at least ${MIN_PASSWORD} characters.`, false);
      return;
    }
    setPwBusy(true);
    try {
      const { error } = await supabase.auth.updateUser({ password: newPassword });
      if (error) throw new Error(error.message);
      setNewPassword("");
      setConfirmPassword("");
      showToast("Password updated.", true);
    } catch (e) {
      showToast(e instanceof Error ? e.message : "Password update failed.", false);
    } finally {
      setPwBusy(false);
    }
  }

  return (
    <div className="mx-auto w-full min-w-0 max-w-6xl px-4 py-8 sm:px-6 md:py-10 lg:px-10">
      <Link
        href="/"
        className="mb-6 inline-flex items-center gap-2 text-sm font-medium text-muted-foreground transition hover:text-foreground md:mb-8"
      >
        <ArrowLeft className="h-4 w-4" />
        Back
      </Link>

      {toast && (
        <div
          role="status"
          className={[
            "mb-6 rounded-lg border px-4 py-3 text-sm font-medium lg:mb-8",
            toast.ok
              ? "border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-700/50 dark:bg-emerald-950/90 dark:text-emerald-200"
              : "border-rose-200 bg-rose-50 text-rose-800 dark:border-rose-700/50 dark:bg-rose-950/90 dark:text-rose-200",
          ].join(" ")}
        >
          {toast.msg}
        </div>
      )}

      <div className="mb-8 flex items-center gap-4 md:mb-10">
        <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-2xl bg-sky-100 dark:bg-sky-950/50 md:h-14 md:w-14">
          <UserRound className="h-6 w-6 text-sky-600 dark:text-sky-400 md:h-7 md:w-7" />
        </div>
        <div className="min-w-0">
          <h1 className="text-2xl font-bold tracking-tight md:text-3xl">Profile</h1>
          <p className="mt-0.5 text-sm text-muted-foreground md:text-base">
            Your account — <span className="text-foreground">{canonicalRoleLabel}</span>
          </p>
        </div>
      </div>

      {loading ? (
        <div className="flex items-center gap-2 py-12 text-muted-foreground">
          <Loader2 className="h-5 w-5 animate-spin" />
          Loading…
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-8 lg:grid-cols-2 lg:items-start lg:gap-10">
          <section className="rounded-xl border border-border bg-card p-6 shadow-sm md:p-8">
            <h2 className="mb-6 text-base font-semibold text-foreground">Identity</h2>
            <div className="flex flex-col gap-8 lg:flex-row lg:gap-10">
              <div className="flex shrink-0 flex-col items-center gap-2 lg:w-44 lg:items-start">
                <UserProfileAvatar name={fullName || email || "User"} photoUrl={photoUrl} />
                <label className="cursor-pointer text-xs font-medium text-primary underline">
                  <input
                    type="file"
                    accept="image/*"
                    className="hidden"
                    disabled={photoBusy || !actorUserId}
                    onChange={(e) => void onPhotoChange(e)}
                  />
                  {photoBusy ? "Uploading…" : "Change photo"}
                </label>
              </div>
              <form onSubmit={(e) => void onSaveName(e)} className="min-w-0 flex-1 space-y-4 md:space-y-5">
                <div>
                  <label className={LABEL} htmlFor="profile-email">Email</label>
                  <input
                    id="profile-email"
                    type="email"
                    className={INPUT}
                    value={email ?? ""}
                    readOnly
                    disabled
                    autoComplete="email"
                  />
                  <p className="mt-1 text-xs text-muted-foreground">Email is managed by your administrator.</p>
                </div>
                <div>
                  <label className={LABEL} htmlFor="profile-name">Display name</label>
                  <input
                    id="profile-name"
                    className={INPUT}
                    value={fullName}
                    onChange={(e) => setFullName(e.target.value)}
                    required
                    autoComplete="name"
                  />
                </div>
                <button type="submit" className={BTN_PRIMARY} disabled={savingName}>
                  {savingName ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                  Save name
                </button>
              </form>
            </div>
          </section>

          <section className="rounded-xl border border-border bg-card p-6 shadow-sm md:p-8">
            <h2 className="mb-6 flex items-center gap-2 text-base font-semibold text-foreground">
              <KeyRound className="h-4 w-4 shrink-0" />
              Password
            </h2>
            <form onSubmit={(e) => void onChangePassword(e)} className="space-y-4 md:space-y-5">
              <div>
                <label className={LABEL} htmlFor="npw">New password</label>
                <input
                  id="npw"
                  type="password"
                  className={INPUT}
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  minLength={MIN_PASSWORD}
                  autoComplete="new-password"
                />
              </div>
              <div>
                <label className={LABEL} htmlFor="cpw">Confirm new password</label>
                <input
                  id="cpw"
                  type="password"
                  className={INPUT}
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  minLength={MIN_PASSWORD}
                  autoComplete="new-password"
                />
              </div>
              <p className="text-xs text-muted-foreground">Minimum {MIN_PASSWORD} characters.</p>
              <button type="submit" className={BTN_PRIMARY} disabled={pwBusy}>
                {pwBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : <KeyRound className="h-4 w-4" />}
                Update password
              </button>
            </form>
          </section>
        </div>
      )}
    </div>
  );
}
