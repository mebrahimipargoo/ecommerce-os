"use client";

import React, { useCallback, useEffect, useState } from "react";
import { ImageIcon, Loader2, Save } from "lucide-react";
import Link from "next/link";
import { useRbacPermissions } from "../../../hooks/useRbacPermissions";
import { useUserRole } from "../../../components/UserRoleContext";
import { usePlatformBranding } from "../../../components/PlatformBrandingContext";
import { BRAND_LOGO_IMG_CLASSNAME } from "../../../lib/brand-logo-classes";
import { savePlatformSettingsAction } from "./platform-settings-actions";
import { uploadPlatformLogoAction } from "./upload-platform-logo-action";

const DEBUG =
  typeof process !== "undefined" && process.env.NEXT_PUBLIC_BRANDING_DEBUG === "1";

export default function PlatformSettingsPage() {
  const perms = useRbacPermissions();
  const userRole = useUserRole();
  const { actorUserId, profileLoading, role, canonicalRoleKey } = userRole;

  useEffect(() => {
    if (!DEBUG) return;
    console.log("[platform-settings] rbac-state", {
      role,
      canonicalRoleKey,
      profileLoading,
      actorUserId,
      canSeePlatformAdmin: perms.canSeePlatformAdmin,
    });
  }, [role, canonicalRoleKey, profileLoading, actorUserId, perms.canSeePlatformAdmin]);
  const { refresh: refreshPlatformBranding, platformAppName, platformLogoUrl } = usePlatformBranding();

  const [appName, setAppName] = useState("");
  const [logoUrl, setLogoUrl] = useState("");
  const [saving, setSaving] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setAppName(platformAppName);
    setLogoUrl(platformLogoUrl);
  }, [platformAppName, platformLogoUrl]);

  const onSave = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault();
      setSaving(true);
      setError(null);
      setMessage(null);
      const res = await savePlatformSettingsAction({
        actorProfileId: actorUserId,
        app_name: appName.trim(),
        logo_url: logoUrl.trim() || null,
      });
      setSaving(false);
      if (!res.ok) {
        setError(res.error);
        return;
      }
      setMessage("Platform branding saved.");
      void refreshPlatformBranding();
    },
    [actorUserId, appName, logoUrl, refreshPlatformBranding],
  );

  async function onLogoFile(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file) return;
    setUploading(true);
    setError(null);
    setMessage(null);
    try {
      const fd = new FormData();
      fd.append("file", file);
      if (actorUserId) fd.append("actor_profile_id", actorUserId);
      const res = await uploadPlatformLogoAction(fd);
      if (!res.ok) throw new Error(res.error ?? "Upload failed.");
      setLogoUrl(res.publicUrl);
      setMessage("Logo uploaded and saved to platform settings.");
      void refreshPlatformBranding();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Upload failed.");
    } finally {
      setUploading(false);
    }
  }

  if (profileLoading) {
    return (
      <div className="mx-auto flex max-w-lg items-center gap-3 p-12 text-sm text-slate-600 dark:text-slate-400">
        <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
        Loading your session…
      </div>
    );
  }

  if (!perms.canSeePlatformAdmin) {
    return (
      <div className="mx-auto max-w-lg p-8">
        <h1 className="text-lg font-semibold text-slate-800 dark:text-slate-100">Platform settings</h1>
        <p className="mt-2 text-sm text-slate-600 dark:text-slate-400">
          This page is only available to super administrators.
        </p>
        <Link href="/settings" className="mt-4 inline-block text-sm font-medium text-violet-600 hover:underline dark:text-violet-400">
          ← Back to Settings
        </Link>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-xl space-y-6 p-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight text-slate-900 dark:text-slate-50">Platform branding</h1>
        <p className="mt-1 text-sm text-slate-600 dark:text-slate-400">
          Product name and logo for the app shell (sidebar and mobile header). Stored in{" "}
          <code className="rounded bg-slate-100 px-1 text-xs dark:bg-slate-800">platform_settings</code>
          {" "}— separate from tenant white-label in{" "}
          <code className="rounded bg-slate-100 px-1 text-xs dark:bg-slate-800">organization_settings</code>.
        </p>
        <Link
          href="/settings"
          className="mt-3 inline-block text-xs font-medium text-violet-600 hover:underline dark:text-violet-400"
        >
          ← Back to Settings
        </Link>
      </div>

      {error ? (
        <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900/50 dark:bg-red-950/40 dark:text-red-200">
          {error}
        </div>
      ) : null}
      {message ? (
        <div className="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-900 dark:border-emerald-900/40 dark:bg-emerald-950/30 dark:text-emerald-100">
          {message}
        </div>
      ) : null}

      <form onSubmit={onSave} className="space-y-5 rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-800 dark:bg-slate-950">
        <div>
          <label htmlFor="platform-app-name" className="mb-1.5 block text-sm font-medium text-slate-800 dark:text-slate-200">
            Platform app name
          </label>
          <input
            id="platform-app-name"
            type="text"
            value={appName}
            onChange={(ev) => setAppName(ev.target.value)}
            className="h-10 w-full rounded-lg border border-slate-200 bg-white px-3 text-sm dark:border-slate-700 dark:bg-slate-900"
            required
            autoComplete="off"
          />
        </div>

        <div>
          <label htmlFor="platform-logo-url" className="mb-1.5 block text-sm font-medium text-slate-800 dark:text-slate-200">
            Logo URL
          </label>
          <input
            id="platform-logo-url"
            type="url"
            value={logoUrl}
            onChange={(ev) => setLogoUrl(ev.target.value)}
            placeholder="https://… or upload below"
            className="h-10 w-full rounded-lg border border-slate-200 bg-white px-3 text-sm dark:border-slate-700 dark:bg-slate-900"
          />
          <p className="mt-1 text-xs text-slate-500">Leave empty to use the two-letter monogram from the app name.</p>
        </div>

        <div>
          <label className="mb-1.5 flex items-center gap-2 text-sm font-medium text-slate-800 dark:text-slate-200">
            <ImageIcon className="h-4 w-4" />
            Upload logo file
          </label>
          <label
            className={`flex w-full cursor-pointer items-center justify-center gap-2 rounded-lg border-2 border-dashed border-slate-200 py-3 text-sm dark:border-slate-700 ${uploading ? "text-slate-400" : "hover:bg-slate-50 dark:hover:bg-slate-900"}`}
          >
            {uploading ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                Uploading…
              </>
            ) : (
              <>
                <ImageIcon className="h-4 w-4" />
                Choose image…
              </>
            )}
            <input type="file" accept="image/*" className="hidden" disabled={uploading} onChange={onLogoFile} />
          </label>
        </div>

        {logoUrl ? (
          <div className="flex items-center gap-3 rounded-lg border border-slate-100 bg-slate-50 px-3 py-2 dark:border-slate-800 dark:bg-slate-900/50">
            <div className="flex h-12 w-12 shrink-0 items-center justify-center overflow-hidden rounded-lg border border-slate-200 bg-white dark:border-slate-700">
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img src={logoUrl} alt="" className={["max-h-full max-w-full object-contain", BRAND_LOGO_IMG_CLASSNAME].join(" ")} />
            </div>
            <button
              type="button"
              className="text-xs text-rose-600 underline"
              onClick={() => setLogoUrl("")}
            >
              Clear preview
            </button>
          </div>
        ) : null}

        <button
          type="submit"
          disabled={saving}
          className="inline-flex items-center gap-2 rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-700 disabled:opacity-50"
        >
          {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
          Save
        </button>
      </form>
    </div>
  );
}
