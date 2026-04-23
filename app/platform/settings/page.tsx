"use client";

import React, { useEffect, useState } from "react";
import { ImageIcon, Loader2, Save } from "lucide-react";
import {
  getPlatformSettingsAction,
  savePlatformSettingsAction,
} from "./platform-settings-actions";
import { uploadPlatformLogoAction } from "./upload-platform-logo-action";
import {
  responsiveFormInput,
  responsivePageInner,
  responsivePageNarrow,
  responsivePageOuter,
} from "../../../lib/responsive-page-shell";
import { PageHeaderWithInfo } from "../components/page-header-with-info";

export default function PlatformSettingsPage() {
  const [loading, setLoading] = useState(true);
  const [accessDenied, setAccessDenied] = useState<"not_authenticated" | "forbidden" | null>(null);
  const [appName, setAppName] = useState("");
  const [logoUrl, setLogoUrl] = useState("");
  const [uploadingLogo, setUploadingLogo] = useState(false);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const res = await getPlatformSettingsAction();
      if (cancelled) return;
      setAppName(res.app_name);
      setLogoUrl(res.logo_url);
      setAccessDenied(res.accessDenied);
      setLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  async function onSave(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    setError(null);
    setMessage(null);
    const res = await savePlatformSettingsAction({
      app_name: appName.trim(),
      logo_url: logoUrl.trim() || null,
    });
    setSaving(false);
    if (!res.ok) {
      setError(res.error);
      return;
    }
    setMessage("Platform branding saved.");
  }

  async function onLogoFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file) return;
    setUploadingLogo(true);
    setError(null);
    setMessage(null);
    try {
      const formData = new FormData();
      formData.append("file", file);
      const res = await uploadPlatformLogoAction(formData);
      if (!res.ok) {
        setError(res.error);
        return;
      }
      setLogoUrl(res.publicUrl);
      setMessage("Logo uploaded.");
    } finally {
      setUploadingLogo(false);
    }
  }

  if (loading) {
    return (
      <div className={responsivePageOuter}>
        <div className={`${responsivePageInner} flex min-h-[40vh] items-center justify-center`}>
          <div className="flex items-center gap-3 text-sm text-muted-foreground">
            <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
            Loading platform settings...
          </div>
        </div>
      </div>
    );
  }

  if (accessDenied) {
    return (
      <div className={responsivePageOuter}>
        <div className={responsivePageNarrow}>
          <h1 className="text-lg font-semibold text-foreground">Platform settings</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            {accessDenied === "not_authenticated"
              ? "You must be signed in to view this page."
              : "Only super_admin can edit platform settings."}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className={responsivePageOuter}>
      <div className={`${responsivePageInner} space-y-6`}>
        <PageHeaderWithInfo
          title="Platform branding"
          titleClassName="text-2xl font-bold tracking-tight text-foreground sm:text-3xl"
          infoAriaLabel="About platform branding"
        >
          <p>
            Super-admin-only platform identity. This writes only to{" "}
            <code className="rounded-md bg-muted px-1.5 py-0.5 font-mono text-[11px]">
              platform_settings
            </code>
            .
          </p>
        </PageHeaderWithInfo>

        {error ? (
          <div className="rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
            {error}
          </div>
        ) : null}
        {message ? (
          <div className="rounded-xl border border-emerald-500/30 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-900 dark:text-emerald-100">
            {message}
          </div>
        ) : null}

        <form
          onSubmit={onSave}
          className="grid gap-6 rounded-2xl border border-border bg-card p-4 shadow-sm sm:p-6 lg:grid-cols-2 lg:gap-8 lg:p-8"
        >
          <div className="min-w-0 space-y-4">
            <div>
              <label
                htmlFor="platform-app-name"
                className="mb-1.5 block text-sm font-medium text-foreground"
              >
                Platform app name
              </label>
              <input
                id="platform-app-name"
                type="text"
                value={appName}
                onChange={(ev) => setAppName(ev.target.value)}
                className={responsiveFormInput}
                required
                autoComplete="off"
              />
            </div>
          </div>

          <div className="min-w-0 space-y-3">
            <span className="mb-1.5 block text-sm font-medium text-foreground">Platform logo</span>
            <label className="inline-flex cursor-pointer items-center gap-2 rounded-lg border border-border bg-muted/40 px-3 py-2 text-sm font-medium transition hover:bg-muted">
              {uploadingLogo ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Uploading...
                </>
              ) : (
                <>
                  <ImageIcon className="h-4 w-4" />
                  Upload logo file
                </>
              )}
              <input
                type="file"
                accept="image/png,image/jpeg,image/jpg,image/webp,image/gif,image/svg+xml"
                className="hidden"
                disabled={uploadingLogo}
                onChange={onLogoFileChange}
              />
            </label>
            <p className="text-xs text-muted-foreground">
              Upload updates <code className="rounded bg-muted px-1 font-mono text-[10px]">platform_settings.logo_url</code>{" "}
              automatically.
            </p>
            {logoUrl ? (
              <div className="rounded-lg border border-border bg-muted/30 p-3">
                <div className="flex h-24 w-full max-w-full items-center justify-center overflow-hidden rounded-lg border border-border bg-background sm:h-28 lg:max-w-md">
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img
                    src={logoUrl}
                    alt="Platform logo preview"
                    className="max-h-full w-full max-w-full object-contain p-2"
                  />
                </div>
                <button
                  type="button"
                  className="mt-2 text-xs font-medium text-destructive underline"
                  onClick={() => setLogoUrl("")}
                >
                  Remove logo and save
                </button>
              </div>
            ) : null}
          </div>

          <div className="flex flex-col gap-3 border-t border-border pt-6 lg:col-span-2 sm:flex-row sm:items-center sm:justify-end">
            <button
              type="submit"
              disabled={saving}
              className="inline-flex w-full items-center justify-center gap-2 rounded-lg bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white shadow-sm hover:bg-violet-700 disabled:opacity-50 sm:w-auto"
            >
              {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
              Save
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
