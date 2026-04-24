"use client";

import React, { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { ImageIcon, Loader2, Save } from "lucide-react";
import {
  getCompanySettingsAction,
  saveCompanySettingsAction,
} from "./company-settings-actions";
import { uploadOrganizationLogoAction } from "../upload-organization-logo-action";
import { useUserRole } from "@/components/UserRoleContext";
import {
  responsiveFormInput,
  responsivePageInner,
  responsivePageNarrow,
  responsivePageOuter,
} from "../../../lib/responsive-page-shell";
import { SettingsPageAccessPanel } from "@/components/settings/SettingsPageAccessPanel";

export default function CompanySettingsPage() {
  const { organizationId: workspaceOrganizationId, profileLoading } = useUserRole();
  const [loading, setLoading] = useState(true);
  const [accessDenied, setAccessDenied] = useState<"not_authenticated" | "forbidden" | null>(
    null,
  );
  const [organizationId, setOrganizationId] = useState<string | null>(null);
  const [companyDisplayName, setCompanyDisplayName] = useState("");
  const [fallbackOrganizationName, setFallbackOrganizationName] = useState("");
  const [logoUrl, setLogoUrl] = useState("");
  const [uploadingLogo, setUploadingLogo] = useState(false);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const effectiveDisplayName = useMemo(() => {
    const explicit = companyDisplayName.trim();
    if (explicit) return explicit;
    return fallbackOrganizationName.trim();
  }, [companyDisplayName, fallbackOrganizationName]);

  useEffect(() => {
    if (profileLoading) return;
    let cancelled = false;
    void (async () => {
      setLoading(true);
      const res = await getCompanySettingsAction(workspaceOrganizationId);
      if (cancelled) return;
      setAccessDenied(res.accessDenied);
      setOrganizationId(res.organizationId);
      setCompanyDisplayName(res.companyDisplayName);
      setFallbackOrganizationName(res.fallbackOrganizationName);
      setLogoUrl(res.logoUrl);
      setLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [workspaceOrganizationId, profileLoading]);

  async function onSave(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    setError(null);
    setMessage(null);
    const res = await saveCompanySettingsAction({
      company_display_name: companyDisplayName.trim(),
      logo_url: logoUrl.trim() || null,
      organization_id: workspaceOrganizationId,
    });
    setSaving(false);
    if (!res.ok) {
      setError(res.error);
      return;
    }
    setMessage("Company branding saved.");
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
      if (workspaceOrganizationId) {
        formData.append("organization_id", workspaceOrganizationId);
      }
      const res = await uploadOrganizationLogoAction(formData);
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
            Loading company settings...
          </div>
        </div>
      </div>
    );
  }

  if (accessDenied) {
    return (
      <div className={responsivePageOuter}>
        <div className={responsivePageNarrow}>
          <h1 className="text-lg font-semibold text-foreground">Company settings</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            {accessDenied === "not_authenticated"
              ? "You must be signed in to view this page."
              : "You do not have access to edit company branding."}
          </p>
          <Link
            href="/settings"
            className="mt-4 inline-block text-sm font-medium text-primary hover:underline"
          >
            ← Back to Settings
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className={responsivePageOuter}>
      <div className={`${responsivePageInner} space-y-6`}>
        <header className="space-y-2">
          <h1 className="text-2xl font-bold tracking-tight text-foreground sm:text-3xl">
            Company branding
          </h1>
          <p className="max-w-3xl text-sm text-muted-foreground">
            Tenant branding for your effective organization. This reads and writes only{" "}
            <code className="rounded-md bg-muted px-1.5 py-0.5 font-mono text-[11px]">
              organization_settings.company_display_name
            </code>{" "}
            and{" "}
            <code className="rounded-md bg-muted px-1.5 py-0.5 font-mono text-[11px]">
              organization_settings.logo_url
            </code>
            .
          </p>
          <p className="text-xs text-muted-foreground">
            Effective organization:{" "}
            <code className="rounded-md bg-muted px-1.5 py-0.5 font-mono text-[10px]">
              {organizationId ?? "unknown"}
            </code>
          </p>
          <Link
            href="/settings"
            className="inline-block text-xs font-medium text-primary hover:underline"
          >
            ← Back to Settings
          </Link>
        </header>

        {organizationId ? (
          <SettingsPageAccessPanel organizationId={organizationId} pageFeature="company" />
        ) : null}

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
                htmlFor="company-display-name"
                className="mb-1.5 block text-sm font-medium text-foreground"
              >
                Company display name
              </label>
              <input
                id="company-display-name"
                type="text"
                value={companyDisplayName}
                onChange={(ev) => setCompanyDisplayName(ev.target.value)}
                placeholder={fallbackOrganizationName || "Enter a display name"}
                className={responsiveFormInput}
              />
              <p className="mt-1 text-xs text-muted-foreground">
                Fallback when empty: <strong>{fallbackOrganizationName || "none"}</strong>
              </p>
            </div>

            <div className="rounded-lg border border-border bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
              Effective displayed company name:{" "}
              <span className="font-semibold text-foreground">{effectiveDisplayName || "(empty)"}</span>
            </div>
          </div>

          <div className="min-w-0 space-y-3">
            <span className="mb-1.5 block text-sm font-medium text-foreground">Company logo</span>
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
              Upload updates <code className="rounded bg-muted px-1 font-mono text-[10px]">organization_settings.logo_url</code>{" "}
              automatically.
            </p>
            {logoUrl ? (
              <div className="rounded-lg border border-border bg-muted/30 p-3">
                <div className="flex h-24 w-full max-w-full items-center justify-center overflow-hidden rounded-lg border border-border bg-background sm:h-28 lg:max-w-md">
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img
                    src={logoUrl}
                    alt="Company logo preview"
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
