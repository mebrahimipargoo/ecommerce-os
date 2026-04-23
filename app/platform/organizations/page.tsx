"use client";

import React, { useEffect, useState } from "react";
import { Loader2, PlusCircle } from "lucide-react";
import Link from "next/link";
import { getNewOrganizationPageAccessAction } from "./create-organization-actions";
import { getPlatformAccessPageAccessAction } from "../access/access-actions";
import {
  listPlatformOrganizationsAction,
  type PlatformOrganizationListRow,
} from "./edit-organization-actions";
import {
  responsivePageInner,
  responsivePageNarrow,
  responsivePageOuter,
} from "../../../lib/responsive-page-shell";
import { PageHeaderWithInfo } from "../components/page-header-with-info";

function formatCreatedAt(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(d);
}

export default function OrganizationsListPage() {
  const [loading, setLoading] = useState(true);
  const [accessDenied, setAccessDenied] = useState<"not_authenticated" | "forbidden" | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [rows, setRows] = useState<PlatformOrganizationListRow[]>([]);
  const [canCreate, setCanCreate] = useState(false);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const platformRes = await getPlatformAccessPageAccessAction();
      if (cancelled) return;
      setAccessDenied(platformRes.accessDenied);
      if (platformRes.accessDenied) {
        setLoading(false);
        return;
      }

      const [provRes, listRes] = await Promise.all([
        getNewOrganizationPageAccessAction(),
        listPlatformOrganizationsAction(),
      ]);
      if (cancelled) return;
      setCanCreate(!provRes.accessDenied);
      if (!listRes.ok) {
        setLoadError(listRes.error);
        setLoading(false);
        return;
      }
      setRows(listRes.rows);
      setLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  if (loading) {
    return (
      <div className={responsivePageOuter}>
        <div className={`${responsivePageInner} flex min-h-[40vh] items-center justify-center`}>
          <div className="flex items-center gap-3 text-sm text-muted-foreground">
            <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
            Loading organizations…
          </div>
        </div>
      </div>
    );
  }

  if (accessDenied) {
    return (
      <div className={responsivePageOuter}>
        <div className={responsivePageNarrow}>
          <h1 className="text-lg font-semibold text-foreground">Organizations</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            {accessDenied === "not_authenticated"
              ? "You must be signed in to view this page."
              : "This page is restricted to super_admin only."}
          </p>
        </div>
      </div>
    );
  }

  if (loadError) {
    return (
      <div className={responsivePageOuter}>
        <div className={responsivePageNarrow}>
          <h1 className="text-lg font-semibold text-foreground">Organizations</h1>
          <p className="mt-2 text-sm text-destructive">{loadError}</p>
        </div>
      </div>
    );
  }

  return (
    <div className={responsivePageOuter}>
      <div className={`${responsivePageInner} min-w-0 space-y-6`}>
        <header className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <PageHeaderWithInfo
            className="min-w-0 flex-1 mb-0"
            title="Organizations"
            titleClassName="text-2xl font-bold tracking-tight text-foreground sm:text-3xl"
            helpPanelClassName="mt-3 max-w-3xl space-y-2 rounded-lg border border-border bg-muted/20 p-3 text-sm text-muted-foreground sm:text-base"
            infoAriaLabel="About Organizations"
          >
            <p>
              Internal registry — rows from{" "}
              <code className="rounded-md bg-muted px-1.5 py-0.5 font-mono text-[11px]">
                public.organizations
              </code>
              .
            </p>
          </PageHeaderWithInfo>
          {canCreate ? (
            <Link
              href="/platform/organizations/new"
              className="inline-flex shrink-0 items-center justify-center gap-2 rounded-lg bg-violet-600 px-4 py-2.5 text-sm font-semibold text-white shadow-sm hover:bg-violet-700 sm:py-2"
            >
              <PlusCircle className="h-4 w-4" />
              Create organization
            </Link>
          ) : null}
        </header>

        <div className="overflow-x-auto rounded-2xl border border-border bg-card shadow-sm">
          <table className="w-full min-w-[640px] border-collapse text-left text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/40">
                <th className="px-4 py-3 font-semibold text-foreground">Name</th>
                <th className="px-4 py-3 font-semibold text-foreground">Slug</th>
                <th className="px-4 py-3 font-semibold text-foreground">Plan</th>
                <th className="px-4 py-3 font-semibold text-foreground">Active</th>
                <th className="px-4 py-3 font-semibold text-foreground">Created</th>
                <th className="px-4 py-3 font-semibold text-foreground w-[1%] whitespace-nowrap">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
                    No organizations.
                  </td>
                </tr>
              ) : (
                rows.map((r) => (
                  <tr key={r.id} className="border-b border-border last:border-0">
                    <td className="px-4 py-3 font-medium text-foreground">{r.name}</td>
                    <td className="px-4 py-3 font-mono text-xs text-muted-foreground">{r.slug}</td>
                    <td className="px-4 py-3 capitalize">{r.plan}</td>
                    <td className="px-4 py-3">
                      {r.is_active ? (
                        <span className="rounded-full bg-emerald-500/15 px-2 py-0.5 text-xs font-medium text-emerald-800 dark:text-emerald-200">
                          Active
                        </span>
                      ) : (
                        <span className="rounded-full bg-muted px-2 py-0.5 text-xs font-medium text-muted-foreground">
                          Inactive
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-3 text-muted-foreground tabular-nums">
                      {formatCreatedAt(r.created_at)}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                        <Link
                          href={`/platform/organizations/${r.id}`}
                          className="font-medium text-primary hover:underline"
                        >
                          Edit
                        </Link>
                        <span className="text-muted-foreground" aria-hidden>
                          ·
                        </span>
                        <Link
                          href={`/platform/organizations/${r.id}/modules`}
                          className="font-medium text-primary hover:underline"
                        >
                          Modules &amp; entitlements
                        </Link>
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
