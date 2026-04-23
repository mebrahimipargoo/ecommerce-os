"use client";

import React, { useCallback, useEffect, useState } from "react";
import { Shield, Loader2, ChevronRight } from "lucide-react";
import { normalizeRoleKeyForBranding } from "@/lib/tenant-branding-permissions";
import { useUserRole } from "@/components/UserRoleContext";
import { getSettingsPageAccessDataAction, type SettingsPageFeature } from "@/app/settings/settings-page-access-actions";
import { SettingsPageAccessDialog } from "./SettingsPageAccessDialog";

type Props = {
  organizationId: string | null | undefined;
  pageFeature: SettingsPageFeature;
};

export function SettingsPageAccessPanel({ organizationId, pageFeature }: Props) {
  const { canonicalRoleKey } = useUserRole();
  const isSuperAdmin = normalizeRoleKeyForBranding(String(canonicalRoleKey ?? "")) === "super_admin";
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [roleN, setRoleN] = useState(0);
  const [userN, setUserN] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [ver, setVer] = useState(0);

  const load = useCallback(async () => {
    const oid = organizationId?.trim();
    if (!oid || !isSuperAdmin) {
      return;
    }
    setLoading(true);
    setError(null);
    const r = await getSettingsPageAccessDataAction(oid, pageFeature);
    setLoading(false);
    if (!r.ok) {
      setError(r.error);
      return;
    }
    const d = r.data;
    setRoleN(d.roles.filter((x) => x.level !== "none").length);
    setUserN(d.userOverrides.length);
  }, [organizationId, pageFeature, isSuperAdmin, ver]);

  useEffect(() => {
    void load();
  }, [load]);

  if (!isSuperAdmin) return null;
  const oid = organizationId?.trim();
  if (!oid) return null;

  return (
    <>
      <div className="mb-4 flex flex-col gap-2 rounded-lg border border-violet-200/80 bg-violet-50/80 px-3 py-2.5 dark:border-violet-800/50 dark:bg-violet-950/30 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex min-w-0 items-start gap-2 sm:items-center">
          <div className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-violet-200/60 dark:bg-violet-900/50">
            <Shield className="h-4 w-4 text-violet-800 dark:text-violet-200" />
          </div>
          <div className="min-w-0">
            <p className="text-sm font-semibold text-foreground">Page access</p>
            <p className="text-xs text-muted-foreground">
              <span className="font-mono">settings.{pageFeature}</span>
              {loading ? (
                <span className="ml-2 inline-flex items-center gap-1">
                  <Loader2 className="h-3 w-3 animate-spin" />
                </span>
              ) : error ? (
                <span className="ml-1 text-amber-700 dark:text-amber-300"> · {error}</span>
              ) : (
                <span className="ml-1">
                  {" "}
                  · {roleN} role{roleN === 1 ? "" : "s"} with access · {userN} user
                  {userN === 1 ? "" : "s"} (overrides)
                </span>
              )}
            </p>
          </div>
        </div>
        <button
          type="button"
          onClick={() => setOpen(true)}
          className="inline-flex shrink-0 items-center justify-center gap-1 rounded-md border border-violet-300 bg-background px-3 py-1.5 text-xs font-semibold text-violet-900 shadow-sm transition hover:bg-violet-100 dark:border-violet-600 dark:text-violet-100 dark:hover:bg-violet-900/40"
        >
          Manage access
          <ChevronRight className="h-3.5 w-3.5" />
        </button>
      </div>

      <SettingsPageAccessDialog
        open={open}
        onClose={() => {
          setOpen(false);
          setVer((v) => v + 1);
        }}
        organizationId={oid}
        feature={pageFeature}
        onAfterChange={() => setVer((v) => v + 1)}
      />
    </>
  );
}
