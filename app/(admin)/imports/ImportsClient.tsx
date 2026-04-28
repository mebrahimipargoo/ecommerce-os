"use client";

/**
 * ImportsClient — orchestrator for the Imports admin page.
 *
 * Tenant scope:
 * - organizationId (useUserRole): home org from profiles; enforced by server actions for non–super-admins.
 * - superAdminOrgFilter: super_admin / system_employee page-level org dropdown — ALWAYS an `organizations.id`.
 * - The chosen target store lives inside UniversalImporter only. It is NEVER copied into
 *   `superAdminOrgFilter` (a store id is not an org id, and writing one there breaks the dropdown +
 *   the tenant scope forwarded back into UniversalImporter on the next render).
 *
 * effectiveOrgId (super_admin): page filter, otherwise home org. Always an org id.
 * historyCompanyId: same — feeds RawReportImportsPanel as `organizationId`.
 */

import React, { useCallback, useEffect, useState } from "react";
import { usePathname } from "next/navigation";
import { Building2, Database } from "lucide-react";
import { UniversalImporter } from "./UniversalImporter";
import { RawReportImportsPanel } from "./RawReportImportsPanel";
import { useUserRole } from "../../../components/UserRoleContext";
import {
  listWorkspaceOrganizationsForAdmin,
  type WorkspaceOrganizationOption,
} from "../../session/tenant-actions";
import { SettingsPageAccessPanel } from "@/components/settings/SettingsPageAccessPanel";

export function ImportsClient() {
  const pathname = usePathname();
  const isSettingsImportsRoute = (pathname ?? "").includes("/settings/imports");
  const { role, organizationId } = useUserRole();

  /** Bumps to tell Import History to refetch — do NOT remount the panel (avoids empty-table flicker). */
  const [historyRefreshSignal, setHistoryRefreshSignal] = useState(0);
  const refreshHistory = useCallback(() => setHistoryRefreshSignal((k) => k + 1), []);

  const isSuperAdmin = role === "super_admin" || role === "system_employee";
  const [superAdminOrgFilter, setSuperAdminOrgFilter] = useState<string>("");
  const [orgOptions, setOrgOptions] = useState<WorkspaceOrganizationOption[]>([]);

  useEffect(() => {
    if (!isSuperAdmin) return;
    let cancelled = false;
    void listWorkspaceOrganizationsForAdmin().then((res) => {
      if (cancelled || !res.ok) return;
      setOrgOptions(res.rows);
      if (!superAdminOrgFilter && organizationId) {
        setSuperAdminOrgFilter(organizationId);
      }
    });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isSuperAdmin]);

  // effectiveOrgId is an ORG id — never a store id. Falling back to a store id
  // here was the bug that caused: pick org → auto-select store → org filter
  // got overwritten with the store id → dropdown reverted to "All organizations"
  // and the importer's "No active stores in the selected organization" warning
  // appeared because it tried to filter stores by an id that is not an org id.
  const effectiveOrgId: string = isSuperAdmin
    ? (superAdminOrgFilter.trim() || organizationId || "")
    : (organizationId ?? "");

  // Same reasoning for the History panel: it accepts an `organizationId`, so
  // we must never pass a store id. listRawReportUploads validates and discards
  // non-org ids, but only because of a defensive fallback — keeping a clean
  // contract here avoids relying on that.
  const historyCompanyId: string | null = effectiveOrgId || organizationId || null;

  const handleStoreChange = useCallback((_id: string) => {
    // The store id stays inside UniversalImporter — we do NOT mirror it into
    // superAdminOrgFilter (a store id is not an organization id) and we do not
    // need a separate page-level copy for History, which scopes by org only.
    // Just bump the History panel so its row count reflects the new context.
    refreshHistory();
  }, [refreshHistory]);

  const handleOrgFilterChange = useCallback((e: React.ChangeEvent<HTMLSelectElement>) => {
    setSuperAdminOrgFilter(e.target.value);
    setHistoryRefreshSignal((k) => k + 1);
  }, []);

  const activeOrgLabel = orgOptions.find(
    (o) => o.organization_id === superAdminOrgFilter,
  )?.display_name ?? null;

  return (
    <div className="mx-auto w-full max-w-[min(100%,96rem)] space-y-6 px-3 py-6 sm:px-6 lg:px-10">
      {/* ── Page header ─────────────────────────────────────────────────────── */}
      <div>
        <div className="flex items-center gap-2 text-muted-foreground">
          <Database className="h-4 w-4 shrink-0" aria-hidden />
          <span className="text-xs font-semibold uppercase tracking-widest">
            Data Management
          </span>
        </div>
        <h1 className="mt-1 text-2xl font-semibold tracking-tight text-foreground">
          Imports
        </h1>
        <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
          Upload Amazon report files. Each file goes through an automated pipeline to process, sync, and enrich data.
        </p>
      </div>

      {isSuperAdmin && orgOptions.length > 0 && (
        <div className="mb-6 flex flex-wrap items-center gap-3 rounded-xl border border-violet-200 bg-violet-50 px-4 py-3 dark:border-violet-800/50 dark:bg-violet-950/30">
          <Building2
            className="h-4 w-4 shrink-0 text-violet-600 dark:text-violet-400"
            aria-hidden
          />
          <span className="whitespace-nowrap text-xs font-semibold text-violet-800 dark:text-violet-300">
            Organization scope
          </span>

          <select
            value={superAdminOrgFilter}
            onChange={handleOrgFilterChange}
            className="h-9 min-w-[200px] rounded-lg border border-violet-300 bg-white px-3 text-xs font-semibold text-foreground shadow-sm dark:border-violet-700 dark:bg-background"
            aria-label="Filter imports by organization"
          >
            <option value="">All organizations</option>
            {orgOptions.map((o) => (
              <option key={o.organization_id} value={o.organization_id}>
                {o.display_name}
              </option>
            ))}
          </select>

          <span className="text-xs text-violet-600 dark:text-violet-400">
            {superAdminOrgFilter && activeOrgLabel
              ? `Showing: ${activeOrgLabel}`
              : superAdminOrgFilter
                ? `Org: ${superAdminOrgFilter.slice(0, 8)}…`
                : "Showing all organizations"}
          </span>
        </div>
      )}

      {isSettingsImportsRoute && (effectiveOrgId || organizationId) ? (
        <SettingsPageAccessPanel
          organizationId={(effectiveOrgId || organizationId || "").trim() || null}
          pageFeature="imports"
        />
      ) : null}

      {/* ── Importer card ───────────────────────────────────────────────────── */}
      <UniversalImporter
        onUploadComplete={refreshHistory}
        onTargetStoreChange={handleStoreChange}
        organizationId={(effectiveOrgId || organizationId || "").trim() || null}
      />

      {/* ── History panel ───────────────────────────────────────────────────── */}
      <RawReportImportsPanel
        organizationId={historyCompanyId}
        refreshSignal={historyRefreshSignal}
      />
    </div>
  );
}
