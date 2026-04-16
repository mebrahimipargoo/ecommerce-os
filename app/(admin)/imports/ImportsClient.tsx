"use client";

/**
 * ImportsClient — orchestrator for the Imports admin page.
 *
 * Tenant scope:
 * - organizationId (useUserRole): home org from profiles; enforced by server actions for non–super-admins.
 * - superAdminOrgFilter: super_admin / system_employee page-level org dropdown.
 * - selectedStoreId (UniversalImporter): mirrors the importer store into the filter so History stays aligned.
 *
 * effectiveOrgId (super_admin): page filter, then importer store, then home org.
 * historyCompanyId: selectedStoreId ?? effectiveOrgId ?? organizationId ?? null (null = all orgs for super_admin).
 *
 * Avoid mixing ?? and || without parentheses: use `a ?? (b || c)` when needed.
 */

import React, { useCallback, useEffect, useState } from "react";
import { Building2, Database } from "lucide-react";
import { UniversalImporter } from "./UniversalImporter";
import { RawReportImportsPanel } from "./RawReportImportsPanel";
import { useUserRole } from "../../../components/UserRoleContext";
import {
  listWorkspaceOrganizationsForAdmin,
  type WorkspaceOrganizationOption,
} from "../../session/tenant-actions";

export function ImportsClient() {
  const { role, organizationId } = useUserRole();

  /** Bumps to tell Import History to refetch — do NOT remount the panel (avoids empty-table flicker). */
  const [historyRefreshSignal, setHistoryRefreshSignal] = useState(0);
  const refreshHistory = useCallback(() => setHistoryRefreshSignal((k) => k + 1), []);

  const isSuperAdmin = role === "super_admin" || role === "system_employee";
  const [superAdminOrgFilter, setSuperAdminOrgFilter] = useState<string>("");
  const [orgOptions, setOrgOptions] = useState<WorkspaceOrganizationOption[]>([]);

  const [selectedStoreId, setSelectedStoreId] = useState<string | null>(null);

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

  const effectiveOrgId: string = isSuperAdmin
    ? (superAdminOrgFilter.trim() || selectedStoreId || organizationId || "")
    : (organizationId ?? "");

  const historyCompanyId: string | null =
    selectedStoreId ?? (effectiveOrgId || organizationId) ?? null;

  const handleStoreChange = useCallback((id: string) => {
    setSelectedStoreId(id);
    if (isSuperAdmin && id) setSuperAdminOrgFilter(id);
    refreshHistory();
  }, [isSuperAdmin, refreshHistory]);

  const handleOrgFilterChange = useCallback((e: React.ChangeEvent<HTMLSelectElement>) => {
    const value = e.target.value;
    setSuperAdminOrgFilter(value);
    setSelectedStoreId(null);
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

      {/* ── Importer card ───────────────────────────────────────────────────── */}
      <UniversalImporter
        onUploadComplete={refreshHistory}
        onTargetStoreChange={handleStoreChange}
      />

      {/* ── History panel ───────────────────────────────────────────────────── */}
      <RawReportImportsPanel
        organizationId={historyCompanyId}
        refreshSignal={historyRefreshSignal}
      />
    </div>
  );
}
