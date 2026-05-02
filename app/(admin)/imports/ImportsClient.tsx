"use client";

/**
 * ImportsClient — orchestrator for the Imports admin page.
 *
 * Tenant scope:
 * - The active organization comes only from the global workspace/view selector
 *   in the app header (`useUserRole().organizationId`).
 * - This page intentionally has no separate organization picker. A page-local
 *   org selector drifted from the header context and caused imports to be
 *   written/read under different tenants.
 */

import React, { useCallback, useState } from "react";
import { usePathname } from "next/navigation";
import { Database } from "lucide-react";
import { UniversalImporter } from "./UniversalImporter";
import { RawReportImportsPanel } from "./RawReportImportsPanel";
import { useUserRole } from "../../../components/UserRoleContext";
import { SettingsPageAccessPanel } from "@/components/settings/SettingsPageAccessPanel";

export function ImportsClient() {
  const pathname = usePathname();
  const isSettingsImportsRoute = (pathname ?? "").includes("/settings/imports");
  const { organizationId, organizationName } = useUserRole();

  /** Bumps to tell Import History to refetch — do NOT remount the panel (avoids empty-table flicker). */
  const [historyRefreshSignal, setHistoryRefreshSignal] = useState(0);
  const refreshHistory = useCallback(() => setHistoryRefreshSignal((k) => k + 1), []);
  const activeOrgId = (organizationId ?? "").trim() || null;

  const handleStoreChange = useCallback((_id: string) => {
    // The store id stays inside UniversalImporter — we do NOT mirror it into
    // any page-level organization state. History scopes by the header-selected
    // active org only.
    // Just bump the History panel so its row count reflects the new context.
    refreshHistory();
  }, [refreshHistory]);

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
        <p className="mt-2 text-xs font-medium text-muted-foreground">
          Active organization: <span className="text-foreground">{organizationName}</span>
        </p>
      </div>

      {isSettingsImportsRoute && activeOrgId ? (
        <SettingsPageAccessPanel
          organizationId={activeOrgId}
          pageFeature="imports"
        />
      ) : null}

      {/* ── Importer card ───────────────────────────────────────────────────── */}
      <UniversalImporter
        onUploadComplete={refreshHistory}
        onTargetStoreChange={handleStoreChange}
        organizationId={activeOrgId}
      />

      {/* ── History panel ───────────────────────────────────────────────────── */}
      <RawReportImportsPanel
        organizationId={activeOrgId}
        refreshSignal={historyRefreshSignal}
      />
    </div>
  );
}
