"use client";

/**
 * ImportsClient — orchestrator for the Imports admin page.
 *
 * توضیح منطق چند-tenant به فارسی:
 * ─────────────────────────────────────────────────────────────────────────────
 * این کامپوننت سه لایه از organization_id را مدیریت می‌کند:
 *
 *  ۱. organizationId (از useUserRole)
 *     org ثابت کاربر جاری از جدول profiles — برای کاربران معمولی همیشه ثابت
 *     است و server actions آن را enforce می‌کنند.
 *
 *  ۲. superAdminOrgFilter (state صفحه)
 *     فقط برای نقش‌های super_admin و system_employee: سازمان انتخاب‌شده از
 *     dropdown بنفش رنگ. انتخاب سازمان باعث می‌شود:
 *       - تاریخچه آپلود (RawReportImportsPanel) فقط برای آن org بارگذاری شود.
 *       - Ledger Uploader نیز با همان org target کار کند.
 *
 *  ۳. ledgerCompanyId (callback از AmazonLedgerUploader)
 *     وقتی Ledger Uploader یک org جداگانه انتخاب می‌کند، این callback آتش
 *     می‌گیرد و superAdminOrgFilter را به‌روز می‌کند تا همه بخش‌ها همگام باشند.
 *
 *  effectiveOrgId = ترکیب هوشمند سه لایه بالا:
 *    → برای super_admin: filter صفحه > ledger target > org پیش‌فرض
 *    → برای بقیه: همیشه org خود کاربر (server actions آن را enforce می‌کنند)
 *
 *  historyCompanyId = سازمانی که RawReportImportsPanel باید تاریخچه آن را
 *    نمایش دهد. اولویت با ledgerCompanyId است (وقتی تازه از ledger تنظیم شده)،
 *    سپس effectiveOrgId، و در نهایت org پیش‌فرض کاربر.
 *
 *  اصلاح خطای Syntax:
 *    استفاده همزمان از ?? و || بدون پرانتز در TypeScript/ESLint خطا می‌دهد:
 *      ❌  a ?? b || c
 *      ✅  a ?? (b || c)
 *    برای جلوگیری از ابهام در precedence عملگرها، همیشه از پرانتز استفاده کنید.
 * ─────────────────────────────────────────────────────────────────────────────
 */

import React, { useCallback, useEffect, useState } from "react";
import { Building2, Database } from "lucide-react";
import { AmazonLedgerUploader } from "./AmazonLedgerUploader";
import { RawReportImportsPanel } from "./RawReportImportsPanel";
import { useUserRole } from "../../../components/UserRoleContext";
import {
  listWorkspaceOrganizationsForAdmin,
  type WorkspaceOrganizationOption,
} from "../../session/tenant-actions";

export function ImportsClient() {
  // ── Auth context ─────────────────────────────────────────────────────────
  // organizationId: pulled from the user's profile row (profiles.organization_id).
  // This is the canonical, RLS-compatible org for all server actions.
  const { role, organizationId } = useUserRole();

  // ── History refresh key ──────────────────────────────────────────────────
  // Bumping this forces RawReportImportsPanel to remount and reload its data.
  const [historyKey, setHistoryKey] = useState(0);
  const refreshHistory = useCallback(() => setHistoryKey((k) => k + 1), []);

  // ── Super-admin org filter ───────────────────────────────────────────────
  const isSuperAdmin = role === "super_admin" || role === "system_employee";
  const [superAdminOrgFilter, setSuperAdminOrgFilter] = useState<string>("");
  const [orgOptions, setOrgOptions] = useState<WorkspaceOrganizationOption[]>([]);

  // Ledger uploader tells us its chosen target company so we can mirror it
  // into the page-level filter and keep Import History in sync.
  const [ledgerCompanyId, setLedgerCompanyId] = useState<string | null>(null);

  // Load the full org list for the super-admin picker on first mount.
  useEffect(() => {
    if (!isSuperAdmin) return;
    let cancelled = false;
    void listWorkspaceOrganizationsForAdmin().then((res) => {
      if (cancelled || !res.ok) return;
      setOrgOptions(res.rows);
      // Pre-select the user's own org so the list is scoped on first load.
      if (!superAdminOrgFilter && organizationId) {
        setSuperAdminOrgFilter(organizationId);
      }
    });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isSuperAdmin]);

  // ── Effective organization computation ───────────────────────────────────
  //
  // This is the org used for BOTH reading history and scoping new uploads.
  // Server actions (createRawReportUploadSession, etc.) call
  // resolveWriteOrganizationId(actorProfileId, requestedOrg) which enforces
  // RLS: non-super-admins always get their profile org; super_admin may
  // override with a requested org.
  //
  // Priority chain (super_admin):
  //   1. Explicitly chosen org in the page dropdown  (superAdminOrgFilter)
  //   2. Ledger uploader's own target company        (ledgerCompanyId)
  //   3. Actor's home org from auth context          (organizationId)
  //   4. Empty string fallback (server will use env default)
  //
  // For all other roles: always the home org — the server enforces it anyway.
  const effectiveOrgId: string = isSuperAdmin
    ? (superAdminOrgFilter.trim() || ledgerCompanyId || organizationId || "")
    : (organizationId ?? "");

  // Fix: ?? must not be mixed with || without explicit parentheses.
  // historyCompanyId precedence:
  //   ledgerCompanyId (most specific — set by ledger uploader callback)
  //   → effectiveOrgId (page-level selection or home org)
  //   → organizationId (raw home org as last resort)
  //   → null (RawReportImportsPanel shows all rows for super_admin)
  const historyCompanyId: string | null =
    ledgerCompanyId ?? (effectiveOrgId || organizationId) ?? null;

  // ── Handlers ──────────────────────────────────────────────────────────────
  // Both handlers MUST be stable references (useCallback) because
  // AmazonLedgerUploader includes onTargetCompanyChange in a useEffect dep
  // array. A new function reference on every render would re-fire the effect
  // and cause an infinite setState → re-render → re-fire loop.

  const handleLedgerTargetCompanyChange = useCallback((id: string) => {
    setLedgerCompanyId(id);
    if (isSuperAdmin && id) setSuperAdminOrgFilter(id);
    refreshHistory();
  }, [isSuperAdmin, refreshHistory]);

  const handleOrgFilterChange = useCallback((e: React.ChangeEvent<HTMLSelectElement>) => {
    const value = e.target.value;
    setSuperAdminOrgFilter(value);
    setLedgerCompanyId(null);
    setHistoryKey((k) => k + 1);
  }, []);

  // ── Active org label for status text ─────────────────────────────────────
  const activeOrgLabel = orgOptions.find(
    (o) => o.organization_id === superAdminOrgFilter,
  )?.display_name ?? null;

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div className="mx-auto w-full max-w-6xl px-4 py-6 md:px-6">

      {/* Page heading */}
      <div className="mb-8 flex flex-col gap-2">
        <div className="flex items-center gap-2 text-muted-foreground">
          <Database className="h-5 w-5 shrink-0" aria-hidden />
          <span className="text-xs font-semibold uppercase tracking-widest">
            Data Management
          </span>
        </div>
        <h1 className="text-2xl font-semibold tracking-tight text-foreground">
          Imports
        </h1>
        <p className="max-w-2xl text-sm text-muted-foreground">
          Amazon Inventory Ledger supports date filters and manual CSV uploads
          into the ledger staging area. Files go to Storage first, then import
          in chunks.
        </p>
      </div>

      {/* ── Super-Admin / System-Employee global org filter ──────────────── */}
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

      {/* ── Amazon Inventory Ledger uploader ─────────────────────────────── */}
      <div className="space-y-8">
        <AmazonLedgerUploader
          onTargetCompanyChange={handleLedgerTargetCompanyChange}
          onLedgerSessionUpdated={refreshHistory}
        />
      </div>

      {/* ── Import history (scoped to historyCompanyId) ───────────────────── */}
      {/*
        key={historyKey} forces a full remount when the org filter changes,
        ensuring stale data from the previous tenant is never shown.

        organizationId is passed as string | null:
          - string  → RawReportImportsPanel filters listRawReportUploads by this org.
          - null    → Super-admin sees uploads for ALL organizations.
      */}
      <RawReportImportsPanel
        key={historyKey}
        organizationId={historyCompanyId}
      />
    </div>
  );
}
