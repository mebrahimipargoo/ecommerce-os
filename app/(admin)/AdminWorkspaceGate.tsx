"use client";

import React, { useEffect, useState } from "react";
import { Loader2 } from "lucide-react";
import { isAdminRole, useUserRole } from "../../components/UserRoleContext";
import { isUuidString } from "../../lib/uuid";
import type { CompanyOption } from "../../lib/imports-types";
import { listCompaniesForImports, saveHomeCompanyForProfile } from "./imports/companies-actions";

/**
 * Syncs `profiles` bootstrap to the signed-in user (`auth.uid()` === `profiles.id`).
 * Blocks admin routes when `organization_id` is unset until the user picks a company.
 */
export function AdminWorkspaceGate({ children }: { children: React.ReactNode }) {
  const { role, actorUserId, homeOrganizationId, profileLoading, refreshProfile } = useUserRole();
  const [companies, setCompanies] = useState<CompanyOption[]>([]);
  const [pick, setPick] = useState("");
  const [saving, setSaving] = useState(false);
  const [gateErr, setGateErr] = useState<string | null>(null);

  useEffect(() => {
    if (!isAdminRole(role)) return;
    let cancelled = false;
    void listCompaniesForImports(actorUserId).then((res) => {
      if (cancelled || !res.ok) return;
      setCompanies(res.rows);
    });
    return () => {
      cancelled = true;
    };
  }, [role, actorUserId]);

  const needsCompany = isAdminRole(role) && !homeOrganizationId && !profileLoading;

  async function handleSaveCompany() {
    const cid = pick.trim();
    const aid = actorUserId?.trim();
    if (!aid || !isUuidString(cid)) {
      setGateErr("Select a company.");
      return;
    }
    setSaving(true);
    setGateErr(null);
    const res = await saveHomeCompanyForProfile(aid, cid);
    setSaving(false);
    if (!res.ok) {
      setGateErr(res.error);
      return;
    }
    await refreshProfile();
  }

  if (profileLoading) {
    return (
      <div className="flex min-h-[40vh] items-center justify-center gap-2 text-muted-foreground">
        <Loader2 className="h-6 w-6 animate-spin" aria-hidden />
        <span className="text-sm">Loading workspace…</span>
      </div>
    );
  }

  if (needsCompany) {
    return (
      <div className="mx-auto flex min-h-[50vh] max-w-lg flex-col justify-center px-4 py-12">
        <h1 className="text-xl font-semibold text-foreground">Choose a company</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          Your profile has no company yet. Select the workspace you belong to before using admin tools.
        </p>
        {gateErr && (
          <div className="mt-4 rounded-lg border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive">
            {gateErr}
          </div>
        )}
        <label className="mt-6 block text-xs font-medium text-muted-foreground">
          Company
          <select
            value={pick}
            onChange={(e) => setPick(e.target.value)}
            className="mt-1 w-full rounded-lg border border-border bg-background px-3 py-2 text-sm"
          >
            <option value="">
              {companies.length === 0 ? "Loading companies…" : "Select a company…"}
            </option>
            {companies.map((c) => (
              <option key={c.id} value={c.id}>
                {c.display_name}
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          disabled={saving || !isUuidString(pick.trim())}
          onClick={() => void handleSaveCompany()}
          className="mt-4 inline-flex items-center justify-center rounded-lg bg-primary px-4 py-2 text-sm font-medium text-primary-foreground disabled:opacity-50"
        >
          {saving ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Saving…
            </>
          ) : (
            "Continue"
          )}
        </button>
      </div>
    );
  }

  return <>{children}</>;
}
