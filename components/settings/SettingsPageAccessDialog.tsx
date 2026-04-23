"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Loader2, Search, X } from "lucide-react";
import { normalizeRoleKeyForBranding } from "@/lib/tenant-branding-permissions";
import { useUserRole } from "@/components/UserRoleContext";
import {
  getSettingsPageAccessDataAction,
  searchSettingsPageOrgUsersAction,
  setSettingsPageLevelForRoleAction,
  setSettingsPageUserOverrideAction,
  type SettingsPageAccessData,
  type SettingsPageAccessEntityRow,
  type SettingsPageFeature,
  type SettingsPageUiLevel,
} from "@/app/settings/settings-page-access-actions";

const LEVELS: { value: SettingsPageUiLevel; label: string }[] = [
  { value: "none", label: "None" },
  { value: "read", label: "Read" },
  { value: "write", label: "Write" },
];

const USER_LEVELS: { value: "inherit" | "none" | "read" | "write"; label: string }[] = [
  { value: "inherit", label: "Inherit" },
  { value: "none", label: "None" },
  { value: "read", label: "Read" },
  { value: "write", label: "Write" },
];

function levelLabel(l: string): string {
  const t = l.toLowerCase();
  if (t === "none") return "None";
  if (t === "read") return "Read";
  if (t === "write") return "Write";
  if (t === "manage") return "Manage";
  return l;
}

function entityLevelToSettingsUi(l: string): SettingsPageUiLevel {
  const t = l.toLowerCase() as "none" | "read" | "write" | "manage";
  if (t === "none" || t === "read") return t;
  if (t === "write" || t === "manage") return "write";
  return "none";
}

type Props = {
  open: boolean;
  onClose: () => void;
  organizationId: string;
  feature: SettingsPageFeature;
  onAfterChange: () => void;
};

export function SettingsPageAccessDialog({
  open,
  onClose,
  organizationId,
  feature,
  onAfterChange,
}: Props) {
  const { canonicalRoleKey } = useUserRole();
  const isSuperAdmin = normalizeRoleKeyForBranding(String(canonicalRoleKey ?? "")) === "super_admin";
  const [data, setData] = useState<SettingsPageAccessData | null>(null);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const [userSearch, setUserSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [searchResults, setSearchResults] = useState<
    { profileId: string; fullName: string; email: string }[]
  >([]);
  const [searchLoading, setSearchLoading] = useState(false);

  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(userSearch.trim()), 300);
    return () => clearTimeout(t);
  }, [userSearch]);

  useEffect(() => {
    if (!open) {
      setUserSearch("");
      setDebouncedSearch("");
      setSearchResults([]);
    }
  }, [open]);

  const load = useCallback(async () => {
    if (!open || !organizationId?.trim() || !isSuperAdmin) return;
    setLoading(true);
    setErr(null);
    const r = await getSettingsPageAccessDataAction(organizationId, feature);
    setLoading(false);
    if (!r.ok) {
      setErr(r.error);
      setData(null);
      return;
    }
    setData(r.data);
  }, [open, organizationId, feature, isSuperAdmin]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (!open || !organizationId?.trim() || !isSuperAdmin) {
      setSearchResults([]);
      return;
    }
    if (debouncedSearch.length < 2) {
      setSearchResults([]);
      setSearchLoading(false);
      return;
    }
    let cancelled = false;
    setSearchLoading(true);
    void searchSettingsPageOrgUsersAction(organizationId, debouncedSearch).then((res) => {
      if (cancelled) return;
      setSearchLoading(false);
      if (res.ok) {
        setSearchResults(res.rows);
      } else {
        setSearchResults([]);
      }
    });
    return () => {
      cancelled = true;
    };
  }, [open, organizationId, debouncedSearch, isSuperAdmin]);

  const overrideById = useMemo(() => {
    const m = new Map<string, "none" | "read" | "write">();
    for (const r of data?.userOverrides ?? []) {
      m.set(r.profileId, r.override);
    }
    return m;
  }, [data?.userOverrides]);

  async function saveRole(r: SettingsPageAccessEntityRow, next: SettingsPageUiLevel) {
    setSaving(`role:${r.id}`);
    setErr(null);
    const res = await setSettingsPageLevelForRoleAction({
      organizationId,
      feature,
      roleId: r.id,
      level: next,
    });
    setSaving(null);
    if (!res.ok) {
      setErr(res.error);
      return;
    }
    onAfterChange();
    await load();
  }

  async function saveUser(profileId: string, next: "inherit" | "none" | "read" | "write") {
    setSaving(`user:${profileId}`);
    setErr(null);
    const res = await setSettingsPageUserOverrideAction({
      organizationId,
      feature,
      profileId,
      level: next,
    });
    setSaving(null);
    if (!res.ok) {
      setErr(res.error);
      return;
    }
    onAfterChange();
    await load();
  }

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center p-4"
      role="dialog"
      aria-modal="true"
      aria-labelledby="settings-access-title"
    >
      <button
        type="button"
        className="absolute inset-0 bg-foreground/40"
        onClick={onClose}
        aria-label="Close"
      />
      <div
        className="relative z-[101] max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-xl border border-border bg-background p-5 shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="mb-4 flex items-start justify-between gap-2">
          <div>
            <h2 id="settings-access-title" className="text-lg font-semibold">
              Manage access · {data?.featureLabel ?? "…"}
            </h2>
            <p className="text-xs text-muted-foreground">
              {data ? (
                <span>
                  <span className="font-mono">settings.{data.featureKey}</span>
                  {data.moduleFeatureName ? <span> · {data.moduleFeatureName}</span> : null}
                </span>
              ) : null}
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg p-1.5 text-muted-foreground transition hover:bg-accent hover:text-foreground"
            aria-label="Close"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {err ? (
          <p className="mb-3 rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-sm text-destructive">
            {err}
          </p>
        ) : null}

        {loading || !data ? (
          <div className="flex items-center justify-center gap-2 py-12 text-sm text-muted-foreground">
            <Loader2 className="h-5 w-5 animate-spin" />
            Loading access…
          </div>
        ) : (
          <div className="space-y-6">
            <section>
              <h3 className="mb-2 text-xs font-bold uppercase tracking-widest text-muted-foreground">Roles</h3>
              <div className="max-h-48 space-y-1.5 overflow-y-auto rounded-md border border-border/80">
                {data.roles.length === 0 ? (
                  <p className="p-2 text-sm text-muted-foreground">No roles in catalog.</p>
                ) : (
                  data.roles.map((r) => (
                    <div
                      key={r.id}
                      className="flex flex-wrap items-center justify-between gap-2 border-b border-border/50 px-2 py-1.5 last:border-0"
                    >
                      <div className="min-w-0">
                        <p className="truncate text-sm font-medium">{r.name}</p>
                        <p className="truncate font-mono text-xs text-muted-foreground">{r.key}</p>
                      </div>
                      <div className="flex shrink-0 items-center gap-1">
                        <span className="text-[10px] text-muted-foreground">Now: {levelLabel(r.level)}</span>
                        <select
                          className="h-8 rounded border border-input bg-background px-2 text-xs"
                          value={entityLevelToSettingsUi(r.level)}
                          disabled={saving === `role:${r.id}`}
                          onChange={(e) => {
                            void saveRole(r, e.target.value as SettingsPageUiLevel);
                          }}
                        >
                          {LEVELS.map((l) => (
                            <option key={l.value} value={l.value}>
                              {l.label}
                            </option>
                          ))}
                        </select>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </section>

            <section>
              <h3 className="mb-2 text-xs font-bold uppercase tracking-widest text-muted-foreground">Users</h3>
              <p className="mb-2 text-xs text-muted-foreground">
                Search by name or email, then set a per-user override for this feature. &quot;Inherit&quot; follows
                role-based access only.
              </p>
              <div className="relative mb-3">
                <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <input
                  type="search"
                  value={userSearch}
                  onChange={(e) => setUserSearch(e.target.value)}
                  placeholder="Search (min. 2 characters)…"
                  className="h-9 w-full rounded-md border border-input bg-background pl-9 pr-3 text-sm"
                  autoComplete="off"
                />
              </div>
              {userSearch.trim().length > 0 && userSearch.trim().length < 2 ? (
                <p className="text-xs text-muted-foreground">Type at least 2 characters to search.</p>
              ) : null}
              {searchLoading && debouncedSearch.length >= 2 ? (
                <div className="flex items-center gap-2 py-2 text-xs text-muted-foreground">
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  Searching…
                </div>
              ) : null}
              {!searchLoading && debouncedSearch.length >= 2 && searchResults.length === 0 ? (
                <p className="rounded-md border border-dashed border-border/80 px-2 py-3 text-sm text-muted-foreground">
                  No users match your search in this organization.
                </p>
              ) : null}
              {searchResults.length > 0 ? (
                <div className="max-h-64 space-y-1.5 overflow-y-auto rounded-md border border-border/80">
                  {searchResults.map((u) => {
                    const o = overrideById.get(u.profileId);
                    const value: "inherit" | "none" | "read" | "write" = o != null ? o : "inherit";
                    return (
                      <div
                        key={u.profileId}
                        className="flex flex-wrap items-center justify-between gap-2 border-b border-border/50 px-2 py-2 last:border-0"
                      >
                        <div className="min-w-0 flex-1">
                          <p className="truncate text-sm font-medium">{u.fullName}</p>
                          <p className="truncate text-xs text-muted-foreground">{u.email}</p>
                        </div>
                        <select
                          className="h-8 shrink-0 rounded border border-input bg-background px-2 text-xs"
                          value={value}
                          disabled={saving === `user:${u.profileId}`}
                          onChange={(e) => {
                            void saveUser(
                              u.profileId,
                              e.target.value as "inherit" | "none" | "read" | "write",
                            );
                          }}
                        >
                          {USER_LEVELS.map((l) => (
                            <option key={l.value} value={l.value}>
                              {l.label}
                            </option>
                          ))}
                        </select>
                      </div>
                    );
                  })}
                </div>
              ) : null}
            </section>
          </div>
        )}

        <div className="mt-4 flex justify-end">
          <button
            type="button"
            onClick={onClose}
            className="rounded-md border border-border bg-background px-3 py-1.5 text-sm font-medium transition hover:bg-accent"
          >
            Done
          </button>
        </div>
      </div>
    </div>
  );
}
