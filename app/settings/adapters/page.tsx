"use client";

import React, { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  ArrowLeft,
  Globe,
  Tag,
  CheckCircle2,
  Loader2,
  ShoppingBag,
  Trash2,
  Pencil,
  BadgeCheck,
  TriangleAlert,
  X,
  RefreshCw,
} from "lucide-react";

import {
  testConnection,
  syncClaims,
  insertMarketplace,
  updateMarketplace,
  deleteMarketplace,
  listMarketplaces,
} from "./actions";
import { ADAPTER_LIST, getAdapterConfig, type AdapterProviderKey } from "../../../lib/adapters/configs";

const ICON_MAP = { shopping_bag: ShoppingBag, globe: Globe, tag: Tag };

type MarketplaceRecord = {
  id: string;
  provider: AdapterProviderKey;
  nickname: string;
  display_id?: string;
  organization_id: string;
  role_required: string;
  created_at: string;
};

function cx(...classes: Array<string | false | null | undefined>): string {
  return classes.filter(Boolean).join(" ");
}

function maskSecret(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) return "";
  if (trimmed.length <= 6) return "•".repeat(trimmed.length);
  return `${"•".repeat(8)}${trimmed.slice(-4)}`;
}

function getInitialFormValues(provider: AdapterProviderKey): Record<string, string> {
  const config = getAdapterConfig(provider);
  const values: Record<string, string> = { nickname: "" };
  for (const f of config?.formFields ?? []) {
    values[f.key] = "";
  }
  return values;
}

function buildCredentials(provider: AdapterProviderKey, form: Record<string, string>): Record<string, string> {
  const config = getAdapterConfig(provider);
  const creds: Record<string, string> = {};
  for (const f of config?.formFields ?? []) {
    const credKey = f.credentialsKey ?? f.key;
    const val = form[f.key]?.trim();
    if (val) creds[credKey] = val;
  }
  return creds;
}

function validateForm(provider: AdapterProviderKey, form: Record<string, string>, isEditing: boolean): string[] {
  const errors: string[] = [];
  if (!form.nickname?.trim()) errors.push("Nickname is required.");
  const config = getAdapterConfig(provider);
  for (const f of config?.formFields ?? []) {
    if (f.required && !isEditing && !form[f.key]?.trim()) {
      errors.push(`${f.label} is required.`);
      break;
    }
  }
  return errors;
}

function providerLabel(provider: AdapterProviderKey): string {
  const config = getAdapterConfig(provider);
  return config?.name?.split(" ")[0] ?? provider;
}

export default function Page() {
  const [activeKey, setActiveKey] = useState<AdapterProviderKey | null>(null);
  const [formsByKey, setFormsByKey] = useState<Record<string, Record<string, string>>>({});
  const [inlineError, setInlineError] = useState<string | null>(null);
  const [isModalOpen, setIsModalOpen] = useState<boolean>(false);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState<boolean>(false);
  const [connections, setConnections] = useState<MarketplaceRecord[]>([]);
  const [isLoadingConnections, setIsLoadingConnections] = useState<boolean>(true);
  const [isDeletingId, setIsDeletingId] = useState<string | null>(null);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [connectionVerificationById, setConnectionVerificationById] = useState<
    Record<string, { status: "idle" | "verifying" | "verified" | "error"; message?: string }>
  >({});
  const [syncStatusById, setSyncStatusById] = useState<
    Record<string, { status: "idle" | "syncing" | "done" | "error"; claimsCount?: number; message?: string }>
  >({});

  useEffect(() => {
    if (!toastMessage) return;
    const id = window.setTimeout(() => setToastMessage(null), 2400);
    return () => window.clearTimeout(id);
  }, [toastMessage]);

  useEffect(() => {
    let cancelled = false;
    async function loadConnections() {
      try {
        setIsLoadingConnections(true);
        setInlineError(null);
        const result = await listMarketplaces();
        if (!cancelled) {
          if (result.ok && result.data) setConnections(result.data);
          else if (result.error) setInlineError(result.error);
        }
      } catch (error) {
        if (!cancelled) {
          setInlineError(error instanceof Error ? error.message : "Failed to load connections.");
        }
      } finally {
        if (!cancelled) setIsLoadingConnections(false);
      }
    }
    void loadConnections();
    return () => { cancelled = true; };
  }, []);

  function updateField(provider: AdapterProviderKey, field: string, value: string) {
    setFormsByKey((prev) => ({
      ...prev,
      [provider]: { ...(prev[provider] ?? getInitialFormValues(provider)), [field]: value },
    }));
  }

  function handleConnectClick(provider: AdapterProviderKey) {
    setInlineError(null);
    setEditingId(null);
    setActiveKey(provider);
    setFormsByKey((prev) => ({ ...prev, [provider]: getInitialFormValues(provider) }));
    setIsModalOpen(true);
  }

  async function handleSave(provider: AdapterProviderKey) {
    const form = formsByKey[provider] ?? getInitialFormValues(provider);
    const isEditing = Boolean(editingId);
    const errors = validateForm(provider, form, isEditing);
    if (errors.length > 0) {
      setInlineError(errors[0]);
      return;
    }

    try {
      setIsSaving(true);
      setInlineError(null);

      const nickname = form.nickname?.trim() ?? "";
      const credentials = buildCredentials(provider, form);

      if (isEditing) {
        const updatePayload: { nickname: string; credentials?: Record<string, string> } = { nickname };
        if (Object.keys(credentials).length > 0) updatePayload.credentials = credentials;
        const result = await updateMarketplace(editingId!, updatePayload);
        if (result.ok && result.data) {
          setConnections((prev) => prev.map((c) => (c.id === editingId ? result.data! : c)));
          setIsModalOpen(false);
          setActiveKey(null);
          setEditingId(null);
          setToastMessage("Connection updated successfully.");
        } else {
          setInlineError(result.error ?? "Failed to update.");
        }
      } else {
        const result = await insertMarketplace({ provider, nickname, credentials });
        if (result.ok && result.data) {
          setConnections((prev) => [result.data!, ...prev]);
          setIsModalOpen(false);
          setActiveKey(null);
          setToastMessage("Connection saved successfully.");
        } else {
          setInlineError(result.error ?? "Failed to save.");
        }
      }
      setFormsByKey((prev) => ({ ...prev, [provider]: getInitialFormValues(provider) }));
    } catch (error) {
      setInlineError(error instanceof Error ? error.message : "Failed to save.");
    } finally {
      setIsSaving(false);
    }
  }

  const activeCard = useMemo(() => {
    if (!activeKey) return null;
    return ADAPTER_LIST.find((c) => c.provider === activeKey) ?? null;
  }, [activeKey]);

  function openEdit(record: MarketplaceRecord) {
    setInlineError(null);
    setEditingId(record.id);
    setActiveKey(record.provider);
    const config = getAdapterConfig(record.provider);
    const initial: Record<string, string> = { nickname: record.nickname ?? "" };
    for (const f of config?.formFields ?? []) {
      initial[f.key] = ""; // Never send credentials to client; user re-enters to update
    }
    setFormsByKey((prev) => ({ ...prev, [record.provider]: initial }));
    setIsModalOpen(true);
  }

  async function handleDelete(record: MarketplaceRecord) {
    if (!window.confirm(`Delete "${record.nickname}" (${providerLabel(record.provider)})? This cannot be undone.`)) return;
    try {
      setIsDeletingId(record.id);
      setInlineError(null);
      const result = await deleteMarketplace(record.id);
      if (result.ok) {
        setConnections((prev) => prev.filter((c) => c.id !== record.id));
        setConnectionVerificationById((prev) => {
          const next = { ...prev };
          delete next[record.id];
          return next;
        });
        setToastMessage("Connection deleted.");
      } else {
        setInlineError(result.error ?? "Failed to delete.");
      }
    } catch (error) {
      setInlineError(error instanceof Error ? error.message : "Failed to delete.");
    } finally {
      setIsDeletingId(null);
    }
  }

  async function handleTestConnection(record: MarketplaceRecord) {
    setConnectionVerificationById((prev) => ({ ...prev, [record.id]: { status: "verifying" } }));
    setInlineError(null);
    const result = await testConnection(record.id);
    if (result.ok) {
      setConnectionVerificationById((prev) => ({ ...prev, [record.id]: { status: "verified" } }));
      setToastMessage(`${providerLabel(record.provider)} connection verified.`);
    } else {
      setConnectionVerificationById((prev) => ({
        ...prev,
        [record.id]: { status: "error", message: result.error ?? "Invalid credentials." },
      }));
    }
  }

  async function handleSyncClaims(record: MarketplaceRecord) {
    setSyncStatusById((prev) => ({ ...prev, [record.id]: { status: "syncing" } }));
    setInlineError(null);
    const result = await syncClaims(record.id);
    if (result.ok) {
      setSyncStatusById((prev) => ({
        ...prev,
        [record.id]: { status: "done", claimsCount: result.claimsCount ?? 0 },
      }));
      setToastMessage(
        result.claimsCount === 0
          ? "Sync complete. No new claims found."
          : `Sync complete. ${result.claimsCount} claim${result.claimsCount === 1 ? "" : "s"} added from ${providerLabel(record.provider)}.`
      );
    } else {
      setSyncStatusById((prev) => ({
        ...prev,
        [record.id]: { status: "error", message: result.error ?? "Sync failed." },
      }));
      setInlineError(result.error ?? "Claims sync failed.");
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-white to-slate-100 text-slate-900 dark:from-slate-950 dark:via-slate-950 dark:to-slate-900 dark:text-slate-50 font-sans">
      {toastMessage && (
        <div className="fixed left-1/2 top-4 z-50 w-[92vw] max-w-md -translate-x-1/2">
          <div className="flex items-center gap-2 rounded-2xl border border-emerald-200 bg-white/90 px-4 py-3 text-sm font-medium text-emerald-900 shadow-lg backdrop-blur-xl dark:border-emerald-800/60 dark:bg-slate-950/80 dark:text-emerald-100">
            <CheckCircle2 className="h-5 w-5 shrink-0" />
            {toastMessage}
          </div>
        </div>
      )}

      <div className="mx-auto flex w-full max-w-[100vw] flex-col gap-6 px-4 py-6 lg:px-8">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <Link
            href="/"
            className="inline-flex min-h-[44px] min-w-[44px] w-fit items-center justify-center gap-2 rounded-full border border-slate-200 bg-white/80 px-4 py-2.5 text-sm font-medium text-slate-700 shadow-sm backdrop-blur-xl transition hover:bg-white dark:border-slate-800 dark:bg-slate-950/70 dark:text-slate-200 dark:hover:bg-slate-950"
          >
            <ArrowLeft className="h-5 w-5" />
            Back to Dashboard
          </Link>
          <div className="flex items-center gap-3">
            <span className="hidden text-sm text-muted-foreground sm:inline">
              Apple-like, minimal, enterprise-ready.
            </span>
          </div>
        </div>

        <header className="rounded-2xl border border-slate-200 bg-white/80 p-6 shadow-sm backdrop-blur-xl dark:border-slate-800 dark:bg-slate-950/70">
          <div className="flex flex-col gap-2">
            <h1 className="text-xl font-semibold tracking-tight text-slate-900 dark:text-slate-50">Marketplace Management</h1>
            <p className="text-base leading-relaxed text-slate-600 dark:text-slate-300">
              Connect marketplaces to power unified returns intelligence and claim recovery across channels.
            </p>
          </div>
        </header>

        {inlineError && (
          <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-900 dark:border-rose-800/60 dark:bg-rose-950/40 dark:text-rose-100">
            <span className="font-semibold">Notice:</span> {inlineError}
          </div>
        )}

        <section className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
          {ADAPTER_LIST.map((card) => {
            const Icon = ICON_MAP[card.iconKey] ?? ShoppingBag;
            return (
              <div
                key={card.provider}
                className={cx(
                  "relative flex min-h-full min-w-0 flex-col overflow-hidden rounded-2xl border bg-white/80 shadow-sm backdrop-blur-xl transition",
                  "dark:bg-slate-950/70 dark:border-slate-800",
                  "ring-1 ring-inset ring-slate-200/70 dark:ring-slate-800/70",
                  isModalOpen && activeKey === card.provider && card.accentRingClass
                )}
              >
                <div className="absolute inset-0 pointer-events-none bg-gradient-to-br from-slate-900/[0.03] via-transparent to-transparent dark:from-white/[0.04]" />
                <div className="relative flex min-h-[200px] flex-col gap-4 p-6">
                  <div className="flex items-start gap-4">
                    <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-slate-100 ring-1 ring-slate-200 dark:bg-slate-800/60 dark:ring-slate-700">
                      <Icon className="h-6 w-6 shrink-0 text-slate-700 dark:text-slate-200" />
                    </div>
                    <div className="flex min-w-0 flex-1 flex-col gap-2">
                      <div className="flex flex-wrap items-center gap-2">
                        <p className="text-base font-semibold text-slate-900 dark:text-slate-50">{card.name}</p>
                        <span className="shrink-0 rounded-full border border-slate-200 bg-white px-2.5 py-1 text-xs font-medium text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
                          {card.badge}
                        </span>
                      </div>
                      <p className="text-sm leading-relaxed text-slate-600 dark:text-slate-300">{card.description}</p>
                    </div>
                  </div>
                  <div className="mt-auto flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
                    <div className={cx("inline-flex min-h-[44px] w-fit items-center gap-2 rounded-full border px-3 py-2 text-sm font-medium", "border-slate-200 bg-white text-slate-700 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200")}>
                      <span className="h-2 w-2 shrink-0 rounded-full bg-slate-400 dark:bg-slate-500" />
                      Disconnected
                    </div>
                    <button
                      type="button"
                      onClick={() => handleConnectClick(card.provider)}
                      className={cx(
                        "inline-flex min-h-[44px] min-w-[44px] w-fit shrink-0 items-center justify-center gap-2 rounded-full px-4 py-2.5 text-sm font-semibold shadow-sm transition",
                        "border border-slate-200 bg-slate-900 text-white hover:bg-slate-800",
                        "dark:border-slate-700 dark:bg-slate-50 dark:text-slate-900 dark:hover:bg-white"
                      )}
                    >
                      <ShoppingBag className="h-5 w-5 shrink-0" />
                      Connect
                    </button>
                  </div>
                </div>
              </div>
            );
          })}
        </section>

        <section className="rounded-2xl border border-slate-200 bg-white/80 shadow-sm backdrop-blur-xl dark:border-slate-800 dark:bg-slate-950/70">
          <div className="flex flex-col gap-2 border-b border-slate-200 px-4 py-4 dark:border-slate-800 sm:flex-row sm:items-center sm:justify-between sm:gap-4 sm:px-6">
            <div className="flex flex-col gap-1">
              <h2 className="text-base font-semibold tracking-tight text-slate-900 dark:text-slate-50">Active Connections</h2>
              <p className="text-sm text-slate-600 dark:text-slate-300">Manage connected marketplace stores.</p>
            </div>
            <div className="text-sm text-muted-foreground">
              {isLoadingConnections ? "Loading…" : `${connections.length} connected`}
            </div>
          </div>

          <div className="px-4 py-4 sm:px-6 sm:py-6">
            {isLoadingConnections ? (
              <div className="flex flex-col items-center justify-center gap-6 py-16">
                <Loader2 className="h-10 w-10 animate-spin text-muted-foreground" />
                <p className="text-sm font-medium text-muted-foreground">Loading connections…</p>
                <div className="grid w-full max-w-sm gap-4">
                  <div className="h-14 w-full animate-pulse rounded-xl bg-slate-200/70 dark:bg-slate-800/60" />
                  <div className="h-14 w-full animate-pulse rounded-xl bg-slate-200/60 dark:bg-slate-800/50" />
                  <div className="h-14 w-full animate-pulse rounded-xl bg-slate-200/50 dark:bg-slate-800/40" />
                </div>
              </div>
            ) : connections.length === 0 ? (
              <div className="flex flex-col items-center justify-center gap-6 rounded-2xl border border-dashed border-slate-200 bg-gradient-to-b from-slate-50/80 to-white/60 px-6 py-16 text-center dark:border-slate-800 dark:from-slate-950/50 dark:to-slate-950/30">
                <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-muted/60">
                  <ShoppingBag className="h-8 w-8 text-muted-foreground" />
                </div>
                <div className="flex flex-col gap-2">
                  <h3 className="text-lg font-semibold text-slate-900 dark:text-slate-50">No connections yet</h3>
                  <p className="max-w-sm text-base leading-relaxed text-muted-foreground">
                    Connect Amazon, Walmart, or another marketplace above to unify returns data and streamline claim recovery.
                  </p>
                </div>
                <p className="text-sm text-muted-foreground">
                  Tap <span className="font-semibold">Connect</span> on any card to get started.
                </p>
              </div>
            ) : (
              <div className="overflow-hidden rounded-2xl border border-border">
                <div className="hidden grid-cols-12 gap-4 bg-white/60 px-4 py-3 text-sm font-semibold text-slate-600 dark:bg-slate-950/30 dark:text-slate-300 md:grid">
                  <div className="col-span-4">Marketplace</div>
                  <div className="col-span-2">Provider</div>
                  <div className="col-span-2">Status</div>
                  <div className="col-span-4 text-right">Actions</div>
                </div>
                <div className="divide-y divide-slate-200 bg-white/40 dark:divide-slate-800 dark:bg-slate-950/20">
                  {connections.map((c) => (
                    <div key={c.id} className="flex flex-col gap-4 px-4 py-4 md:grid md:grid-cols-12 md:items-center md:gap-4 md:py-4">
                      <div className="min-w-0 md:col-span-4">
                        <div className="truncate text-base font-medium text-slate-900 dark:text-slate-50">{c.nickname}</div>
                        {c.display_id && (
                          <div className="truncate text-sm text-muted-foreground">
                            ID: {c.display_id}
                          </div>
                        )}
                      </div>
                      <div className="text-sm text-slate-700 dark:text-slate-200 md:col-span-2">{providerLabel(c.provider)}</div>
                      <div className="md:col-span-2">
                        <span className="inline-flex items-center gap-2 rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1.5 text-sm font-semibold text-emerald-800 dark:border-emerald-800/60 dark:bg-emerald-950/40 dark:text-emerald-200">
                          <span className="h-2 w-2 rounded-full bg-emerald-500" />
                          Active
                        </span>
                      </div>
                      <div className="flex flex-wrap items-center justify-end gap-2 md:col-span-4">
                        <div className="flex flex-col items-end gap-1">
                          <button
                            type="button"
                            onClick={() => void handleTestConnection(c)}
                            className={cx(
                              "inline-flex min-h-[44px] min-w-[44px] items-center justify-center gap-2 rounded-full border px-4 py-2.5 text-sm font-semibold shadow-sm transition",
                              connectionVerificationById[c.id]?.status === "verified"
                                ? "border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-800/60 dark:bg-emerald-950/40 dark:text-emerald-200"
                                : "border-slate-200 bg-white text-slate-700 hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-200 dark:hover:bg-slate-900"
                            )}
                            disabled={connectionVerificationById[c.id]?.status === "verifying"}
                            title={connectionVerificationById[c.id]?.status === "error" ? connectionVerificationById[c.id]?.message : undefined}
                          >
                            {connectionVerificationById[c.id]?.status === "verifying" ? (
                              <>
                                <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
                                Testing
                              </>
                            ) : connectionVerificationById[c.id]?.status === "verified" ? (
                              <>
                                <BadgeCheck className="h-5 w-5 shrink-0" />
                                Verified
                              </>
                            ) : connectionVerificationById[c.id]?.status === "error" ? (
                              <>
                                <TriangleAlert className="h-5 w-5 shrink-0" />
                                Failed
                              </>
                            ) : (
                              "Test Connection"
                            )}
                          </button>
                          {connectionVerificationById[c.id]?.status === "error" && connectionVerificationById[c.id]?.message && (
                            <span className="max-w-full text-right text-xs leading-tight text-rose-600 dark:text-rose-400 sm:max-w-[220px]" title={connectionVerificationById[c.id].message}>
                              {connectionVerificationById[c.id].message}
                            </span>
                          )}
                        </div>
                        <div className="flex flex-col items-end gap-1">
                          <button
                            type="button"
                            onClick={() => void handleSyncClaims(c)}
                            disabled={syncStatusById[c.id]?.status === "syncing"}
                            className={cx(
                              "inline-flex min-h-[44px] min-w-[44px] items-center justify-center gap-2 rounded-full border px-4 py-2.5 text-sm font-semibold shadow-sm transition",
                              syncStatusById[c.id]?.status === "done"
                                ? "border-sky-200 bg-sky-50 text-sky-800 dark:border-sky-800/60 dark:bg-sky-950/40 dark:text-sky-200"
                                : syncStatusById[c.id]?.status === "error"
                                ? "border-rose-200 bg-white text-rose-700 hover:bg-rose-50 dark:border-rose-900/60 dark:bg-slate-950 dark:text-rose-200"
                                : "border-slate-200 bg-white text-slate-700 hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-200 dark:hover:bg-slate-900",
                              syncStatusById[c.id]?.status === "syncing" && "cursor-not-allowed opacity-70"
                            )}
                          >
                            {syncStatusById[c.id]?.status === "syncing" ? (
                              <>
                                <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
                                Syncing…
                              </>
                            ) : syncStatusById[c.id]?.status === "done" ? (
                              <>
                                <CheckCircle2 className="h-5 w-5 shrink-0" />
                                {syncStatusById[c.id].claimsCount} Claims Synced
                              </>
                            ) : syncStatusById[c.id]?.status === "error" ? (
                              <>
                                <TriangleAlert className="h-5 w-5 shrink-0" />
                                Sync Failed
                              </>
                            ) : (
                              <>
                                <RefreshCw className="h-5 w-5 shrink-0" />
                                Sync Claims
                              </>
                            )}
                          </button>
                          {syncStatusById[c.id]?.status === "error" && syncStatusById[c.id]?.message && (
                            <span className="max-w-full text-right text-xs leading-tight text-rose-600 dark:text-rose-400 sm:max-w-[220px]">
                              {syncStatusById[c.id].message}
                            </span>
                          )}
                        </div>
                        <button type="button" onClick={() => openEdit(c)} className="inline-flex min-h-[44px] min-w-[44px] items-center justify-center gap-2 rounded-full border border-slate-200 bg-white px-4 py-2.5 text-sm font-semibold text-slate-700 shadow-sm transition hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-200 dark:hover:bg-slate-900">
                          <Pencil className="h-5 w-5" />
                          Edit
                        </button>
                        <button
                          type="button"
                          onClick={() => void handleDelete(c)}
                          disabled={isDeletingId === c.id}
                          className={cx(
                            "inline-flex min-h-[44px] min-w-[44px] items-center justify-center gap-2 rounded-full border px-4 py-2.5 text-sm font-semibold shadow-sm transition",
                            "border-rose-200 bg-white text-rose-700 hover:bg-rose-50",
                            "dark:border-rose-900/60 dark:bg-slate-950 dark:text-rose-200 dark:hover:bg-rose-950/30",
                            isDeletingId === c.id && "cursor-not-allowed opacity-70"
                          )}
                        >
                          <Trash2 className="h-5 w-5" />
                          {isDeletingId === c.id ? "Deleting" : "Delete"}
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </section>
      </div>

      {/* Connection Modal / Drawer: mobile = slide-up drawer, desktop = centered modal */}
      {isModalOpen && activeKey && activeCard && (
        <div
          className="fixed inset-0 z-50 flex flex-col justify-end sm:flex-row sm:items-center sm:justify-center sm:p-4"
          role="dialog"
          aria-modal="true"
          aria-label="Connect marketplace"
        >
          <button
            type="button"
            className="absolute inset-0 bg-slate-950/30 backdrop-blur-2xl sm:bg-slate-950/40"
            onClick={() => { setIsModalOpen(false); setActiveKey(null); setEditingId(null); setInlineError(null); }}
            aria-label="Close modal"
          />
          <div
            className={cx(
              "relative flex w-full flex-col overflow-y-auto bg-white shadow-2xl dark:bg-slate-950",
              "sm:max-h-[85vh] sm:max-w-lg sm:rounded-2xl sm:border sm:border-slate-200 sm:bg-white/95 sm:shadow-2xl sm:backdrop-blur-xl sm:dark:border-slate-800 sm:dark:bg-slate-950/95",
              "max-h-[90vh] rounded-t-3xl border-0 border-t border-border",
              "animate-drawer-slide-up"
            )}
          >
            {/* Drawer handle (mobile only) */}
            <div className="flex justify-center pt-3 sm:hidden">
              <div className="h-1 w-10 rounded-full bg-slate-300 dark:bg-slate-600" />
            </div>
            <div className="flex items-start justify-between gap-4 border-b border-slate-200 px-4 py-4 dark:border-slate-800 sm:px-6 sm:py-5">
              <div className="flex items-start gap-4">
                {(() => {
                  const Icon = ICON_MAP[activeCard.iconKey] ?? ShoppingBag;
                  return (
                    <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-slate-900/5 ring-1 ring-slate-200 dark:bg-white/5 dark:ring-slate-800">
                      <Icon className="h-6 w-6 text-slate-700 dark:text-slate-200" />
                    </div>
                  );
                })()}
                <div className="min-w-0 flex-1">
                  <p className="text-base font-semibold text-slate-900 dark:text-slate-50">{editingId ? "Edit" : "Connect"} {activeCard.name}</p>
                  <p className="mt-1 text-sm text-slate-600 dark:text-slate-300">
                    {editingId ? "Update nickname or credentials." : "Add credentials to enable ingestion and automation."}
                  </p>
                </div>
              </div>
              <button
                type="button"
                onClick={() => { setIsModalOpen(false); setActiveKey(null); setEditingId(null); setInlineError(null); }}
                className="inline-flex min-h-[44px] min-w-[44px] shrink-0 items-center justify-center rounded-full border border-slate-200 bg-white text-slate-700 shadow-sm transition hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-200 dark:hover:bg-slate-900"
                aria-label="Close"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="px-4 py-4 sm:px-6 sm:py-6">
              <div className="grid gap-4">
                <label className="grid gap-2">
                  <span className="text-sm font-medium text-slate-700 dark:text-slate-200">Nickname</span>
                  <input
                    value={formsByKey[activeKey]?.nickname ?? ""}
                    onChange={(e) => updateField(activeKey, "nickname", e.target.value)}
                    placeholder='e.g. "Main Store"'
                    className="min-h-[44px] w-full rounded-xl border border-slate-200 bg-white px-4 text-base text-slate-900 shadow-sm outline-none transition placeholder:text-slate-400 focus:border-sky-500/70 focus:ring-2 focus:ring-sky-500/20 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-50 dark:placeholder:text-slate-500 dark:focus:border-sky-400/70 dark:focus:ring-sky-400/20"
                  />
                </label>

                {activeCard.formFields.map((f) => (
                  <label key={f.key} className="grid gap-2">
                    {f.type === "password" ? (
                      <div className="flex items-center justify-between gap-4">
                        <span className="text-sm font-medium text-slate-700 dark:text-slate-200">{f.label}</span>
                        <span className="font-mono text-xs text-slate-500">{maskSecret(formsByKey[activeKey]?.[f.key] ?? "")}</span>
                      </div>
                    ) : (
                      <span className="text-sm font-medium text-slate-700 dark:text-slate-200">{f.label}</span>
                    )}
                    <input
                      type={f.type}
                      value={formsByKey[activeKey]?.[f.key] ?? ""}
                      onChange={(e) => updateField(activeKey, f.key, e.target.value)}
                      placeholder={f.placeholder}
                      className="min-h-[44px] w-full rounded-xl border border-slate-200 bg-white px-4 text-base text-slate-900 shadow-sm outline-none transition placeholder:text-slate-400 focus:border-sky-500/70 focus:ring-2 focus:ring-sky-500/20 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-50 dark:placeholder:text-slate-500 dark:focus:border-sky-400/70 dark:focus:ring-sky-400/20"
                    />
                  </label>
                ))}

                {editingId && (
                  <p className="text-sm text-muted-foreground">
                    Leave credential fields blank to keep existing values.
                  </p>
                )}

                <div className="mt-4 flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-end sm:gap-4">
                  <button
                    type="button"
                    onClick={() => { if (!isSaving) { setIsModalOpen(false); setActiveKey(null); setEditingId(null); setInlineError(null); } }}
                    className="inline-flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full border border-slate-200 bg-white px-3 py-2.5 text-sm font-semibold text-slate-700 shadow-sm transition hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-200 dark:hover:bg-slate-900"
                  >
                    Cancel
                  </button>
                  <button
                    type="button"
                    onClick={() => void handleSave(activeKey)}
                    disabled={isSaving}
                    className={cx(
                      "inline-flex min-h-[44px] min-w-[44px] items-center justify-center gap-2 rounded-full bg-sky-600 px-4 py-2.5 text-sm font-semibold text-white shadow-sm transition focus:outline-none focus:ring-2 focus:ring-sky-500/30 dark:bg-sky-500 dark:hover:bg-sky-400 dark:focus:ring-sky-400/30",
                      isSaving ? "cursor-not-allowed opacity-70" : "hover:bg-sky-500"
                    )}
                  >
                    {isSaving ? <><Loader2 className="h-5 w-5 animate-spin" /> Saving</> : editingId ? "Update Connection" : "Save Connection"}
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
