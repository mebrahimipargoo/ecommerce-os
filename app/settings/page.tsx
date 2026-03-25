"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import {
  ArrowLeft, CheckCircle2, ChevronDown, ChevronUp, Cpu, Globe, HardDrive,
  KeyRound, Loader2, Plus, Printer, Save, ScanLine, ShieldAlert, Tag,
  Trash2, Wifi, X, Zap,
} from "lucide-react";
import {
  clearAIUnifiedKeyFromStorage,
  clearGeminiApiKeyFromStorage,
  clearOpenAIApiKeyFromStorage,
  DEFAULT_BASE_URLS,
  getAIConfigsFromStorage,
  getAIRoleAssignmentsFromStorage,
  getBarcodeModeFromStorage,
  getLabelPrinterFromStorage,
  setAIConfigsInStorage,
  setAIRoleAssignmentsInStorage,
  setBarcodeModeInStorage,
  setLabelPrinterInStorage,
  type AIConfig,
  type AIConfigStatus,
  type AIProvider,
  type AIRole,
  type AIRoleAssignments,
  type BarcodeMode,
  type LabelPrinter,
} from "../../lib/openai-settings";
import { useUserRole } from "../../components/UserRoleContext";

// ─── constants ────────────────────────────────────────────────────────────────

type ToastState = { msg: string; ok: boolean } | null;
type TabId = "ai" | "hardware";

const SELECT_CLS =
  "h-12 w-full rounded-xl border border-border bg-background px-4 text-sm focus:border-sky-500 focus:outline-none focus:ring-2 focus:ring-sky-500/20";
const INPUT_CLS =
  "h-12 w-full rounded-xl border border-border bg-background px-4 text-sm focus:border-sky-500 focus:outline-none focus:ring-2 focus:ring-sky-500/20";
const LABEL_CLS  = "mb-1.5 block text-sm font-semibold";
const HINT_CLS   = "mb-2 text-xs text-muted-foreground";

const PROVIDER_LABELS: Record<AIProvider, string> = {
  openai: "OpenAI",
  gemini: "Google Gemini",
  custom: "Custom / Other",
};

const STATUS_META: Record<AIConfigStatus, { label: string; cls: string }> = {
  untested: { label: "Untested",  cls: "border-slate-200 bg-slate-50 text-slate-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-400" },
  active:   { label: "Active ✓", cls: "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-700/50 dark:bg-emerald-950/40 dark:text-emerald-300" },
  testing:  { label: "Testing…",  cls: "border-amber-200 bg-amber-50 text-amber-600 dark:border-amber-700/50 dark:bg-amber-950/40 dark:text-amber-300" },
  error:    { label: "Error",     cls: "border-rose-200 bg-rose-50 text-rose-600 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-400" },
};

function newBlankConfig(): Omit<AIConfig, "id"> {
  return {
    providerName: "",
    provider:     "openai",
    baseURL:      DEFAULT_BASE_URLS.openai,
    apiKey:       "",
    role:         "default",
    status:       "untested",
    isGlobalOverride: false,
  };
}

// ─── Main page ────────────────────────────────────────────────────────────────

export default function SettingsPage() {
  const { role } = useUserRole();

  const [activeTab, setActiveTab] = useState<TabId>("ai");
  const [mounted,   setMounted]   = useState(false);

  // ── AI configs (multi-API) ─────────────────────────────────────────────────
  const [configs,    setConfigs]    = useState<AIConfig[]>([]);
  const [showForm,   setShowForm]   = useState(false);
  const [formData,   setFormData]   = useState<Omit<AIConfig, "id">>(newBlankConfig());
  const [testingId,  setTestingId]  = useState<string | null>(null);
  const [savingForm, setSavingForm] = useState(false);

  // ── Role assignments ───────────────────────────────────────────────────────
  const [assignments, setAssignments] = useState<AIRoleAssignments>({ defaultGeneral: null, defaultVision: null });

  // ── Hardware ───────────────────────────────────────────────────────────────
  const [barcodeMode,  setBarcodeMode]  = useState<BarcodeMode>("physical");
  const [labelPrinter, setLabelPrinter] = useState<LabelPrinter>("system");
  const [hwSaved,      setHwSaved]      = useState(false);

  const [toast, setToast] = useState<ToastState>(null);

  // ── Load from storage ──────────────────────────────────────────────────────
  useEffect(() => {
    setMounted(true);
    setConfigs(getAIConfigsFromStorage());
    setAssignments(getAIRoleAssignmentsFromStorage());
    setBarcodeMode(getBarcodeModeFromStorage());
    setLabelPrinter(getLabelPrinterFromStorage());
  }, []);

  function showToast(msg: string, ok: boolean) {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 4500);
  }

  // ── Provider helpers ───────────────────────────────────────────────────────
  function handleFormProviderChange(p: AIProvider) {
    setFormData((prev) => ({
      ...prev,
      provider: p,
      baseURL: p !== "custom" ? DEFAULT_BASE_URLS[p] : prev.baseURL,
    }));
  }

  // ── Test connection ────────────────────────────────────────────────────────
  async function runTest(cfg: Pick<AIConfig, "provider" | "baseURL" | "apiKey">, id: string | null) {
    if (!cfg.apiKey.trim()) { showToast("Enter an API key first.", false); return; }
    if (!cfg.baseURL.trim()) { showToast("Enter a Base URL first.", false); return; }
    const resolvedId = id ?? "__form__";
    setTestingId(resolvedId);

    // Mark existing config as "testing"
    if (id) {
      const updated = configs.map((c) => c.id === id ? { ...c, status: "testing" as AIConfigStatus } : c);
      setConfigs(updated);
      setAIConfigsInStorage(updated);
    }

    let ok = false;
    try {
      if (cfg.provider === "gemini") {
        const res = await fetch(
          `${cfg.baseURL.replace(/\/$/, "")}/models?key=${encodeURIComponent(cfg.apiKey.trim())}`,
        );
        ok = res.ok;
        if (res.ok) {
          showToast("Google Gemini connection successful ✓", true);
        } else {
          const body = await res.json().catch(() => ({}));
          showToast(`Gemini error ${res.status}: ${body?.error?.message ?? "Invalid key"}`, false);
        }
      } else {
        const res = await fetch(`${cfg.baseURL.replace(/\/$/, "")}/models`, {
          headers: { Authorization: `Bearer ${cfg.apiKey.trim()}` },
        });
        ok = res.ok;
        if (res.ok) {
          showToast(`${cfg.provider === "openai" ? "OpenAI" : "Custom API"} connection successful ✓`, true);
        } else {
          const body = await res.json().catch(() => ({}));
          showToast(`Error ${res.status}: ${body?.error?.message ?? "Connection failed"}`, false);
        }
      }
    } catch {
      showToast("Network error — check your connection or CORS policy.", false);
    } finally {
      setTestingId(null);
      if (id) {
        const newStatus: AIConfigStatus = ok ? "active" : "error";
        const updated = configs.map((c) => c.id === id ? { ...c, status: newStatus } : c);
        setConfigs(updated);
        setAIConfigsInStorage(updated);
      }
    }
  }

  // ── Save new config ────────────────────────────────────────────────────────
  function handleAddConfig(e: React.FormEvent) {
    e.preventDefault();
    if (!formData.apiKey.trim())  { showToast("API key is required.", false); return; }
    if (!formData.baseURL.trim()) { showToast("Base URL is required.", false); return; }
    setSavingForm(true);

    // If this is flagged as global override, clear override on all others
    let base = configs.map((c) =>
      formData.isGlobalOverride ? { ...c, isGlobalOverride: false } : c,
    );

    const newCfg: AIConfig = {
      ...formData,
      id:           `cfg_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
      providerName: formData.providerName.trim() || PROVIDER_LABELS[formData.provider],
      apiKey:       formData.apiKey.trim(),
      baseURL:      formData.baseURL.trim(),
      status:       "untested",
    };
    const updated = [...base, newCfg];
    setConfigs(updated);
    setAIConfigsInStorage(updated);
    setFormData(newBlankConfig());
    setShowForm(false);
    setSavingForm(false);
    showToast("API connection saved.", true);
  }

  // ── Toggle global override on a saved config ───────────────────────────────
  function handleToggleGlobalOverride(id: string) {
    const target = configs.find((c) => c.id === id);
    if (!target) return;
    const willBeGlobal = !target.isGlobalOverride;
    const updated = configs.map((c) => ({
      ...c,
      isGlobalOverride: c.id === id ? willBeGlobal : false,
    }));
    setConfigs(updated);
    setAIConfigsInStorage(updated);
    showToast(
      willBeGlobal
        ? `"${target.providerName}" is now the global provider for all tasks.`
        : `Global override removed.`,
      true,
    );
  }

  // ── Update role tag for an existing config ─────────────────────────────────
  function handleRoleChange(id: string, newRole: AIRole) {
    const updated = configs.map((c) => c.id === id ? { ...c, role: newRole } : c);
    setConfigs(updated);
    setAIConfigsInStorage(updated);
  }

  // ── Save role assignments ──────────────────────────────────────────────────
  function handleSaveAssignments() {
    setAIRoleAssignmentsInStorage(assignments);
    showToast("Role assignments saved.", true);
  }

  // ── Delete config — guarded by confirmation ───────────────────────────────
  function handleDeleteConfig(id: string) {
    if (!window.confirm("Are you sure you want to remove this API configuration? This cannot be undone.")) return;
    const updated = configs.filter((c) => c.id !== id);
    setConfigs(updated);
    setAIConfigsInStorage(updated);
    // Clean up role assignments if they referenced this id
    const newAssign: AIRoleAssignments = {
      defaultGeneral: assignments.defaultGeneral === id ? null : assignments.defaultGeneral,
      defaultVision:  assignments.defaultVision  === id ? null : assignments.defaultVision,
    };
    setAssignments(newAssign);
    setAIRoleAssignmentsInStorage(newAssign);
    showToast("Configuration removed.", true);
  }

  // ── Clear all (legacy + configs) — guarded ────────────────────────────────
  function handleClearAll() {
    if (!window.confirm("Are you sure you want to delete ALL saved API configurations and keys? This cannot be undone.")) return;
    clearAIUnifiedKeyFromStorage();
    clearOpenAIApiKeyFromStorage();
    clearGeminiApiKeyFromStorage();
    setAIConfigsInStorage([]);
    setAIRoleAssignmentsInStorage({ defaultGeneral: null, defaultVision: null });
    setConfigs([]);
    setAssignments({ defaultGeneral: null, defaultVision: null });
    showToast("All API configurations cleared.", true);
  }

  // ── Save hardware settings ─────────────────────────────────────────────────
  function handleSaveHardware(e: React.FormEvent) {
    e.preventDefault();
    setBarcodeModeInStorage(barcodeMode);
    setLabelPrinterInStorage(labelPrinter);
    setHwSaved(true);
    setTimeout(() => setHwSaved(false), 2500);
  }

  // ── Access guard (hide entirely — do not show blocked page to operators) ──
  if (!mounted) {
    return (
      <div className="flex min-h-[60vh] items-center justify-center">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (role !== "admin") return null;

  const globalProvider = configs.find((c) => c.isGlobalOverride) ?? null;
  const isOverridden = globalProvider !== null;

  // ── Main UI ───────────────────────────────────────────────────────────────
  return (
    <div className="mx-auto max-w-2xl px-4 py-8">
      <Link
        href="/"
        className="mb-6 inline-flex items-center gap-2 text-sm font-medium text-muted-foreground transition hover:text-foreground"
      >
        <ArrowLeft className="h-4 w-4" />
        Back
      </Link>

      {/* Page header */}
      <div className="mb-6 flex items-center gap-3">
        <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-violet-100 dark:bg-violet-950/50">
          <KeyRound className="h-6 w-6 text-violet-600 dark:text-violet-400" />
        </div>
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Settings</h1>
          <p className="text-sm text-muted-foreground">
            Configuration stored locally in this browser. Admin-only.
          </p>
        </div>
      </div>

      {/* Toast */}
      {toast && (
        <div
          className={[
            "mb-6 flex items-center gap-3 rounded-2xl border px-4 py-3 text-sm font-medium",
            toast.ok
              ? "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-700/50 dark:bg-emerald-950/30 dark:text-emerald-300"
              : "border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-400",
          ].join(" ")}
        >
          {toast.ok
            ? <CheckCircle2 className="h-4 w-4 shrink-0" />
            : <ShieldAlert   className="h-4 w-4 shrink-0" />}
          {toast.msg}
        </div>
      )}

      {/* ── Tab bar ──────────────────────────────────────────────────────────── */}
      <div className="flex gap-1 rounded-2xl border border-border bg-muted/40 p-1">
        {(["ai", "hardware"] as TabId[]).map((tab) => (
          <button
            key={tab}
            type="button"
            onClick={() => setActiveTab(tab)}
            className={[
              "flex flex-1 items-center justify-center gap-2 rounded-xl px-4 py-2.5 text-sm font-semibold transition-all",
              activeTab === tab
                ? "bg-background text-foreground shadow-sm"
                : "text-muted-foreground hover:text-foreground",
            ].join(" ")}
          >
            {tab === "ai"
              ? <Cpu       className="h-4 w-4" />
              : <HardDrive className="h-4 w-4" />}
            {tab === "ai" ? "AI & System" : "Hardware & Devices"}
          </button>
        ))}
      </div>

      {/* ════════════════ AI & SYSTEM TAB ════════════════ */}
      {activeTab === "ai" && (
        <div className="mt-6 space-y-4">

          {/* ── Global override banner ──────────────────────────────────── */}
          {isOverridden && (
            <div className="flex items-center gap-3 rounded-2xl border-2 border-violet-300 bg-violet-50 px-4 py-3 dark:border-violet-600/50 dark:bg-violet-950/30">
              <Zap className="h-5 w-5 shrink-0 text-violet-600 dark:text-violet-400" />
              <div className="flex-1 min-w-0">
                <p className="text-sm font-bold text-violet-700 dark:text-violet-300">
                  Global Override Active
                </p>
                <p className="truncate text-xs text-violet-600 dark:text-violet-400">
                  All tasks are routed through <strong>{globalProvider!.providerName}</strong>.
                  Role assignments are disabled.
                </p>
              </div>
            </div>
          )}

          {/* ── Saved connections list ──────────────────────────────────────── */}
          <div className="rounded-2xl border border-border bg-card p-6 shadow-sm">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h2 className="text-base font-bold">API Connections</h2>
                <p className="text-xs text-muted-foreground">
                  Add multiple providers — assign roles below, or use the global override.
                </p>
              </div>
              <button
                type="button"
                onClick={() => setShowForm((v) => !v)}
                className="inline-flex shrink-0 items-center gap-1.5 rounded-xl border border-sky-200 bg-sky-50 px-3 py-2 text-xs font-semibold text-sky-700 transition hover:bg-sky-100 dark:border-sky-700/50 dark:bg-sky-950/40 dark:text-sky-300"
              >
                {showForm ? <ChevronUp className="h-3.5 w-3.5" /> : <Plus className="h-3.5 w-3.5" />}
                {showForm ? "Cancel" : "Add Connection"}
              </button>
            </div>

            {/* Empty state */}
            {configs.length === 0 && !showForm && (
              <div className="flex flex-col items-center gap-2 rounded-2xl border border-dashed border-border bg-muted/30 py-10 text-center">
                <KeyRound className="h-8 w-8 text-muted-foreground/40" />
                <p className="text-sm font-medium text-muted-foreground">No API connections saved yet.</p>
                <p className="text-xs text-muted-foreground">Click <strong>Add Connection</strong> to get started.</p>
              </div>
            )}

            {/* Config cards */}
            {configs.length > 0 && (
              <div className="space-y-3">
                {configs.map((cfg) => {
                  const isTesting    = testingId === cfg.id;
                  const statusMeta   = STATUS_META[cfg.status ?? "untested"];
                  const isGlobal     = !!cfg.isGlobalOverride;
                  return (
                    <div
                      key={cfg.id}
                      className={[
                        "group rounded-xl border bg-background p-4 transition",
                        isGlobal
                          ? "border-violet-300 ring-1 ring-violet-200 dark:border-violet-600/60 dark:ring-violet-800/40"
                          : "border-border hover:border-sky-300 dark:hover:border-sky-700",
                      ].join(" ")}
                    >
                      {/* Top row: name + badges + actions */}
                      <div className="flex flex-wrap items-start gap-2">
                        {/* Provider name */}
                        <p className="font-semibold text-sm text-foreground leading-tight min-w-0 flex-1">
                          {cfg.providerName || PROVIDER_LABELS[cfg.provider]}
                        </p>

                        {/* Global override badge */}
                        {isGlobal && (
                          <span className="inline-flex items-center gap-1 rounded-full border border-violet-200 bg-violet-50 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide text-violet-700 dark:border-violet-700/50 dark:bg-violet-950/40 dark:text-violet-300">
                            <Zap className="h-3 w-3" />Global
                          </span>
                        )}

                        {/* Status badge */}
                        <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold ${statusMeta.cls}`}>
                          {isTesting ? "Testing…" : statusMeta.label}
                        </span>

                        {/* Provider type */}
                        <span className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[10px] font-semibold text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300">
                          {PROVIDER_LABELS[cfg.provider]}
                        </span>
                      </div>

                      {/* URL + masked key */}
                      <p className="mt-1.5 font-mono text-[11px] text-muted-foreground truncate">{cfg.baseURL}</p>
                      <p className="mt-0.5 font-mono text-[11px] text-muted-foreground">
                        {cfg.apiKey.length > 8
                          ? `${cfg.apiKey.slice(0, 4)}${"•".repeat(Math.min(cfg.apiKey.length - 6, 20))}${cfg.apiKey.slice(-2)}`
                          : "••••••••"}
                      </p>

                      {/* Bottom row: role tag + global toggle + actions */}
                      <div className="mt-3 flex flex-wrap items-center gap-2">
                        {/* Role tag dropdown */}
                        <select
                          value={cfg.role}
                          onChange={(e) => handleRoleChange(cfg.id, e.target.value as AIRole)}
                          className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-0.5 text-[11px] font-semibold text-slate-600 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"
                          aria-label="Role tag"
                        >
                          <option value="default">Tag: Default</option>
                          <option value="ocr_vision">Tag: OCR / Vision</option>
                        </select>

                        {/* "Use for all tasks" toggle */}
                        <label className="flex cursor-pointer items-center gap-1.5">
                          <input
                            type="checkbox"
                            checked={isGlobal}
                            onChange={() => handleToggleGlobalOverride(cfg.id)}
                            className="h-3.5 w-3.5 rounded accent-violet-600"
                          />
                          <span className="text-[11px] font-medium text-muted-foreground">
                            Use for all tasks
                          </span>
                        </label>

                        <div className="flex-1" />

                        {/* Test */}
                        <button
                          type="button"
                          onClick={() => runTest(cfg, cfg.id)}
                          disabled={isTesting}
                          className="inline-flex items-center gap-1.5 rounded-lg border border-border px-2.5 py-1 text-xs font-medium text-muted-foreground transition hover:border-sky-300 hover:text-sky-600 disabled:opacity-50"
                        >
                          {isTesting
                            ? <Loader2 className="h-3.5 w-3.5 animate-spin" />
                            : <Wifi    className="h-3.5 w-3.5" />}
                          {isTesting ? "Testing…" : "Test"}
                        </button>

                        {/* Delete — guarded */}
                        <button
                          type="button"
                          onClick={() => handleDeleteConfig(cfg.id)}
                          title="Remove this API configuration"
                          className="inline-flex items-center gap-1.5 rounded-lg border border-rose-200 bg-rose-50 px-2.5 py-1 text-xs font-medium text-rose-600 transition hover:bg-rose-100 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-400"
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                          Remove
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* ── Add new connection form ────────────────────────────────────── */}
          {showForm && (
            <form
              onSubmit={handleAddConfig}
              className="space-y-5 rounded-2xl border border-sky-200 bg-sky-50/40 p-6 shadow-sm dark:border-sky-700/50 dark:bg-sky-950/20"
            >
              <div className="flex items-center justify-between">
                <h3 className="text-sm font-bold text-sky-800 dark:text-sky-200">New API Connection</h3>
                <button
                  type="button"
                  onClick={() => { setShowForm(false); setFormData(newBlankConfig()); }}
                  className="rounded-full p-1 text-muted-foreground hover:bg-accent"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>

              {/* Friendly name */}
              <div>
                <label className={LABEL_CLS}>
                  Connection Name
                </label>
                <p className={HINT_CLS}>A label to identify this connection (e.g. &quot;Gemini Flash — Chat&quot;).</p>
                <input
                  type="text"
                  value={formData.providerName}
                  onChange={(e) => setFormData((p) => ({ ...p, providerName: e.target.value }))}
                  placeholder="e.g. GPT-4o Vision"
                  className={INPUT_CLS}
                />
              </div>

              {/* Provider */}
              <div>
                <label className={LABEL_CLS}>Provider Type</label>
                <select
                  value={formData.provider}
                  onChange={(e) => handleFormProviderChange(e.target.value as AIProvider)}
                  className={SELECT_CLS}
                >
                  <option value="openai">OpenAI  (GPT-4o, o3, o4-mini…)</option>
                  <option value="gemini">Google Gemini</option>
                  <option value="custom">Custom / Other  (Ollama, Groq, Azure OpenAI, LM Studio…)</option>
                </select>
              </div>

              {/* Role tag */}
              <div>
                <label className={LABEL_CLS}>
                  <Tag className="mr-1 inline h-3.5 w-3.5" />
                  Role Tag
                </label>
                <p className={HINT_CLS}>Tag this connection — used by the Role Assignment section below.</p>
                <select
                  value={formData.role}
                  onChange={(e) => setFormData((p) => ({ ...p, role: e.target.value as AIRole }))}
                  className={SELECT_CLS}
                >
                  <option value="default">Default — general AI tasks</option>
                  <option value="ocr_vision">OCR / Vision — image analysis &amp; packing-slip scanning</option>
                </select>
              </div>

              {/* Base URL */}
              <div>
                <label className={LABEL_CLS}>Base URL</label>
                <p className={HINT_CLS}>Auto-filled for known providers; edit freely for custom deployments.</p>
                <input
                  type="text"
                  autoComplete="off"
                  spellCheck={false}
                  value={formData.baseURL}
                  onChange={(e) => setFormData((p) => ({ ...p, baseURL: e.target.value }))}
                  placeholder="https://api.openai.com/v1"
                  className={`${INPUT_CLS} font-mono`}
                />
                {formData.provider === "custom" && (
                  <p className="mt-1.5 text-xs text-amber-600 dark:text-amber-400">
                    Custom providers: ensure CORS allows requests from this origin, or proxy via your backend.
                  </p>
                )}
              </div>

              {/* API Key */}
              <div>
                <label className={LABEL_CLS}>API Key</label>
                <p className={HINT_CLS}>
                  Stored only in this browser&apos;s localStorage — never sent to our servers.
                </p>
                <div className="flex gap-2">
                  <input
                    type="password"
                    autoComplete="off"
                    value={formData.apiKey}
                    onChange={(e) => setFormData((p) => ({ ...p, apiKey: e.target.value }))}
                    placeholder={
                      formData.provider === "openai" ? "sk-…"
                      : formData.provider === "gemini" ? "AIza…"
                      : "Your API key…"
                    }
                    className={`${INPUT_CLS} min-w-0 flex-1 font-mono`}
                  />
                  <button
                    type="button"
                    onClick={() => runTest(formData, null)}
                    disabled={testingId === "__form__"}
                    title="Test connection"
                    className="inline-flex h-12 shrink-0 items-center gap-2 rounded-xl border border-sky-200 bg-sky-50 px-4 text-sm font-semibold text-sky-700 transition hover:bg-sky-100 disabled:opacity-50 dark:border-sky-700/50 dark:bg-sky-950/40 dark:text-sky-300"
                  >
                    {testingId === "__form__"
                      ? <Loader2 className="h-4 w-4 animate-spin" />
                      : <Wifi    className="h-4 w-4" />}
                    {testingId === "__form__" ? "Testing…" : "Test"}
                  </button>
                </div>
                <p className="mt-1.5 text-xs text-muted-foreground">
                  Test fires{" "}
                  <code className="rounded bg-muted px-1 py-0.5 font-mono text-[11px]">
                    GET {"{baseURL}"}/models
                  </code>.
                </p>
              </div>

              {/* Use for all tasks */}
              <div className="rounded-xl border border-violet-200 bg-violet-50/60 p-4 dark:border-violet-700/40 dark:bg-violet-950/20">
                <label className="flex cursor-pointer items-start gap-3">
                  <input
                    type="checkbox"
                    checked={!!formData.isGlobalOverride}
                    onChange={(e) => setFormData((p) => ({ ...p, isGlobalOverride: e.target.checked }))}
                    className="mt-0.5 h-4 w-4 rounded accent-violet-600"
                  />
                  <div>
                    <p className="text-sm font-bold text-violet-800 dark:text-violet-200">
                      Use this configuration for all roles
                    </p>
                    <p className="mt-0.5 text-xs text-violet-700 dark:text-violet-400">
                      When checked, this API becomes the sole provider for every task (general chat AND
                      OCR/Vision). Role assignment dropdowns will be disabled.
                    </p>
                  </div>
                </label>
              </div>

              <div className="flex gap-2 pt-1">
                <button
                  type="submit"
                  disabled={savingForm}
                  className="inline-flex flex-1 items-center justify-center gap-2 rounded-xl bg-sky-600 py-3 text-sm font-semibold text-white transition hover:bg-sky-700 disabled:opacity-50"
                >
                  <Save className="h-4 w-4" />
                  Save Connection
                </button>
              </div>
            </form>
          )}

          {/* ── Role Assignment section ────────────────────────────────────── */}
          {configs.length > 0 && (
            <div
              className={[
                "rounded-2xl border bg-card p-6 shadow-sm transition",
                isOverridden
                  ? "border-border opacity-60 pointer-events-none select-none"
                  : "border-border",
              ].join(" ")}
            >
              <div className="mb-4">
                <div className="flex items-center gap-2">
                  <Globe className="h-4 w-4 text-sky-600 dark:text-sky-400" />
                  <h2 className="text-base font-bold">Role Assignment</h2>
                  {isOverridden && (
                    <span className="ml-1 inline-flex items-center gap-1 rounded-full border border-violet-200 bg-violet-50 px-2 py-0.5 text-[10px] font-bold text-violet-700 dark:border-violet-700/50 dark:bg-violet-950/40 dark:text-violet-300">
                      <Zap className="h-3 w-3" />Overridden by Global Provider
                    </span>
                  )}
                </div>
                <p className="mt-1 text-xs text-muted-foreground">
                  Choose which saved API handles each task type. Example: fast model for chat, vision model for packing slips.
                </p>
              </div>

              <div className="grid gap-5 sm:grid-cols-2">
                {/* Default General */}
                <div>
                  <label className={LABEL_CLS}>Default General API</label>
                  <p className={HINT_CLS}>Used for general chat, decision-making, and summaries.</p>
                  <select
                    value={assignments.defaultGeneral ?? ""}
                    onChange={(e) => setAssignments((p) => ({ ...p, defaultGeneral: e.target.value || null }))}
                    disabled={isOverridden}
                    className={SELECT_CLS}
                  >
                    <option value="">— Not assigned —</option>
                    {configs.map((c) => (
                      <option key={c.id} value={c.id}>
                        {c.providerName || PROVIDER_LABELS[c.provider]}
                      </option>
                    ))}
                  </select>
                </div>

                {/* Default OCR/Vision */}
                <div>
                  <label className={LABEL_CLS}>Default OCR / Vision API</label>
                  <p className={HINT_CLS}>Used for packing-slip photos and image analysis.</p>
                  <select
                    value={assignments.defaultVision ?? ""}
                    onChange={(e) => setAssignments((p) => ({ ...p, defaultVision: e.target.value || null }))}
                    disabled={isOverridden}
                    className={SELECT_CLS}
                  >
                    <option value="">— Not assigned —</option>
                    {configs.map((c) => (
                      <option key={c.id} value={c.id}>
                        {c.providerName || PROVIDER_LABELS[c.provider]}
                      </option>
                    ))}
                  </select>
                </div>
              </div>

              {!isOverridden && (
                <div className="mt-5">
                  <button
                    type="button"
                    onClick={handleSaveAssignments}
                    className="inline-flex items-center gap-2 rounded-xl bg-sky-600 px-5 py-2.5 text-sm font-semibold text-white transition hover:bg-sky-700"
                  >
                    <Save className="h-4 w-4" />
                    Save Role Assignments
                  </button>
                </div>
              )}
            </div>
          )}

          {/* ── Clear all (destructive, guarded) ──────────────────────────── */}
          {configs.length > 0 && (
            <div className="flex justify-end">
              <button
                type="button"
                onClick={handleClearAll}
                className="inline-flex items-center gap-2 rounded-xl border border-rose-200 bg-rose-50 px-4 py-2.5 text-sm font-semibold text-rose-600 transition hover:bg-rose-100 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-400"
              >
                <Trash2 className="h-4 w-4" />
                Clear All Configurations
              </button>
            </div>
          )}
        </div>
      )}

      {/* ════════════════ HARDWARE & DEVICES TAB ════════════════ */}
      {activeTab === "hardware" && (
        <form onSubmit={handleSaveHardware} className="mt-6 space-y-6">
          <div className="space-y-6 rounded-2xl border border-border bg-card p-6 shadow-sm">

            {/* Barcode Input Mode */}
            <div>
              <div className="mb-2 flex items-center gap-2">
                <ScanLine className="h-4 w-4 text-sky-600 dark:text-sky-400" />
                <label className="text-sm font-semibold">Barcode Input Mode</label>
              </div>
              <p className={HINT_CLS}>
                Physical / keyboard-wedge scanners (e.g. Netum C750) emulate a keyboard — they type
                characters very fast and press Enter. <strong>No camera window opens.</strong>{" "}
                The app detects rapid keystrokes and fills the focused input automatically.
                Select &quot;Web / Camera&quot; only if you use a browser-based QR camera scanner.
              </p>
              <select
                value={barcodeMode}
                onChange={(e) => setBarcodeMode(e.target.value as BarcodeMode)}
                className={SELECT_CLS}
              >
                <option value="physical">Physical Scanner  (Keyboard Emulation — Netum, Honeywell, Zebra…)</option>
                <option value="camera">Web / Camera Scanner</option>
              </select>
              {barcodeMode === "physical" && (
                <p className="mt-2 flex items-center gap-1.5 text-xs font-medium text-emerald-600 dark:text-emerald-400">
                  <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
                  Keyboard-wedge mode — &quot;Scan&quot; buttons show a ready indicator; no camera window.
                  Rapid keystrokes (&lt;30 ms apart ending in Enter) are captured globally.
                </p>
              )}
              {barcodeMode === "camera" && (
                <p className="mt-2 flex items-center gap-1.5 text-xs font-medium text-sky-600 dark:text-sky-400">
                  <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
                  Camera mode — &quot;Scan&quot; buttons open a live QR/barcode camera modal.
                </p>
              )}
            </div>

            {/* Default Label Printer */}
            <div>
              <div className="mb-2 flex items-center gap-2">
                <Printer className="h-4 w-4 text-sky-600 dark:text-sky-400" />
                <label className="text-sm font-semibold">Default Label Printer</label>
              </div>
              <p className={HINT_CLS}>Select the default printer for shipping and return labels.</p>
              <select
                value={labelPrinter}
                onChange={(e) => setLabelPrinter(e.target.value as LabelPrinter)}
                className={SELECT_CLS}
              >
                <option value="system">System Default</option>
                <option value="zebra_zd410">Zebra ZD410</option>
                <option value="zebra_zd620">Zebra ZD620</option>
                <option value="brother_ql">Brother QL Series</option>
              </select>
            </div>

            {/* Info card */}
            <div className="rounded-xl border border-sky-200 bg-sky-50 px-4 py-3 dark:border-sky-700/50 dark:bg-sky-950/40">
              <p className="text-xs font-semibold text-sky-700 dark:text-sky-300">
                Physical Scanner Hook  (usePhysicalScanner)
              </p>
              <p className="mt-1 text-xs text-sky-600 dark:text-sky-400">
                A global <code className="rounded bg-sky-100 px-1 dark:bg-sky-900">keydown</code> listener
                measures inter-keystroke timing. When characters arrive in &lt;30 ms bursts followed by
                Enter, the string is captured as a barcode and broadcast to listening inputs — even
                when no input is focused. Wired into the <strong>Item</strong> and <strong>Package</strong> forms.
              </p>
            </div>
          </div>

          <div className="flex flex-wrap gap-2">
            <button
              type="submit"
              className="inline-flex min-w-[160px] flex-1 items-center justify-center gap-2 rounded-xl bg-sky-600 py-3 text-sm font-semibold text-white transition hover:bg-sky-700"
            >
              <Save className="h-4 w-4" />
              Save Hardware Settings
            </button>
          </div>

          {hwSaved && (
            <p className="text-sm font-medium text-emerald-600 dark:text-emerald-400">Saved.</p>
          )}
        </form>
      )}

      <p className="mt-6 text-center text-xs text-muted-foreground">
        <Link
          href="/settings/adapters"
          className="font-medium text-sky-600 underline hover:text-sky-700 dark:text-sky-400"
        >
          Marketplace adapters
        </Link>
      </p>
    </div>
  );
}
