"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import {
  ArrowLeft, CheckCircle2, Cpu, HardDrive, KeyRound,
  Loader2, Printer, Save, ScanLine, ShieldAlert, Trash2, Wifi,
} from "lucide-react";
import {
  clearAIUnifiedKeyFromStorage,
  clearGeminiApiKeyFromStorage,
  clearOpenAIApiKeyFromStorage,
  DEFAULT_BASE_URLS,
  getAIBaseURLFromStorage,
  getAIProviderFromStorage,
  getAIUnifiedKeyFromStorage,
  getBarcodeModeFromStorage,
  getGeminiApiKeyFromStorage,
  getLabelPrinterFromStorage,
  getOpenAIApiKeyFromStorage,
  setAIBaseURLInStorage,
  setAIProviderInStorage,
  setAIUnifiedKeyInStorage,
  setBarcodeModeInStorage,
  setGeminiApiKeyInStorage,
  setLabelPrinterInStorage,
  setOpenAIApiKeyInStorage,
  type AIProvider,
  type BarcodeMode,
  type LabelPrinter,
} from "../../lib/openai-settings";
import { useUserRole } from "../../components/UserRoleContext";

type ToastState = { msg: string; ok: boolean } | null;
type TabId = "ai" | "hardware";

const SELECT_CLS =
  "h-12 w-full rounded-xl border border-border bg-background px-4 text-sm focus:border-sky-500 focus:outline-none focus:ring-2 focus:ring-sky-500/20";
const INPUT_CLS =
  "h-12 w-full rounded-xl border border-border bg-background px-4 text-sm focus:border-sky-500 focus:outline-none focus:ring-2 focus:ring-sky-500/20";
const LABEL_CLS = "mb-1.5 block text-sm font-semibold";
const HINT_CLS  = "mb-2 text-xs text-muted-foreground";

export default function SettingsPage() {
  const { role } = useUserRole();

  const [activeTab, setActiveTab] = useState<TabId>("ai");

  // AI settings
  const [provider,  setProvider]  = useState<AIProvider>("openai");
  const [baseURL,   setBaseURL]   = useState(DEFAULT_BASE_URLS.openai);
  const [apiKey,    setApiKey]    = useState("");

  // Hardware settings
  const [barcodeMode,   setBarcodeMode]   = useState<BarcodeMode>("physical");
  const [labelPrinter,  setLabelPrinter]  = useState<LabelPrinter>("system");

  const [saved,   setSaved]   = useState(false);
  const [testing, setTesting] = useState(false);
  const [toast,   setToast]   = useState<ToastState>(null);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    const p = getAIProviderFromStorage();
    setProvider(p);
    setBaseURL(getAIBaseURLFromStorage(p));
    // Prefer unified key; fall back to per-provider legacy keys
    const unified = getAIUnifiedKeyFromStorage();
    if (unified) {
      setApiKey(unified);
    } else if (p === "openai") {
      setApiKey(getOpenAIApiKeyFromStorage() ?? "");
    } else if (p === "gemini") {
      setApiKey(getGeminiApiKeyFromStorage() ?? "");
    }
    setBarcodeMode(getBarcodeModeFromStorage());
    setLabelPrinter(getLabelPrinterFromStorage());
  }, []);

  function handleProviderChange(p: AIProvider) {
    setProvider(p);
    if (p !== "custom") setBaseURL(DEFAULT_BASE_URLS[p]);
  }

  function showToast(msg: string, ok: boolean) {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 4500);
  }

  function handleSave(e: React.FormEvent) {
    e.preventDefault();
    setAIProviderInStorage(provider);
    setAIBaseURLInStorage(baseURL);
    if (apiKey.trim()) {
      setAIUnifiedKeyInStorage(apiKey);
      // Sync to legacy per-provider keys so existing code keeps working
      if (provider === "openai") setOpenAIApiKeyInStorage(apiKey);
      else if (provider === "gemini") setGeminiApiKeyInStorage(apiKey);
    } else {
      clearAIUnifiedKeyFromStorage();
      clearOpenAIApiKeyFromStorage();
      clearGeminiApiKeyFromStorage();
    }
    setBarcodeModeInStorage(barcodeMode);
    setLabelPrinterInStorage(labelPrinter);
    setSaved(true);
    setTimeout(() => setSaved(false), 2500);
  }

  function handleClear() {
    clearAIUnifiedKeyFromStorage();
    clearOpenAIApiKeyFromStorage();
    clearGeminiApiKeyFromStorage();
    setApiKey("");
    showToast("All API keys cleared.", true);
  }

  async function handleTestConnection() {
    if (!apiKey.trim()) { showToast("Enter an API key first.", false); return; }
    if (!baseURL.trim()) { showToast("Enter a Base URL first.", false); return; }
    setTesting(true);
    try {
      if (provider === "gemini") {
        // Gemini uses query-param auth, not Bearer header
        const res = await fetch(
          `${baseURL.replace(/\/$/, "")}/models?key=${encodeURIComponent(apiKey.trim())}`,
        );
        if (res.ok) {
          showToast("Google Gemini connection successful ✓", true);
        } else {
          const body = await res.json().catch(() => ({}));
          showToast(
            `Gemini error ${res.status}: ${body?.error?.message ?? "Invalid key"}`,
            false,
          );
        }
      } else {
        // OpenAI or Custom: Bearer token against /models
        const res = await fetch(`${baseURL.replace(/\/$/, "")}/models`, {
          headers: { Authorization: `Bearer ${apiKey.trim()}` },
        });
        if (res.ok) {
          showToast(
            `${provider === "openai" ? "OpenAI" : "Custom API"} connection successful ✓`,
            true,
          );
        } else {
          const body = await res.json().catch(() => ({}));
          showToast(
            `Error ${res.status}: ${body?.error?.message ?? "Connection failed"}`,
            false,
          );
        }
      }
    } catch {
      showToast("Network error — check your connection or CORS policy.", false);
    } finally {
      setTesting(false);
    }
  }

  // ── Access guard ──────────────────────────────────────────────────────────────
  if (mounted && role !== "admin") {
    return (
      <div className="mx-auto max-w-lg px-4 py-16 text-center">
        <div className="mb-6 flex justify-center">
          <div className="flex h-20 w-20 items-center justify-center rounded-3xl bg-rose-100 dark:bg-rose-950/50">
            <ShieldAlert className="h-10 w-10 text-rose-600 dark:text-rose-400" />
          </div>
        </div>
        <h1 className="text-2xl font-bold tracking-tight">Access Denied</h1>
        <p className="mt-2 text-muted-foreground">Admin privileges required to view Settings.</p>
        <Link
          href="/"
          className="mt-6 inline-flex items-center gap-2 rounded-2xl bg-slate-900 px-6 py-3 text-sm font-semibold text-white transition hover:bg-slate-700 dark:bg-slate-100 dark:text-slate-900 dark:hover:bg-slate-200"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to Dashboard
        </Link>
      </div>
    );
  }

  // ── Main UI ───────────────────────────────────────────────────────────────────
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

      {/* ── Tab bar ─────────────────────────────────────────────────────────── */}
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

      {/* ── Form (shared submit for both tabs) ──────────────────────────────── */}
      <form onSubmit={handleSave} className="mt-6 space-y-6">

        {/* ════════════════ AI & SYSTEM TAB ════════════════ */}
        {activeTab === "ai" && (
          <div className="space-y-6 rounded-2xl border border-border bg-card p-6 shadow-sm">

            {/* Provider Name */}
            <div>
              <label className={LABEL_CLS}>Provider Name</label>
              <p className={HINT_CLS}>Select the AI model provider for OCR and analysis.</p>
              <select
                value={mounted ? provider : "openai"}
                onChange={(e) => handleProviderChange(e.target.value as AIProvider)}
                className={SELECT_CLS}
              >
                <option value="openai">OpenAI  (GPT-4o, o3, o4-mini…)</option>
                <option value="gemini">Google Gemini</option>
                <option value="custom">Custom / Other  (Ollama, Groq, Azure OpenAI, LM Studio…)</option>
              </select>
            </div>

            {/* Base URL */}
            <div>
              <label htmlFor="base-url" className={LABEL_CLS}>Base URL</label>
              <p className={HINT_CLS}>
                API endpoint root. Auto-filled for known providers; edit freely for custom deployments.
              </p>
              <input
                id="base-url"
                type="text"
                autoComplete="off"
                spellCheck={false}
                value={mounted ? baseURL : DEFAULT_BASE_URLS.openai}
                onChange={(e) => setBaseURL(e.target.value)}
                placeholder="https://api.openai.com/v1"
                className={`${INPUT_CLS} font-mono`}
              />
              {mounted && provider === "custom" && (
                <p className="mt-1.5 text-xs text-amber-600 dark:text-amber-400">
                  Custom providers: make sure CORS allows requests from this origin, or proxy via your backend.
                </p>
              )}
            </div>

            {/* API Key */}
            <div>
              <label htmlFor="api-key" className={LABEL_CLS}>API Key</label>
              <p className={HINT_CLS}>
                Your secret key. Stored only in this browser&apos;s localStorage — never sent to our servers.
              </p>
              <div className="flex gap-2">
                <input
                  id="api-key"
                  type="password"
                  autoComplete="off"
                  value={mounted ? apiKey : ""}
                  onChange={(e) => setApiKey(e.target.value)}
                  placeholder={
                    provider === "openai" ? "sk-…"
                    : provider === "gemini" ? "AIza…"
                    : "Your API key…"
                  }
                  className={`${INPUT_CLS} min-w-0 flex-1 font-mono`}
                />
                <button
                  type="button"
                  onClick={handleTestConnection}
                  disabled={testing}
                  title="Test connection"
                  className="inline-flex h-12 shrink-0 items-center gap-2 rounded-xl border border-sky-200 bg-sky-50 px-4 text-sm font-semibold text-sky-700 transition hover:bg-sky-100 disabled:opacity-50 dark:border-sky-700/50 dark:bg-sky-950/40 dark:text-sky-300"
                >
                  {testing
                    ? <Loader2 className="h-4 w-4 animate-spin" />
                    : <Wifi    className="h-4 w-4" />}
                  {testing ? "Testing…" : "Test"}
                </button>
              </div>
              <p className="mt-1.5 text-xs text-muted-foreground">
                Test fires{" "}
                <code className="rounded bg-muted px-1 py-0.5 font-mono text-[11px]">
                  GET {"{baseURL}"}/models
                </code>{" "}
                with <code className="rounded bg-muted px-1 py-0.5 font-mono text-[11px]">Authorization: Bearer …</code>.
              </p>
            </div>
          </div>
        )}

        {/* ════════════════ HARDWARE & DEVICES TAB ════════════════ */}
        {activeTab === "hardware" && (
          <div className="space-y-6 rounded-2xl border border-border bg-card p-6 shadow-sm">

            {/* Barcode Input Mode */}
            <div>
              <div className="mb-2 flex items-center gap-2">
                <ScanLine className="h-4 w-4 text-sky-600 dark:text-sky-400" />
                <label className="text-sm font-semibold">Barcode Input Mode</label>
              </div>
              <p className={HINT_CLS}>
                Physical scanners emulate a keyboard — they type characters very fast and press Enter.
                The app&apos;s global scanner hook detects this pattern.
              </p>
              <select
                value={mounted ? barcodeMode : "physical"}
                onChange={(e) => setBarcodeMode(e.target.value as BarcodeMode)}
                className={SELECT_CLS}
              >
                <option value="physical">Physical Scanner  (Keyboard Emulation)</option>
                <option value="camera">Web / Camera Scanner</option>
              </select>
              {mounted && barcodeMode === "physical" && (
                <p className="mt-2 flex items-center gap-1.5 text-xs font-medium text-emerald-600 dark:text-emerald-400">
                  <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
                  Scanner hook active — rapid keystrokes (&lt;30 ms apart, ending Enter) are captured globally.
                </p>
              )}
            </div>

            {/* Default Label Printer */}
            <div>
              <div className="mb-2 flex items-center gap-2">
                <Printer className="h-4 w-4 text-sky-600 dark:text-sky-400" />
                <label className="text-sm font-semibold">Default Label Printer</label>
              </div>
              <p className={HINT_CLS}>
                Select the default printer for shipping and return labels.
              </p>
              <select
                value={mounted ? labelPrinter : "system"}
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
                measures inter-keystroke timing. When characters arrive in &lt;30 ms bursts followed
                by Enter, the string is captured as a barcode and broadcast to listening inputs — even
                when no input is focused. The Enter key is suppressed to prevent accidental form
                submissions.
              </p>
            </div>
          </div>
        )}

        {/* ── Save / Clear ────────────────────────────────────────────────────── */}
        <div className="flex flex-wrap gap-2">
          <button
            type="submit"
            className="inline-flex min-w-[120px] flex-1 items-center justify-center gap-2 rounded-xl bg-sky-600 py-3 text-sm font-semibold text-white transition hover:bg-sky-700"
          >
            <Save className="h-4 w-4" />
            Save Settings
          </button>
          <button
            type="button"
            onClick={handleClear}
            className="inline-flex items-center justify-center gap-2 rounded-xl border border-border px-4 py-3 text-sm font-semibold text-muted-foreground transition hover:bg-muted"
          >
            <Trash2 className="h-4 w-4" />
            Clear All Keys
          </button>
        </div>

        {saved && (
          <p className="text-sm font-medium text-emerald-600 dark:text-emerald-400">Saved.</p>
        )}
      </form>

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
