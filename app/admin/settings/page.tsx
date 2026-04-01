"use client";

import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  Ban,
  CalendarRange,
  FileText,
  Loader2,
  Plus,
  SquareX,
  Trash2,
  Upload,
  UploadCloud,
} from "lucide-react";
import { useUserRole } from "../../../components/UserRoleContext";
import { isUuidString } from "../../../lib/uuid";
import {
  deletePlatformMarketplace,
  listPlatformMarketplaces,
  type PlatformMarketplaceRow,
  upsertPlatformMarketplace,
} from "../platform-actions";
import {
  deleteAmazonLedgerStagingByDateRange,
  deleteAmazonLedgerStorageFile,
  insertAmazonLedgerStagingBatch,
  purgeAmazonLedgerStagingRetention,
} from "../amazon-ledger-actions";
import { guessLedgerSnapshotDate, parseCsvToRecords } from "../../../lib/csv-parse-basic";
import {
  listWorkspaceOrganizationsForAdmin,
  type WorkspaceOrganizationOption,
} from "../../session/tenant-actions";

const LEDGER_CHUNK = 500;

// ─── Exact Amazon date column names to check before the heuristic fallback ───
const EXACT_DATE_KEYS = ["Date", "Date-Time", "date", "date-time"];

function extractRowDate(row: Record<string, string>): string | null {
  for (const key of EXACT_DATE_KEYS) {
    const v = row[key]?.trim();
    if (v) {
      const d = new Date(v);
      if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 10);
    }
  }
  return guessLedgerSnapshotDate(row);
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

// ─── Upload phase state machine ───────────────────────────────────────────────
// idle → file-ready → uploading → storage-ready → processing → done
type LedgerPhase =
  | "idle"
  | "file-ready"
  | "uploading"
  | "storage-ready"
  | "processing"
  | "done";

type LedgerProgress = {
  pct: number;
  imported: number;
  skipped: number;
  total: number;       // rows that pass the date filter (to be imported)
  totalInFile: number; // all rows in the file
  message: string;
};

export default function AdminSettingsPage() {
  const { role, actorUserId, organizationId } = useUserRole();

  // ── Platform marketplace state ────────────────────────────────────────────
  const [rows, setRows] = useState<PlatformMarketplaceRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [name, setName] = useState("");
  const [slug, setSlug] = useState("");
  const [iconUrl, setIconUrl] = useState("");
  const [editId, setEditId] = useState<string | null>(null);

  // ── Amazon ledger — org / date / options ─────────────────────────────────
  const [ledgerRetentionDays, setLedgerRetentionDays] = useState(60);
  const [ledgerRetentionEnabled, setLedgerRetentionEnabled] = useState(true);
  const [companyOptions, setCompanyOptions] = useState<WorkspaceOrganizationOption[]>([]);
  const [ledgerTargetOrg, setLedgerTargetOrg] = useState("");
  const [ledgerStartDate, setLedgerStartDate] = useState("");
  const [ledgerEndDate, setLedgerEndDate] = useState("");
  const [ledgerDedup, setLedgerDedup] = useState(true);

  // ── Amazon ledger — controlled upload phase machine ───────────────────────
  const [ledgerPhase, setLedgerPhase] = useState<LedgerPhase>("idle");
  const [ledgerFile, setLedgerFile] = useState<File | null>(null);
  const [storagePath, setStoragePath] = useState<string | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [ledgerProgress, setLedgerProgress] = useState<LedgerProgress | null>(null);
  const [ledgerErr, setLedgerErr] = useState<string | null>(null);

  const uploadAbortRef = useRef<AbortController | null>(null);
  const stopProcessingRef = useRef(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // ── Derived: whether both date fields are filled and logically valid ───────
  const dateRangeReady =
    Boolean(ledgerStartDate.trim() && ledgerEndDate.trim()) &&
    ledgerStartDate.trim() <= ledgerEndDate.trim();

  const canStartUpload =
    ledgerPhase === "file-ready" && dateRangeReady && ledgerFile !== null;

  // ── Load marketplace rows ─────────────────────────────────────────────────
  const load = useCallback(async () => {
    setLoading(true);
    setErr(null);
    const res = await listPlatformMarketplaces(actorUserId);
    if (!res.ok) {
      setErr(res.error ?? "Could not load platforms.");
      setRows([]);
    } else {
      setRows(res.rows);
    }
    setLoading(false);
  }, [actorUserId]);

  useEffect(() => {
    if (role !== "super_admin") return;
    void load();
  }, [role, load]);

  useEffect(() => {
    if (role !== "super_admin") return;
    let cancelled = false;
    void listWorkspaceOrganizationsForAdmin().then((res) => {
      if (!cancelled && res.ok) setCompanyOptions(res.rows);
    });
    return () => { cancelled = true; };
  }, [role]);

  useEffect(() => {
    if (organizationId && !ledgerTargetOrg.trim()) {
      setLedgerTargetOrg(organizationId);
    }
  }, [organizationId, ledgerTargetOrg]);

  // ── Platform CRUD ─────────────────────────────────────────────────────────
  async function handleSavePlatform(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    setErr(null);
    const res = await upsertPlatformMarketplace({
      actorProfileId: actorUserId,
      id: editId,
      name,
      slug,
      icon_url: iconUrl || null,
    });
    setSaving(false);
    if (!res.ok) {
      setErr(res.error ?? "Save failed.");
      return;
    }
    setName(""); setSlug(""); setIconUrl(""); setEditId(null);
    await load();
  }

  function startEdit(r: PlatformMarketplaceRow) {
    setEditId(r.id); setName(r.name); setSlug(r.slug); setIconUrl(r.icon_url ?? "");
  }

  async function handleDelete(id: string) {
    if (!window.confirm("Delete this marketplace entry?")) return;
    setErr(null);
    const res = await deletePlatformMarketplace(actorUserId, id);
    if (!res.ok) { setErr(res.error ?? "Delete failed."); return; }
    await load();
  }

  // ── Ledger: file selection (drag or click) — NO auto-start ───────────────
  function acceptFile(file: File) {
    if (!file.name.toLowerCase().endsWith(".csv") && file.type !== "text/csv") {
      setLedgerErr("Please select a CSV file.");
      return;
    }
    setLedgerFile(file);
    setLedgerPhase("file-ready");
    setLedgerProgress(null);
    setLedgerErr(null);
    setStoragePath(null);
  }

  function handleFileInputChange(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0] ?? null;
    e.target.value = "";
    if (f) acceptFile(f);
  }

  function handleDragOver(e: React.DragEvent) {
    e.preventDefault();
    setIsDragging(true);
  }

  function handleDragLeave() {
    setIsDragging(false);
  }

  function handleDrop(e: React.DragEvent) {
    e.preventDefault();
    setIsDragging(false);
    const f = e.dataTransfer.files?.[0] ?? null;
    if (f) acceptFile(f);
  }

  // ── Ledger: Step 1 — upload raw file to Supabase Storage ─────────────────
  async function handleStartUpload() {
    if (!canStartUpload || !ledgerFile) return;
    if (!actorUserId) {
      setLedgerErr("Not authenticated.");
      return;
    }

    setLedgerPhase("uploading");
    setLedgerErr(null);

    const controller = new AbortController();
    uploadAbortRef.current = controller;

    try {
      const fd = new FormData();
      fd.append("file", ledgerFile);
      fd.append("org_id", ledgerTargetOrg.trim() || organizationId || "unknown");

      const res = await fetch("/api/admin/ledger/upload", {
        method: "POST",
        body: fd,
        signal: controller.signal,
      });

      if (!res.ok) {
        const body = (await res.json().catch(() => ({}))) as { error?: string };
        throw new Error(body.error ?? `Upload failed (HTTP ${res.status}).`);
      }

      const body = (await res.json()) as { path?: string };
      if (!body.path) throw new Error("No storage path returned.");

      setStoragePath(body.path);
      setLedgerPhase("storage-ready");
    } catch (e) {
      if ((e as Error).name === "AbortError") {
        setLedgerPhase("file-ready");
        setLedgerErr("Upload cancelled.");
      } else {
        setLedgerPhase("file-ready");
        setLedgerErr(e instanceof Error ? e.message : "Upload failed.");
      }
    } finally {
      uploadAbortRef.current = null;
    }
  }

  function handleCancelUpload() {
    uploadAbortRef.current?.abort();
  }

  // ── Ledger: Step 2 — parse + filter + insert into DB ─────────────────────
  async function handleProcessToDb() {
    if (!ledgerFile || ledgerPhase !== "storage-ready") return;
    if (!actorUserId) { setLedgerErr("Not authenticated."); return; }

    const start = ledgerStartDate.trim();
    const end = ledgerEndDate.trim();

    setLedgerPhase("processing");
    setLedgerErr(null);
    stopProcessingRef.current = false;

    try {
      // 1. Parse the local file (we still have it in memory)
      setLedgerProgress({
        pct: 0, imported: 0, skipped: 0,
        total: 0, totalInFile: 0,
        message: "Reading and parsing CSV…",
      });

      const text = await ledgerFile.text();
      const allRecords = parseCsvToRecords(text);

      if (allRecords.length === 0) {
        setLedgerErr("No data rows found in the CSV.");
        setLedgerPhase("storage-ready");
        setLedgerProgress(null);
        return;
      }

      // 2. Apply date filter
      const filtered: Record<string, string>[] = [];
      let skippedByFilter = 0;
      for (const row of allRecords) {
        if (stopProcessingRef.current) break;
        const dateStr = extractRowDate(row);
        if (dateStr && dateStr >= start && dateStr <= end) {
          filtered.push(row);
        } else {
          skippedByFilter++;
        }
      }

      setLedgerProgress({
        pct: 0,
        imported: 0,
        skipped: skippedByFilter,
        total: filtered.length,
        totalInFile: allRecords.length,
        message: `Found ${allRecords.length.toLocaleString()} rows · ${filtered.length.toLocaleString()} in date range · ${skippedByFilter.toLocaleString()} outside filter`,
      });

      if (stopProcessingRef.current) {
        setLedgerPhase("storage-ready");
        setLedgerErr("Processing stopped before inserting.");
        return;
      }

      if (filtered.length === 0) {
        setLedgerProgress((p) =>
          p ? { ...p, pct: 100, message: "No rows matched the date filter — nothing inserted." } : p,
        );
        setLedgerPhase("done");
        return;
      }

      const requestedOrg =
        role === "super_admin" && isUuidString(ledgerTargetOrg.trim())
          ? ledgerTargetOrg.trim()
          : undefined;

      // 3. Optional dedup: delete existing rows for this org + date range
      if (ledgerDedup) {
        setLedgerProgress((p) =>
          p ? { ...p, message: "Removing existing rows for this date range…" } : p,
        );
        const del = await deleteAmazonLedgerStagingByDateRange({
          actorProfileId: actorUserId,
          requestedOrganizationId: requestedOrg,
          startDate: start,
          endDate: end,
        });
        if (!del.ok) {
          setLedgerErr(del.error ?? "Dedup delete failed.");
          setLedgerPhase("storage-ready");
          return;
        }
      }

      // 4. Optional retention purge
      if (ledgerRetentionEnabled) {
        setLedgerProgress((p) =>
          p ? { ...p, message: "Purging rows outside retention window…" } : p,
        );
        const purge = await purgeAmazonLedgerStagingRetention({
          actorProfileId: actorUserId,
          requestedOrganizationId: requestedOrg,
          retentionDays: ledgerRetentionDays,
        });
        if (!purge.ok) {
          setLedgerErr(purge.error ?? "Purge failed.");
          setLedgerPhase("storage-ready");
          return;
        }
      }

      // 5. Chunked insert with progress + stop support
      let imported = 0;
      const total = filtered.length;

      for (let i = 0; i < total; i += LEDGER_CHUNK) {
        if (stopProcessingRef.current) {
          setLedgerProgress((p) =>
            p
              ? {
                  ...p,
                  message: `Stopped. ${imported.toLocaleString()} of ${total.toLocaleString()} rows inserted.`,
                }
              : p,
          );
          setLedgerPhase("storage-ready");
          return;
        }

        const batch = filtered.slice(i, i + LEDGER_CHUNK);
        const res = await insertAmazonLedgerStagingBatch({
          actorProfileId: actorUserId,
          requestedOrganizationId: requestedOrg,
          rows: batch,
        });

        if (!res.ok) {
          setLedgerErr(res.error ?? "Insert failed.");
          setLedgerPhase("storage-ready");
          return;
        }

        imported += res.inserted;
        const pct = Math.round((imported / total) * 100);
        setLedgerProgress((p) =>
          p
            ? {
                ...p,
                pct,
                imported,
                message: `Inserting… ${imported.toLocaleString()} / ${total.toLocaleString()} rows`,
              }
            : p,
        );
      }

      setLedgerProgress((p) =>
        p
          ? {
              ...p,
              pct: 100,
              imported,
              message: `Done! ${imported.toLocaleString()} row(s) inserted, ${skippedByFilter.toLocaleString()} skipped by date filter.`,
            }
          : p,
      );
      setLedgerPhase("done");
    } catch (e) {
      setLedgerErr(e instanceof Error ? e.message : "Processing failed.");
      setLedgerPhase("storage-ready");
    }
  }

  function handleStopProcessing() {
    stopProcessingRef.current = true;
  }

  // ── Ledger: Delete file from Storage + reset state ────────────────────────
  async function handleDeleteFile() {
    if (!storagePath || !actorUserId) return;
    if (!window.confirm("Remove this CSV from Storage and reset the uploader?")) return;

    const res = await deleteAmazonLedgerStorageFile({
      actorProfileId: actorUserId,
      storagePath,
    });

    if (!res.ok) {
      setLedgerErr(res.error ?? "Storage delete failed.");
      return;
    }
    resetLedger();
  }

  function resetLedger() {
    setLedgerPhase("idle");
    setLedgerFile(null);
    setStoragePath(null);
    setLedgerProgress(null);
    setLedgerErr(null);
    stopProcessingRef.current = false;
  }

  // ── Access guard ──────────────────────────────────────────────────────────
  if (role !== "super_admin") {
    return (
      <div className="mx-auto max-w-lg p-8">
        <h1 className="text-lg font-semibold text-slate-800 dark:text-slate-100">Admin Settings</h1>
        <p className="mt-2 text-sm text-slate-600 dark:text-slate-400">
          This page is only available to Super Admins.
        </p>
      </div>
    );
  }

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div className="mx-auto max-w-3xl space-y-10 p-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight text-slate-900 dark:text-slate-50">Admin Settings</h1>
        <p className="mt-1 text-sm text-slate-600 dark:text-slate-400">
          Global marketplace catalog and Amazon ledger staging (does not modify returns).
        </p>
      </div>

      {err && (
        <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900/50 dark:bg-red-950/40 dark:text-red-200">
          {err}
        </div>
      )}

      {/* ════════════════════════════════════════════════════════════════════
          PLATFORM MARKETPLACES
      ════════════════════════════════════════════════════════════════════ */}
      <section className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-800 dark:bg-slate-950">
        <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">Marketplaces</h2>
        <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
          Name, slug, and icon URL. Icons appear on the Returns list when linked by ID or matching slug.
        </p>

        {loading ? (
          <div className="mt-6 flex items-center gap-2 text-sm text-slate-500">
            <Loader2 className="h-5 w-5 animate-spin" /> Loading…
          </div>
        ) : (
          <ul className="mt-4 divide-y divide-slate-100 dark:divide-slate-800">
            {rows.map((r) => (
              <li key={r.id} className="flex flex-wrap items-center gap-3 py-3">
                {r.icon_url ? (
                  // eslint-disable-next-line @next/next/no-img-element
                  <img src={r.icon_url} alt="" className="h-8 w-8 object-contain" />
                ) : (
                  <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-slate-100 text-xs text-slate-400 dark:bg-slate-800">—</div>
                )}
                <div className="min-w-0 flex-1">
                  <p className="font-medium text-slate-900 dark:text-slate-100">{r.name}</p>
                  <p className="text-xs text-slate-500">{r.slug}</p>
                </div>
                <button
                  type="button"
                  onClick={() => startEdit(r)}
                  className="rounded-lg border border-slate-200 px-3 py-1.5 text-xs font-medium hover:bg-slate-50 dark:border-slate-700 dark:hover:bg-slate-900"
                >
                  Edit
                </button>
                <button
                  type="button"
                  onClick={() => void handleDelete(r.id)}
                  className="rounded-lg p-2 text-red-600 hover:bg-red-50 dark:text-red-400 dark:hover:bg-red-950/40"
                  title="Delete"
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              </li>
            ))}
          </ul>
        )}

        <form onSubmit={handleSavePlatform} className="mt-6 space-y-3 rounded-xl border border-dashed border-slate-200 p-4 dark:border-slate-700">
          <p className="text-sm font-medium text-slate-800 dark:text-slate-200">
            {editId ? "Update marketplace" : "Add marketplace"}
          </p>
          <div className="grid gap-3 sm:grid-cols-2">
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-400">
              Name
              <input
                value={name}
                onChange={(e) => setName(e.target.value)}
                className="mt-1 w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
                required
              />
            </label>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-400">
              Slug
              <input
                value={slug}
                onChange={(e) => setSlug(e.target.value)}
                className="mt-1 w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
                placeholder="amazon"
                required
              />
            </label>
          </div>
          <label className="block text-xs font-medium text-slate-600 dark:text-slate-400">
            Icon URL
            <input
              value={iconUrl}
              onChange={(e) => setIconUrl(e.target.value)}
              className="mt-1 w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
              placeholder="https://…"
            />
          </label>
          <div className="flex flex-wrap gap-2">
            <button
              type="submit"
              disabled={saving}
              className="inline-flex items-center gap-2 rounded-xl bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50"
            >
              {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
              {editId ? "Save changes" : "Add"}
            </button>
            {editId && (
              <button
                type="button"
                onClick={() => { setEditId(null); setName(""); setSlug(""); setIconUrl(""); }}
                className="rounded-xl border border-slate-200 px-4 py-2 text-sm dark:border-slate-700"
              >
                Cancel edit
              </button>
            )}
          </div>
        </form>
      </section>

      {/* ════════════════════════════════════════════════════════════════════
          AMAZON INVENTORY LEDGER — controlled step-by-step upload
      ════════════════════════════════════════════════════════════════════ */}
      <section className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-800 dark:bg-slate-950">
        <h2 className="flex items-center gap-2 text-lg font-semibold text-slate-900 dark:text-slate-100">
          <CalendarRange className="h-5 w-5 text-sky-500" />
          Amazon Inventory Ledger
        </h2>
        <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
          Two-step process: upload the raw CSV to Storage, then process rows into{" "}
          <code className="rounded bg-slate-100 px-1 dark:bg-slate-800">amazon_ledger_staging</code>{" "}
          in chunks of {LEDGER_CHUNK}. Never modifies returns.
        </p>

        {ledgerErr && (
          <div className="mt-4 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900/50 dark:bg-red-950/40 dark:text-red-200">
            {ledgerErr}
          </div>
        )}

        {/* ── STEP 0: Date filter — ALWAYS shown first, prominent ─────────── */}
        <div className={`mt-5 rounded-xl border p-4 transition-colors ${
          dateRangeReady
            ? "border-sky-200 bg-sky-50/60 dark:border-sky-800/50 dark:bg-sky-950/20"
            : "border-amber-200 bg-amber-50/60 dark:border-amber-800/50 dark:bg-amber-950/20"
        }`}>
          <p className={`text-xs font-semibold uppercase tracking-wide ${
            dateRangeReady ? "text-sky-700 dark:text-sky-400" : "text-amber-700 dark:text-amber-400"
          }`}>
            Step 1 — Set date filter{" "}
            <span className="ml-1 rounded bg-red-100 px-1.5 py-0.5 text-red-600 dark:bg-red-900/40 dark:text-red-400">
              required
            </span>
          </p>
          <p className="mt-0.5 text-xs text-slate-500 dark:text-slate-400">
            Only rows whose <strong>Date</strong> or <strong>Date-Time</strong> column falls within this range
            will be inserted. Rows outside the range are counted and skipped.
          </p>
          <div className="mt-3 grid gap-3 sm:grid-cols-2">
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-400">
              Start date <span className="text-red-500">*</span>
              <input
                type="date"
                value={ledgerStartDate}
                onChange={(e) => setLedgerStartDate(e.target.value)}
                disabled={ledgerPhase === "uploading" || ledgerPhase === "processing"}
                className={`mt-1 w-full rounded-lg border px-3 py-2 text-sm dark:bg-slate-900 disabled:opacity-50 ${
                  ledgerStartDate ? "border-slate-200 dark:border-slate-700" : "border-amber-300 dark:border-amber-700"
                }`}
              />
            </label>
            <label className="block text-xs font-medium text-slate-600 dark:text-slate-400">
              End date <span className="text-red-500">*</span>
              <input
                type="date"
                value={ledgerEndDate}
                onChange={(e) => setLedgerEndDate(e.target.value)}
                disabled={ledgerPhase === "uploading" || ledgerPhase === "processing"}
                className={`mt-1 w-full rounded-lg border px-3 py-2 text-sm dark:bg-slate-900 disabled:opacity-50 ${
                  ledgerEndDate ? "border-slate-200 dark:border-slate-700" : "border-amber-300 dark:border-amber-700"
                }`}
              />
            </label>
          </div>
          {!dateRangeReady && (
            <p className="mt-2 text-xs text-amber-600 dark:text-amber-400">
              Both dates are required before you can start the upload.
            </p>
          )}
        </div>

        {/* ── Target company (super admin only) ────────────────────────────── */}
        {role === "super_admin" && (
          <label className="mt-4 block text-xs font-medium text-slate-600 dark:text-slate-400">
            Target company
            <select
              value={ledgerTargetOrg}
              onChange={(e) => setLedgerTargetOrg(e.target.value)}
              disabled={ledgerPhase === "uploading" || ledgerPhase === "processing"}
              className="mt-1 w-full max-w-md rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900 disabled:opacity-50"
            >
              {companyOptions.map((o) => (
                <option key={o.organization_id} value={o.organization_id}>
                  {o.display_name}
                </option>
              ))}
            </select>
          </label>
        )}

        {/* ── Dedup + retention options ─────────────────────────────────────── */}
        <div className="mt-4 space-y-2">
          <label className="flex items-start gap-2 text-sm text-slate-700 dark:text-slate-300">
            <input
              type="checkbox"
              checked={ledgerDedup}
              onChange={(e) => setLedgerDedup(e.target.checked)}
              disabled={ledgerPhase === "uploading" || ledgerPhase === "processing"}
              className="mt-0.5 rounded border-slate-300 disabled:opacity-50"
            />
            <span>
              <span className="font-medium">Replace existing data for this date range</span>
              <span className="ml-1 text-slate-500 text-xs">
                — deletes any rows already in staging for this company + date range before inserting.
              </span>
            </span>
          </label>
          <label className="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300">
            <input
              type="checkbox"
              checked={ledgerRetentionEnabled}
              onChange={(e) => setLedgerRetentionEnabled(e.target.checked)}
              disabled={ledgerPhase === "uploading" || ledgerPhase === "processing"}
              className="rounded border-slate-300 disabled:opacity-50"
            />
            Also purge rows older than{" "}
            <input
              type="number"
              min={1}
              max={3650}
              value={ledgerRetentionDays}
              onChange={(e) => setLedgerRetentionDays(Number(e.target.value) || 60)}
              disabled={!ledgerRetentionEnabled || ledgerPhase === "uploading" || ledgerPhase === "processing"}
              className="w-16 rounded border border-slate-200 px-2 py-0.5 text-sm dark:border-slate-700 dark:bg-slate-900 disabled:opacity-50"
            />{" "}
            days for this company
          </label>
        </div>

        {/* ── STEP 2: Drop zone — shown when idle or file-ready ────────────── */}
        {(ledgerPhase === "idle" || ledgerPhase === "file-ready") && (
          <div
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
            onClick={() => fileInputRef.current?.click()}
            className={`mt-5 flex cursor-pointer flex-col items-center justify-center gap-3 rounded-xl border-2 border-dashed px-6 py-10 text-center transition-colors ${
              isDragging
                ? "border-sky-400 bg-sky-50/80 dark:border-sky-600 dark:bg-sky-950/40"
                : ledgerPhase === "file-ready"
                  ? "border-emerald-300 bg-emerald-50/50 dark:border-emerald-700 dark:bg-emerald-950/20"
                  : "border-slate-200 bg-slate-50/50 hover:border-slate-300 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900/30 dark:hover:border-slate-600"
            }`}
          >
            <UploadCloud className={`h-8 w-8 ${
              ledgerPhase === "file-ready" ? "text-emerald-500" : "text-slate-400"
            }`} />
            {ledgerPhase === "file-ready" && ledgerFile ? (
              <>
                <div className="flex items-center gap-2">
                  <FileText className="h-4 w-4 text-emerald-600" />
                  <span className="text-sm font-semibold text-emerald-700 dark:text-emerald-300">
                    {ledgerFile.name}
                  </span>
                </div>
                <span className="text-xs text-slate-500">{formatBytes(ledgerFile.size)}</span>
                <span className="text-xs text-slate-400">Click or drop a different file to replace</span>
              </>
            ) : (
              <>
                <p className="text-sm font-medium text-slate-700 dark:text-slate-300">
                  Step 2 — Drop your CSV file here
                </p>
                <p className="text-xs text-slate-400">or click to browse · .csv files only</p>
              </>
            )}
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv,text/csv"
              className="hidden"
              onChange={handleFileInputChange}
            />
          </div>
        )}

        {/* ── File info bar — shown in all post-idle phases ─────────────────── */}
        {ledgerFile && ledgerPhase !== "idle" && ledgerPhase !== "file-ready" && (
          <div className="mt-5 flex items-center gap-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 dark:border-slate-700 dark:bg-slate-900">
            <FileText className="h-5 w-5 shrink-0 text-slate-400" />
            <div className="min-w-0 flex-1">
              <p className="truncate text-sm font-medium text-slate-800 dark:text-slate-200">
                {ledgerFile.name}
              </p>
              <p className="text-xs text-slate-500">{formatBytes(ledgerFile.size)}</p>
            </div>
            {storagePath && (
              <span className="rounded-full bg-emerald-100 px-2 py-0.5 text-[11px] font-semibold text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300">
                In Storage
              </span>
            )}
          </div>
        )}

        {/* ── Action buttons — context-sensitive per phase ──────────────────── */}
        <div className="mt-5 flex flex-wrap items-center gap-3">

          {/* Start Upload — visible in file-ready phase */}
          {ledgerPhase === "file-ready" && (
            <button
              type="button"
              onClick={() => void handleStartUpload()}
              disabled={!canStartUpload}
              title={!dateRangeReady ? "Set both Start Date and End Date first" : !ledgerFile ? "Select a file first" : undefined}
              className="inline-flex items-center gap-2 rounded-xl bg-sky-600 px-5 py-2.5 text-sm font-semibold text-white hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-40"
            >
              <Upload className="h-4 w-4" />
              Start Upload
            </button>
          )}

          {/* Uploading spinner + Cancel */}
          {ledgerPhase === "uploading" && (
            <>
              <div className="inline-flex items-center gap-2 rounded-xl bg-sky-100 px-5 py-2.5 text-sm font-semibold text-sky-700 dark:bg-sky-950/40 dark:text-sky-300">
                <Loader2 className="h-4 w-4 animate-spin" />
                Uploading to Storage…
              </div>
              <button
                type="button"
                onClick={handleCancelUpload}
                className="inline-flex items-center gap-2 rounded-xl border border-red-200 bg-red-50 px-4 py-2.5 text-sm font-semibold text-red-700 hover:bg-red-100 dark:border-red-800 dark:bg-red-950/40 dark:text-red-300 dark:hover:bg-red-950/70"
              >
                <Ban className="h-4 w-4" />
                Cancel
              </button>
            </>
          )}

          {/* Process to DB + Delete File — visible in storage-ready phase */}
          {ledgerPhase === "storage-ready" && (
            <>
              <button
                type="button"
                onClick={() => void handleProcessToDb()}
                className="inline-flex items-center gap-2 rounded-xl bg-emerald-600 px-5 py-2.5 text-sm font-semibold text-white hover:bg-emerald-700"
              >
                <UploadCloud className="h-4 w-4" />
                Process to Database
              </button>
              <button
                type="button"
                onClick={() => void handleDeleteFile()}
                className="inline-flex items-center gap-2 rounded-xl border border-red-200 bg-red-50 px-4 py-2.5 text-sm font-semibold text-red-700 hover:bg-red-100 dark:border-red-800 dark:bg-red-950/40 dark:text-red-300"
              >
                <Trash2 className="h-4 w-4" />
                Delete File
              </button>
            </>
          )}

          {/* Stop Processing — visible while inserting rows */}
          {ledgerPhase === "processing" && (
            <button
              type="button"
              onClick={handleStopProcessing}
              className="inline-flex items-center gap-2 rounded-xl border border-amber-200 bg-amber-50 px-4 py-2.5 text-sm font-semibold text-amber-700 hover:bg-amber-100 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-300"
            >
              <SquareX className="h-4 w-4" />
              Stop Processing
            </button>
          )}

          {/* Done: Delete File + Start Over */}
          {ledgerPhase === "done" && (
            <>
              {storagePath && (
                <button
                  type="button"
                  onClick={() => void handleDeleteFile()}
                  className="inline-flex items-center gap-2 rounded-xl border border-red-200 bg-red-50 px-4 py-2.5 text-sm font-semibold text-red-700 hover:bg-red-100 dark:border-red-800 dark:bg-red-950/40 dark:text-red-300"
                >
                  <Trash2 className="h-4 w-4" />
                  Delete File from Storage
                </button>
              )}
              <button
                type="button"
                onClick={resetLedger}
                className="inline-flex items-center gap-2 rounded-xl border border-slate-200 px-4 py-2.5 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800"
              >
                Start Over
              </button>
            </>
          )}
        </div>

        {/* ── Progress + stats ──────────────────────────────────────────────── */}
        {ledgerProgress && (
          <div className="mt-5 space-y-3">
            {/* Stats row */}
            {(ledgerProgress.totalInFile > 0 || ledgerProgress.total > 0 || ledgerProgress.skipped > 0) && (
              <div className="flex flex-wrap gap-2">
                {ledgerProgress.totalInFile > 0 && (
                  <span className="inline-flex items-center gap-1.5 rounded-lg border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-400">
                    <span className="h-1.5 w-1.5 rounded-full bg-slate-400" />
                    Total in file: {ledgerProgress.totalInFile.toLocaleString()}
                  </span>
                )}
                {ledgerProgress.total > 0 && (
                  <span className="inline-flex items-center gap-1.5 rounded-lg border border-sky-200 bg-sky-50 px-3 py-1 text-xs font-medium text-sky-700 dark:border-sky-800 dark:bg-sky-950/30 dark:text-sky-300">
                    <span className="h-1.5 w-1.5 rounded-full bg-sky-500" />
                    In date range: {ledgerProgress.total.toLocaleString()}
                  </span>
                )}
                {ledgerProgress.imported > 0 && (
                  <span className="inline-flex items-center gap-1.5 rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-medium text-emerald-700 dark:border-emerald-800 dark:bg-emerald-950/30 dark:text-emerald-300">
                    <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
                    Imported: {ledgerProgress.imported.toLocaleString()}
                  </span>
                )}
                {ledgerProgress.skipped > 0 && (
                  <span className="inline-flex items-center gap-1.5 rounded-lg border border-amber-200 bg-amber-50 px-3 py-1 text-xs font-medium text-amber-700 dark:border-amber-800 dark:bg-amber-950/30 dark:text-amber-300">
                    <span className="h-1.5 w-1.5 rounded-full bg-amber-500" />
                    Skipped by filter: {ledgerProgress.skipped.toLocaleString()}
                  </span>
                )}
                {(ledgerPhase === "processing" || ledgerPhase === "done") && ledgerProgress.total > 0 && (
                  <span className="inline-flex items-center gap-1.5 rounded-lg border border-violet-200 bg-violet-50 px-3 py-1 text-xs font-medium text-violet-700 dark:border-violet-800 dark:bg-violet-950/30 dark:text-violet-300">
                    <span className="h-1.5 w-1.5 rounded-full bg-violet-500" />
                    Progress: {ledgerProgress.pct}%
                  </span>
                )}
              </div>
            )}

            {/* Progress bar */}
            {(ledgerPhase === "processing" || ledgerPhase === "done") && (
              <div className="h-2.5 w-full overflow-hidden rounded-full bg-slate-100 dark:bg-slate-800">
                <div
                  className={`h-2.5 rounded-full transition-all duration-300 ${
                    ledgerProgress.pct >= 100 ? "bg-emerald-500" : "bg-sky-500"
                  }`}
                  style={{ width: `${Math.max(ledgerProgress.pct, ledgerProgress.pct > 0 ? 4 : 0)}%` }}
                />
              </div>
            )}

            {/* Status message */}
            <p className="text-sm text-slate-600 dark:text-slate-400">{ledgerProgress.message}</p>
          </div>
        )}
      </section>
    </div>
  );
}
