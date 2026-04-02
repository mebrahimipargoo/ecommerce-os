"use client";

import React, { useEffect, useRef, useState } from "react";
import {
  CalendarRange,
  FileText,
  Loader2,
  SquareX,
  Trash2,
  Upload,
  UploadCloud,
} from "lucide-react";
import { isAdminRole, useUserRole } from "../../../components/UserRoleContext";
import { isUuidString } from "../../../lib/uuid";
import {
  deleteAmazonLedgerStagingByDateRange,
  deleteAmazonLedgerStorageFile,
  insertAmazonLedgerStagingBatch,
  purgeAmazonLedgerStagingRetention,
} from "../lib/amazon-ledger-actions";
import { guessLedgerSnapshotDate, parseCsvToRecords } from "../../../lib/csv-parse-basic";
import { supabase } from "../../../src/lib/supabase";
import {
  listCompaniesForImports,
  saveHomeCompanyForProfile,
  type CompanyOption,
} from "./companies-actions";
import { DB_TABLES } from "../lib/constants";

const RAW_REPORTS_BUCKET = "raw-reports";

const LEDGER_CHUNK = 500;

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

/** Min/max calendar days from parsed row dates (YYYY-MM-DD), or null if none parse. */
function computeLedgerDateBounds(records: Record<string, string>[]): { min: string; max: string } | null {
  let min = "";
  let max = "";
  for (const row of records) {
    const dateStr = extractRowDate(row);
    if (!dateStr) continue;
    const day = dateStr.slice(0, 10);
    if (!min || day < min) min = day;
    if (!max || day > max) max = day;
  }
  if (!min || !max) return null;
  return { min, max };
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

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
  total: number;
  totalInFile: number;
  message: string;
};

type AmazonLedgerUploaderProps = {
  /** Fired when “Target company” resolves so History can scope by `company_id`. */
  onTargetCompanyChange?: (companyId: string) => void;
};

export function AmazonLedgerUploader({ onTargetCompanyChange }: AmazonLedgerUploaderProps = {}) {
  const { role, actorUserId, organizationId, homeCompanyId, refreshProfile } = useUserRole();

  const [ledgerRetentionDays, setLedgerRetentionDays] = useState(60);
  const [ledgerRetentionEnabled, setLedgerRetentionEnabled] = useState(true);
  const [companyOptions, setCompanyOptions] = useState<CompanyOption[]>([]);
  const [ledgerTargetOrg, setLedgerTargetOrg] = useState("");
  const [ledgerStartDate, setLedgerStartDate] = useState("");
  const [ledgerEndDate, setLedgerEndDate] = useState("");
  const [ledgerDedup, setLedgerDedup] = useState(true);
  /** When true, date pickers are hidden and every CSV row is processed. */
  const [importFullFileNoDateFilter, setImportFullFileNoDateFilter] = useState(false);

  const [ledgerPhase, setLedgerPhase] = useState<LedgerPhase>("idle");
  const [ledgerFile, setLedgerFile] = useState<File | null>(null);
  const [storagePath, setStoragePath] = useState<string | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [ledgerProgress, setLedgerProgress] = useState<LedgerProgress | null>(null);
  const [ledgerErr, setLedgerErr] = useState<string | null>(null);

  const stopProcessingRef = useRef(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  /** Matches `profiles.id` / `auth.users.id` when the Supabase session is present. */
  const [sessionUserId, setSessionUserId] = useState<string | null>(null);

  const effectiveActorId = actorUserId ?? sessionUserId;

  const dateRangeReady =
    importFullFileNoDateFilter ||
    (Boolean(ledgerStartDate.trim() && ledgerEndDate.trim()) &&
      ledgerStartDate.trim() <= ledgerEndDate.trim());

  const companyReady =
    isUuidString(ledgerTargetOrg.trim()) &&
    companyOptions.some((c) => c.id === ledgerTargetOrg.trim());

  const canStartUpload =
    ledgerPhase === "file-ready" &&
    dateRangeReady &&
    companyReady &&
    ledgerFile !== null;

  useEffect(() => {
    void supabase.auth.getUser().then(({ data: { user } }) => {
      setSessionUserId(user?.id && isUuidString(user.id) ? user.id : null);
    });
    const { data: sub } = supabase.auth.onAuthStateChange((_event, session) => {
      const uid = session?.user?.id;
      setSessionUserId(uid && isUuidString(uid) ? uid : null);
    });
    return () => {
      sub.subscription.unsubscribe();
    };
  }, []);

  useEffect(() => {
    if (!isAdminRole(role)) return;
    let cancelled = false;
    void listCompaniesForImports().then((res) => {
      if (cancelled || !res.ok) return;
      setCompanyOptions(res.rows);
      setLedgerTargetOrg((prev) => {
        const p = prev.trim();
        if (p && res.rows.some((r) => r.id === p)) return p;
        if (organizationId && res.rows.some((r) => r.id === organizationId)) {
          return organizationId;
        }
        if (homeCompanyId && res.rows.some((r) => r.id === homeCompanyId)) {
          return homeCompanyId;
        }
        return "";
      });
    });
    return () => {
      cancelled = true;
    };
  }, [role, organizationId, homeCompanyId]);

  useEffect(() => {
    if (organizationId && !ledgerTargetOrg.trim()) {
      setLedgerTargetOrg(organizationId);
    }
  }, [organizationId, ledgerTargetOrg]);

  useEffect(() => {
    const id = ledgerTargetOrg.trim();
    if (id && isUuidString(id)) onTargetCompanyChange?.(id);
  }, [ledgerTargetOrg, onTargetCompanyChange]);

  async function handleTargetCompanyChange(nextRaw: string) {
    const nextId = nextRaw.trim();
    setLedgerTargetOrg(nextId);
    setLedgerErr(null);
    if (homeCompanyId) return;
    const aid = actorUserId ?? sessionUserId;
    if (!aid || !isUuidString(nextId)) return;
    const res = await saveHomeCompanyForProfile(aid, nextId);
    if (res.ok) {
      void refreshProfile();
    } else {
      setLedgerErr(res.error);
    }
  }

  function acceptFile(file: File) {
    if (!dateRangeReady) {
      setLedgerErr(
        importFullFileNoDateFilter
          ? "Confirm import mode before choosing a file."
          : "Set both Start date and End date before choosing a file.",
      );
      return;
    }
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

  async function handleStartUpload() {
    if (!canStartUpload || !ledgerFile) return;

    const { data: authData, error: authErr } = await supabase.auth.getUser();
    if (authErr || !authData.user?.id) {
      setLedgerErr("Not authenticated. Sign in again.");
      return;
    }

    const orgId = ledgerTargetOrg.trim();
    if (!isUuidString(orgId)) {
      setLedgerErr("Select a valid target company.");
      return;
    }

    setLedgerPhase("uploading");
    setLedgerErr(null);

    try {
      const ext = ledgerFile.name.split(".").pop() ?? "csv";
      const ts = Date.now();
      const path = `amazon-ledger/${orgId}/${ts}.${ext}`;

      const { error: upErr } = await supabase.storage.from(RAW_REPORTS_BUCKET).upload(path, ledgerFile, {
        upsert: true,
        contentType: ledgerFile.type || "text/csv",
      });

      if (upErr) {
        throw new Error(upErr.message);
      }

      setStoragePath(path);
      setLedgerPhase("storage-ready");
    } catch (e) {
      setLedgerPhase("file-ready");
      setLedgerErr(e instanceof Error ? e.message : "Upload failed.");
    }
  }

  async function handleProcessToDb() {
    if (!ledgerFile || ledgerPhase !== "storage-ready") return;
    if (!effectiveActorId) {
      setLedgerErr("Not authenticated. Sign in again.");
      return;
    }

    const start = ledgerStartDate.trim();
    const end = ledgerEndDate.trim();

    setLedgerPhase("processing");
    setLedgerErr(null);
    stopProcessingRef.current = false;

    try {
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

      const filtered: Record<string, string>[] = [];
      let skippedByFilter = 0;

      if (importFullFileNoDateFilter) {
        for (const row of allRecords) {
          if (stopProcessingRef.current) break;
          filtered.push(row);
        }
      } else {
        for (const row of allRecords) {
          if (stopProcessingRef.current) break;
          const dateStr = extractRowDate(row);
          if (dateStr && dateStr >= start && dateStr <= end) {
            filtered.push(row);
          } else {
            skippedByFilter++;
          }
        }
      }

      const summaryMsg = importFullFileNoDateFilter
        ? `Found ${allRecords.length.toLocaleString()} rows · full file import (no date filter)`
        : `Found ${allRecords.length.toLocaleString()} rows · ${filtered.length.toLocaleString()} in date range · ${skippedByFilter.toLocaleString()} outside filter`;

      setLedgerProgress({
        pct: 0,
        imported: 0,
        skipped: skippedByFilter,
        total: filtered.length,
        totalInFile: allRecords.length,
        message: summaryMsg,
      });

      if (stopProcessingRef.current) {
        setLedgerPhase("storage-ready");
        setLedgerErr("Processing stopped before inserting.");
        return;
      }

      if (filtered.length === 0) {
        setLedgerProgress((p) =>
          p
            ? {
                ...p,
                pct: 100,
                message: importFullFileNoDateFilter
                  ? "No rows to insert."
                  : "No rows matched the date filter — nothing inserted.",
              }
            : p,
        );
        setLedgerPhase("done");
        return;
      }

      const requestedOrg =
        role === "super_admin" && isUuidString(ledgerTargetOrg.trim())
          ? ledgerTargetOrg.trim()
          : undefined;

      if (ledgerDedup) {
        let dedupStart = start;
        let dedupEnd = end;
        if (importFullFileNoDateFilter) {
          const bounds = computeLedgerDateBounds(allRecords);
          if (!bounds) {
            setLedgerErr(
              "Replace existing data requires at least one row with a parseable Date / Date-Time so the staging range can be determined.",
            );
            setLedgerPhase("storage-ready");
            return;
          }
          dedupStart = bounds.min;
          dedupEnd = bounds.max;
        }

        setLedgerProgress((p) =>
          p ? { ...p, message: "Removing existing rows for this date range…" } : p,
        );
        const del = await deleteAmazonLedgerStagingByDateRange({
          actorProfileId: effectiveActorId,
          requestedOrganizationId: requestedOrg,
          startDate: dedupStart,
          endDate: dedupEnd,
        });
        if (!del.ok) {
          setLedgerErr(del.error ?? "Dedup delete failed.");
          setLedgerPhase("storage-ready");
          return;
        }
      }

      if (ledgerRetentionEnabled) {
        setLedgerProgress((p) =>
          p ? { ...p, message: "Purging rows outside retention window…" } : p,
        );
        const purge = await purgeAmazonLedgerStagingRetention({
          actorProfileId: effectiveActorId,
          requestedOrganizationId: requestedOrg,
          retentionDays: ledgerRetentionDays,
        });
        if (!purge.ok) {
          setLedgerErr(purge.error ?? "Purge failed.");
          setLedgerPhase("storage-ready");
          return;
        }
      }

      let imported = 0;
      const total = filtered.length;

      for (let i = 0; i < total; i += LEDGER_CHUNK) {
        if (stopProcessingRef.current) {
          setLedgerProgress((p) =>
            p ? { ...p, message: `Stopped. ${imported.toLocaleString()} of ${total.toLocaleString()} rows inserted.` } : p,
          );
          setLedgerPhase("storage-ready");
          return;
        }

        const batch = filtered.slice(i, i + LEDGER_CHUNK);
        const res = await insertAmazonLedgerStagingBatch({
          actorProfileId: effectiveActorId,
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
          p ? { ...p, pct, imported, message: `Inserting… ${imported.toLocaleString()} / ${total.toLocaleString()} rows` } : p,
        );
      }

      const doneMsg = importFullFileNoDateFilter
        ? `Done! ${imported.toLocaleString()} row(s) inserted (full file, no date filter).`
        : `Done! ${imported.toLocaleString()} row(s) inserted, ${skippedByFilter.toLocaleString()} skipped by date filter.`;

      setLedgerProgress((p) => (p ? { ...p, pct: 100, imported, message: doneMsg } : p));
      setLedgerPhase("done");
    } catch (e) {
      setLedgerErr(e instanceof Error ? e.message : "Processing failed.");
      setLedgerPhase("storage-ready");
    }
  }

  function handleStopProcessing() {
    stopProcessingRef.current = true;
  }

  async function handleDeleteFile() {
    if (!storagePath || !effectiveActorId) return;
    if (!window.confirm("Remove this CSV from Storage and reset the uploader?")) return;

    const res = await deleteAmazonLedgerStorageFile({
      actorProfileId: effectiveActorId,
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

  // Imports nav is shown for admin + super_admin; ledger must match (not super_admin-only).
  if (!isAdminRole(role)) return null;

  /** Mirrors legacy Imports “Upload progress” / “Processing status” bars, tied to this uploader only. */
  const linkedUploadPct =
    ledgerPhase === "idle" || ledgerPhase === "file-ready"
      ? 0
      : ledgerPhase === "uploading"
        ? 50
        : ledgerPhase === "storage-ready" || ledgerPhase === "processing" || ledgerPhase === "done"
          ? 100
          : 0;

  const linkedProcessPct =
    ledgerPhase === "processing" || ledgerPhase === "done"
      ? Math.min(100, ledgerProgress?.pct ?? (ledgerPhase === "done" ? 100 : 0))
      : 0;

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-800 dark:bg-slate-950">
      <h2 className="flex items-center gap-2 text-lg font-semibold text-slate-900 dark:text-slate-100">
        <CalendarRange className="h-5 w-5 text-sky-500" />
        Amazon Inventory Ledger
        <span className="ml-auto rounded-full bg-violet-100 px-2.5 py-0.5 text-[11px] font-semibold uppercase tracking-wide text-violet-700 dark:bg-violet-900/40 dark:text-violet-300">
          {role === "super_admin" ? "Super Admin" : "Admin"}
        </span>
      </h2>
      <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
        Two-step process: upload the raw CSV to Storage, then process rows into{" "}
        <code className="rounded bg-slate-100 px-1 dark:bg-slate-800">{DB_TABLES.amazonLedgerStaging}</code>{" "}
        in chunks of {LEDGER_CHUNK}. Never modifies returns.
      </p>

      {ledgerErr && (
        <div className="mt-4 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900/50 dark:bg-red-950/40 dark:text-red-200">
          {ledgerErr}
        </div>
      )}

      {/* Step 1: Import mode + optional date filter */}
      <div
        className={`mt-5 rounded-xl border p-4 transition-colors ${
          dateRangeReady
            ? "border-sky-200 bg-sky-50/60 dark:border-sky-800/50 dark:bg-sky-950/20"
            : "border-amber-200 bg-amber-50/60 dark:border-amber-800/50 dark:bg-amber-950/20"
        }`}
      >
        <p
          className={`text-xs font-semibold uppercase tracking-wide ${
            dateRangeReady ? "text-sky-700 dark:text-sky-400" : "text-amber-700 dark:text-amber-400"
          }`}
        >
          Step 1 — Import scope
        </p>
        <label className="mt-3 flex cursor-pointer items-start gap-2 text-sm text-slate-700 dark:text-slate-300">
          <input
            type="checkbox"
            checked={importFullFileNoDateFilter}
            onChange={(e) => {
              const next = e.target.checked;
              setImportFullFileNoDateFilter(next);
              setLedgerErr(null);
              if (next) {
                setLedgerStartDate("");
                setLedgerEndDate("");
              }
            }}
            disabled={ledgerPhase === "uploading" || ledgerPhase === "processing"}
            className="mt-0.5 rounded border-slate-300 disabled:opacity-50"
          />
          <span>
            <span className="font-medium">Import Full File (No Date Filter)</span>
            <span className="ml-1 text-xs text-slate-500 dark:text-slate-400">
              — every row in the CSV is processed; date pickers are not used.
            </span>
          </span>
        </label>

        {!importFullFileNoDateFilter ? (
          <>
            <p className="mt-3 text-xs font-semibold uppercase tracking-wide text-slate-600 dark:text-slate-400">
              Date range <span className="text-red-500">*</span>
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
          </>
        ) : null}
      </div>

      {/* Target company (required; saves to profile when yours was unset) */}
      <label className="mt-4 block text-xs font-medium text-slate-600 dark:text-slate-400">
        Target company <span className="text-red-500">*</span>
        {!homeCompanyId && (
          <span className="ml-2 font-normal text-amber-600 dark:text-amber-400">
            Your profile has no company yet — choose one to attach it to your account.
          </span>
        )}
        <select
          value={ledgerTargetOrg}
          onChange={(e) => void handleTargetCompanyChange(e.target.value)}
          required
          disabled={ledgerPhase === "uploading" || ledgerPhase === "processing"}
          className="mt-1 w-full max-w-md rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900 disabled:opacity-50"
        >
          <option value="" disabled>
            {companyOptions.length === 0 ? "Loading companies…" : "Select a company…"}
          </option>
          {companyOptions.map((o) => (
            <option key={o.id} value={o.id}>
              {o.display_name}
            </option>
          ))}
        </select>
      </label>
      {!companyReady && companyOptions.length > 0 && (
        <p className="mt-1 text-xs text-amber-600 dark:text-amber-400">
          Select a target company before uploading.
        </p>
      )}

      {/* Dedup + retention */}
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

      {/* Drop zone */}
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
          <UploadCloud className={`h-8 w-8 ${ledgerPhase === "file-ready" ? "text-emerald-500" : "text-slate-400"}`} />
          {ledgerPhase === "file-ready" && ledgerFile ? (
            <>
              <div className="flex items-center gap-2">
                <FileText className="h-4 w-4 text-emerald-600" />
                <span className="text-sm font-semibold text-emerald-700 dark:text-emerald-300">{ledgerFile.name}</span>
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

      {/* Ledger-only progress (reference layout: directly under Step 2 drop zone) */}
      <div className="mt-5 max-w-3xl space-y-2">
        <div className="flex items-center justify-between text-xs font-medium text-slate-600 dark:text-slate-400">
          <span>Upload progress</span>
          <span>{linkedUploadPct}%</span>
        </div>
        <div className="h-2 w-full overflow-hidden rounded-full bg-slate-100 dark:bg-slate-800">
          <div
            className="h-full rounded-full bg-blue-600 transition-[width] duration-300 ease-out dark:bg-blue-500"
            style={{ width: `${Math.min(100, linkedUploadPct)}%` }}
          />
        </div>
        <div className="flex items-center justify-between text-xs font-medium text-slate-600 dark:text-slate-400">
          <span>Processing status</span>
          <span>{linkedProcessPct}%</span>
        </div>
        <div className="h-2 w-full overflow-hidden rounded-full bg-slate-100 dark:bg-slate-800">
          <div
            className="h-full rounded-full bg-blue-500/90 transition-[width] duration-300 ease-out dark:bg-blue-400/90"
            style={{ width: `${Math.min(100, linkedProcessPct)}%` }}
          />
        </div>
      </div>

      {/* File info bar */}
      {ledgerFile && ledgerPhase !== "idle" && ledgerPhase !== "file-ready" && (
        <div className="mt-5 flex items-center gap-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 dark:border-slate-700 dark:bg-slate-900">
          <FileText className="h-5 w-5 shrink-0 text-slate-400" />
          <div className="min-w-0 flex-1">
            <p className="truncate text-sm font-medium text-slate-800 dark:text-slate-200">{ledgerFile.name}</p>
            <p className="text-xs text-slate-500">{formatBytes(ledgerFile.size)}</p>
          </div>
          {storagePath && (
            <span className="rounded-full bg-emerald-100 px-2 py-0.5 text-[11px] font-semibold text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300">
              In Storage
            </span>
          )}
        </div>
      )}

      {/* Action buttons */}
      <div className="mt-5 flex flex-wrap items-center gap-3">
        {ledgerPhase === "file-ready" && (
          <button
            type="button"
            onClick={() => void handleStartUpload()}
            disabled={!canStartUpload}
            title={
              !dateRangeReady
                ? importFullFileNoDateFilter
                  ? "Confirm import mode"
                  : "Set both Start Date and End Date first"
                : !companyReady
                  ? "Select a target company"
                  : !ledgerFile
                    ? "Select a file first"
                    : undefined
            }
            className="inline-flex items-center gap-2 rounded-xl bg-sky-600 px-5 py-2.5 text-sm font-semibold text-white hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-40"
          >
            <Upload className="h-4 w-4" />
            Start Upload
          </button>
        )}

        {ledgerPhase === "uploading" && (
          <div className="inline-flex items-center gap-2 rounded-xl bg-sky-100 px-5 py-2.5 text-sm font-semibold text-sky-700 dark:bg-sky-950/40 dark:text-sky-300">
            <Loader2 className="h-4 w-4 animate-spin" />
            Uploading to Storage…
          </div>
        )}

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

      {/* Progress + stats */}
      {ledgerProgress && (
        <div className="mt-5 space-y-3">
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
                  {importFullFileNoDateFilter ? "To import" : "In date range"}:{" "}
                  {ledgerProgress.total.toLocaleString()}
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

          <p className="text-sm text-slate-600 dark:text-slate-400">{ledgerProgress.message}</p>
        </div>
      )}
    </section>
  );
}
