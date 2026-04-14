"use client";

/**
 * UniversalImporter — Phase 1 only.
 *
 * Upload button sequence (strict):
 *   1. Insert raw_report_uploads row with status='uploading'  ← FIRST
 *   2. router.refresh()                                       ← History shows row immediately
 *   3. Upload file to Storage as a single object
 *   4. Read CSV headers → call AI classify API
 *   5. Update DB: report_type, column_mapping, status='mapped'|'needs_mapping'
 *   6. router.refresh()
 *   STOP — no CSV row parsing, no chunking here.
 *
 * Phase 2 (Process), Phase 3 (Sync), and Phase 4 (Generate Worklist for removals)
 * are triggered from this panel.
 */

import React, { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { CheckCircle2, FileText, Loader2, RefreshCw, SquareX, Trash2, UploadCloud, X, Zap } from "lucide-react";
import { isAdminRole, useUserRole } from "../../../components/UserRoleContext";
import { isUuidString } from "../../../lib/uuid";
import {
  createRawReportUploadSession,
  deleteRawReportUpload,
  finalizeRawReportUpload,
  getImportLedgerActorUserId,
  updateUploadSessionClassification,
} from "./import-actions";
import { createLedgerStorageSignedUploadUrl } from "../lib/amazon-ledger-actions";
import { classifyCsvHeadersRuleBased } from "../../../lib/csv-import-detected-type";
import { findHeaderRowIndex, parseCsvToMatrix } from "../../../lib/csv-parse-basic";
import {
  contentSuggestsReportsRepositorySample,
  fileNameSuggestsReportsRepository,
  findReportsRepositoryHeaderLineIndex,
  REPORTS_REPOSITORY_PREAMBLE_LINE_COUNT,
  sliceCsvFromHeaderLine,
} from "../../../lib/reports-repository-header";
import { supabase } from "../../../src/lib/supabase";
import type { RawReportType } from "../../../lib/raw-report-types";
import { isListingReportType } from "../../../lib/raw-report-types";
import { listStores } from "../../settings/adapters/actions";
import { formatImportPhaseLabel } from "../../../lib/pipeline/import-phase-labels";

const RAW_REPORTS_BUCKET = "raw-reports";

type ReportTypeChoice =
  | "AUTO"
  | "FBA_RETURNS"
  | "REMOVAL_ORDER"
  | "INVENTORY_LEDGER"
  | "REIMBURSEMENTS"
  | "SETTLEMENT"
  | "SAFET_CLAIMS"
  | "TRANSACTIONS"
  | "TRANSACTIONS_REPORTS_REPO"
  | "CATEGORY_LISTINGS"
  | "ALL_LISTINGS"
  | "ACTIVE_LISTINGS";

const REPORT_TYPE_OPTIONS: { value: ReportTypeChoice; label: string }[] = [
  { value: "AUTO",           label: "✨ Auto-Detect (Recommended)" },
  { value: "FBA_RETURNS",    label: "FBA Returns" },
  { value: "REMOVAL_ORDER",  label: "Removal Orders" },
  { value: "INVENTORY_LEDGER", label: "Inventory Ledger" },
  { value: "REIMBURSEMENTS", label: "Reimbursements" },
  { value: "SETTLEMENT",     label: "Settlements" },
  { value: "SAFET_CLAIMS",   label: "SAFE-T Claims" },
  { value: "TRANSACTIONS",   label: "Transactions" },
  { value: "TRANSACTIONS_REPORTS_REPO", label: "Transactions (Reports Repository)" },
  { value: "CATEGORY_LISTINGS", label: "Category Listings" },
  { value: "ALL_LISTINGS", label: "All Listings" },
  { value: "ACTIVE_LISTINGS", label: "Active Listings" },
];

type Phase =
  | "idle"
  | "uploading"       // Phase 1 in progress
  | "mapped"          // Phase 1 done — ready for Process
  | "needs_mapping"   // Phase 1 done but AI mapping incomplete
  | "unsupported"     // AI identified file but it is not a supported report type
  | "processing"      // Phase 2 in progress
  | "staged"          // Phase 2 done — ready for Sync
  | "syncing"         // Phase 3 in progress
  | "synced"          // Phase 3 done (removal files: ready for Phase 4)
  | "worklisting"     // Phase 4 in progress (expected_packages)
  | "worklisted"      // Phase 4 done
  | "error";

type StoreOption = { id: string; name: string; platform: string; is_default?: boolean | null };

type Props = {
  onUploadComplete?: () => void;
  onTargetStoreChange?: (storeId: string) => void;
};

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

/** Full-file SHA-256 (64 hex). Matches RawReportUploader — enables same-file cleanup (`metadata.content_sha256`) on sync. */
async function sha256HexFullFile(file: File): Promise<string> {
  const buf = await file.arrayBuffer();
  const hashBuf = await crypto.subtle.digest("SHA-256", buf);
  return Array.from(new Uint8Array(hashBuf))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/** Legacy 32-hex fingerprint for `md5_hash` validation (first 32 chars of full SHA-256). */
function first32HexOfSha256(fullSha256: string): string {
  return fullSha256.slice(0, 32).toLowerCase();
}

async function getBrowserSessionUserId(): Promise<string | null> {
  const { data: s } = await supabase.auth.getSession();
  if (s.session?.user?.id && isUuidString(s.session.user.id)) return s.session.user.id;
  const { data: r } = await supabase.auth.refreshSession();
  if (r.session?.user?.id && isUuidString(r.session.user.id)) return r.session.user.id;
  const {
    data: { user },
  } = await supabase.auth.getUser();
  return user?.id && isUuidString(user.id) ? user.id : null;
}

function choiceToInitialReportType(choice: ReportTypeChoice): RawReportType {
  if (choice === "AUTO") return "UNKNOWN";
  if (choice === "FBA_RETURNS") return "FBA_RETURNS";
  if (choice === "REMOVAL_ORDER") return "REMOVAL_ORDER";
  if (choice === "INVENTORY_LEDGER") return "INVENTORY_LEDGER";
  if (choice === "REIMBURSEMENTS") return "REIMBURSEMENTS";
  if (choice === "SETTLEMENT") return "SETTLEMENT";
  if (choice === "SAFET_CLAIMS") return "SAFET_CLAIMS";
  if (choice === "TRANSACTIONS") return "TRANSACTIONS";
  if (choice === "TRANSACTIONS_REPORTS_REPO") return "REPORTS_REPOSITORY";
  if (choice === "CATEGORY_LISTINGS") return "CATEGORY_LISTINGS";
  if (choice === "ALL_LISTINGS") return "ALL_LISTINGS";
  if (choice === "ACTIVE_LISTINGS") return "ACTIVE_LISTINGS";
  return "UNKNOWN";
}

/** Keyword/regex failed → optional gpt-4o-mini line index; else Amazon default row 9. */
async function resolveReportsRepositoryHeaderLineIndexWithFallback(
  firstTwentyLinesJoined: string,
  fileName: string,
  manualRepo: boolean,
  actorUserId: string,
): Promise<number> {
  const det = findReportsRepositoryHeaderLineIndex(firstTwentyLinesJoined);
  if (det.method !== "fallback_zero") return det.index;
  if (!manualRepo && !fileNameSuggestsReportsRepository(fileName)) return det.index;
  const lines = firstTwentyLinesJoined.split(/\r?\n/).slice(0, 20);
  try {
    const res = await fetch("/api/settings/imports/reports-repo-header-ai", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lines, actor_user_id: actorUserId }),
    });
    const j = (await res.json()) as { ok?: boolean; line_index?: number | null };
    if (res.ok && j.ok && typeof j.line_index === "number" && j.line_index >= 0 && j.line_index < 20) {
      return j.line_index;
    }
  } catch {
    /* optional AI */
  }
  return REPORTS_REPOSITORY_PREAMBLE_LINE_COUNT;
}

export function UniversalImporter({ onUploadComplete, onTargetStoreChange }: Props = {}) {
  const router = useRouter();
  const { role, actorUserId } = useUserRole();

  const [reportTypeChoice, setReportTypeChoice] = useState<ReportTypeChoice>("AUTO");

  const [stores, setStores] = useState<StoreOption[]>([]);
  const [storeId, setStoreId] = useState("");

  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [importFullFile, setImportFullFile] = useState(false);
  const [dedup, setDedup] = useState(true);
  const [retentionEnabled, setRetentionEnabled] = useState(true);
  const [retentionDays, setRetentionDays] = useState(60);

  const [file, setFile] = useState<File | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const [phase, setPhase] = useState<Phase>("idle");
  const [uploadPct, setUploadPct] = useState(0);
  const [progressMsg, setProgressMsg] = useState("");
  const [err, setErr] = useState<string | null>(null);

  // What the AI detected — shown as a label after Phase 1
  const [detectedType, setDetectedType] = useState<string | null>(null);

  // Row counting: total from Phase 1, processed/pct from Phase 2 polling
  const [totalRows, setTotalRows] = useState(0);
  const [processedRows, setProcessedRows] = useState(0);
  const [processPct, setProcessPct] = useState(0);
  const [syncPct, setSyncPct] = useState(0);
  const [worklistPct, setWorklistPct] = useState(0);
  /** Remount Phase 4 bar so width/transition does not carry over from Phase 3’s completed state. */
  const [worklistBarEpoch, setWorklistBarEpoch] = useState(0);
  /** Listing process: raw_archive | canonical_sync | done — from upload metadata while processing. */
  const [listingImportSubphase, setListingImportSubphase] = useState<string | null>(null);
  const [phaseLabel, setPhaseLabel] = useState<string>("");

  const pollRef2 = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopRef = useRef(false);
  const [sessionUploadId, setSessionUploadId] = useState<string | null>(null);
  const [sessionUserId, setSessionUserId] = useState<string | null>(null);
  const effectiveActorId = actorUserId ?? sessionUserId;

  const isUploading = phase === "uploading";
  const isProcessing = phase === "processing";
  const isSyncing = phase === "syncing";
  const isWorklisting = phase === "worklisting";
  const isActive = isUploading || isProcessing || isSyncing || isWorklisting;

  const isRemovalReport =
    detectedType === "REMOVAL_ORDER" || detectedType === "REMOVAL_SHIPMENT";
  const isListingCatalogReport = detectedType != null && isListingReportType(detectedType);

  const dateRangeValid = importFullFile || (!!startDate && !!endDate && startDate <= endDate);
  const needsDateRange = reportTypeChoice === "INVENTORY_LEDGER" && !importFullFile;

  const canUpload =
    !isActive &&
    phase === "idle" &&
    !!file &&
    !!storeId &&
    (needsDateRange ? dateRangeValid : true);

  useEffect(() => {
    void supabase.auth.getUser().then(({ data: { user } }) => {
      if (user?.id && isUuidString(user.id)) setSessionUserId(user.id);
    });
    const { data: sub } = supabase.auth.onAuthStateChange((_e, session) => {
      const uid = session?.user?.id;
      setSessionUserId(uid && isUuidString(uid) ? uid : null);
    });
    return () => sub.subscription.unsubscribe();
  }, []);

  useEffect(() => {
    let cancelled = false;
    void listStores().then((res) => {
      if (cancelled) return;
      if (!res.ok || !res.data) {
        console.error("[UniversalImporter] stores fetch error:", res.ok === false ? res.error : "No data");
        return;
      }
      console.log("[UniversalImporter] Fetched stores:", res.data.length, "row(s)");
      const active = res.data
        .filter((s) => s.is_active !== false)
        .map((s) => ({
          id: s.id,
          name: s.name,
          platform: s.platform,
          is_default: s.is_default ?? null,
        }));
      setStores(active);

      // Auto-populate the Target Store dropdown on mount so the user doesn't
      // have to select it manually.  Priority order:
      //   1. Store explicitly marked is_default for this organisation.
      //   2. Single active store → always auto-select.
      //   3. First Amazon / FBA platform store (most common import source).
      //   4. First store overall as a reasonable fallback.
      const defaultStore = active.find((s) => s.is_default === true);
      if (defaultStore) {
        setStoreId(defaultStore.id);
      } else if (active.length === 1) {
        setStoreId(active[0].id);
      } else if (active.length > 1) {
        const amazon = active.find((s) => {
          const p = s.platform.toLowerCase();
          return p.includes("amazon") || p === "fba" || p === "amazon_fba";
        });
        setStoreId((amazon ?? active[0]).id);
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (storeId && isUuidString(storeId)) onTargetStoreChange?.(storeId);
  }, [storeId, onTargetStoreChange]);

  function bumpHistory() {
    onUploadComplete?.();
    router.refresh();
  }

  /** Validate the dropped/selected file and set state — no DB interaction here. */
  function acceptFile(f: File) {
    const ext = f.name.split(".").pop()?.toLowerCase() ?? "";
    const allowed = ["csv", "txt", "xlsx", "xls"];
    if (!allowed.includes(ext)) {
      setErr(`Unsupported file type .${ext}. Allowed: ${allowed.map((e) => `.${e}`).join(", ")}`);
      return;
    }
    if (!storeId) {
      setErr("Select a target store before choosing a file.");
      return;
    }
    if (pollRef2.current) {
      clearInterval(pollRef2.current);
      pollRef2.current = null;
    }
    setFile(f);
    setErr(null);
    setPhase("idle");
    setUploadPct(0);
    setProgressMsg("");
    setSessionUploadId(null);
    setTotalRows(0);
    setProcessedRows(0);
    setProcessPct(0);
    setSyncPct(0);
    stopRef.current = false;
  }

  function clearForm() {
    setErr(null);
    setFile(null);
    setPhase("idle");
    setUploadPct(0);
    setProgressMsg("");
    setSessionUploadId(null);
    setDetectedType(null);
    setTotalRows(0);
    setProcessedRows(0);
    setProcessPct(0);
    setSyncPct(0);
    setWorklistPct(0);
    setWorklistBarEpoch(0);
    setListingImportSubphase(null);
    setPhaseLabel("");
    stopRef.current = false;
    if (pollRef2.current) { clearInterval(pollRef2.current); pollRef2.current = null; }
  }

  async function reset() {
    clearForm();
    bumpHistory();
  }

  async function deleteCurrentImport() {
    if (!sessionUploadId) { clearForm(); return; }
    if (!window.confirm("Delete this import record and its file from storage? This cannot be undone.")) return;
    await deleteRawReportUpload(sessionUploadId).catch(() => {});
    clearForm();
    bumpHistory();
  }

  /** Phase 2: read CSV from Storage → date-filter → insert into amazon_staging → status='staged'. */
  async function runProcess() {
    if (!sessionUploadId) return;
    setErr(null);
    setPhase("processing");
    setProcessPct(0);
    setProcessedRows(0);
    // Listing imports use /process → catalog_products; others use /stage. Prefer UI state
    // (detectedType) because the client read of report_type can be null/late vs. what Phase 1 set.
    const listingFromUi = isListingReportType(detectedType);
    setProgressMsg(
      listingFromUi ? "Parsing file lines and storing raw rows…" : "Processing CSV into staging table…",
    );
    setListingImportSubphase("raw_archive");
    setPhaseLabel(listingFromUi ? "Staging" : "Staging");

    // Poll DB frequently; prefer `file_processing_status` (true progress source).
    const uploadIdSnap = sessionUploadId;
    if (pollRef2.current) clearInterval(pollRef2.current);
    pollRef2.current = setInterval(() => {
      void Promise.all([
        supabase.from("raw_report_uploads").select("metadata").eq("id", uploadIdSnap).maybeSingle(),
        supabase.from("file_processing_status").select("*").eq("upload_id", uploadIdSnap).maybeSingle(),
      ]).then(([rpu, fps]) => {
        const m = rpu.data?.metadata as Record<string, unknown> | null;
        const fpsRow = fps.data as Record<string, unknown> | null | undefined;
        if (fpsRow && typeof fpsRow.process_pct === "number") {
          setProcessPct(Math.min(100, Math.max(0, Number(fpsRow.process_pct))));
          if (typeof fpsRow.sync_pct === "number") {
            setSyncPct(Math.min(100, Math.max(0, Number(fpsRow.sync_pct))));
          }
          if (typeof fpsRow.upload_pct === "number") {
            setUploadPct(Math.min(100, Math.max(0, Number(fpsRow.upload_pct))));
          }
          if (typeof fpsRow.processed_rows === "number") setProcessedRows(Number(fpsRow.processed_rows));
          if (typeof fpsRow.total_rows === "number" && fpsRow.total_rows > 0) setTotalRows(Number(fpsRow.total_rows));
          const cp = typeof fpsRow.current_phase === "string" ? fpsRow.current_phase : null;
          if (cp) setPhaseLabel(formatImportPhaseLabel(cp));
        } else if (m) {
          if (typeof m.process_progress === "number") setProcessPct(m.process_progress);
          if (typeof m.row_count === "number") setProcessedRows(m.row_count);
          if (typeof m.total_rows === "number" && m.total_rows > 0) setTotalRows(m.total_rows as number);
          const im = m.import_metrics as { current_phase?: string } | undefined;
          if (im?.current_phase) setPhaseLabel(formatImportPhaseLabel(im.current_phase));
        }
        if (m) {
          const lip = m.catalog_listing_import_phase;
          if (typeof lip === "string") {
            setListingImportSubphase(lip);
            setPhaseLabel(formatImportPhaseLabel(lip === "raw_archive" ? "staging" : lip === "canonical_sync" ? "processing" : lip));
          }
        }
      });
    }, listingFromUi ? 350 : 400);

    try {
      const { data: upRow } = await supabase
        .from("raw_report_uploads")
        .select("report_type")
        .eq("id", sessionUploadId)
        .maybeSingle();
      const sessionRt = String((upRow as { report_type?: string } | null)?.report_type ?? "").trim();
      const useListingProcess = listingFromUi || isListingReportType(sessionRt);

      const res = await fetch("/api/settings/imports/process", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(
          useListingProcess
            ? { upload_id: sessionUploadId }
            : {
                upload_id: sessionUploadId,
                start_date: importFullFile ? null : (startDate || null),
                end_date: importFullFile ? null : (endDate || null),
                import_full_file: importFullFile,
              },
        ),
      });
      const json = (await res.json()) as {
        ok?: boolean;
        error?: string;
        rowsStaged?: number;
        rowsProcessed?: number;
        totalRows?: number;
        catalogListing?: {
          message?: string;
          data_rows_seen?: number;
          physical_lines_after_header?: number;
          raw_rows_stored?: number;
          raw_rows_skipped_empty?: number;
          raw_rows_skipped_malformed?: number;
          canonical_new?: number;
          canonical_updated?: number;
          canonical_unchanged?: number;
          canonical_invalid_for_merge?: number;
        };
      };
      if (!res.ok || !json.ok) throw new Error(json.error ?? "Processing failed.");
      const staged = json.rowsStaged ?? json.rowsProcessed ?? 0;
      const total = json.totalRows ?? json.catalogListing?.data_rows_seen ?? totalRows;
      setProcessPct(100);
      setProcessedRows(staged);
      if (total > 0) setTotalRows(total);
      if (useListingProcess) {
        const cat = json.catalogListing;
        setListingImportSubphase("done");
        setProgressMsg(
          typeof cat?.message === "string" && cat.message.trim() !== ""
            ? cat.message
            : `Data rows: ${staged.toLocaleString()}`,
        );
        setPhase("synced");
      } else {
        setProgressMsg(
          total > 0 && total !== staged
            ? `Staged ${staged.toLocaleString()} / ${total.toLocaleString()} rows (date filter applied). Ready to Sync.`
            : `Staged ${staged.toLocaleString()} rows. Ready to Sync.`,
        );
        setPhase("staged");
      }
      bumpHistory();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Processing failed.");
      setPhase("mapped");
      bumpHistory();
    } finally {
      if (pollRef2.current) { clearInterval(pollRef2.current); pollRef2.current = null; }
    }
  }

  /** Phase 3: move rows from staging → domain tables → status='synced'. */
  async function runSync() {
    if (!sessionUploadId) return;
    setErr(null);
    setPhase("syncing");
    setSyncPct(0);
    setProgressMsg("Syncing to final tables…");
    setPhaseLabel("Syncing");
    const uploadIdSnap = sessionUploadId;
    if (pollRef2.current) clearInterval(pollRef2.current);
    pollRef2.current = setInterval(() => {
      void Promise.all([
        supabase.from("raw_report_uploads").select("metadata").eq("id", uploadIdSnap).maybeSingle(),
        supabase.from("file_processing_status").select("*").eq("upload_id", uploadIdSnap).maybeSingle(),
      ]).then(([rpu, fps]) => {
        const fpsRow = fps.data as Record<string, unknown> | null | undefined;
        if (fpsRow && typeof fpsRow.sync_pct === "number") {
          setSyncPct(Math.min(100, Math.max(0, Number(fpsRow.sync_pct))));
        } else {
          const m = rpu.data?.metadata as Record<string, unknown> | null;
          if (m && typeof m.sync_progress === "number") setSyncPct(m.sync_progress);
        }
        if (fpsRow && typeof fpsRow.current_phase === "string") {
          setPhaseLabel(formatImportPhaseLabel(fpsRow.current_phase));
        }
        const im = fpsRow?.import_metrics as
          | { rows_synced?: number; total_staging_rows?: number }
          | undefined;
        const tr = fpsRow && typeof fpsRow.total_rows === "number" ? Number(fpsRow.total_rows) : 0;
        const pr = fpsRow && typeof fpsRow.processed_rows === "number" ? Number(fpsRow.processed_rows) : 0;
        if (im && typeof im.rows_synced === "number" && typeof im.total_staging_rows === "number") {
          setProgressMsg(
            `Syncing… ${im.rows_synced.toLocaleString()} / ${im.total_staging_rows.toLocaleString()} rows`,
          );
        } else if (tr > 0 && pr >= 0) {
          setProgressMsg(`Syncing… ${pr.toLocaleString()} / ${tr.toLocaleString()} rows`);
        }
      });
    }, 400);
    try {
      const res = await fetch("/api/settings/imports/sync", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: sessionUploadId }),
      });
      const json = (await res.json()) as {
        ok?: boolean;
        error?: string;
        rowsSynced?: number;
        kind?: string;
      };
      if (!res.ok || !json.ok) throw new Error(json.error ?? "Sync failed.");
      if (json.kind === "REMOVAL_ORDER" || json.kind === "REMOVAL_SHIPMENT") {
        setDetectedType(json.kind);
      }
      setSyncPct(100);
      const rows = json.rowsSynced?.toLocaleString() ?? "?";
      const removal = json.kind === "REMOVAL_ORDER" || json.kind === "REMOVAL_SHIPMENT";
      setProgressMsg(
        removal
          ? json.kind === "REMOVAL_SHIPMENT"
            ? `Phase 3 complete — ${rows} shipment line(s) archived to amazon_removal_shipments; tracking applied to amazon_removals where rows matched. Run Generate Worklist (Phase 4) for expected_packages.`
            : `Phase 3 complete — ${rows} row(s) synced to amazon_removals. Run Generate Worklist (Phase 4) for expected_packages.`
          : `Sync complete — ${rows} rows written.`,
      );
      setPhase("synced");
      bumpHistory();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Sync failed.");
      setPhase("staged");
      bumpHistory();
    } finally {
      if (pollRef2.current) {
        clearInterval(pollRef2.current);
        pollRef2.current = null;
      }
    }
  }

  /** Phase 4 (removal imports only): FastAPI → expected_packages worklist for the warehouse scanner. */
  async function runWorklist() {
    if (!sessionUploadId) return;
    setErr(null);
    setWorklistBarEpoch((n) => n + 1);
    setWorklistPct(0);
    setPhase("worklisting");
    setProgressMsg("Building expected_packages worklist…");

    const uploadIdSnap = sessionUploadId;
    if (pollRef2.current) clearInterval(pollRef2.current);
    pollRef2.current = setInterval(() => {
      void supabase
        .from("raw_report_uploads")
        .select("metadata")
        .eq("id", uploadIdSnap)
        .maybeSingle()
        .then(({ data }) => {
          const m = data?.metadata as Record<string, unknown> | null;
          if (!m) return;
          if (typeof m.worklist_progress === "number") setWorklistPct(m.worklist_progress);
        });
    }, 400);

    try {
      const res = await fetch("/api/settings/imports/generate-worklist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: sessionUploadId }),
      });
      const json = (await res.json()) as { ok?: boolean; error?: string; message?: string };
      if (!res.ok || !json.ok) throw new Error(json.error ?? "Worklist generation failed.");
      setWorklistPct(100);
      setProgressMsg(json.message?.trim() || "expected_packages worklist is ready.");
      setPhase("worklisted");
      bumpHistory();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Worklist generation failed.");
      setPhase("synced");
      bumpHistory();
    } finally {
      if (pollRef2.current) {
        clearInterval(pollRef2.current);
        pollRef2.current = null;
      }
    }
  }

  /**
   * Phase 1 — the ONLY thing the Upload button does.
   * Strict sequence: DB insert → History refresh → Storage upload → AI classify → DB update → History refresh → STOP.
   */
  async function startUpload() {
    if (!canUpload || !file) return;

    setErr(null);
    setPhase("uploading");
    setUploadPct(0);
    setTotalRows(0);
    setProcessedRows(0);
    setProcessPct(0);
    setSyncPct(0);
    setProgressMsg("Creating import record…");
    stopRef.current = false;

    // ── Resolve actor ─────────────────────────────────────────────────────────
    let actor = effectiveActorId;
    if (!actor) {
      actor = await getBrowserSessionUserId();
      if (actor) setSessionUserId(actor);
    }
    if (!actor) {
      const srv = await getImportLedgerActorUserId();
      if (srv.ok) {
        actor = srv.actorUserId;
        setSessionUserId(actor);
      }
    }
    if (!actor) {
      setErr("Not authenticated. Please sign in again.");
      setPhase("error");
      return;
    }

    const selectedStore = storeId.trim();
    if (!isUuidString(selectedStore)) {
      setErr("Select a valid target store.");
      setPhase("error");
      return;
    }

    const ext = file.name.split(".").pop()?.toLowerCase() ?? "csv";
    const contentSha256 = await sha256HexFullFile(file);
    const md5Hash = first32HexOfSha256(contentSha256);
    const initialType = choiceToInitialReportType(reportTypeChoice);

    // ── STEP 1: Insert DB row FIRST ───────────────────────────────────────────
    // IMPORTANT: organizationId is intentionally NOT passed here.
    // The server action derives it from the actor's profile (guaranteeing a valid
    // organizations FK). Passing storeId directly would cause an FK violation
    // because stores.id != organizations.id.
    console.log("[UniversalImporter] creating session — actor:", actor, "store:", selectedStore, "file:", file.name);
    const session = await createRawReportUploadSession({
      fileName: file.name,
      totalBytes: file.size,
      reportType: initialType,
      md5Hash,
      contentSha256,
      fileExtension: ext,
      fileSizeBytes: file.size,
      uploadChunksCount: 1,
      initialStatus: "uploading",
      actorUserId: actor,
      // organizationId deliberately omitted — resolved server-side from actor's profile.
    });

    if (!session.ok) {
      setErr(session.error);
      setPhase("error");
      return;
    }

    const uploadId = session.id;
    const storagePrefix = session.storagePrefix;
    const filePath = `${storagePrefix}/original.${ext}`;
    setSessionUploadId(uploadId);

    // ── STEP 2: Force UI update — row appears in History immediately ──────────
    bumpHistory();

    try {
      // ── STEP 3: Upload file to Storage (Supabase SDK — same auth/RLS as before) ─
      setProgressMsg("Uploading to storage…");

      const signedRes = await createLedgerStorageSignedUploadUrl({
        actorProfileId: actor,
        path: filePath,
      });
      if (!signedRes.ok || !signedRes.token) {
        throw new Error(signedRes.error ?? "Could not create upload URL.");
      }

      const totalSz = file.size > 0 ? file.size : 1;
      let lastProgressPost = 0;
      const postByteProgress = (loaded: number, total: number) => {
        const pct = total > 0 ? Math.min(99, Math.round((loaded / total) * 100)) : 0;
        setUploadPct(pct);
        const now = Date.now();
        if (now - lastProgressPost < 120 && loaded < total) return;
        lastProgressPost = now;
        void fetch("/api/settings/imports/upload-progress", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            upload_id: uploadId,
            uploaded_bytes: loaded,
            total_bytes: total,
          }),
        });
      };

      const putUrl = signedRes.signedUrl?.trim() ?? "";
      if (putUrl.startsWith("http")) {
        await new Promise<void>((resolve, reject) => {
          const xhr = new XMLHttpRequest();
          xhr.open("PUT", putUrl);
          xhr.upload.onprogress = (ev) => {
            if (ev.lengthComputable) postByteProgress(ev.loaded, ev.total);
          };
          xhr.onload = () => {
            if (xhr.status >= 200 && xhr.status < 300) resolve();
            else reject(new Error(xhr.statusText || `Upload failed (${xhr.status})`));
          };
          xhr.onerror = () => reject(new Error("Network error during upload"));
          const ct = file.type && file.type.length > 0 ? file.type : "text/csv";
          xhr.setRequestHeader("Content-Type", ct);
          xhr.send(file);
        });
        postByteProgress(totalSz, totalSz);
      } else {
        const { error: upErr } = await supabase.storage
          .from(RAW_REPORTS_BUCKET)
          .uploadToSignedUrl(filePath, signedRes.token, file, {
            contentType: file.type || "text/csv",
          });
        if (upErr) throw new Error(upErr.message);
        postByteProgress(totalSz, totalSz);
      }

      setUploadPct(100);

      // ── STEP 4: Full text + dynamic Reports Repository header row ─────────────
      setProgressMsg("Reading CSV headers…");
      const fullText = await file.text();
      const delim = ext === "txt" ? "\t" : undefined;
      const contentSample = fullText.slice(0, 65536);

      const headerPeek = fullText.slice(0, Math.min(fullText.length, 65536));
      const matrixRaw = parseCsvToMatrix(headerPeek.trim(), delim);
      const headerRowIdxRaw = findHeaderRowIndex(matrixRaw);
      const headersRaw = (matrixRaw[headerRowIdxRaw] ?? []).map((h) => h.trim()).filter(Boolean);

      const first20 = fullText.split(/\r?\n/).slice(0, 20).join("\n");
      const manualReportsRepo = reportTypeChoice === "TRANSACTIONS_REPORTS_REPO";

      const detProbe = findReportsRepositoryHeaderLineIndex(first20);
      const probeSlice = fullText.split(/\r?\n/).slice(0, 20).slice(detProbe.index).join("\n");
      const probeHeaders = (parseCsvToMatrix(probeSlice.trim(), delim)[0] ?? [])
        .map((h) => h.trim())
        .filter(Boolean);
      const strippedRule = classifyCsvHeadersRuleBased(probeHeaders);

      const autoReportsRepo =
        reportTypeChoice === "AUTO" &&
        (strippedRule.reportType === "REPORTS_REPOSITORY" ||
          fileNameSuggestsReportsRepository(file.name) ||
          contentSuggestsReportsRepositorySample(contentSample));
      const useReportsRepoPreamble = manualReportsRepo || autoReportsRepo;

      let headerRowIdx: number;
      let headers: string[];
      let csvTotalRows: number;

      if (useReportsRepoPreamble) {
        headerRowIdx = await resolveReportsRepositoryHeaderLineIndexWithFallback(
          first20,
          file.name,
          manualReportsRepo,
          actor,
        );
        const csvFromHeader = sliceCsvFromHeaderLine(fullText, headerRowIdx);
        const matrix = parseCsvToMatrix(csvFromHeader.trim(), delim);
        headers = (matrix[0] ?? []).map((h) => h.trim()).filter(Boolean);
        setProgressMsg("Counting rows…");
        const lineCount = csvFromHeader.split(/\r?\n/).filter((l) => l.trim().length > 0).length;
        csvTotalRows = Math.max(0, lineCount - 1);
        setTotalRows(csvTotalRows);
      } else {
        headerRowIdx = headerRowIdxRaw;
        headers = headersRaw;
        setProgressMsg("Counting rows…");
        const lineCount = fullText.split(/\r?\n/).filter((l) => l.trim().length > 0).length;
        csvTotalRows = Math.max(0, lineCount - headerRowIdx - 1);
        setTotalRows(csvTotalRows);
      }

      // ── STEP 5: AI auto-detect ────────────────────────────────────────────────
      setProgressMsg("Running AI header classification…");
      const clsRes = await fetch("/api/settings/imports/classify-headers", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          headers,
          actor_user_id: actor,
          file_name: file.name,
          content_sample: contentSample,
        }),
      });
      const clsJson = (await clsRes.json()) as {
        ok?: boolean;
        report_type?: string;
        column_mapping?: Record<string, string>;
        needs_mapping?: boolean;
        detected_file_type?: string;
        is_supported?: boolean;
        message?: string;
        error?: string;
      };

      const userHint = choiceToInitialReportType(reportTypeChoice);
      const apiType =
        clsRes.ok && clsJson.ok && clsJson.report_type ? clsJson.report_type.trim() : "";
      let resolvedType: RawReportType;
      if (reportTypeChoice === "TRANSACTIONS_REPORTS_REPO") {
        resolvedType = "REPORTS_REPOSITORY";
      } else if (apiType && apiType !== "UNKNOWN") {
        resolvedType = apiType as RawReportType;
      } else if (userHint !== "UNKNOWN") {
        resolvedType = userHint;
      } else {
        resolvedType = "UNKNOWN";
      }
      const columnMapping = (clsRes.ok && clsJson.ok ? clsJson.column_mapping : null) ?? {};
      const needsMapping = (clsRes.ok && clsJson.ok ? clsJson.needs_mapping : true) ?? true;
      const detectedFileTypeName = clsJson.detected_file_type ?? (resolvedType !== "UNKNOWN" ? resolvedType : null);
      const isSupported = clsJson.is_supported !== false; // default true for backward-compat
      const aiMessage = clsJson.message ?? "";

      // ── STEP 6: Update DB — classification + file path + store + row count ──
      await updateUploadSessionClassification({
        uploadId,
        reportType: resolvedType,
        columnMapping,
        csvHeaders: headers,
        actorUserId: actor,
        rawFilePath: filePath,
        storeId: selectedStore,
        totalRows: csvTotalRows,
        importFullFile,
        startDate: importFullFile ? null : (startDate || null),
        endDate: importFullFile ? null : (endDate || null),
        headerRowIndex: headerRowIdx,
      });

      // ── STEP 7: Set final status ──────────────────────────────────────────────
      const dbTargetStatus = (!isSupported || needsMapping) ? "needs_mapping" : "mapped";
      await finalizeRawReportUpload({
        uploadId,
        actorUserId: actor,
        targetStatus: dbTargetStatus,
      });

      // ── STEP 8: Force UI update again ─────────────────────────────────────────
      setDetectedType(resolvedType !== "UNKNOWN" ? resolvedType : null);
      bumpHistory();

      if (!isSupported) {
        setProgressMsg(
          aiMessage ||
          `File identified as "${detectedFileTypeName ?? "unknown"}" — no database table configured for this type.`,
        );
        setPhase("unsupported");
      } else if (needsMapping) {
        setProgressMsg("AI mapping incomplete — columns need manual review before processing.");
        setPhase("needs_mapping");
      } else {
        setProgressMsg(
          aiMessage ||
          `AI recognized: ${detectedFileTypeName ?? resolvedType}. Click Process Data to stage rows.`,
        );
        setPhase("mapped");
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Upload failed.";
      setErr(msg);
      setPhase("error");
      await finalizeRawReportUpload({
        uploadId,
        actorUserId: actor,
        targetStatus: "needs_mapping",
      }).catch(() => {});
      bumpHistory();
    }
  }


  if (!isAdminRole(role)) return null;

  return (
    <section className="relative z-10 rounded-2xl border border-border bg-card p-6 shadow-sm">
      {/* Header */}
      <div className="flex items-center gap-2">
        <Zap className="h-5 w-5 text-sky-500" aria-hidden />
        <h2 className="text-lg font-semibold text-foreground">Universal Data Importer</h2>
        <span className="ml-auto rounded-full bg-violet-100 px-2.5 py-0.5 text-[11px] font-semibold uppercase tracking-wide text-violet-700 dark:bg-violet-900/40 dark:text-violet-300">
          {role === "super_admin" ? "Super Admin" : "Admin"}
        </span>
      </div>
      <p className="mt-1 text-xs text-muted-foreground">
        <strong>Removal Order / Removal Shipment</strong> files use four steps: Upload → Process → Sync to{" "}
        <code className="rounded bg-muted px-1">amazon_removals</code> →{" "}
        <strong>Generate Worklist</strong> (<code className="rounded bg-muted px-1">expected_packages</code>).
        Other report types use three steps. History mirrors every status change below.
      </p>

      {/* Error */}
      {err && (
        <div className="mt-4 flex items-start gap-2 rounded-xl border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          <X className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
          <span>{err}</span>
        </div>
      )}

      {/* ── Report type + Target store — 2-column grid ────────────────────── */}
      <div className="relative z-20 mt-5 grid grid-cols-1 gap-x-6 gap-y-4 sm:grid-cols-2">

        {/* Report type */}
        <div className="relative z-20 flex flex-col gap-1.5">
          <label
            className="block text-xs font-semibold uppercase tracking-wide text-muted-foreground"
            htmlFor="report-type-select"
          >
            Report type
          </label>
          <select
            id="report-type-select"
            value={reportTypeChoice}
            disabled={isActive || phase !== "idle"}
            onChange={(e) => setReportTypeChoice(e.target.value as ReportTypeChoice)}
            className="relative z-20 h-10 w-full rounded-lg border border-border bg-background px-3 text-sm text-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50"
          >
            {REPORT_TYPE_OPTIONS.map(({ value, label }) => (
              <option key={value} value={value}>{label}</option>
            ))}
          </select>
          {reportTypeChoice !== "AUTO" && (
            <p className="text-[11px] text-amber-600 dark:text-amber-400">
              Manual override active — AI will still verify the headers.
            </p>
          )}
        </div>

        {/* Target store */}
        <div className="relative z-20 flex flex-col gap-1.5">
          <label
            className="block text-xs font-semibold uppercase tracking-wide text-muted-foreground"
            htmlFor="target-store-select"
          >
            Target store <span className="text-destructive">*</span>
          </label>
          <select
            id="target-store-select"
            value={storeId}
            disabled={isActive || phase !== "idle"}
            onChange={(e) => setStoreId(e.target.value)}
            className="relative z-20 h-10 w-full rounded-lg border border-border bg-background px-3 text-sm text-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50"
          >
            <option value="">Select a store…</option>
            {stores.map((s) => (
              <option key={s.id} value={s.id}>
                {s.name} ({s.platform})
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* ── Import options ───────────────────────────────────────────────────── */}
      <div className="mt-5 space-y-3 rounded-xl border border-border bg-muted/40 px-4 py-3 overflow-visible">
        <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          Import options
        </p>

        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={importFullFile}
            onChange={(e) => setImportFullFile(e.target.checked)}
            disabled={isActive}
            className="rounded"
          />
          Import full file (ignore date range)
        </label>

        {!importFullFile && (
          <div className="flex flex-wrap items-center gap-3">
            <div>
              <label className="block text-xs text-muted-foreground">Start date</label>
              <input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                disabled={isActive}
                className="mt-1 h-8 rounded-lg border border-border bg-background px-2 text-sm disabled:opacity-50"
              />
            </div>
            <div>
              <label className="block text-xs text-muted-foreground">End date</label>
              <input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                disabled={isActive}
                className="mt-1 h-8 rounded-lg border border-border bg-background px-2 text-sm disabled:opacity-50"
              />
            </div>
          </div>
        )}

        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={dedup}
            onChange={(e) => setDedup(e.target.checked)}
            disabled={isActive}
            className="rounded"
          />
          Replace existing rows for the selected date range
        </label>

        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={retentionEnabled}
            onChange={(e) => setRetentionEnabled(e.target.checked)}
            disabled={isActive}
            className="rounded"
          />
          Purge rows older than{" "}
          <input
            type="number"
            min={1}
            max={3650}
            value={retentionDays}
            onChange={(e) => setRetentionDays(Number(e.target.value))}
            disabled={isActive || !retentionEnabled}
            className="w-16 rounded border border-border bg-background px-1 text-center text-sm disabled:opacity-50"
          />{" "}
          days
        </label>
      </div>

      {/* ── Dropzone ─────────────────────────────────────────────────────────── */}
      <div className="mt-5">
        <label className="block text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          CSV file <span className="text-destructive">*</span>
        </label>

        {!file ? (
          <div
            role="button"
            tabIndex={0}
            aria-label="Drop CSV file here or click to browse"
            className={`mt-2 flex cursor-pointer flex-col items-center justify-center gap-2 rounded-xl border-2 border-dashed px-6 py-10 text-center transition-colors ${
              isDragging
                ? "border-primary bg-primary/5"
                : "border-border hover:border-primary/50 hover:bg-muted/30"
            } ${!storeId || isActive ? "pointer-events-none opacity-50" : ""}`}
            onClick={() => fileInputRef.current?.click()}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") fileInputRef.current?.click();
            }}
            onDragOver={(e) => {
              e.preventDefault();
              setIsDragging(true);
            }}
            onDragLeave={() => setIsDragging(false)}
            onDrop={(e) => {
              e.preventDefault();
              setIsDragging(false);
              const f = e.dataTransfer.files[0];
              if (f) acceptFile(f);
            }}
          >
            <UploadCloud className="h-8 w-8 text-muted-foreground" aria-hidden />
            <p className="text-sm font-medium text-foreground">
              Drop your CSV here, or{" "}
              <span className="text-primary underline underline-offset-2">browse</span>
            </p>
            <p className="text-xs text-muted-foreground">.csv · .txt · .xlsx · .xls</p>
            {!storeId && (
              <p className="text-xs font-medium text-amber-600 dark:text-amber-400">
                Select a store first
              </p>
            )}
          </div>
        ) : (
          <div className="mt-2 flex items-center gap-3 rounded-xl border border-border bg-muted/40 px-4 py-3">
            <FileText className="h-5 w-5 shrink-0 text-sky-500" aria-hidden />
            <div className="min-w-0 flex-1">
              <p className="truncate text-sm font-medium text-foreground">{file.name}</p>
              <p className="text-xs text-muted-foreground">{formatBytes(file.size)}</p>
            </div>
            {!isActive && (
              <button
                type="button"
                onClick={() => reset()}
                className="rounded p-1 text-muted-foreground hover:text-destructive"
                aria-label="Remove file"
              >
                <SquareX className="h-4 w-4" aria-hidden />
              </button>
            )}
          </div>
        )}

        <input
          ref={fileInputRef}
          type="file"
          accept=".csv,.txt,.xlsx,.xls"
          className="sr-only"
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (f) acceptFile(f);
            e.target.value = "";
          }}
        />
      </div>

      {/* ── Progress + phase control panel ───────────────────────────────────── */}
      {phase !== "idle" && (
        <div className="mt-5 space-y-4 rounded-xl border border-border bg-muted/30 px-4 py-4">

          {/* Status message */}
          {progressMsg && (
            <p className="flex items-center gap-1.5 text-xs text-muted-foreground">
              {isActive && <Loader2 className="h-3 w-3 shrink-0 animate-spin" aria-hidden />}
              {progressMsg}
            </p>
          )}

          {/* ── Phase 1 bars ───────────────────────────────────────────────── */}
          <div className="space-y-2">
            {/* Upload progress */}
            <div>
              <div className="mb-1 flex justify-between text-[11px] font-medium text-muted-foreground">
                <span>{isListingCatalogReport ? "Phase 1 — Upload raw file" : "Phase 1 — Upload"}</span>
                <span className={uploadPct === 100 ? "text-emerald-500" : ""}>{uploadPct}%</span>
              </div>
              <div className="relative h-2 overflow-hidden rounded-full bg-muted">
                {isUploading && uploadPct < 100 && (
                  <div className="absolute inset-0 animate-pulse rounded-full bg-sky-400/50" />
                )}
                <div
                  className="h-full rounded-full bg-sky-500 transition-all duration-500"
                  style={{ width: `${uploadPct}%` }}
                />
              </div>
            </div>

            {/* AI classification */}
            <div>
              <div className="mb-1 flex justify-between text-[11px] font-medium text-muted-foreground">
                <span>Phase 1 — AI Classification</span>
                <span>
                  {phase === "mapped" ? "✓ mapped"
                   : phase === "needs_mapping" ? "⚠ needs review"
                   : phase === "unsupported" ? "🚫 not supported"
                   : (isUploading && uploadPct === 100) ? "running…"
                   : isUploading ? "pending…"
                   : "✓ done"}
                </span>
              </div>
              <div className="relative h-2 overflow-hidden rounded-full bg-muted">
                {isUploading && uploadPct === 100 && (
                  <div className="absolute inset-0 animate-pulse rounded-full bg-violet-400/50" />
                )}
                <div
                  className={`h-full rounded-full transition-all duration-500 ${
                    phase === "needs_mapping" ? "bg-amber-500"
                    : phase === "unsupported" ? "bg-rose-500"
                    : "bg-violet-500"
                  }`}
                  style={{
                    width: [
                      "mapped",
                      "needs_mapping",
                      "unsupported",
                      "processing",
                      "staged",
                      "syncing",
                      "synced",
                      "worklisting",
                      "worklisted",
                    ].includes(phase)
                      ? "100%"
                      : isUploading && uploadPct === 100 ? "55%" : "0%",
                  }}
                />
              </div>
            </div>
          </div>

          {/* ── AI detected label ─────────────────────────────────────────────── */}
          {detectedType &&
            (phase === "mapped" ||
              phase === "processing" ||
              phase === "staged" ||
              phase === "syncing" ||
              phase === "synced" ||
              phase === "worklisting" ||
              phase === "worklisted") && (
            <div className="flex items-center gap-2 rounded-lg border border-emerald-400/40 bg-emerald-500/10 px-3 py-2">
              <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-500" aria-hidden />
              <span className="text-xs font-semibold text-emerald-700 dark:text-emerald-300">
                ✨ AI Detected File Type: <span className="font-bold">{detectedType}</span>
              </span>
            </div>
          )}
          {phase === "needs_mapping" && (
            <div className="rounded-lg border border-amber-400/40 bg-amber-500/10 px-3 py-2 text-xs font-medium text-amber-700 dark:text-amber-300">
              ⚠ AI could not fully map columns. Use <strong>Map Columns</strong> in the History table below, then Process.
            </div>
          )}
          {phase === "unsupported" && (
            <div className="rounded-xl border border-rose-400/50 bg-rose-500/10 px-4 py-3 text-sm text-rose-700 dark:text-rose-300">
              <p className="font-bold mb-1">🚫 File type not supported</p>
              <p className="text-xs leading-relaxed">{progressMsg}</p>
              <p className="mt-2 text-xs text-muted-foreground">
                To add support, a new database table and ETL mapping must be configured by an admin.
              </p>
            </div>
          )}

          {/* ── Phase 2 bar (Process) ─────────────────────────────────────────── */}
          {(phase === "processing" ||
            phase === "staged" ||
            phase === "syncing" ||
            phase === "synced" ||
            phase === "worklisting" ||
            phase === "worklisted") && (
            <div>
              <div className="mb-1 flex justify-between text-[11px] font-medium text-muted-foreground">
                <span>
                  {isListingCatalogReport
                    ? phase === "synced"
                      ? "✓ Phases 2–3 complete"
                      : listingImportSubphase === "canonical_sync"
                        ? "Phase 3 — Sync canonical catalog"
                        : "Phase 2 — Parse and store raw rows"
                    : phaseLabel
                      ? `${phaseLabel} — staging`
                      : "Staging — copy rows to amazon_staging"}
                </span>
                <span className={phase !== "processing" ? "text-emerald-500" : "tabular-nums"}>
                  {phase === "processing"
                    ? totalRows > 0
                      ? `${processedRows.toLocaleString()} / ${totalRows.toLocaleString()} rows (${processPct}%)`
                      : processPct > 0
                        ? `${processPct}%`
                        : "running…"
                    : processedRows > 0
                      ? `✓ ${processedRows.toLocaleString()}${totalRows > 0 && totalRows !== processedRows ? ` / ${totalRows.toLocaleString()}` : ""} rows`
                      : "✓ done"}
                </span>
              </div>
              <div className="relative h-2 overflow-hidden rounded-full bg-muted">
                {phase === "processing" && (
                  <div className="absolute inset-0 animate-pulse rounded-full bg-sky-400/50" />
                )}
                <div
                  className="h-full rounded-full bg-sky-500 transition-all duration-500"
                  style={{ width: phase === "processing" ? `${Math.max(5, processPct)}%` : "100%" }}
                />
              </div>
            </div>
          )}

          {/* ── Phase 3 bar (Sync) — not used for listing catalog imports (raw + catalog run in Process). ── */}
          {!isListingCatalogReport &&
            (phase === "syncing" ||
              phase === "synced" ||
              phase === "worklisting" ||
              phase === "worklisted") && (
            <div>
              <div className="mb-1 flex justify-between text-[11px] font-medium text-muted-foreground">
                <span>Phase 3 — Sync to amazon_removals</span>
                <span
                  className={
                    phase === "synced" || phase === "worklisting" || phase === "worklisted"
                      ? "text-emerald-500"
                      : "tabular-nums"
                  }
                >
                  {phase === "syncing"
                    ? `${Math.max(0, Math.min(100, syncPct))}%`
                    : "✓ complete"}
                </span>
              </div>
              <div className="relative h-2 overflow-hidden rounded-full bg-muted">
                {phase === "syncing" && (
                  <div className="absolute inset-0 animate-pulse rounded-full bg-emerald-400/50" />
                )}
                <div
                  className="h-full rounded-full bg-emerald-500 transition-all duration-500"
                  style={{
                    width: `${phase === "syncing" ? Math.max(5, Math.min(100, syncPct)) : 100}%`,
                  }}
                />
              </div>
            </div>
          )}

          {/* ── Phase 4 bar (Generate Worklist — removal types only) ─────────── */}
          {isRemovalReport &&
            (phase === "synced" || phase === "worklisting" || phase === "worklisted") && (
              <div key={`worklist-bar-${worklistBarEpoch}`}>
                <div className="mb-1 flex justify-between text-[11px] font-medium text-muted-foreground">
                  <span>Phase 4 — Generate Worklist (expected_packages)</span>
                  <span
                    className={
                      phase === "worklisted" ? "text-emerald-500" : "tabular-nums"
                    }
                  >
                    {phase === "worklisting"
                      ? worklistPct <= 0
                        ? "starting…"
                        : `${Math.max(0, Math.min(100, worklistPct))}%`
                      : phase === "worklisted"
                        ? "✓ complete"
                        : "pending"}
                  </span>
                </div>
                <div className="relative h-2 overflow-hidden rounded-full bg-muted">
                  {phase === "worklisting" && (
                    <div className="absolute inset-0 animate-pulse rounded-full bg-amber-400/50" />
                  )}
                  <div
                    className="h-full rounded-full bg-amber-500 transition-all duration-500"
                    style={{
                      width: `${
                        phase === "worklisting"
                          ? worklistPct <= 0
                            ? 0
                            : Math.max(3, Math.min(100, worklistPct))
                          : phase === "worklisted"
                            ? 100
                            : 0
                      }%`,
                    }}
                  />
                </div>
              </div>
            )}
        </div>
      )}

      {/* ── Action buttons ────────────────────────────────────────────────────── */}
      <div className="mt-6 flex flex-wrap items-center gap-3">

        {/* Phase 1: Upload & AI Map — visible only when idle */}
        {phase === "idle" && (
          <button
            type="button"
            disabled={!canUpload}
            onClick={() => void startUpload()}
            className="inline-flex items-center gap-2 rounded-xl bg-primary px-5 py-2.5 text-sm font-semibold text-primary-foreground shadow transition hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <UploadCloud className="h-4 w-4" aria-hidden />
            Upload &amp; AI Map
          </button>
        )}

        {/* Phase 1 in progress */}
        {isUploading && (
          <button type="button" disabled className="inline-flex items-center gap-2 rounded-xl bg-primary px-5 py-2.5 text-sm font-semibold text-primary-foreground opacity-70">
            <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            Uploading &amp; Mapping…
          </button>
        )}

        {/* Phase 2: Process Data — appears after Phase 1 succeeds */}
        {phase === "mapped" && (
          <button
            type="button"
            onClick={() => void runProcess()}
            className="inline-flex items-center gap-2 rounded-xl bg-sky-600 px-5 py-2.5 text-sm font-semibold text-white shadow transition hover:bg-sky-700"
          >
            <Loader2 className="h-4 w-4" aria-hidden />
            Process Data
          </button>
        )}
        {isProcessing && (
          <button type="button" disabled className="inline-flex items-center gap-2 rounded-xl bg-sky-600 px-5 py-2.5 text-sm font-semibold text-white opacity-70">
            <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            Processing…
          </button>
        )}

        {/* Phase 3: Sync to Final Tables — appears after Phase 2 succeeds */}
        {phase === "staged" && (
          <button
            type="button"
            onClick={() => void runSync()}
            className="inline-flex items-center gap-2 rounded-xl bg-emerald-600 px-5 py-2.5 text-sm font-semibold text-white shadow transition hover:bg-emerald-700"
          >
            <CheckCircle2 className="h-4 w-4" aria-hidden />
            Sync to Final Tables
          </button>
        )}
        {isSyncing && (
          <button type="button" disabled className="inline-flex items-center gap-2 rounded-xl bg-emerald-600 px-5 py-2.5 text-sm font-semibold text-white opacity-70">
            <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            Syncing…
          </button>
        )}

        {/* Phase 4: Generate Worklist — removal imports only, after Phase 3 */}
        {phase === "synced" && isRemovalReport && (
          <button
            type="button"
            onClick={() => void runWorklist()}
            className="inline-flex items-center gap-2 rounded-xl bg-amber-600 px-5 py-2.5 text-sm font-semibold text-white shadow transition hover:bg-amber-700"
          >
            <CheckCircle2 className="h-4 w-4" aria-hidden />
            Generate Worklist
          </button>
        )}
        {phase === "worklisting" && (
          <button type="button" disabled className="inline-flex items-center gap-2 rounded-xl bg-amber-600 px-5 py-2.5 text-sm font-semibold text-white opacity-70">
            <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            Generating worklist…
          </button>
        )}

        {/* Divider when an upload is active */}
        {phase !== "idle" && <span className="mx-1 text-muted-foreground/40">|</span>}

        {/* Reset / Clear Form — always visible after upload starts */}
        {phase !== "idle" && (
          <button
            type="button"
            disabled={isActive}
            onClick={() => void reset()}
            className="inline-flex items-center gap-1.5 rounded-xl border border-border px-4 py-2.5 text-sm font-medium text-muted-foreground transition hover:bg-muted disabled:opacity-40"
          >
            <RefreshCw className="h-3.5 w-3.5" aria-hidden />
            Reset / New Upload
          </button>
        )}

        {/* Delete Import — deletes DB row + Storage */}
        {sessionUploadId && !isActive && (
          <button
            type="button"
            onClick={() => void deleteCurrentImport()}
            className="inline-flex items-center gap-1.5 rounded-xl border border-destructive/50 bg-destructive/10 px-4 py-2.5 text-sm font-medium text-destructive transition hover:bg-destructive/20"
          >
            <Trash2 className="h-3.5 w-3.5" aria-hidden />
            Delete Import
          </button>
        )}

        {/* Completion notes */}
        {phase === "synced" && !isRemovalReport && !isListingCatalogReport && (
          <p className="text-xs font-semibold text-emerald-600 dark:text-emerald-400">
            All 3 phases complete. Data is in the destination tables.
          </p>
        )}
        {phase === "synced" && isListingCatalogReport && progressMsg && (
          <p className="whitespace-pre-line text-xs font-semibold text-emerald-600 dark:text-emerald-400">
            {progressMsg}
          </p>
        )}
        {phase === "synced" && isRemovalReport && (
          <p className="text-xs font-semibold text-amber-700 dark:text-amber-400">
            Phase 3 complete (amazon_removals). Click <strong>Generate Worklist</strong> to sync{" "}
            <code className="rounded bg-muted px-1 text-foreground">expected_packages</code> for the scanner.
          </p>
        )}
        {phase === "worklisted" && isRemovalReport && (
          <p className="text-xs font-semibold text-emerald-600 dark:text-emerald-400">
            All 4 phases complete. Removal data and warehouse worklist are ready.
          </p>
        )}
      </div>
    </section>
  );
}
