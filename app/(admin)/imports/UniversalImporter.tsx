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

import React, { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { CheckCircle2, FileText, Loader2, Lock, MapPin, RefreshCw, SquareX, Trash2, UploadCloud, X, Zap } from "lucide-react";
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
import { AMAZON_LEDGER_UPLOAD_SOURCE } from "../../../lib/raw-report-upload-metadata";
import {
  inferUniversalImporterPhase,
  resolveImportUiActionState,
  type ImportUiActionInput,
} from "../../../lib/import-ui-action-state";
import {
  buildImportUiActionInputForRemovalShipment,
  buildRemovalShipmentProgressModel,
  buildRemovalShipmentTopCardResultMessage,
  REMOVAL_SHIPMENT_UI_LABELS,
  resolveRemovalShipmentPrimaryCta,
} from "../../../lib/import-removal-shipment-ui";
import {
  buildListingPipelineSteps,
  buildListingTopCardResultMessage,
  LISTING_IMPORT_UI_LABELS,
  resolveListingImportUiState,
} from "../../../lib/listing-import-ui";

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
  | "synced"          // Pipeline sync complete (or after generic when applicable)
  | "raw_synced"      // Phase 3 raw landing done — run Generic (listing / removal shipment)
  | "genericing"      // Phase 4 generic in progress
  | "worklisting"     // Generate worklist (FastAPI) in progress
  | "worklisted"      // Worklist done
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
  const isActiveRef = useRef(false);

  const stopRef = useRef(false);
  const [sessionUploadId, setSessionUploadId] = useState<string | null>(null);
  /** Latest server row + FPS — drives action buttons when not mid-flight. */
  const [serverImportInput, setServerImportInput] = useState<ImportUiActionInput | null>(null);
  /** Full `file_processing_status` row for REMOVAL_SHIPMENT progress (real counters). */
  const [sessionFpsRow, setSessionFpsRow] = useState<Record<string, unknown> | null>(null);
  const [sessionUserId, setSessionUserId] = useState<string | null>(null);
  const effectiveActorId = actorUserId ?? sessionUserId;

  const isUploading = phase === "uploading";
  const isProcessing = phase === "processing";
  const isSyncing = phase === "syncing";
  const isWorklisting = phase === "worklisting";
  const isGenericing = phase === "genericing";
  const isActive = isUploading || isProcessing || isSyncing || isWorklisting || isGenericing;
  isActiveRef.current = isActive;

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

  useEffect(() => {
    if (!sessionUploadId) {
      setServerImportInput(null);
      setSessionFpsRow(null);
      return;
    }
    let cancelled = false;
    async function tick() {
      if (isActiveRef.current) return;
      const { data: rpu, error: rErr } = await supabase
        .from("raw_report_uploads")
        .select("report_type, status, metadata")
        .eq("id", sessionUploadId)
        .maybeSingle();
      if (cancelled || rErr || !rpu) return;
      const { data: fps } = await supabase
        .from("file_processing_status")
        .select(
          "phase2_status, phase3_status, phase4_status, current_phase, current_phase_label, current_target_table, status, upload_pct, process_pct, sync_pct, phase1_upload_pct, phase2_stage_pct, phase3_raw_sync_pct, phase4_generic_pct, staged_rows_written, raw_rows_written, raw_rows_skipped_existing, generic_rows_written, total_rows, processed_rows, file_rows_total, data_rows_total",
        )
        .eq("upload_id", sessionUploadId)
        .maybeSingle();
      if (cancelled) return;
      setSessionFpsRow(fps && typeof fps === "object" ? (fps as Record<string, unknown>) : null);
      const meta =
        rpu.metadata && typeof rpu.metadata === "object" && !Array.isArray(rpu.metadata)
          ? (rpu.metadata as Record<string, unknown>)
          : null;
      const fpsSnap = fps
        ? {
            phase2_status: fps.phase2_status != null ? String(fps.phase2_status) : null,
            phase3_status: fps.phase3_status != null ? String(fps.phase3_status) : null,
            phase4_status: fps.phase4_status != null ? String(fps.phase4_status) : null,
            current_phase: fps.current_phase != null ? String(fps.current_phase) : null,
            current_phase_label: fps.current_phase_label != null ? String(fps.current_phase_label) : null,
            current_target_table: fps.current_target_table != null ? String(fps.current_target_table) : null,
            row_status: fps.status != null ? String(fps.status) : null,
          }
        : null;
      const input: ImportUiActionInput = {
        reportType: String(rpu.report_type ?? ""),
        status: String(rpu.status ?? ""),
        metadata: meta,
        fps: fpsSnap,
        isLedgerSession: meta?.source === AMAZON_LEDGER_UPLOAD_SOURCE,
      };
      setServerImportInput(input);
      const built = buildImportUiActionInputForRemovalShipment(input, detectedType);
      const inferred = inferUniversalImporterPhase(built);
      if (inferred != null) setPhase(inferred);
      const rt = String(rpu.report_type ?? "").trim();
      if (rt && rt !== "UNKNOWN") setDetectedType(rt);
    }
    const id = setInterval(() => void tick(), 2000);
    void tick();
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [sessionUploadId, detectedType]);

  const importActionInput = useMemo(
    () =>
      serverImportInput ? buildImportUiActionInputForRemovalShipment(serverImportInput, detectedType) : null,
    [serverImportInput, detectedType],
  );
  const topUi = importActionInput ? resolveImportUiActionState(importActionInput) : null;
  /** Prefer DB `report_type` from the poll snapshot so actions match Import History. */
  const effectiveRt = (serverImportInput?.reportType || detectedType || "").trim();
  const effectiveListing = effectiveRt !== "" && isListingReportType(effectiveRt);
  const listingImportUi = isListingCatalogReport || effectiveListing;
  /** Coerce UNKNOWN → REMOVAL_SHIPMENT when session detection matches (same source as top action resolver). */
  const removalShipmentUi =
    importActionInput != null
      ? importActionInput.reportType === "REMOVAL_SHIPMENT"
      : detectedType === "REMOVAL_SHIPMENT";
  const importLedgerSession = serverImportInput?.isLedgerSession === true;
  const rsCta =
    removalShipmentUi && topUi && serverImportInput
      ? resolveRemovalShipmentPrimaryCta(topUi, serverImportInput.status, importLedgerSession)
      : null;
  const removalProgress = useMemo(() => {
    if (!removalShipmentUi || !serverImportInput) return null;
    return buildRemovalShipmentProgressModel(
      serverImportInput.metadata as Record<string, unknown> | null,
      sessionFpsRow,
    );
  }, [removalShipmentUi, serverImportInput, sessionFpsRow]);
  const removalResultLine = useMemo(() => {
    if (!removalShipmentUi || !topUi || !removalProgress) return null;
    return buildRemovalShipmentTopCardResultMessage(topUi, removalProgress);
  }, [removalShipmentUi, topUi, removalProgress]);

  const listingUi = useMemo(() => {
    if (!listingImportUi || removalShipmentUi || !importActionInput) return null;
    return resolveListingImportUiState({
      ...importActionInput,
      client: {
        localPhase: phase,
        isProcessing,
        isSyncing,
        isGenericing,
        isUploading,
      },
    });
  }, [
    listingImportUi,
    removalShipmentUi,
    importActionInput,
    phase,
    isProcessing,
    isSyncing,
    isGenericing,
    isUploading,
  ]);

  const listingProgress = listingUi?.progress ?? null;

  const listingPipelineSteps = useMemo(() => {
    if (!listingImportUi || removalShipmentUi) return null;
    if (!importActionInput && !(sessionUploadId && isUploading)) return null;
    const status =
      importActionInput?.status ??
      (isUploading ? "uploading" : sessionUploadId ? "pending" : "pending");
    const meta = (importActionInput?.metadata ?? null) as Record<string, unknown> | null;
    return buildListingPipelineSteps({
      status,
      metadata: meta,
      fps: sessionFpsRow,
      localUploadPct: isUploading ? uploadPct : undefined,
      localFileSizeBytes: file?.size,
    });
  }, [
    listingImportUi,
    removalShipmentUi,
    importActionInput,
    sessionFpsRow,
    isUploading,
    uploadPct,
    file?.size,
    sessionUploadId,
  ]);

  const listingTopLine = useMemo(() => {
    if (!listingImportUi || removalShipmentUi || !importActionInput) return null;
    return buildListingTopCardResultMessage({
      ...importActionInput,
      client: {
        localPhase: phase,
        isProcessing,
        isSyncing,
        isGenericing,
        isUploading,
      },
    });
  }, [
    listingImportUi,
    removalShipmentUi,
    importActionInput,
    phase,
    isProcessing,
    isSyncing,
    isGenericing,
    isUploading,
  ]);

  /** Listing: show logical Sync/Generic controls (disabled) alongside Process — server runs them inside one Process. */
  const showListingPhaseActionRow = listingImportUi && !removalShipmentUi && phase !== "idle";
  const showMapListingTop = showListingPhaseActionRow && phase === "needs_mapping";

  const showSyncTop =
    removalShipmentUi
      ? rsCta != null && !isSyncing && (rsCta === "sync" || rsCta === "retry_sync")
      : listingImportUi && !removalShipmentUi
        ? showListingPhaseActionRow
        : !isSyncing &&
          phase !== "raw_synced" &&
          phase !== "synced" &&
          phase !== "genericing" &&
          (phase === "staged" || topUi?.showSync === true);
  const showGenericTop =
    removalShipmentUi
      ? rsCta != null && !isGenericing && (rsCta === "generic" || rsCta === "generic_retry")
      : listingImportUi && !removalShipmentUi
        ? showListingPhaseActionRow
        : !isGenericing &&
          (topUi
            ? topUi.showGeneric || topUi.showRetryGeneric
            : phase === "raw_synced" && effectiveListing);
  const showProcessTop =
    removalShipmentUi
      ? rsCta != null &&
        !isProcessing &&
        !isUploading &&
        phase !== "needs_mapping" &&
        (rsCta === "process" || rsCta === "retry_process")
      : listingImportUi && !removalShipmentUi
        ? Boolean(listingUi?.showProcessCta) && !isUploading
        : phase === "mapped";
  const showWorklistPrimary =
    phase === "worklisting" ||
    (effectiveRt === "REMOVAL_ORDER" &&
      phase !== "worklisted" &&
      (topUi ? topUi.showWorklist : phase === "synced"));

  function bumpHistory() {
    onUploadComplete?.();
    router.refresh();
  }

  function scrollToSessionInHistory() {
    const id = sessionUploadId;
    window.requestAnimationFrame(() => {
      if (id) {
        const el = document.querySelector(`[data-upload-id="${id}"]`);
        if (el) {
          el.scrollIntoView({ behavior: "smooth", block: "center" });
          return;
        }
      }
      document.getElementById("data-import-history")?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
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
      listingFromUi
        ? "Processing listing import (raw archive + catalog)…"
        : "Processing CSV into staging table…",
    );
    setListingImportSubphase("raw_archive");
    setPhaseLabel(listingFromUi ? "Process listing import" : "Staging");

    // Poll DB frequently; prefer `file_processing_status` (true progress source).
    const uploadIdSnap = sessionUploadId;
    if (pollRef2.current) clearInterval(pollRef2.current);
    pollRef2.current = setInterval(() => {
      void Promise.all([
        supabase.from("raw_report_uploads").select("report_type, status, metadata").eq("id", uploadIdSnap).maybeSingle(),
        supabase.from("file_processing_status").select("*").eq("upload_id", uploadIdSnap).maybeSingle(),
      ]).then(([rpu, fps]) => {
        const m = rpu.data?.metadata as Record<string, unknown> | null;
        const fpsRow = fps.data as Record<string, unknown> | null | undefined;
        if (listingFromUi && rpu.data) {
          const row = rpu.data as { report_type?: string; status?: string; metadata?: unknown };
          const metaPoll =
            row.metadata && typeof row.metadata === "object" && !Array.isArray(row.metadata)
              ? (row.metadata as Record<string, unknown>)
              : null;
          setSessionFpsRow(fpsRow && typeof fpsRow === "object" ? fpsRow : null);
          const fpsSnap = fpsRow
            ? {
                phase2_status: fpsRow.phase2_status != null ? String(fpsRow.phase2_status) : null,
                phase3_status: fpsRow.phase3_status != null ? String(fpsRow.phase3_status) : null,
                phase4_status: fpsRow.phase4_status != null ? String(fpsRow.phase4_status) : null,
                current_phase: fpsRow.current_phase != null ? String(fpsRow.current_phase) : null,
                current_phase_label:
                  fpsRow.current_phase_label != null ? String(fpsRow.current_phase_label) : null,
                current_target_table:
                  fpsRow.current_target_table != null ? String(fpsRow.current_target_table) : null,
                row_status: fpsRow.status != null ? String(fpsRow.status) : null,
              }
            : null;
          setServerImportInput({
            reportType: String(row.report_type ?? ""),
            status: String(row.status ?? ""),
            metadata: metaPoll,
            fps: fpsSnap,
            isLedgerSession: metaPoll?.source === AMAZON_LEDGER_UPLOAD_SOURCE,
          });
        }
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
        setListingImportSubphase("done");
        setProgressMsg(
          total > 0 && total !== staged
            ? `Listing import complete — ${staged.toLocaleString()} data row(s) processed (${total.toLocaleString()} lines in file). Raw archive and catalog snapshot are updated.`
            : `Listing import complete — ${staged.toLocaleString()} data row(s). Raw archive and catalog snapshot are updated.`,
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
        rowsSkippedCrossUploadDuplicate?: number;
        kind?: string;
      };
      if (!res.ok || !json.ok) throw new Error(json.error ?? "Sync failed.");
      if (json.kind === "REMOVAL_ORDER" || json.kind === "REMOVAL_SHIPMENT") {
        setDetectedType(json.kind);
      }
      setSyncPct(100);
      const rows = json.rowsSynced?.toLocaleString() ?? "?";
      const removal = json.kind === "REMOVAL_ORDER" || json.kind === "REMOVAL_SHIPMENT";
      const { data: upRow } = await supabase
        .from("raw_report_uploads")
        .select("status")
        .eq("id", sessionUploadId)
        .maybeSingle();
      const st = String((upRow as { status?: string } | null)?.status ?? "");
      const skipDup = json.rowsSkippedCrossUploadDuplicate ?? 0;
      let syncMsg = `Sync complete — ${rows} rows written.`;
      if (json.kind && isListingReportType(json.kind)) {
        syncMsg =
          st === "raw_synced"
            ? `Phase 3 complete — raw rows synced to amazon_listing_report_rows_raw. Next: Generic (catalog) for catalog_products.`
            : `Listing sync finished — ${rows} row(s).`;
      } else if (removal) {
        if (json.kind === "REMOVAL_SHIPMENT") {
          const skipNote =
            skipDup > 0 ? ` ${skipDup.toLocaleString()} line(s) skipped (already archived from another upload).` : "";
          syncMsg =
            st === "raw_synced"
              ? `Phase 3 complete — ${rows} shipment line(s) written to amazon_removal_shipments.${skipNote} Run Generic for shipment tree / expected_packages enrich.`
              : `Phase 3 complete — ${rows} shipment line(s) archived.${skipNote}`;
        } else {
          syncMsg = `Phase 3 complete — ${rows} row(s) in amazon_removals. Run Generate Worklist for expected_packages when ready.`;
        }
      }
      setProgressMsg(syncMsg);
      setPhase(st === "raw_synced" ? "raw_synced" : "synced");
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

  /** Phase 4 — listing catalog or removal shipment tree (after raw_synced). */
  async function runGeneric() {
    if (!sessionUploadId) return;
    setErr(null);
    setPhase("genericing");
    setProgressMsg("Running generic phase…");
    try {
      const res = await fetch("/api/settings/imports/generic", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ upload_id: sessionUploadId }),
      });
      const json = (await res.json()) as { ok?: boolean; error?: string; skipped?: boolean; kind?: string };
      if (!res.ok || !json.ok) throw new Error(json.error ?? "Generic phase failed.");
      setProgressMsg(
        json.skipped === true
          ? json.kind === "REMOVAL_SHIPMENT"
            ? "Shipment tree / expected_packages enrich already complete."
            : "Generic phase already complete for this upload."
          : "Generic phase complete.",
      );
      setPhase("synced");
      bumpHistory();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Generic phase failed.");
      setPhase("raw_synced");
      bumpHistory();
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

          {/* Status message — REMOVAL_SHIPMENT uses FPS-backed result line when idle */}
          {(() => {
            const line = isActive
              ? progressMsg
              : removalShipmentUi
                ? removalResultLine ?? progressMsg
                : listingImportUi && !removalShipmentUi
                  ? listingTopLine ?? progressMsg
                  : progressMsg;
            if (!line) return null;
            const isRsResult =
              !isActive && removalShipmentUi && removalResultLine != null && line === removalResultLine;
            const isListingResult =
              !isActive &&
              listingImportUi &&
              !removalShipmentUi &&
              listingTopLine != null &&
              line === listingTopLine;
            const listingLines =
              isListingResult && line.includes("\n") ? line.split("\n").filter(Boolean) : null;
            return listingLines && listingLines.length > 1 ? (
              <div className="space-y-1 text-xs font-semibold text-amber-800 dark:text-amber-200">
                {listingLines.map((l, idx) => (
                  <p key={idx} className="flex items-center gap-1.5 leading-snug">
                    {idx === 0 && isActive && (
                      <Loader2 className="h-3 w-3 shrink-0 animate-spin" aria-hidden />
                    )}
                    {l}
                  </p>
                ))}
              </div>
            ) : (
              <p
                className={`flex items-center gap-1.5 text-xs ${
                  isRsResult || isListingResult
                    ? "font-semibold text-amber-800 dark:text-amber-200"
                    : "text-muted-foreground"
                }`}
              >
                {isActive && <Loader2 className="h-3 w-3 shrink-0 animate-spin" aria-hidden />}
                {line}
              </p>
            );
          })()}

          {listingImportUi && !removalShipmentUi && listingPipelineSteps && (
            <div className="rounded-xl border border-border/80 bg-background/80 px-4 py-3 shadow-sm">
              <div className="mb-3 flex items-center justify-between gap-2 border-b border-border/60 pb-2">
                <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  Listing pipeline
                </h3>
                <span className="text-[10px] text-muted-foreground">
                  Live progress from storage + row counts
                </span>
              </div>
              <ol className="space-y-3">
                {listingPipelineSteps.map((step, idx) => {
                  const barTint =
                    step.tone === "active"
                      ? "bg-sky-500"
                      : step.tone === "done"
                        ? "bg-emerald-500"
                        : step.tone === "warning"
                          ? "bg-amber-500"
                          : "bg-muted-foreground/40";
                  const ring =
                    step.tone === "active"
                      ? "ring-2 ring-sky-500/35 bg-sky-500/[0.06]"
                      : step.tone === "warning"
                        ? "ring-2 ring-amber-500/35 bg-amber-500/[0.06]"
                        : step.tone === "done"
                          ? "border border-emerald-500/25 bg-emerald-500/[0.04]"
                          : "border border-border/70 bg-muted/20";
                  return (
                    <li key={step.key} className={`rounded-lg px-3 py-2.5 ${ring}`}>
                      <div className="mb-1.5 flex items-start gap-3">
                        <span
                          className={[
                            "flex h-6 w-6 shrink-0 items-center justify-center rounded-full text-[11px] font-bold",
                            step.tone === "done"
                              ? "bg-emerald-600 text-white"
                              : step.tone === "active"
                                ? "bg-sky-600 text-white"
                                : step.tone === "warning"
                                  ? "bg-amber-600 text-white"
                                  : "bg-muted text-muted-foreground",
                          ].join(" ")}
                        >
                          {idx + 1}
                        </span>
                        <div className="min-w-0 flex-1">
                          <div className="flex flex-wrap items-baseline justify-between gap-x-2 gap-y-0.5">
                            <div>
                              <p className="text-sm font-semibold text-foreground">{step.title}</p>
                              <p className="text-[11px] text-muted-foreground">{step.subtitle}</p>
                            </div>
                            <p className="text-right text-[11px] font-medium tabular-nums text-foreground">
                              {step.rightLabel}
                            </p>
                          </div>
                          {step.subLabel ? (
                            <p className="mt-0.5 text-[10px] font-medium text-violet-700 dark:text-violet-300">
                              {step.subLabel}
                            </p>
                          ) : null}
                        </div>
                      </div>
                      <div className="relative h-2 overflow-hidden rounded-full bg-muted pl-9">
                        <div
                          className={`h-full rounded-full transition-[width] duration-500 ease-out ${barTint}`}
                          style={{ width: `${Math.max(step.tone === "upcoming" ? 0 : 2, Math.min(100, step.pct))}%` }}
                        />
                      </div>
                    </li>
                  );
                })}
              </ol>
            </div>
          )}

          {/* ── Phase 1 bars ───────────────────────────────────────────────── */}
          {!(listingImportUi && !removalShipmentUi) && (
          <div className="space-y-2">
            {/* Upload progress */}
            <div>
              <div className="mb-1 flex justify-between text-[11px] font-medium text-muted-foreground">
                <span
                  title={removalShipmentUi ? REMOVAL_SHIPMENT_UI_LABELS.phase1Subtitle : undefined}
                >
                  {removalShipmentUi
                    ? REMOVAL_SHIPMENT_UI_LABELS.phase1Title
                    : listingImportUi
                      ? LISTING_IMPORT_UI_LABELS.phase1Title
                      : "Phase 1 — Upload"}
                </span>
                <span
                  className={
                    removalShipmentUi && removalProgress && removalProgress.totalBytes > 0
                      ? removalProgress.uploadPct >= 100
                        ? "text-emerald-500"
                        : "tabular-nums"
                      : listingImportUi && !removalShipmentUi && listingProgress && listingProgress.totalBytes > 0
                        ? listingProgress.uploadPct >= 100
                          ? "text-emerald-500"
                          : "tabular-nums"
                        : uploadPct === 100
                          ? "text-emerald-500"
                          : ""
                  }
                >
                  {removalShipmentUi && removalProgress && removalProgress.totalBytes > 0
                    ? `${formatBytes(removalProgress.uploadedBytes)} / ${formatBytes(removalProgress.totalBytes)} (${removalProgress.uploadPct}%)`
                    : listingImportUi && !removalShipmentUi && listingProgress && listingProgress.totalBytes > 0
                      ? `${formatBytes(listingProgress.uploadedBytes)} / ${formatBytes(listingProgress.totalBytes)} (${listingProgress.uploadPct}%)`
                      : `${uploadPct}%`}
                </span>
              </div>
              <div className="relative h-2 overflow-hidden rounded-full bg-muted">
                {isUploading && uploadPct < 100 && (
                  <div className="absolute inset-0 animate-pulse rounded-full bg-sky-400/50" />
                )}
                <div
                  className="h-full rounded-full bg-sky-500 transition-all duration-500"
                  style={{
                    width: `${
                      removalShipmentUi && removalProgress && removalProgress.totalBytes > 0
                        ? removalProgress.uploadPct
                        : listingImportUi && !removalShipmentUi && listingUi
                          ? listingUi.phase1Pct
                          : uploadPct
                    }%`,
                  }}
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
                      "raw_synced",
                      "genericing",
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
          )}

          {/* ── AI detected label ─────────────────────────────────────────────── */}
          {detectedType &&
            (phase === "mapped" ||
              phase === "processing" ||
              phase === "staged" ||
              phase === "syncing" ||
              phase === "synced" ||
              phase === "raw_synced" ||
              phase === "genericing" ||
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

          {!(listingImportUi && !removalShipmentUi) && (
          <>
          {/* ── Phase 2 bar (Process) ─────────────────────────────────────────── */}
          {(phase === "processing" ||
            phase === "staged" ||
            phase === "syncing" ||
            phase === "raw_synced" ||
            phase === "genericing" ||
            phase === "synced" ||
            phase === "worklisting" ||
            phase === "worklisted") && (
            <div>
              <div className="mb-1 flex justify-between text-[11px] font-medium text-muted-foreground">
                <span
                  title={
                    removalShipmentUi
                      ? REMOVAL_SHIPMENT_UI_LABELS.phase2Subtitle
                      : listingImportUi
                        ? LISTING_IMPORT_UI_LABELS.phase2Subtitle
                        : undefined
                  }
                >
                  {removalShipmentUi
                    ? REMOVAL_SHIPMENT_UI_LABELS.phase2Title
                    : listingImportUi
                      ? phase === "processing"
                        ? LISTING_IMPORT_UI_LABELS.phase2Title
                        : `✓ ${LISTING_IMPORT_UI_LABELS.phase2Title}`
                      : phaseLabel
                        ? `${phaseLabel} — staging`
                        : "Phase 2 — Stage to amazon_staging"}
                </span>
                <span className={phase !== "processing" ? "text-emerald-500" : "tabular-nums"}>
                  {removalShipmentUi && removalProgress
                    ? phase === "processing"
                      ? removalProgress.dataRowsTotal > 0 || removalProgress.stagedRowsWritten > 0
                        ? `${removalProgress.stagedRowsWritten.toLocaleString()} / ${removalProgress.dataRowsTotal.toLocaleString()} (${removalProgress.phase2Pct}%)`
                        : processPct > 0
                          ? `${processPct}%`
                          : "running…"
                      : `✓ ${removalProgress.stagedRowsWritten.toLocaleString()} / ${removalProgress.dataRowsTotal.toLocaleString()} row(s)`
                    : listingImportUi && !removalShipmentUi && listingProgress
                      ? phase === "processing"
                        ? listingProgress.dataRowsTotal > 0 || listingProgress.stagedRowsWritten > 0
                          ? `${listingProgress.stagedRowsWritten.toLocaleString()} / ${listingProgress.dataRowsTotal.toLocaleString()} (${listingUi?.phase2Pct ?? listingProgress.phase2Pct}%)`
                          : totalRows > 0
                            ? `${processedRows.toLocaleString()} / ${totalRows.toLocaleString()} rows (${processPct}%)`
                            : processPct > 0
                              ? `${processPct}%`
                              : "running…"
                        : `✓ ${listingProgress.stagedRowsWritten.toLocaleString()} / ${listingProgress.dataRowsTotal.toLocaleString()} row(s)`
                    : phase === "processing"
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
                  style={{
                    width: `${
                      removalShipmentUi && removalProgress
                        ? phase === "processing"
                          ? Math.max(5, removalProgress.phase2Pct)
                          : 100
                        : listingImportUi && !removalShipmentUi && listingProgress
                          ? phase === "processing"
                            ? Math.max(5, listingUi?.phase2Pct ?? listingProgress.phase2Pct)
                            : 100
                          : phase === "processing"
                            ? Math.max(5, processPct)
                            : 100
                    }%`,
                  }}
                />
              </div>
            </div>
          )}

          {/* ── Phase 3 bar (raw sync) ───────────────────────────────────────── */}
          {(phase === "syncing" ||
            phase === "raw_synced" ||
            phase === "genericing" ||
            phase === "synced" ||
            phase === "worklisting" ||
            phase === "worklisted") &&
            !(listingImportUi && !removalShipmentUi) && (
            <div>
              <div className="mb-1 flex justify-between text-[11px] font-medium text-muted-foreground">
                <span
                  title={
                    removalShipmentUi
                      ? REMOVAL_SHIPMENT_UI_LABELS.phase3Subtitle
                      : listingImportUi
                        ? LISTING_IMPORT_UI_LABELS.phase3Subtitle
                        : undefined
                  }
                >
                  {listingImportUi
                    ? LISTING_IMPORT_UI_LABELS.phase3Title
                    : removalShipmentUi
                      ? REMOVAL_SHIPMENT_UI_LABELS.phase3Title
                      : isRemovalReport && detectedType === "REMOVAL_SHIPMENT"
                        ? "Phase 3 — Raw sync → amazon_removal_shipments"
                        : "Phase 3 — Raw sync to destination table"}
                </span>
                <span
                  className={
                    phase === "raw_synced" ||
                    phase === "genericing" ||
                    phase === "synced" ||
                    phase === "worklisting" ||
                    phase === "worklisted"
                      ? "text-emerald-500"
                      : "tabular-nums"
                  }
                >
                  {phase === "syncing"
                    ? `${Math.max(0, Math.min(100, syncPct))}%`
                    : removalShipmentUi && removalProgress
                      ? `✓ archived ${removalProgress.rawRowsWritten.toLocaleString()} shipment line(s), skipped ${removalProgress.rawRowsSkippedExisting.toLocaleString()} already-archived line(s)`
                      : listingImportUi && !removalShipmentUi && listingProgress
                        ? `✓ raw ${listingProgress.listingRawArchived.toLocaleString()} archived, skipped existing ${listingProgress.listingRawSkipped.toLocaleString()} (${listingUi?.phase3Pct ?? listingProgress.phase3Pct}%)`
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
                    width: `${
                      removalShipmentUi && removalProgress
                        ? phase === "syncing"
                          ? Math.max(5, Math.min(100, syncPct))
                          : Math.min(100, removalProgress.phase3Pct)
                        : listingImportUi && !removalShipmentUi && listingProgress
                          ? phase === "syncing"
                            ? Math.max(5, Math.min(100, syncPct))
                            : Math.min(100, listingUi?.phase3Pct ?? listingProgress.phase3Pct)
                          : phase === "syncing"
                            ? Math.max(5, Math.min(100, syncPct))
                            : 100
                    }%`,
                  }}
                />
              </div>
            </div>
          )}

          {/* ── Phase 4 — removal shipment tree OR listing catalog (same phases; labels differ). ── */}
          {(phase === "raw_synced" ||
            phase === "genericing" ||
            phase === "synced" ||
            phase === "worklisting" ||
            phase === "worklisted") &&
            removalShipmentUi && (
            <div>
              <div className="mb-1 flex justify-between text-[11px] font-medium text-muted-foreground">
                <span
                  title={
                    removalShipmentUi
                      ? REMOVAL_SHIPMENT_UI_LABELS.phase4Subtitle
                      : LISTING_IMPORT_UI_LABELS.phase4Subtitle
                  }
                >
                  {removalShipmentUi
                    ? REMOVAL_SHIPMENT_UI_LABELS.phase4Title
                    : LISTING_IMPORT_UI_LABELS.phase4Title}
                </span>
                <span
                  className={
                    phase === "genericing"
                      ? "tabular-nums"
                      : topUi && !topUi.phase4Complete
                        ? "text-muted-foreground"
                        : "text-emerald-500"
                  }
                >
                  {removalShipmentUi && removalProgress
                    ? phase === "genericing"
                      ? `${removalProgress.genericRowsWritten.toLocaleString()} / ${removalProgress.genericEligible.toLocaleString()} (${removalProgress.phase4Pct}%)`
                      : topUi?.phase4Complete
                        ? `✓ ${removalProgress.genericRowsWritten.toLocaleString()} / ${removalProgress.genericEligible.toLocaleString()} enriched`
                        : `pending — ${removalProgress.genericRowsWritten.toLocaleString()} / ${removalProgress.genericEligible.toLocaleString()}`
                    : listingImportUi && !removalShipmentUi && listingProgress
                      ? phase === "genericing"
                        ? `${listingProgress.phase4Numerator.toLocaleString()} / ${listingProgress.catalogEligibleRows.toLocaleString()} (${listingUi?.phase4Pct ?? listingProgress.phase4Pct}%)`
                        : topUi?.phase4Complete
                          ? `✓ new ${listingProgress.catalogRowsNew.toLocaleString()}, updated ${listingProgress.catalogRowsUpdated.toLocaleString()}, unchanged ${listingProgress.catalogRowsUnchanged.toLocaleString()}`
                          : `pending — Generic (catalog) — ${listingProgress.phase4Numerator.toLocaleString()} / ${listingProgress.catalogEligibleRows > 0 ? listingProgress.catalogEligibleRows.toLocaleString() : "—"} eligible`
                    : phase === "genericing"
                      ? "…"
                      : topUi?.phase4Complete
                        ? "✓ complete"
                        : "pending — run Generic"}
                </span>
              </div>
              <div className="relative h-2 overflow-hidden rounded-full bg-muted">
                {phase === "genericing" && (
                  <div className="absolute inset-0 animate-pulse rounded-full bg-violet-400/50" />
                )}
                <div
                  className="h-full rounded-full bg-violet-500 transition-all duration-500"
                  style={{
                    width: `${
                      removalShipmentUi && removalProgress
                        ? phase === "genericing"
                          ? Math.max(5, removalProgress.phase4Pct)
                          : topUi?.phase4Complete
                            ? 100
                            : Math.max(10, removalProgress.phase4Pct)
                        : listingImportUi && !removalShipmentUi && listingProgress
                          ? phase === "genericing"
                            ? Math.max(5, listingUi?.phase4Pct ?? listingProgress.phase4Pct)
                            : topUi?.phase4Complete
                              ? 100
                              : Math.max(10, listingUi?.phase4Pct ?? listingProgress.phase4Pct)
                          : phase === "genericing"
                            ? 45
                            : topUi?.phase4Complete
                              ? 100
                              : 12
                    }%`,
                  }}
                />
              </div>
            </div>
          )}

          {/* ── Phase 5 bar (Generate Worklist — removal orders only) ───────── */}
          {effectiveRt === "REMOVAL_ORDER" &&
            (phase === "synced" || phase === "worklisting" || phase === "worklisted") && (
              <div key={`worklist-bar-${worklistBarEpoch}`}>
                <div className="mb-1 flex justify-between text-[11px] font-medium text-muted-foreground">
                  <span>Phase 5 — Generate Worklist (expected_packages)</span>
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
          </>
          )}
        </div>
      )}

      {/* ── Action buttons ────────────────────────────────────────────────────── */}
      <div className="mt-6 space-y-2">
        <div className="flex flex-wrap items-center gap-3">

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
        {showProcessTop && (
          <button
            type="button"
            disabled={listingImportUi && !removalShipmentUi && listingUi?.busyAction === "process"}
            onClick={() => void runProcess()}
            className="inline-flex items-center gap-2 rounded-xl bg-sky-600 px-5 py-2.5 text-sm font-semibold text-white shadow transition hover:bg-sky-700 disabled:opacity-70"
          >
            {listingImportUi && !removalShipmentUi && listingUi?.busyAction === "process" ? (
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            ) : (
              <Zap className="h-4 w-4" aria-hidden />
            )}
            {removalShipmentUi
              ? rsCta === "retry_process"
                ? "Retry Process"
                : "Process"
              : listingImportUi && !removalShipmentUi && listingUi
                ? listingUi.topCardButtonLabel
                : "Process Data"}
          </button>
        )}
        {isProcessing && !(listingImportUi && !removalShipmentUi) && (
          <button type="button" disabled className="inline-flex items-center gap-2 rounded-xl bg-sky-600 px-5 py-2.5 text-sm font-semibold text-white opacity-70">
            <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            Processing…
          </button>
        )}

        {showMapListingTop && (
          <button
            type="button"
            onClick={() => scrollToSessionInHistory()}
            title="Scrolls to Import History. Use Map Columns on this file’s row to fix column mapping."
            className="inline-flex items-center gap-2 rounded-xl border border-amber-400/70 bg-amber-500/10 px-5 py-2.5 text-sm font-semibold text-amber-900 shadow-sm transition hover:bg-amber-500/15 dark:border-amber-600/50 dark:text-amber-100"
          >
            <MapPin className="h-4 w-4 shrink-0" aria-hidden />
            Map Columns
          </button>
        )}

        {/* Phase 3: Sync to Final Tables — appears after Phase 2 succeeds */}
        {showSyncTop && (
          <button
            type="button"
            disabled={
              (listingImportUi && !removalShipmentUi) ||
              (!(listingImportUi && !removalShipmentUi) && isSyncing)
            }
            onClick={() => {
              if (listingImportUi && !removalShipmentUi) return;
              void runSync();
            }}
            title={
              listingImportUi && !removalShipmentUi
                ? "Listing imports run raw archive (amazon_listing_report_rows_raw) inside Process — no separate Sync."
                : undefined
            }
            className={[
              "inline-flex items-center gap-2 rounded-xl px-5 py-2.5 text-sm font-semibold shadow transition",
              listingImportUi && !removalShipmentUi
                ? "cursor-not-allowed border border-emerald-500/35 bg-emerald-500/10 text-emerald-900 opacity-80 dark:text-emerald-100"
                : "bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-70",
            ].join(" ")}
          >
            {listingImportUi && !removalShipmentUi ? (
              <Lock className="h-4 w-4 shrink-0 opacity-80" aria-hidden />
            ) : isSyncing ? (
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            ) : (
              <CheckCircle2 className="h-4 w-4" aria-hidden />
            )}
            {removalShipmentUi
              ? rsCta === "retry_sync"
                ? "Retry Sync"
                : "Sync"
              : listingImportUi && !removalShipmentUi
                ? "Sync — raw archive (in Process)"
                : "Sync to Final Tables"}
          </button>
        )}
        {isSyncing && !(listingImportUi && !removalShipmentUi) && (
          <button type="button" disabled className="inline-flex items-center gap-2 rounded-xl bg-emerald-600 px-5 py-2.5 text-sm font-semibold text-white opacity-70">
            <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            Syncing…
          </button>
        )}

        {showGenericTop && (
          <button
            type="button"
            disabled={
              (listingImportUi && !removalShipmentUi) ||
              (!(listingImportUi && !removalShipmentUi) && isGenericing)
            }
            onClick={() => {
              if (listingImportUi && !removalShipmentUi) return;
              void runGeneric();
            }}
            title={
              listingImportUi && !removalShipmentUi
                ? "Listing catalog merge (e.g. catalog_products) runs inside Process — no separate Generic."
                : undefined
            }
            className={[
              "inline-flex items-center gap-2 rounded-xl px-5 py-2.5 text-sm font-semibold shadow transition",
              listingImportUi && !removalShipmentUi
                ? "cursor-not-allowed border border-violet-500/35 bg-violet-500/10 text-violet-950 opacity-85 dark:text-violet-100"
                : "bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-70",
            ].join(" ")}
          >
            {listingImportUi && !removalShipmentUi ? (
              <Lock className="h-4 w-4 shrink-0 opacity-80" aria-hidden />
            ) : isGenericing ? (
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            ) : (
              <CheckCircle2 className="h-4 w-4" aria-hidden />
            )}
            {removalShipmentUi
              ? rsCta === "generic_retry"
                ? "Retry Generic (shipments)"
                : "Generic (shipments)"
              : listingImportUi && !removalShipmentUi
                ? "Generic — catalog (in Process)"
                : topUi?.showRetryGeneric
                  ? "Retry Generic Phase"
                  : "Run Generic Phase"}
          </button>
        )}
        {isGenericing && !(listingImportUi && !removalShipmentUi) && (
          <button type="button" disabled className="inline-flex items-center gap-2 rounded-xl bg-violet-600 px-5 py-2.5 text-sm font-semibold text-white opacity-70">
            <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            Generic phase…
          </button>
        )}

        {/* Generate Worklist — removal orders only (registry); shipment tree generic already enriches expected_packages */}
        {showWorklistPrimary && phase !== "worklisting" && (
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
        </div>

        {listingImportUi && !removalShipmentUi && showListingPhaseActionRow && (
          <p className="max-w-3xl text-[11px] leading-relaxed text-muted-foreground">
            <span className="font-medium text-foreground">Listing files:</span> Sync and Generic stay locked because the
            server runs <strong className="font-semibold text-foreground">raw archive</strong> and{" "}
            <strong className="font-semibold text-foreground">catalog merge</strong> inside the same{" "}
            <strong className="font-semibold text-foreground">Process</strong> request (separate /sync and /generic calls
            are not used). Live row counts and percentages for those steps are in the pipeline card above.
          </p>
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
        {phase === "synced" &&
          effectiveRt !== "REMOVAL_ORDER" &&
          !removalShipmentUi &&
          !listingImportUi && (
          <p className="text-xs font-semibold text-emerald-600 dark:text-emerald-400">
            All 3 phases complete. Data is in the destination tables.
          </p>
        )}
        {phase === "synced" && listingImportUi && progressMsg && (
          <p className="whitespace-pre-line text-xs font-semibold text-emerald-600 dark:text-emerald-400">
            {progressMsg}
          </p>
        )}
        {phase === "synced" && effectiveRt === "REMOVAL_ORDER" && (
          <p className="text-xs font-semibold text-amber-700 dark:text-amber-400">
            Phase 3 complete (amazon_removals). Click <strong>Generate Worklist</strong> to sync{" "}
            <code className="rounded bg-muted px-1 text-foreground">expected_packages</code> for the scanner.
          </p>
        )}
        {(phase === "synced" || phase === "raw_synced") &&
          removalShipmentUi &&
          topUi &&
          !topUi.phase4Complete && (
          <p className="text-xs font-semibold text-amber-700 dark:text-amber-400">
            Phase 3 archived lines to <strong>amazon_removal_shipments</strong>. Run <strong>Generic</strong> for shipment
            tree / expected_packages enrich — no separate Generate Worklist step for this report.
          </p>
        )}
        {phase === "synced" && removalShipmentUi && topUi?.phase4Complete && (
          <p className="text-xs font-semibold text-emerald-600 dark:text-emerald-400">
            All phases complete — shipment tree / expected_packages enrich finished for this upload.
          </p>
        )}
        {phase === "worklisted" && effectiveRt === "REMOVAL_ORDER" && (
          <p className="text-xs font-semibold text-emerald-600 dark:text-emerald-400">
            All phases complete. Removal order data and warehouse worklist are ready.
          </p>
        )}
      </div>
    </section>
  );
}
