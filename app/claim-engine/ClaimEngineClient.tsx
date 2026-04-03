"use client";

/**
 * Agent / AI developer hand-off (V16.4.15+):
 * The `claim_submissions` table is the submission queue backing store. Poll (or subscribe to)
 * rows where `status = 'ready_to_send'` to implement marketplace filing; update `status`,
 * `submission_id`, and `reimbursement_amount` as the Agent completes work. See server helpers in
 * `claim-submission-actions.ts` and `claim-actions.ts`.
 *
 * Golden Rule (identifiers): use `ReturnIdentifiersColumn` (or the same vertical ASIN/FNSKU/SKU +
 * copy + marketplace actions) everywhere — PDFs mirror links in `claim-pdf-document.tsx`.
 */

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";
import type { ChangeEvent, MouseEvent } from "react";
import {
  AlertTriangle,
  ArrowUpDown,
  Ban,
  ChevronDown,
  ChevronUp,
  ClipboardList,
  Clock,
  DollarSign,
  CheckCircle2,
  Eye,
  FileDown,
  FileText,
  History as HistoryIcon,
  Inbox,
  Loader2,
  MessageSquare,
  Percent,
  Search,
  Send,
  ShieldAlert,
  TrendingUp,
} from "lucide-react";
import { useTableSortFilter, useSortFilterState, type SortDir } from "../../hooks/use-table-sort-filter";
import type { CoreSettings } from "../settings/workspace-settings-types";
import type { ClaimEvidenceKey } from "./claim-evidence-settings";
import type { PalletRecord, PackageRecord } from "../returns/returns-action-types";
import { DatabaseTag } from "../../components/DatabaseTag";
import { useUserRole } from "../../components/UserRoleContext";
import { ReturnIdentifiersColumn } from "../../components/ReturnIdentifiersColumn";
import { InlineCopy, StatusBadge } from "../returns/_components";
import type { ClaimRecord } from "./claim-actions";
import { bulkUpdateClaimsStatus, getBulkClaimDetails } from "./claim-actions";
import type { ClaimEngineKpis } from "./claim-crm-actions";
import {
  approveClaimSubmission,
  bulkSubmitClaimsToMarketplace,
  generateDailyClaimReports,
  markClaimSubmissionManualSubmit,
  refreshClaimReportSignedUrl,
  type ClaimSubmissionListRow,
} from "./claim-submission-actions";
import { downloadBulkClaimsPdf, enrichBulkPagesWithDefaultEvidence } from "./claim-pdf-download";
import { prepareClaimEnginePdfPages } from "./claim-pdf-batch-actions";
import { ClaimDetailModal } from "./ClaimDetailModal";
import { ClaimGenerationModal } from "./ClaimGenerationModal";
import { ClaimHistoryModal } from "./ClaimHistoryModal";

type StoreRow = { id: string; name: string; platform: string };

function formatCurrency(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

/** Always two decimals — e.g. requested claim amounts in the submission queue. */
function formatMoneyUsd2(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number.isFinite(value) ? value : 0);
}

function formatEstimatedUsd(value: unknown): string {
  const n = Number(value);
  return formatMoneyUsd2(Number.isFinite(n) ? n : 0);
}

function formatDate(iso: string): string {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(iso));
}

function providerLabel(raw: string | null): string {
  if (!raw) return "—";
  const map: Record<string, string> = {
    amazon_sp_api: "Amazon",
    walmart_api: "Walmart",
    ebay_api: "eBay",
  };
  return map[raw] ?? raw;
}

const STATUS_STYLES: Record<string, string> = {
  pending:
    "border-amber-700/60 bg-amber-950/50 text-amber-300",
  recovered:
    "border-emerald-700/60 bg-emerald-950/50 text-emerald-300",
  suspicious:
    "border-rose-700/60 bg-rose-950/50 text-rose-300",
  cancelled:
    "border-slate-600/60 bg-slate-900/50 text-slate-300",
};

/** `claim_submissions.status` + legacy adapter labels for synced rows */
const CLAIM_ROW_STATUS_STYLES: Record<string, string> = {
  ...STATUS_STYLES,
  draft: "border-slate-600/60 bg-slate-900/50 text-slate-300",
  ready_to_send: "border-sky-700/60 bg-sky-950/50 text-sky-200",
  submitted: "border-amber-700/60 bg-amber-950/50 text-amber-300",
  evidence_requested: "border-amber-600/60 bg-amber-950/40 text-amber-100",
  investigating: "border-violet-600/60 bg-violet-950/40 text-violet-100",
  accepted: STATUS_STYLES.recovered,
  rejected: "border-rose-700/60 bg-rose-950/50 text-rose-300",
  /** Added by Neda's migration — terminal system failure. */
  failed: "border-rose-800/70 bg-rose-950/60 text-rose-200",
};

const SUBMISSION_STATUS_STYLES: Record<string, string> = {
  draft: "border-slate-600/60 bg-slate-900/50 text-slate-300",
  ready_to_send: "border-sky-700/60 bg-sky-950/50 text-sky-200",
  submitted: "border-amber-700/60 bg-amber-950/50 text-amber-200",
  evidence_requested: "border-amber-600/60 bg-amber-950/40 text-amber-100",
  investigating: "border-violet-700/60 bg-violet-950/50 text-violet-200",
  accepted: "border-emerald-700/60 bg-emerald-950/50 text-emerald-200",
  rejected: "border-rose-700/60 bg-rose-950/50 text-rose-200",
  /** Added by Neda's migration — terminal system failure, shown in queue and history. */
  failed: "border-rose-800/70 bg-rose-950/60 text-rose-200",
};

function resolveStore(claim: ClaimRecord, stores: StoreRow[]): StoreRow | null {
  if (!claim.store_id) return null;
  return stores.find((s) => s.id === claim.store_id) ?? null;
}

function storePlatformForSubmission(row: ClaimSubmissionListRow, stores: StoreRow[]): string | null {
  if (!row.store_id) return null;
  return stores.find((s) => s.id === row.store_id)?.platform ?? null;
}

function DataTableSortHeader({
  label,
  colKey,
  sortKey,
  sortDir,
  onToggle,
  align = "left",
}: {
  label: string;
  colKey: string;
  sortKey: string | null;
  sortDir: SortDir;
  onToggle: (k: string) => void;
  align?: "left" | "right";
}) {
  const active = sortKey === colKey;
  return (
    <th
      className={`px-4 py-2.5 text-[11px] font-medium uppercase tracking-wide text-slate-500 ${
        align === "right" ? "text-right" : "text-left"
      }`}
    >
      <button
        type="button"
        className={`inline-flex w-full items-center gap-1 hover:text-slate-800 dark:hover:text-slate-200 ${
          align === "right" ? "justify-end" : "justify-start"
        }`}
        onClick={() => onToggle(colKey)}
      >
        {label}
        {active ? (
          sortDir === "asc" ? (
            <ChevronUp className="h-3 w-3 shrink-0" />
          ) : (
            <ChevronDown className="h-3 w-3 shrink-0" />
          )
        ) : (
          <ArrowUpDown className="h-3 w-3 shrink-0 opacity-40" />
        )}
      </button>
    </th>
  );
}

export function ClaimEngineClient({
  claims: initialClaims,
  claimsError,
  coreSettings,
  stores,
  organizationId,
  claimSubmissions: initialSubmissions,
  submissionsError,
  kpis,
  kpisError,
  defaultClaimEvidence,
}: {
  claims: ClaimRecord[];
  claimsError: string | null;
  coreSettings: CoreSettings;
  stores: StoreRow[];
  organizationId: string;
  claimSubmissions: ClaimSubmissionListRow[];
  submissionsError: string | null;
  kpis: ClaimEngineKpis | null;
  kpisError: string | null;
  defaultClaimEvidence: Record<ClaimEvidenceKey, boolean>;
}) {
  const router = useRouter();
  const { actorUserId } = useUserRole();
  const [claims, setClaims] = useState<ClaimRecord[]>(initialClaims);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [modalClaim, setModalClaim] = useState<ClaimRecord | null>(null);
  const [bulkBusy, setBulkBusy] = useState(false);
  const [toast, setToast] = useState<{ msg: string; kind: "success" | "error" | "warning" } | null>(null);
  const [activeTab, setActiveTab] = useState<"workspace" | "queue">("workspace");
  const [generateBusy, setGenerateBusy] = useState(false);
  const [bulkSubmitBusy, setBulkSubmitBusy] = useState(false);
  const [queueBusyId, setQueueBusyId] = useState<string | null>(null);
  const [approveBusyId, setApproveBusyId] = useState<string | null>(null);
  const [historySubmissionId, setHistorySubmissionId] = useState<string | null>(null);
  /** Submission-queue row selection for PDF batch (takes priority over workspace selection). */
  const [queueSelectedIds, setQueueSelectedIds] = useState<Set<string>>(new Set());
  const [queueBulkPdfBusy, setQueueBulkPdfBusy] = useState(false);
  const [claimGenSubmissionId, setClaimGenSubmissionId] = useState<string | null>(null);
  const [claimGenAmountNote, setClaimGenAmountNote] = useState<string | undefined>(undefined);

  const claimsSf = useSortFilterState();
  const queueSf = useSortFilterState();

  useEffect(() => {
    setClaims(initialClaims);
  }, [initialClaims]);

  const submissionQueueRows = useMemo(() => {
    const rank = (status: string) => (status === "ready_to_send" ? 0 : 1);
    return [...initialSubmissions].sort((a, b) => {
      const d = rank(a.status) - rank(b.status);
      if (d !== 0) return d;
      return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
    });
  }, [initialSubmissions]);

  const readyToSendCount = useMemo(
    () => submissionQueueRows.filter((r) => r.status === "ready_to_send").length,
    [submissionQueueRows],
  );

  const claimColumns = useMemo(
    () => [
      {
        key: "identifiers",
        pickText: (c: ClaimRecord) =>
          [c.item_name, c.asin, c.fnsku, c.sku].filter(Boolean).join(" "),
      },
      {
        key: "store",
        pickText: (c: ClaimRecord) =>
          resolveStore(c, stores)?.platform ?? c.marketplace_provider ?? "",
      },
      { key: "type", pickText: (c: ClaimRecord) => c.claim_type ?? "" },
      { key: "order", pickText: (c: ClaimRecord) => c.amazon_order_id ?? "" },
      { key: "amount", pickText: (c: ClaimRecord) => String(Number(c.amount) || 0) },
      { key: "status", pickText: (c: ClaimRecord) => c.status },
      { key: "date", pickText: (c: ClaimRecord) => c.created_at },
    ],
    [stores],
  );

  const queueColumns = useMemo(
    () => [
      {
        key: "identifiers",
        pickText: (row: ClaimSubmissionListRow) =>
          [row.item_name, row.asin, row.fnsku, row.sku].filter(Boolean).join(" "),
      },
      { key: "status", pickText: (row: ClaimSubmissionListRow) => row.status },
      { key: "amount", pickText: (row: ClaimSubmissionListRow) => String(row.claim_amount) },
      { key: "created", pickText: (row: ClaimSubmissionListRow) => row.created_at },
    ],
    [],
  );

  const displayClaims = useTableSortFilter(claims, {
    filter: claimsSf.filter,
    sortKey: claimsSf.sortKey,
    sortDir: claimsSf.sortDir,
    columns: claimColumns,
  });

  const submissionQueueDisplay = useTableSortFilter(submissionQueueRows, {
    filter: queueSf.filter,
    sortKey: queueSf.sortKey,
    sortDir: queueSf.sortDir,
    columns: queueColumns,
  });

  /**
   * Opens the evidence picker for exactly one selected submission (queue or workspace claim row).
   */
  function handleGeneratePdfReport() {
    if (activeTab === "queue") {
      if (queueSelectedIds.size === 1) {
        const id = [...queueSelectedIds][0];
        const row = submissionQueueRows.find((r) => r.id === id);
        setClaimGenAmountNote(row ? String(row.claim_amount ?? "") : undefined);
        setClaimGenSubmissionId(id);
        return;
      }
      showToast(
        "Select exactly one submission in the queue (checkbox) to configure evidence, or use Generate PDF on a row.",
        "warning",
      );
      return;
    }
    if (selectedIds.size === 1) {
      const id = [...selectedIds][0];
      const claim = claims.find((c) => c.id === id);
      setClaimGenAmountNote(claim ? String(claim.amount ?? "") : undefined);
      setClaimGenSubmissionId(id);
      return;
    }
    showToast("Select exactly one claim in the workspace table to configure evidence.", "warning");
  }

  function openClaimGenerationForRow(row: ClaimSubmissionListRow) {
    setClaimGenAmountNote(String(row.claim_amount ?? ""));
    setClaimGenSubmissionId(row.id);
  }

  async function handleEnqueuePipelineReports() {
    setGenerateBusy(true);
    const res = await generateDailyClaimReports(organizationId);
    setGenerateBusy(false);
    if (!res.ok) {
      showToast(res.error ?? "Build queue failed", "error");
      return;
    }
    if (res.generated === 0) {
      showToast("No ready_for_claim returns found for this workspace.", "warning");
    } else {
      showToast(`Successfully added ${res.generated} items to the queue`, "success");
    }
    router.refresh();
  }

  async function handlePreview(row: ClaimSubmissionListRow) {
    let url = row.preview_url;
    if (!url && row.report_url) {
      setQueueBusyId(row.id);
      const r = await refreshClaimReportSignedUrl(row.report_url);
      setQueueBusyId(null);
      url = r.ok ? (r.url ?? null) : null;
      if (!url) {
        showToast(r.error ?? "Could not open PDF", "error");
        return;
      }
    }
    if (!url) {
      showToast("No PDF path on file.", "error");
      return;
    }
    window.open(url, "_blank", "noopener,noreferrer");
  }

  async function handleManualSubmit(row: ClaimSubmissionListRow) {
    const id = window.prompt("Marketplace case / claim ID (e.g. Amazon or Walmart):", row.submission_id ?? "");
    if (id === null) return;
    setQueueBusyId(row.id);
    const res = await markClaimSubmissionManualSubmit(row.id, id, organizationId, actorUserId);
    setQueueBusyId(null);
    if (res.ok) {
      showToast("Marked as submitted.", "success");
      router.refresh();
    } else showToast(res.error ?? "Update failed", "error");
  }

  async function handleBulkMarketplace() {
    setBulkSubmitBusy(true);
    const ids = selectedIds.size > 0 ? [...selectedIds] : null;
    const res = await bulkSubmitClaimsToMarketplace(organizationId, ids, actorUserId);
    setBulkSubmitBusy(false);
    if (res.ok && res.count != null) {
      showToast(`Submitted ${res.count} claim(s) to marketplace workflow.`, "success");
      router.refresh();
    } else showToast(res.error ?? "Bulk submit failed.", "error");
  }

  const showToast = useCallback((msg: string, kind: "success" | "error" | "warning" = "success") => {
    setToast({ msg, kind });
    setTimeout(() => setToast(null), 3200);
  }, []);

  const totalRecoveredDisplay = useMemo(() => {
    if (kpis?.totalRecoveredUsd != null) return kpis.totalRecoveredUsd;
    return claims
      .filter((c) => c.status === "accepted" || c.status === "recovered")
      .reduce((sum, c) => {
        const r = c.reimbursement_amount;
        if (r != null && Number(r) > 0) return sum + Number(r);
        return sum + (Number(c.amount) || 0);
      }, 0);
  }, [kpis, claims]);
  const pendingCount = claims.filter((c) =>
    ["draft", "ready_to_send", "submitted", "pending", "pending_evidence"].includes(c.status),
  ).length;
  const suspiciousCount = claims.filter((c) =>
    ["suspicious", "evidence_requested"].includes(c.status),
  ).length;

  const allSelected =
    displayClaims.length > 0 && displayClaims.every((c) => selectedIds.has(c.id));

  function toggleRow(id: string, e: MouseEvent) {
    e.stopPropagation();
    setSelectedIds((prev) => {
      const n = new Set(prev);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });
  }

  function toggleAll(e: ChangeEvent<HTMLInputElement>) {
    if (e.target.checked) setSelectedIds(new Set(displayClaims.map((c) => c.id)));
    else setSelectedIds(new Set());
  }

  function toggleQueueRow(id: string, e: MouseEvent) {
    e.stopPropagation();
    setQueueSelectedIds((prev) => {
      const n = new Set(prev);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });
  }

  async function handleBulkCancel() {
    const ids = [...selectedIds];
    if (ids.length === 0) return;
    if (!window.confirm(`Cancel ${ids.length} claim(s)?`)) return;
    setBulkBusy(true);
    const res = await bulkUpdateClaimsStatus(ids, "cancelled", organizationId, actorUserId);
    setBulkBusy(false);
    if (res.ok) {
      setClaims((prev) =>
        prev.map((c) => (selectedIds.has(c.id) ? { ...c, status: "cancelled" } : c)),
      );
      setSelectedIds(new Set());
      showToast("Claims cancelled", "success");
      router.refresh();
    } else showToast(res.error ?? "Bulk cancel failed", "error");
  }

  async function handleBulkPdf() {
    const ids = [...selectedIds];
    if (ids.length === 0) return;
    setBulkBusy(true);
    const res = await getBulkClaimDetails(ids, organizationId);
    setBulkBusy(false);
    if (!res.ok || !res.data.length) {
      showToast(res.error ?? "Could not load claim details", "error");
      return;
    }
    const pages = res.data.map((detail) => {
      const st = resolveStore(detail.claim, stores);
      return {
        storeName: st?.name ?? providerLabel(detail.claim.marketplace_provider),
        storePlatform: st?.platform ?? "amazon",
        detail,
        claimAmountNote: String(detail.claim.amount ?? ""),
        marketplaceClaimIdNote: detail.claim.marketplace_claim_id ?? undefined,
      };
    });
    try {
      const enriched = await enrichBulkPagesWithDefaultEvidence(pages, defaultClaimEvidence);
      await downloadBulkClaimsPdf({ tenant: coreSettings, pages: enriched });
      showToast("Bulk PDF downloaded", "success");
    } catch {
      showToast("Bulk PDF failed", "error");
    }
  }

  async function handlePrepareBulkQueuePdfReport() {
    const ids = [...queueSelectedIds];
    if (ids.length === 0) return;
    setQueueBulkPdfBusy(true);
    try {
      const res = await prepareClaimEnginePdfPages(organizationId, ids);
      if (!res.ok || !res.pages?.length) {
        showToast(res.error ?? "Could not build PDF report", "error");
        return;
      }
      const mapped = res.pages.map((p) => ({
        storeName: p.storeName,
        storePlatform: p.storePlatform,
        detail: p.detail,
        claimAmountNote: p.claimAmountNote,
        marketplaceClaimIdNote: p.marketplaceClaimIdNote,
      }));
      const enriched = await enrichBulkPagesWithDefaultEvidence(mapped, defaultClaimEvidence);
      await downloadBulkClaimsPdf({
        tenant: coreSettings,
        pages: enriched,
        filename: `claims-bulk-report-${Date.now()}.pdf`,
        reportKind: "batch",
      });
      showToast(
        `Prepared PDF with ${res.pagesBuilt ?? res.pages.length} page(s).`,
        "success",
      );
    } catch {
      showToast("Bulk PDF report failed.", "error");
    } finally {
      setQueueBulkPdfBusy(false);
    }
  }

  async function handleApproveClaim(claim: ClaimRecord) {
    if (claim.status === "accepted") return;
    setApproveBusyId(claim.id);
    const res = await approveClaimSubmission(claim.id, organizationId, actorUserId);
    setApproveBusyId(null);
    if (res.ok) {
      setClaims((prev) =>
        prev.map((c) => (c.id === claim.id ? { ...c, status: "accepted" } : c)),
      );
      showToast("Claim approved", "success");
      router.refresh();
    } else showToast(res.error ?? "Approve failed", "error");
  }

  return (
    <>
      {toast ? (
        <div
          className={`pointer-events-none fixed bottom-6 left-1/2 z-[500] -translate-x-1/2 rounded-full px-4 py-2 text-sm font-semibold shadow-lg ${
            toast.kind === "success"
              ? "bg-emerald-600 text-white"
              : toast.kind === "error"
                ? "bg-rose-600 text-white"
                : "bg-amber-500 text-white"
          }`}
        >
          {toast.msg}
        </div>
      ) : null}

      {activeTab === "queue" && queueSelectedIds.size > 0 ? (
        <div className="pointer-events-auto fixed bottom-20 left-1/2 z-[470] flex w-[min(100vw-2rem,36rem)] max-w-[calc(100vw-2rem)] -translate-x-1/2 flex-col gap-3 rounded-2xl border border-slate-200 bg-white/95 px-4 py-3 shadow-xl backdrop-blur-md dark:border-slate-700 dark:bg-slate-900/95 sm:flex-row sm:items-center sm:justify-between">
          <p className="text-center text-sm font-semibold text-slate-800 dark:text-slate-100 sm:text-left">
            {queueSelectedIds.size} claim{queueSelectedIds.size === 1 ? "" : "s"} selected
          </p>
          <button
            type="button"
            disabled={queueBulkPdfBusy}
            onClick={() => void handlePrepareBulkQueuePdfReport()}
            className="inline-flex items-center justify-center gap-2 rounded-xl bg-emerald-600 px-4 py-2.5 text-sm font-bold text-white transition hover:bg-emerald-500 disabled:opacity-50"
          >
            {queueBulkPdfBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : <FileDown className="h-4 w-4" />}
            Prepare bulk PDF report
          </button>
        </div>
      ) : null}

      <ClaimGenerationModal
        open={claimGenSubmissionId !== null}
        onClose={() => {
          setClaimGenSubmissionId(null);
          setClaimGenAmountNote(undefined);
        }}
        submissionId={claimGenSubmissionId}
        organizationId={organizationId}
        coreSettings={coreSettings}
        stores={stores}
        defaultClaimEvidence={defaultClaimEvidence}
        claimAmountNote={claimGenAmountNote}
        onToast={showToast}
      />

      <ClaimDetailModal
        open={modalClaim !== null}
        onClose={() => setModalClaim(null)}
        claim={modalClaim}
        coreSettings={coreSettings}
        stores={stores}
        organizationId={organizationId}
        defaultClaimEvidence={defaultClaimEvidence}
        onToast={showToast}
        onUpdated={() => {
          setModalClaim(null);
        }}
      />

      <ClaimHistoryModal
        open={historySubmissionId !== null}
        onClose={() => setHistorySubmissionId(null)}
        claimId={historySubmissionId}
        organizationId={organizationId}
      />

      <header className="flex h-16 flex-col gap-2 border-b border-slate-200 bg-white/80 px-4 backdrop-blur-xl dark:border-slate-800 dark:bg-slate-950/70 sm:flex-row sm:items-center sm:justify-between sm:gap-4 md:px-6">
        <div className="flex flex-col">
          <div className="flex items-center gap-2">
            <ShieldAlert className="h-4 w-4 text-sky-500" />
            <h1 className="text-base font-semibold tracking-tight text-slate-900 dark:text-slate-50 sm:text-sm">Claim Engine</h1>
          </div>
          <p className="text-xs text-muted-foreground">
            Enterprise claims: identifiers, PDF exports, and marketplace filing fields.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <Link
            href="/claim-engine/report-history"
            className="text-xs font-medium text-sky-600 hover:text-sky-500 dark:text-sky-400"
          >
            Report history
          </Link>
          <Link href="/" className="text-xs font-medium text-sky-600 hover:text-sky-500 dark:text-sky-400">
            ← Dashboard
          </Link>
        </div>
      </header>

      <main className="flex-1 overflow-y-auto overflow-x-hidden bg-slate-50 dark:bg-gradient-to-br dark:from-slate-950 dark:via-slate-950 dark:to-slate-900">
        <div className="mx-auto flex w-full max-w-[100vw] flex-col gap-6 px-4 py-6 sm:px-4 lg:px-8">
          {(kpisError || kpis) && (
            <section className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-5">
              {kpisError ? (
                <div className="col-span-full rounded-2xl border border-rose-700/50 bg-rose-950/30 px-4 py-3 text-xs text-rose-100">
                  KPI data: {kpisError}
                </div>
              ) : kpis ? (
                <>
                  <div className="relative overflow-hidden rounded-2xl border border-sky-500/30 bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80">
                    <div className="flex items-start justify-between gap-3">
                      <div className="space-y-2">
                        <p className="text-xs font-medium uppercase tracking-wide text-slate-400">Total active claims</p>
                        <p className="text-2xl font-semibold tracking-tight text-slate-50">{kpis.totalActiveClaims}</p>
                      </div>
                      <ClipboardList className="h-5 w-5 text-sky-400" />
                    </div>
                    <p className="mt-3 text-[11px] text-slate-400">Not accepted or denied</p>
                  </div>
                  <div className="relative overflow-hidden rounded-2xl border border-emerald-500/30 bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80">
                    <div className="flex items-start justify-between gap-3">
                      <div className="space-y-2">
                        <p className="text-xs font-medium uppercase tracking-wide text-slate-400">Total claim value</p>
                        <p className="text-2xl font-semibold tracking-tight text-slate-50">
                          {formatCurrency(kpis.totalClaimValueUsd)}
                        </p>
                      </div>
                      <DollarSign className="h-5 w-5 text-emerald-400" />
                    </div>
                    <p className="mt-3 text-[11px] text-slate-400">Sum of claim_amount (active pipeline, USD)</p>
                  </div>
                  <div className="relative overflow-hidden rounded-2xl border border-teal-500/30 bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80">
                    <div className="flex items-start justify-between gap-3">
                      <div className="space-y-2">
                        <p className="text-xs font-medium uppercase tracking-wide text-slate-400">Projected recovery</p>
                        <p className="text-2xl font-semibold tracking-tight text-slate-50">
                          {formatCurrency(kpis.projectedRecoveryUsd)}
                        </p>
                      </div>
                      <TrendingUp className="h-5 w-5 text-teal-400" />
                    </div>
                    <p className="mt-3 text-[11px] text-slate-400">Pending / draft / ready / submitted (USD)</p>
                  </div>
                  <div className="relative overflow-hidden rounded-2xl border border-violet-500/30 bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80">
                    <div className="flex items-start justify-between gap-3">
                      <div className="space-y-2">
                        <p className="text-xs font-medium uppercase tracking-wide text-slate-400">Success rate</p>
                        <p className="text-2xl font-semibold tracking-tight text-slate-50">
                          {kpis.successRatePercent.toFixed(1)}%
                        </p>
                      </div>
                      <Percent className="h-5 w-5 text-violet-400" />
                    </div>
                    <p className="mt-3 text-[11px] text-slate-400">Accepted ÷ (accepted + denied)</p>
                  </div>
                  <div className="relative overflow-hidden rounded-2xl border border-amber-500/30 bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80">
                    <div className="flex items-start justify-between gap-3">
                      <div className="space-y-2">
                        <p className="text-xs font-medium uppercase tracking-wide text-slate-400">Pending evidence</p>
                        <p className="text-2xl font-semibold tracking-tight text-slate-50">{kpis.pendingEvidenceCount}</p>
                      </div>
                      <AlertTriangle className="h-5 w-5 text-amber-400" />
                    </div>
                    <p className="mt-3 text-[11px] text-slate-400">Status: evidence requested</p>
                  </div>
                </>
              ) : null}
            </section>
          )}

          <div className="flex flex-wrap gap-2 border-b border-slate-200 pb-3 dark:border-slate-800">
            <button
              type="button"
              onClick={() => setActiveTab("workspace")}
              className={`inline-flex items-center gap-2 rounded-xl px-3 py-2 text-xs font-semibold transition ${
                activeTab === "workspace"
                  ? "bg-sky-600 text-white shadow-sm"
                  : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800"
              }`}
            >
              <ShieldAlert className="h-3.5 w-3.5" />
              Claims workspace
            </button>
            <button
              type="button"
              onClick={() => setActiveTab("queue")}
              className={`inline-flex items-center gap-2 rounded-xl px-3 py-2 text-xs font-semibold transition ${
                activeTab === "queue"
                  ? "bg-sky-600 text-white shadow-sm"
                  : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800"
              }`}
            >
              <Inbox className="h-3.5 w-3.5" />
              Submission queue
            </button>
          </div>

          {activeTab === "workspace" ? (
            <>
          {claimsError && (
            <div className="rounded-2xl border border-rose-700/60 bg-rose-950/40 px-4 py-3 text-xs text-rose-100">
              <span className="font-semibold">Data warning:</span> {claimsError}
            </div>
          )}

          <section className="grid grid-cols-1 gap-4 sm:grid-cols-3">
            <div className="relative overflow-hidden rounded-2xl border bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80 border-sky-500/30">
              <div className="relative flex items-start justify-between gap-3">
                <div className="space-y-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-400">Total Recovered</p>
                  <p className="text-2xl font-semibold tracking-tight text-slate-50">{formatCurrency(totalRecoveredDisplay)}</p>
                </div>
                <TrendingUp className="h-5 w-5 text-sky-400" />
              </div>
              <p className="relative mt-3 text-[11px] text-slate-400">
                Reimbursement when recorded, else accepted claim_amount (USD)
              </p>
            </div>
            <div className="relative overflow-hidden rounded-2xl border bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80 border-amber-500/30">
              <div className="relative flex items-start justify-between gap-3">
                <div className="space-y-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-400">Pending Claims</p>
                  <p className="text-2xl font-semibold tracking-tight text-slate-50">{pendingCount}</p>
                </div>
                <Clock className="h-5 w-5 text-amber-400" />
              </div>
              <p className="relative mt-3 text-[11px] text-slate-400">Awaiting action from marketplace sync</p>
            </div>
            <div className="relative overflow-hidden rounded-2xl border bg-slate-950/70 px-4 py-4 shadow-sm ring-1 ring-inset ring-slate-800/80 border-rose-500/30">
              <div className="relative flex items-start justify-between gap-3">
                <div className="space-y-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-400">Suspicious</p>
                  <p className="text-2xl font-semibold tracking-tight text-slate-50">{suspiciousCount}</p>
                </div>
                <AlertTriangle className="h-5 w-5 text-rose-400" />
              </div>
              <p className="relative mt-3 text-[11px] text-slate-400">Flagged by adapter rules</p>
            </div>
          </section>

          {selectedIds.size > 0 && (
            <div className="flex flex-wrap items-center gap-3 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 dark:border-sky-900 dark:bg-sky-950/40">
              <span className="text-sm font-semibold text-sky-900 dark:text-sky-100">{selectedIds.size} selected</span>
              <button
                type="button"
                disabled={bulkBusy}
                onClick={() => void handleBulkCancel()}
                className="inline-flex items-center gap-2 rounded-xl border border-rose-300 bg-white px-3 py-2 text-xs font-semibold text-rose-800 disabled:opacity-50 dark:border-rose-800 dark:bg-slate-900 dark:text-rose-200"
              >
                {bulkBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : <Ban className="h-4 w-4" />}
                Cancel claims
              </button>
              <button
                type="button"
                disabled={bulkBusy}
                onClick={() => void handleGeneratePdfReport()}
                className="inline-flex items-center gap-2 rounded-xl border border-emerald-300 bg-emerald-50 px-3 py-2 text-xs font-semibold text-emerald-900 disabled:opacity-50 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-100"
              >
                <FileText className="h-4 w-4" />
                Generate PDF
              </button>
              <button
                type="button"
                disabled={bulkBusy}
                onClick={() => void handleBulkPdf()}
                className="inline-flex items-center gap-2 rounded-xl border border-sky-300 bg-white px-3 py-2 text-xs font-semibold text-sky-900 disabled:opacity-50 dark:border-sky-700 dark:bg-slate-900 dark:text-sky-100"
              >
                {bulkBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : <FileDown className="h-4 w-4" />}
                Export bulk claims PDF
              </button>
              <button
                type="button"
                onClick={() => setSelectedIds(new Set())}
                className="ml-auto text-xs font-medium text-slate-500 hover:text-slate-800 dark:hover:text-slate-200"
              >
                Clear selection
              </button>
            </div>
          )}

          <section className="relative w-full overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-950/70">
            <DatabaseTag table="claim_submissions" />
            <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3 dark:border-slate-800">
              <div>
                <p className="text-xs font-semibold tracking-tight text-slate-900 dark:text-slate-50">Marketplace investigations</p>
                <p className="text-[11px] text-muted-foreground">Filed or in-review submissions (not the pre-send queue).</p>
              </div>
              <FileText className="h-4 w-4 text-muted-foreground" />
            </div>
            <div className="border-b border-slate-200 px-4 py-2 dark:border-slate-800">
              <div className="relative max-w-md">
                <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
                <input
                  type="search"
                  placeholder="Filter claims (identifiers, order, status…)"
                  value={claimsSf.filter}
                  onChange={(e) => claimsSf.setFilter(e.target.value)}
                  className="w-full rounded-lg border border-slate-200 bg-white py-2 pl-9 pr-3 text-sm dark:border-slate-700 dark:bg-slate-900"
                />
              </div>
            </div>

            {claims.length === 0 ? (
              <div className="flex flex-col items-center justify-center gap-3 px-6 py-12 text-center">
                <div className="flex h-12 w-12 items-center justify-center rounded-full bg-slate-100 ring-1 ring-slate-200 dark:bg-slate-900 dark:ring-slate-800">
                  <FileText className="h-5 w-5 text-muted-foreground" />
                </div>
                <p className="text-sm font-medium text-slate-700 dark:text-slate-300">No active marketplace investigations found.</p>
                <p className="max-w-xs text-xs text-slate-500">
                  Investigations appear when submissions move past <span className="font-medium">ready to send</span> (submitted, evidence, accepted, etc.).
                </p>
                <Link
                  href="/settings"
                  className="mt-1 inline-flex items-center gap-1.5 rounded-full border border-slate-200 bg-slate-100 px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:border-slate-600 dark:hover:text-white"
                >
                  Adapter settings
                </Link>
              </div>
            ) : displayClaims.length === 0 ? (
              <div className="px-6 py-10 text-center text-sm text-muted-foreground">
                No claims match this filter. Clear the search box to see all rows.
              </div>
            ) : (
              <>
                <div className="hidden overflow-x-auto md:block">
                  <table className="w-full min-w-[960px] text-sm">
                    <thead>
                      <tr className="border-b border-slate-200 bg-slate-50 dark:border-slate-800 dark:bg-slate-900/40">
                        <th className="w-10 px-2 py-2.5" onClick={(e) => e.stopPropagation()}>
                          <input
                            type="checkbox"
                            checked={allSelected}
                            onChange={toggleAll}
                            className="h-4 w-4 rounded border-slate-300 text-sky-500"
                            aria-label="Select all"
                          />
                        </th>
                        <DataTableSortHeader
                          label="Identifiers"
                          colKey="identifiers"
                          sortKey={claimsSf.sortKey}
                          sortDir={claimsSf.sortDir}
                          onToggle={claimsSf.toggleSort}
                        />
                        <DataTableSortHeader
                          label="Store"
                          colKey="store"
                          sortKey={claimsSf.sortKey}
                          sortDir={claimsSf.sortDir}
                          onToggle={claimsSf.toggleSort}
                        />
                        <DataTableSortHeader
                          label="Claim Type"
                          colKey="type"
                          sortKey={claimsSf.sortKey}
                          sortDir={claimsSf.sortDir}
                          onToggle={claimsSf.toggleSort}
                        />
                        <DataTableSortHeader
                          label="Order / Ref"
                          colKey="order"
                          sortKey={claimsSf.sortKey}
                          sortDir={claimsSf.sortDir}
                          onToggle={claimsSf.toggleSort}
                        />
                        <DataTableSortHeader
                          label="Amount"
                          colKey="amount"
                          sortKey={claimsSf.sortKey}
                          sortDir={claimsSf.sortDir}
                          onToggle={claimsSf.toggleSort}
                          align="right"
                        />
                        <DataTableSortHeader
                          label="Status"
                          colKey="status"
                          sortKey={claimsSf.sortKey}
                          sortDir={claimsSf.sortDir}
                          onToggle={claimsSf.toggleSort}
                        />
                        <DataTableSortHeader
                          label="Date"
                          colKey="date"
                          sortKey={claimsSf.sortKey}
                          sortDir={claimsSf.sortDir}
                          onToggle={claimsSf.toggleSort}
                        />
                        <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">History</th>
                        <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Details</th>
                        <th className="px-4 py-2.5 text-left text-[11px] font-medium uppercase tracking-wide text-slate-500">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60">
                      {displayClaims.map((claim) => (
                        <tr
                          key={claim.id}
                          className="cursor-pointer transition hover:bg-accent/40"
                          onClick={() => setHistorySubmissionId(claim.id)}
                        >
                          <td
                            className="w-10 px-2 py-3"
                            onClick={(e) => toggleRow(claim.id, e)}
                          >
                            <input
                              type="checkbox"
                              checked={selectedIds.has(claim.id)}
                              readOnly
                              className="pointer-events-none h-4 w-4 rounded border-slate-300 text-sky-500"
                              aria-label="Select row"
                            />
                          </td>
                          <td className="px-4 py-3 align-top">
                            <ReturnIdentifiersColumn
                              compact
                              itemName={claim.item_name}
                              asin={claim.asin}
                              fnsku={claim.fnsku}
                              sku={claim.sku}
                              storePlatform={resolveStore(claim, stores)?.platform}
                              onToast={showToast}
                            />
                          </td>
                          <td className="px-4 py-3 align-top">
                            {claim.marketplace_provider ? (
                              <span
                                className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px] font-semibold ${
                                  claim.marketplace_provider === "amazon_sp_api"
                                    ? "border-amber-300 bg-amber-50 text-amber-700 dark:border-amber-700/50 dark:bg-amber-950/30 dark:text-amber-300"
                                    : claim.marketplace_provider === "walmart_api"
                                      ? "border-sky-300 bg-sky-50 text-sky-700 dark:border-sky-700/50 dark:bg-sky-950/30 dark:text-sky-300"
                                      : "border-rose-300 bg-rose-50 text-rose-700 dark:border-rose-700/50 dark:bg-rose-950/30 dark:text-rose-300"
                                }`}
                              >
                                {providerLabel(claim.marketplace_provider)}
                              </span>
                            ) : (
                              <span className="text-xs text-muted-foreground">—</span>
                            )}
                          </td>
                          <td className="px-4 py-3 align-top text-xs text-slate-600 dark:text-slate-300">
                            {claim.claim_type ?? "—"}
                          </td>
                          <td className="px-4 py-3 align-top font-mono text-xs text-muted-foreground">
                            {claim.amazon_order_id ?? "—"}
                          </td>
                          <td className="px-4 py-3 align-top text-right text-xs font-semibold text-slate-900 dark:text-slate-50">
                            {formatCurrency(Number(claim.amount) || 0)}
                          </td>
                          <td className="px-4 py-3 align-top">
                            <span
                              className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] font-medium ${CLAIM_ROW_STATUS_STYLES[claim.status] ?? CLAIM_ROW_STATUS_STYLES.pending}`}
                            >
                              {claim.status.charAt(0).toUpperCase() + claim.status.slice(1)}
                            </span>
                          </td>
                          <td className="px-4 py-3 align-top text-xs text-muted-foreground">
                            {formatDate(claim.created_at)}
                          </td>
                          <td className="px-4 py-3 align-top" onClick={(e) => e.stopPropagation()}>
                            <button
                              type="button"
                              onClick={() => setHistorySubmissionId(claim.id)}
                              className="inline-flex items-center gap-1 rounded-lg border border-slate-200 bg-white px-2 py-1 text-[11px] font-semibold text-slate-800 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                            >
                              <HistoryIcon className="h-3.5 w-3.5" />
                              History
                            </button>
                          </td>
                          <td className="px-4 py-3 align-top" onClick={(e) => e.stopPropagation()}>
                            <button
                              type="button"
                              onClick={() => setModalClaim(claim)}
                              className="inline-flex items-center gap-1 rounded-lg border border-sky-200 bg-sky-50 px-2 py-1 text-[11px] font-semibold text-sky-900 dark:border-sky-800 dark:bg-sky-950/40 dark:text-sky-100"
                            >
                              Details
                            </button>
                          </td>
                          <td className="px-4 py-3 align-top" onClick={(e) => e.stopPropagation()}>
                            <button
                              type="button"
                              disabled={approveBusyId === claim.id || claim.status === "accepted"}
                              onClick={() => void handleApproveClaim(claim)}
                              className="inline-flex items-center gap-1 rounded-lg border border-emerald-200 bg-emerald-50 px-2 py-1 text-[11px] font-semibold text-emerald-900 disabled:opacity-50 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-100"
                            >
                              {approveBusyId === claim.id ? (
                                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                              ) : (
                                <CheckCircle2 className="h-3.5 w-3.5" />
                              )}
                              Approve
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="space-y-3 p-3 md:hidden">
                  {displayClaims.map((claim) => (
                    <div
                      key={claim.id}
                      className="cursor-pointer rounded-xl border border-slate-200 bg-slate-50/80 p-4 dark:border-slate-800 dark:bg-slate-900/50"
                      onClick={() => setHistorySubmissionId(claim.id)}
                      role="presentation"
                    >
                      <p className="text-sm font-semibold text-slate-900 dark:text-slate-50">
                        {claim.item_name?.trim() || "Claim"}
                      </p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        {formatCurrency(Number(claim.amount) || 0)} ·{" "}
                        <span className="font-medium text-slate-700 dark:text-slate-300">{claim.status}</span>
                      </p>
                      <div className="mt-3 flex flex-wrap gap-2" onClick={(e) => e.stopPropagation()}>
                        <button
                          type="button"
                          disabled={approveBusyId === claim.id || claim.status === "accepted"}
                          onClick={() => void handleApproveClaim(claim)}
                          className="inline-flex flex-1 items-center justify-center gap-1 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs font-semibold text-emerald-900 disabled:opacity-50 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-100"
                        >
                          {approveBusyId === claim.id ? (
                            <Loader2 className="h-4 w-4 animate-spin" />
                          ) : (
                            <CheckCircle2 className="h-4 w-4" />
                          )}
                          Approve
                        </button>
                        <button
                          type="button"
                          onClick={() => setHistorySubmissionId(claim.id)}
                          className="inline-flex flex-1 items-center justify-center gap-1 rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-800 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                        >
                          <HistoryIcon className="h-4 w-4" />
                          History
                        </button>
                        <button
                          type="button"
                          onClick={() => setModalClaim(claim)}
                          className="inline-flex flex-1 items-center justify-center rounded-xl border border-sky-200 bg-sky-50 px-3 py-2 text-xs font-semibold text-sky-900 dark:border-sky-800 dark:bg-sky-950/40 dark:text-sky-100"
                        >
                          Details
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              </>
            )}
          </section>
            </>
          ) : (
            <section className="w-full overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-950/70">
              <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 px-4 py-3 dark:border-slate-800">
                <div>
                  <p className="text-xs font-semibold tracking-tight text-slate-900 dark:text-slate-50">Submission queue</p>
                  <p className="text-[11px] text-muted-foreground">
                    Auto-generated claim PDFs (Supabase Storage). <span className="font-semibold text-sky-700 dark:text-sky-400">ready_to_send</span> rows surface first for Agent testing; preview before filing; manual submit records the marketplace case ID.
                  </p>
                </div>
                <div className="flex flex-wrap gap-2">
                  <button
                    type="button"
                    onClick={() => void handleGeneratePdfReport()}
                    className="inline-flex items-center gap-2 rounded-xl border border-emerald-300 bg-emerald-50 px-3 py-2 text-xs font-semibold text-emerald-900 disabled:opacity-50 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-100"
                  >
                    <FileText className="h-4 w-4" />
                    Generate PDF report
                  </button>
                  <button
                    type="button"
                    disabled={generateBusy}
                    onClick={() => void handleEnqueuePipelineReports()}
                    className="inline-flex items-center gap-2 rounded-xl border border-sky-300 bg-sky-50 px-3 py-2 text-xs font-semibold text-sky-900 disabled:opacity-50 dark:border-sky-700 dark:bg-sky-950/40 dark:text-sky-100"
                  >
                    {generateBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : <FileText className="h-4 w-4" />}
                    Build queue from returns
                  </button>
                  <button
                    type="button"
                    disabled={bulkSubmitBusy}
                    onClick={() => void handleBulkMarketplace()}
                    className="inline-flex items-center gap-2 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-xs font-semibold text-slate-800 disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200"
                  >
                    {bulkSubmitBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
                    Bulk submit to marketplace
                  </button>
                </div>
              </div>
              <div className="flex flex-wrap items-center gap-3 border-b border-slate-200 px-4 py-2 dark:border-slate-800">
                <div className="relative min-w-[200px] max-w-md flex-1">
                  <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
                  <input
                    type="search"
                    placeholder="Filter submission queue…"
                    value={queueSf.filter}
                    onChange={(e) => queueSf.setFilter(e.target.value)}
                    className="w-full rounded-lg border border-slate-200 bg-white py-2 pl-9 pr-3 text-sm dark:border-slate-700 dark:bg-slate-900"
                  />
                </div>
                <div className="flex flex-wrap items-center gap-2 text-[11px] text-muted-foreground">
                  <span className="font-medium">Sort</span>
                  <select
                    value={queueSf.sortKey ?? ""}
                    onChange={(e) => queueSf.setSortKey(e.target.value || null)}
                    className="rounded-lg border border-slate-200 bg-white px-2 py-1.5 text-xs dark:border-slate-700 dark:bg-slate-900"
                  >
                    <option value="">(list order)</option>
                    <option value="identifiers">Identifiers</option>
                    <option value="status">Status</option>
                    <option value="amount">Amount</option>
                    <option value="created">Created</option>
                  </select>
                  <select
                    value={queueSf.sortDir}
                    onChange={(e) => queueSf.setSortDir(e.target.value as SortDir)}
                    className="rounded-lg border border-slate-200 bg-white px-2 py-1.5 text-xs dark:border-slate-700 dark:bg-slate-900"
                  >
                    <option value="asc">Asc</option>
                    <option value="desc">Desc</option>
                  </select>
                </div>
                {queueSelectedIds.size > 0 ? (
                  <span className="text-xs font-semibold text-emerald-800 dark:text-emerald-200">
                    {queueSelectedIds.size} row(s) selected for PDF
                  </span>
                ) : null}
              </div>
              {submissionsError ? (
                <div className="border-b border-rose-700/40 bg-rose-950/30 px-4 py-2 text-xs text-rose-100">{submissionsError}</div>
              ) : null}
              {readyToSendCount > 0 ? (
                <div className="border-b border-sky-300/80 bg-gradient-to-r from-sky-100/90 to-sky-50/90 px-4 py-3 dark:border-sky-800 dark:from-sky-950/80 dark:to-slate-950/60">
                  <p className="text-sm font-bold text-sky-950 dark:text-sky-100">
                    {readyToSendCount} claim{readyToSendCount === 1 ? "" : "s"} ready to send
                  </p>
                </div>
              ) : null}
              {submissionQueueRows.length === 0 ? (
                <div className="px-6 py-12 text-center text-sm text-muted-foreground">
                  No generated reports yet. Run <span className="font-semibold text-slate-700 dark:text-slate-300">Generate reports</span> to build PDFs for
                  returns in <span className="font-semibold">ready for claim</span> status.
                </div>
              ) : submissionQueueDisplay.length === 0 ? (
                <div className="px-6 py-10 text-center text-sm text-muted-foreground">
                  No queue rows match this filter.
                </div>
              ) : (
                <ul className="divide-y divide-slate-100 dark:divide-slate-800/60">
                  {submissionQueueDisplay.map((row) => (
                    <li
                      key={row.id}
                      className={[
                        "flex flex-col gap-3 px-4 py-4 sm:flex-row sm:items-stretch",
                        row.status === "ready_to_send"
                          ? "bg-sky-50/50 dark:bg-sky-950/20"
                          : "",
                      ].join(" ")}
                    >
                      <div className="flex min-w-0 flex-1 gap-3">
                        <div
                          className="flex shrink-0 items-start pt-1"
                          onClick={(e) => toggleQueueRow(row.id, e)}
                        >
                          <input
                            type="checkbox"
                            checked={queueSelectedIds.has(row.id)}
                            readOnly
                            className="pointer-events-none mt-0.5 h-4 w-4 rounded border-slate-300 text-emerald-600"
                            aria-label="Select for PDF batch"
                          />
                        </div>
                        <div
                          className="flex h-16 w-14 shrink-0 items-center justify-center rounded-lg border border-slate-200 bg-gradient-to-br from-slate-100 to-slate-200 dark:border-slate-700 dark:from-slate-800 dark:to-slate-900"
                          aria-hidden
                        >
                          <FileText className="h-7 w-7 text-slate-500 dark:text-slate-400" />
                        </div>
                        <div className="min-w-0 flex-1">
                          <ReturnIdentifiersColumn
                            compact
                            itemName={row.item_name}
                            asin={row.asin}
                            fnsku={row.fnsku}
                            sku={row.sku}
                            storePlatform={storePlatformForSubmission(row, stores)}
                            onToast={showToast}
                          />
                          <p className="mt-2 rounded-lg border border-emerald-200/80 bg-emerald-50/50 px-3 py-2 text-sm dark:border-emerald-900/50 dark:bg-emerald-950/30">
                            <span className="font-semibold text-slate-600 dark:text-slate-300">Requested amount </span>
                            <span className="font-bold tabular-nums text-emerald-800 dark:text-emerald-300">
                              {formatMoneyUsd2(Number(row.claim_amount) || 0)}
                            </span>
                          </p>
                          <p className="mt-1 text-[10px] text-muted-foreground">
                            {formatDate(row.created_at)}
                            {row.submission_id ? (
                              <span className="ml-2 font-mono text-slate-600 dark:text-slate-400">Case: {row.submission_id}</span>
                            ) : null}
                            {typeof row.success_probability === "number" && !Number.isNaN(row.success_probability) ? (
                              <span className="ml-2 text-violet-600 dark:text-violet-400">
                                P(success): {row.success_probability.toFixed(0)}%
                              </span>
                            ) : null}
                          </p>
                        </div>
                      </div>
                      <div className="flex flex-wrap items-center gap-2 sm:justify-end">
                        <span
                          className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-medium ${
                            SUBMISSION_STATUS_STYLES[row.status] ?? SUBMISSION_STATUS_STYLES.draft
                          }`}
                        >
                          {row.status.replace(/_/g, " ")}
                        </span>
                        <button
                          type="button"
                          onClick={() => openClaimGenerationForRow(row)}
                          className="inline-flex items-center gap-1.5 rounded-lg border border-emerald-300 bg-emerald-50 px-2.5 py-1.5 text-[11px] font-semibold text-emerald-900 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-100"
                        >
                          <FileText className="h-3.5 w-3.5" />
                          Generate PDF
                        </button>
                        <Link
                          href={`/claim-engine/investigation/${row.id}`}
                          className="inline-flex items-center gap-1.5 rounded-lg border border-violet-200 bg-violet-50 px-2.5 py-1.5 text-[11px] font-semibold text-violet-900 dark:border-violet-800 dark:bg-violet-950/40 dark:text-violet-100"
                        >
                          <MessageSquare className="h-3.5 w-3.5" />
                          Investigate
                        </Link>
                        <button
                          type="button"
                          onClick={() => setHistorySubmissionId(row.id)}
                          className="inline-flex items-center gap-1.5 rounded-lg border border-slate-200 bg-white px-2.5 py-1.5 text-[11px] font-semibold text-slate-800 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                        >
                          <HistoryIcon className="h-3.5 w-3.5" />
                          History
                        </button>
                        <button
                          type="button"
                          disabled={queueBusyId === row.id || !row.report_url}
                          onClick={() => void handlePreview(row)}
                          className="inline-flex items-center gap-1.5 rounded-lg border border-slate-200 bg-white px-2.5 py-1.5 text-[11px] font-semibold text-slate-800 disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                        >
                          {queueBusyId === row.id ? (
                            <Loader2 className="h-3.5 w-3.5 animate-spin" />
                          ) : (
                            <Eye className="h-3.5 w-3.5" />
                          )}
                          Preview
                        </button>
                        <button
                          type="button"
                          disabled={queueBusyId === row.id}
                          onClick={() => void handleManualSubmit(row)}
                          className="inline-flex items-center gap-1.5 rounded-lg border border-emerald-200 bg-emerald-50 px-2.5 py-1.5 text-[11px] font-semibold text-emerald-900 disabled:opacity-50 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-100"
                        >
                          Manual submit
                        </button>
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </section>
          )}
        </div>
      </main>
    </>
  );
}
