"use client";

import { useEffect, useState } from "react";
import { FileDown, Loader2, X } from "lucide-react";
import type { CoreSettings } from "../settings/workspace-settings-types";
import {
  getClaimDetail,
  updateClaimFields,
  type ClaimDetailPayload,
  type ClaimRecord,
} from "./claim-actions";
import { downloadSingleClaimPdf } from "./claim-pdf-download";
import { ReturnIdentifiersColumn } from "../../components/ReturnIdentifiersColumn";
import { InlineCopy } from "../returns/_components";

type StoreRow = { id: string; name: string; platform: string };

function resolveStore(claim: ClaimRecord, stores: StoreRow[]): StoreRow | null {
  if (!claim.store_id) return null;
  return stores.find((s) => s.id === claim.store_id) ?? null;
}

export function ClaimDetailModal({
  open,
  onClose,
  claim,
  coreSettings,
  stores,
  organizationId,
  onToast,
  onUpdated,
}: {
  open: boolean;
  onClose: () => void;
  claim: ClaimRecord | null;
  coreSettings: CoreSettings;
  stores: StoreRow[];
  organizationId: string;
  onToast: (msg: string, kind?: "success" | "error" | "warning") => void;
  onUpdated: () => void;
}) {
  const [loading, setLoading] = useState(false);
  const [detail, setDetail] = useState<ClaimDetailPayload | null>(null);
  const [claimAmount, setClaimAmount] = useState("");
  const [reimbursementAmount, setReimbursementAmount] = useState("");
  const [claimIdField, setClaimIdField] = useState("");
  const [linkStatus, setLinkStatus] = useState("pending");
  const [saving, setSaving] = useState(false);
  const [pdfBusy, setPdfBusy] = useState(false);

  useEffect(() => {
    if (!open || !claim) {
      setDetail(null);
      return;
    }
    let cancelled = false;
    setLoading(true);
    void (async () => {
      const res = await getClaimDetail(claim.id, organizationId);
      if (cancelled) return;
      setLoading(false);
      if (res.ok && res.data) {
        setDetail(res.data);
        setClaimAmount(String(res.data.claim.amount ?? ""));
        const reimb = res.data.claim.reimbursement_amount;
        setReimbursementAmount(reimb != null && reimb !== undefined ? String(reimb) : "");
        setClaimIdField(res.data.claim.marketplace_claim_id ?? "");
        setLinkStatus(res.data.claim.marketplace_link_status ?? "pending");
      } else {
        onToast(res.error ?? "Failed to load claim", "error");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [open, claim, organizationId, onToast]);

  if (!open || !claim) return null;

  const store = resolveStore(claim, stores);
  const storeName = store?.name ?? providerLabel(claim.marketplace_provider);
  const storePlatform = store?.platform ?? "amazon";

  const isSyntheticClaim = claim.id.startsWith("synthetic-return:");

  async function handleSave() {
    if (!claim || isSyntheticClaim) return;
    setSaving(true);
    const amt = parseFloat(claimAmount);
    const reimbStr = reimbursementAmount.trim();
    const reimbParsed = parseFloat(reimbStr);
    const patch: Parameters<typeof updateClaimFields>[1] = {
      amount: Number.isFinite(amt) ? amt : null,
      marketplace_claim_id: claimIdField.trim() || null,
      marketplace_link_status: linkStatus || null,
    };
    if (claim.status === "accepted") {
      patch.reimbursement_amount =
        reimbStr === "" ? null : Number.isFinite(reimbParsed) ? reimbParsed : null;
    }
    const res = await updateClaimFields(claim.id, patch, organizationId);
    setSaving(false);
    if (res.ok) {
      onToast("Claim saved", "success");
      onUpdated();
    } else onToast(res.error ?? "Save failed", "error");
  }

  async function handleExportPdf() {
    if (!detail) {
      onToast("Load claim details first", "warning");
      return;
    }
    setPdfBusy(true);
    try {
      await downloadSingleClaimPdf({
        tenant: coreSettings,
        storeName,
        storePlatform,
        detail,
        claimAmountNote: claimAmount.trim() || undefined,
        marketplaceClaimIdNote: claimIdField.trim() || undefined,
      });
      onToast("PDF downloaded", "success");
    } catch {
      onToast("PDF export failed", "error");
    } finally {
      setPdfBusy(false);
    }
  }

  const ret = detail?.returnRow;
  const itemName = ret?.item_name ?? claim.item_name ?? "";
  const asin = ret?.asin ?? claim.asin ?? "";
  const fnsku = ret?.fnsku ?? claim.fnsku ?? "";
  const sku = ret?.sku ?? claim.sku ?? "";

  return (
    <div className="fixed inset-0 z-[400] flex items-end justify-center bg-black/50 p-4 sm:items-center">
      <div
        role="dialog"
        aria-modal="true"
        className="flex max-h-[92vh] w-full max-w-2xl flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl dark:border-slate-800 dark:bg-slate-950"
      >
        <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3 dark:border-slate-800">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-slate-400">Claim details</p>
            <p className="font-mono text-sm font-bold text-slate-900 dark:text-slate-100">{claim.id}</p>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => void handleExportPdf()}
              disabled={pdfBusy || loading || !detail}
              className="inline-flex items-center gap-2 rounded-xl border border-sky-200 bg-sky-50 px-3 py-2 text-xs font-semibold text-sky-800 disabled:opacity-50 dark:border-sky-800 dark:bg-sky-950/40 dark:text-sky-200"
            >
              {pdfBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : <FileDown className="h-4 w-4" />}
              Export as Claim PDF
            </button>
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg p-2 text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-900"
              aria-label="Close"
            >
              <X className="h-5 w-5" />
            </button>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto px-4 py-4">
          {loading ? (
            <div className="flex justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-sky-500" />
            </div>
          ) : (
            <div className="space-y-6">
              <section>
                <p className="mb-2 text-xs font-bold uppercase tracking-wide text-slate-500">Identifiers</p>
                <ReturnIdentifiersColumn
                  itemName={itemName || "—"}
                  asin={asin}
                  fnsku={fnsku}
                  sku={sku}
                  storePlatform={storePlatform}
                  onToast={onToast}
                />
              </section>

              <section className="rounded-xl border border-slate-200 p-3 dark:border-slate-800">
                <p className="mb-2 text-xs font-bold uppercase tracking-wide text-slate-500">Pallet &amp; package</p>
                <p className="group text-sm">
                  <span className="text-slate-500">Pallet #</span>{" "}
                  <span className="font-mono font-semibold">{detail?.pallet?.pallet_number ?? "—"}</span>
                  {detail?.pallet?.pallet_number ? (
                    <InlineCopy value={detail.pallet.pallet_number} label="Pallet #" onToast={onToast} className="ml-1 align-middle" />
                  ) : null}
                </p>
                <p className="group mt-1 text-sm">
                  <span className="text-slate-500">Package #</span>{" "}
                  <span className="font-mono font-semibold">{detail?.packageRow?.package_number ?? "—"}</span>
                  {detail?.packageRow?.package_number ? (
                    <InlineCopy value={detail.packageRow.package_number} label="Package #" onToast={onToast} className="ml-1 align-middle" />
                  ) : null}
                </p>
                <p className="group mt-1 text-xs text-muted-foreground">
                  Tracking: <span className="font-mono text-foreground">{detail?.packageRow?.tracking_number ?? "—"}</span>
                  {detail?.packageRow?.tracking_number ? (
                    <InlineCopy value={detail.packageRow.tracking_number} label="Tracking #" onToast={onToast} className="ml-1 align-middle" />
                  ) : null}{" "}
                  · Carrier: {detail?.packageRow?.carrier_name ?? "—"}
                </p>
              </section>

              <section>
                <p className="mb-2 text-xs font-bold uppercase tracking-wide text-slate-500">Photo evidence</p>
                <div className="flex flex-wrap gap-2">
                  {ret?.photo_item_url ? (
                    <a
                      href={ret.photo_item_url}
                      target="_blank"
                      rel="noreferrer"
                      className="block h-24 w-24 overflow-hidden rounded-lg border"
                    >
                      {/* eslint-disable-next-line @next/next/no-img-element */}
                      <img src={ret.photo_item_url} alt="Item" className="h-full w-full object-cover" />
                    </a>
                  ) : null}
                  {ret?.photo_expiry_url ? (
                    <a
                      href={ret.photo_expiry_url}
                      target="_blank"
                      rel="noreferrer"
                      className="block h-24 w-24 overflow-hidden rounded-lg border"
                    >
                      {/* eslint-disable-next-line @next/next/no-img-element */}
                      <img src={ret.photo_expiry_url} alt="Expiry" className="h-full w-full object-cover" />
                    </a>
                  ) : null}
                  {!ret?.photo_item_url && !ret?.photo_expiry_url && (
                    <p className="text-sm text-muted-foreground">No item/expiry photos on the linked return.</p>
                  )}
                </div>
              </section>

              <section className="grid gap-3 sm:grid-cols-2">
                <div>
                  <label className="text-xs font-semibold text-slate-500">Claim amount (requested)</label>
                  <div className="relative mt-1">
                    <span className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-sm text-slate-400">
                      $
                    </span>
                    <input
                      type="text"
                      inputMode="decimal"
                      disabled={isSyntheticClaim}
                      className="w-full rounded-lg border border-slate-200 bg-white py-2 pl-7 pr-3 text-sm dark:border-slate-700 dark:bg-slate-900 disabled:opacity-60"
                      value={claimAmount}
                      onChange={(e) => setClaimAmount(e.target.value)}
                      placeholder="0.00"
                    />
                  </div>
                  {isSyntheticClaim ? (
                    <p className="mt-1 text-[11px] text-muted-foreground">
                      Generate a PDF / submission from the queue to persist this return as a claim row before editing amounts.
                    </p>
                  ) : null}
                </div>
                {claim.status === "accepted" ? (
                  <div>
                    <label className="text-xs font-semibold text-slate-500">Reimbursement received (success)</label>
                    <div className="relative mt-1">
                      <span className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-sm text-slate-400">
                        $
                      </span>
                      <input
                        type="text"
                        inputMode="decimal"
                        className="w-full rounded-lg border border-slate-200 bg-white py-2 pl-7 pr-3 text-sm dark:border-slate-700 dark:bg-slate-900"
                        value={reimbursementAmount}
                        onChange={(e) => setReimbursementAmount(e.target.value)}
                        placeholder="Actual payout"
                      />
                    </div>
                    <p className="mt-1 text-[11px] text-muted-foreground">
                      Record what the marketplace paid vs. the requested claim amount (ROI).
                    </p>
                  </div>
                ) : null}
                <div>
                  <label className="text-xs font-semibold text-slate-500">Marketplace claim ID</label>
                  <input
                    type="text"
                    className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 font-mono text-sm dark:border-slate-700 dark:bg-slate-900"
                    value={claimIdField}
                    onChange={(e) => setClaimIdField(e.target.value)}
                    placeholder="From Seller Central / Walmart"
                  />
                </div>
                <div className="sm:col-span-2">
                  <label className="text-xs font-semibold text-slate-500">Marketplace link status</label>
                  <select
                    className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
                    value={linkStatus}
                    onChange={(e) => setLinkStatus(e.target.value)}
                  >
                    <option value="pending">Pending</option>
                    <option value="verified">Verified</option>
                    <option value="broken">Broken</option>
                    <option value="unknown">Unknown</option>
                  </select>
                </div>
              </section>

              <div className="flex justify-end gap-2 border-t border-slate-200 pt-4 dark:border-slate-800">
                <button
                  type="button"
                  onClick={onClose}
                  className="rounded-xl border border-slate-200 px-4 py-2 text-sm font-medium dark:border-slate-700"
                >
                  Close
                </button>
                <button
                  type="button"
                  onClick={() => void handleSave()}
                  disabled={saving || isSyntheticClaim}
                  className="rounded-xl bg-sky-600 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50"
                >
                  {saving ? "Saving…" : "Save"}
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function providerLabel(raw: string | null): string {
  if (!raw) return "Marketplace";
  const map: Record<string, string> = {
    amazon_sp_api: "Amazon",
    walmart_api: "Walmart",
    ebay_api: "eBay",
  };
  return map[raw] ?? raw;
}
