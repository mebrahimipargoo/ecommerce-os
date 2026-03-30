"use client";

import { useEffect, useMemo, useState } from "react";
import { Loader2, X } from "lucide-react";
import type { CoreSettings } from "../settings/workspace-settings-types";
import type { ClaimDetailPayload } from "./claim-actions";
import { getClaimDetail } from "./claim-actions";
import {
  buildClaimEvidenceSlots,
  initialSlotSelection,
  mergeDefaultClaimEvidence,
  type ClaimEvidenceKey,
  type ClaimEvidenceSlot,
} from "./claim-evidence-settings";
import { downloadEnterpriseClaimPdf } from "./claim-pdf-download";

type StoreRow = { id: string; name: string; platform: string };

function resolveStoreFromDetail(detail: ClaimDetailPayload, stores: StoreRow[]): StoreRow | null {
  const sid = detail.claim.store_id;
  if (!sid) return null;
  return stores.find((s) => s.id === sid) ?? null;
}

export function ClaimGenerationModal({
  open,
  onClose,
  submissionId,
  organizationId,
  coreSettings,
  stores,
  defaultClaimEvidence,
  claimAmountNote,
  marketplaceClaimIdNote,
  onToast,
}: {
  open: boolean;
  onClose: () => void;
  submissionId: string | null;
  organizationId: string;
  coreSettings: CoreSettings;
  stores: StoreRow[];
  defaultClaimEvidence: Record<ClaimEvidenceKey, boolean>;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
  onToast: (msg: string, kind?: "success" | "error" | "warning") => void;
}) {
  const [loading, setLoading] = useState(false);
  const [detail, setDetail] = useState<ClaimDetailPayload | null>(null);
  const [busy, setBusy] = useState(false);
  const [selection, setSelection] = useState<Record<string, boolean>>({});

  const mergedDefaults = useMemo(() => mergeDefaultClaimEvidence(defaultClaimEvidence), [defaultClaimEvidence]);

  useEffect(() => {
    if (!open || !submissionId) {
      setDetail(null);
      setSelection({});
      return;
    }
    let cancelled = false;
    setLoading(true);
    void (async () => {
      const res = await getClaimDetail(submissionId, organizationId);
      if (cancelled) return;
      setLoading(false);
      if (res.ok && res.data) {
        setDetail(res.data);
        const slots = buildClaimEvidenceSlots(res.data);
        setSelection(initialSlotSelection(slots, mergedDefaults));
      } else {
        onToast(res.error ?? "Failed to load claim", "error");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [open, submissionId, organizationId, onToast, mergedDefaults]);

  const slots: ClaimEvidenceSlot[] = useMemo(() => (detail ? buildClaimEvidenceSlots(detail) : []), [detail]);

  function toggleSlot(id: string) {
    setSelection((prev) => ({ ...prev, [id]: !prev[id] }));
  }

  async function handleConfirm() {
    if (!detail) return;
    const chosen = slots.filter((s) => selection[s.id]);
    if (chosen.length === 0) {
      onToast("Select at least one photo to include, or cancel.", "warning");
      return;
    }
    setBusy(true);
    try {
      const st = resolveStoreFromDetail(detail, stores);
      const storeName = st?.name ?? "Store";
      const storePlatform = st?.platform ?? "amazon";
      await downloadEnterpriseClaimPdf({
        tenant: coreSettings,
        storeName,
        storePlatform,
        detail,
        claimAmountNote,
        marketplaceClaimIdNote,
        evidenceSlots: chosen.map((s) => ({ label: s.label, url: s.url })),
      });
      onToast("PDF downloaded", "success");
      onClose();
    } catch {
      onToast("PDF generation failed", "error");
    } finally {
      setBusy(false);
    }
  }

  if (!open || !submissionId) return null;

  return (
    <div className="fixed inset-0 z-[480] flex items-end justify-center bg-black/50 p-4 sm:items-center">
      <div
        role="dialog"
        aria-modal="true"
        className="flex max-h-[92vh] w-full max-w-2xl flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl dark:border-slate-800 dark:bg-slate-950"
      >
        <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3 dark:border-slate-800">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-slate-400">Generate claim PDF</p>
            <p className="font-mono text-sm font-bold text-slate-900 dark:text-slate-100">{submissionId}</p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg p-2 text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-900"
            aria-label="Close"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-4 py-4">
          {loading ? (
            <div className="flex justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-sky-500" />
            </div>
          ) : slots.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No inherited photos found for this submission (pallet, package, or item). Add photos in Returns intake, then try again.
            </p>
          ) : (
            <div className="space-y-4">
              <p className="text-xs text-muted-foreground">
                Boxes are pre-checked from <span className="font-semibold text-foreground">System Settings → Default Claim Evidence</span>.
                Adjust for this report only, then confirm.
              </p>
              <ul className="grid gap-3 sm:grid-cols-2">
                {slots.map((s) => (
                  <li
                    key={s.id}
                    className="flex gap-3 rounded-xl border border-slate-200 p-2 dark:border-slate-800"
                  >
                    <label className="flex min-w-0 flex-1 cursor-pointer gap-3">
                      <input
                        type="checkbox"
                        className="mt-1 h-4 w-4 shrink-0 rounded border-slate-300 text-sky-600"
                        checked={selection[s.id] ?? false}
                        onChange={() => toggleSlot(s.id)}
                      />
                      <span className="min-w-0 flex-1">
                        <span className="text-[10px] font-bold uppercase tracking-wide text-slate-400">{s.scope}</span>
                        <span className="mt-0.5 block text-sm font-semibold text-slate-900 dark:text-slate-100">{s.label}</span>
                        {/* eslint-disable-next-line @next/next/no-img-element */}
                        <img
                          src={s.url}
                          alt=""
                          className="mt-2 h-24 w-full rounded-lg border border-slate-200 object-cover dark:border-slate-700"
                        />
                      </span>
                    </label>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>

        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-slate-200 px-4 py-3 dark:border-slate-800">
          <button
            type="button"
            onClick={onClose}
            className="rounded-xl border border-slate-200 px-4 py-2 text-sm font-medium dark:border-slate-700"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={busy || loading || !detail || slots.length === 0}
            onClick={() => void handleConfirm()}
            className="rounded-xl bg-emerald-600 px-4 py-2 text-sm font-bold text-white disabled:opacity-50"
          >
            {busy ? (
              <>
                <Loader2 className="mr-2 inline h-4 w-4 animate-spin" />
                Generating…
              </>
            ) : (
              "Confirm & generate"
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
