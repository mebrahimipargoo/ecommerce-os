"use client";

import { pdf } from "@react-pdf/renderer";
import type { CoreSettings } from "../settings/workspace-settings-types";
import type { ClaimDetailPayload } from "./claim-actions";
import { BulkClaimsPdfDocument, SingleClaimPdfDocument, type BulkClaimPdfPageInput } from "./claim-pdf-document";
import { fetchEvidenceImagesForPdf } from "./claim-pdf-fetch-image";
import {
  buildClaimEvidenceSlots,
  initialSlotSelection,
  mergeDefaultClaimEvidence,
  type ClaimEvidenceKey,
} from "./claim-evidence-settings";

export async function downloadSingleClaimPdf(opts: {
  tenant: CoreSettings;
  storeName: string;
  storePlatform: string;
  detail: ClaimDetailPayload;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
  filename?: string;
}): Promise<void> {
  const slots = buildClaimEvidenceSlots(opts.detail);
  const evidenceImages =
    slots.length > 0
      ? await fetchEvidenceImagesForPdf(slots.map((s) => ({ label: s.label, url: s.url })))
      : [];
  const blob = await pdf(
    <SingleClaimPdfDocument
      tenant={opts.tenant}
      storeName={opts.storeName}
      storePlatform={opts.storePlatform}
      detail={opts.detail}
      claimAmountNote={opts.claimAmountNote}
      marketplaceClaimIdNote={opts.marketplaceClaimIdNote}
      evidenceImages={evidenceImages}
    />,
  ).toBlob();
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = opts.filename ?? `claim-${opts.detail.claim.id}.pdf`;
  a.click();
  URL.revokeObjectURL(a.href);
}

/**
 * Enterprise claim PDF: cover letter + embedded labeled images (fetched client-side to data URIs).
 */
export async function buildEnterpriseClaimPdfBlob(opts: {
  tenant: CoreSettings;
  storeName: string;
  storePlatform: string;
  detail: ClaimDetailPayload;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
  evidenceSlots: { label: string; url: string }[];
}): Promise<Blob> {
  const evidenceImages = await fetchEvidenceImagesForPdf(opts.evidenceSlots);
  return pdf(
    <SingleClaimPdfDocument
      tenant={opts.tenant}
      storeName={opts.storeName}
      storePlatform={opts.storePlatform}
      detail={opts.detail}
      claimAmountNote={opts.claimAmountNote}
      marketplaceClaimIdNote={opts.marketplaceClaimIdNote}
      evidenceImages={evidenceImages}
    />,
  ).toBlob();
}

export async function downloadEnterpriseClaimPdf(opts: {
  tenant: CoreSettings;
  storeName: string;
  storePlatform: string;
  detail: ClaimDetailPayload;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
  /** Selected evidence — label + source URL (fetched and embedded). */
  evidenceSlots: { label: string; url: string }[];
  filename?: string;
}): Promise<void> {
  const blob = await buildEnterpriseClaimPdfBlob(opts);
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = opts.filename ?? `claim-${opts.detail.claim.id}.pdf`;
  a.click();
  URL.revokeObjectURL(a.href);
}

export type BulkClaimPdfPage = BulkClaimPdfPageInput;

/** Uses org default checklist to pick which inherited photos to embed for each bulk page. */
export async function enrichBulkPagesWithDefaultEvidence(
  pages: BulkClaimPdfPage[],
  defaultClaimEvidence: Partial<Record<ClaimEvidenceKey, boolean>> | null | undefined,
): Promise<BulkClaimPdfPage[]> {
  const merged = mergeDefaultClaimEvidence(defaultClaimEvidence);
  const out: BulkClaimPdfPage[] = [];
  for (const p of pages) {
    const slots = buildClaimEvidenceSlots(p.detail);
    const sel = initialSlotSelection(slots, merged);
    const chosen = slots.filter((s) => sel[s.id]);
    const evidenceImages = await fetchEvidenceImagesForPdf(chosen.map((s) => ({ label: s.label, url: s.url })));
    out.push({ ...p, evidenceImages });
  }
  return out;
}

export async function downloadBulkClaimsPdf(opts: {
  tenant: CoreSettings;
  pages: BulkClaimPdfPage[];
  filename?: string;
  reportKind?: "master" | "batch";
}): Promise<void> {
  if (opts.pages.length === 0) return;
  const blob = await pdf(
    <BulkClaimsPdfDocument tenant={opts.tenant} pages={opts.pages} reportKind={opts.reportKind} />,
  ).toBlob();
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = opts.filename ?? `claims-bulk-${Date.now()}.pdf`;
  a.click();
  URL.revokeObjectURL(a.href);
}

/** Opens generated PDF in a new tab for print/preview (Claim Engine). */
export async function openBulkClaimsPdfInNewTab(opts: {
  tenant: CoreSettings;
  pages: BulkClaimPdfPage[];
  filename?: string;
  reportKind?: "master" | "batch";
}): Promise<void> {
  if (opts.pages.length === 0) return;
  const blob = await pdf(
    <BulkClaimsPdfDocument tenant={opts.tenant} pages={opts.pages} reportKind={opts.reportKind} />,
  ).toBlob();
  const url = URL.createObjectURL(blob);
  window.open(url, "_blank", "noopener,noreferrer");
  window.setTimeout(() => URL.revokeObjectURL(url), 120_000);
}
