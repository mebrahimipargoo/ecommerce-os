"use client";

import { pdf } from "@react-pdf/renderer";
import type { CoreSettings } from "../settings/workspace-settings-types";
import type { ClaimDetailPayload } from "./claim-actions";
import { BulkClaimsPdfDocument, SingleClaimPdfDocument } from "./claim-pdf-document";

export async function downloadSingleClaimPdf(opts: {
  tenant: CoreSettings;
  storeName: string;
  storePlatform: string;
  detail: ClaimDetailPayload;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
  filename?: string;
}): Promise<void> {
  const blob = await pdf(
    <SingleClaimPdfDocument
      tenant={opts.tenant}
      storeName={opts.storeName}
      storePlatform={opts.storePlatform}
      detail={opts.detail}
      claimAmountNote={opts.claimAmountNote}
      marketplaceClaimIdNote={opts.marketplaceClaimIdNote}
    />,
  ).toBlob();
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = opts.filename ?? `claim-${opts.detail.claim.id}.pdf`;
  a.click();
  URL.revokeObjectURL(a.href);
}

export type BulkClaimPdfPage = {
  storeName: string;
  storePlatform: string;
  detail: ClaimDetailPayload;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
};

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
