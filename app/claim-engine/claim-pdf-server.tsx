import "server-only";

import React from "react";
import { pdf } from "@react-pdf/renderer";
import type { CoreSettings } from "../settings/workspace-settings-types";
import type { ClaimDetailPayload } from "./claim-actions";
import { SingleClaimPdfDocument } from "./claim-pdf-document";

/** Normalizes workspace JSON keys so PDFs always pick up white-label branding. */
export function resolveCoreSettingsForPdf(core: CoreSettings): CoreSettings {
  const logo =
    (typeof core.company_logo_url === "string" && core.company_logo_url) ||
    (typeof core.logo_url === "string" && core.logo_url) ||
    "";
  const name =
    (typeof core.company_name === "string" && core.company_name.trim()) ||
    (typeof core.workspace_name === "string" && core.workspace_name.trim()) ||
    "";
  return {
    ...core,
    company_logo_url: logo,
    company_name: name,
  };
}

export async function renderSingleClaimPdfBuffer(opts: {
  tenant: CoreSettings;
  storeName: string;
  storePlatform: string;
  detail: ClaimDetailPayload;
  claimAmountNote?: string;
  marketplaceClaimIdNote?: string;
}): Promise<Buffer> {
  const tenant = resolveCoreSettingsForPdf(opts.tenant);
  const instance = pdf(
    <SingleClaimPdfDocument
      tenant={tenant}
      storeName={opts.storeName}
      storePlatform={opts.storePlatform}
      detail={opts.detail}
      claimAmountNote={opts.claimAmountNote}
      marketplaceClaimIdNote={opts.marketplaceClaimIdNote}
      evidenceImages={[]}
    />,
  );
  const stream = await instance.toBuffer();
  const ab = await new Response(stream as unknown as BodyInit).arrayBuffer();
  return Buffer.from(ab);
}
