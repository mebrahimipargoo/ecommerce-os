import { NextResponse } from "next/server";

import {
  buildColumnMappingFromHeaders,
  classifyCsvHeadersRuleBased,
  mappingHasRequiredGaps,
} from "../../../../../lib/csv-import-detected-type";
import { classifyImportHeadersWithGpt } from "../../../../../lib/classify-import-headers-openai";
import { resolveWriteOrganizationId } from "../../../../../lib/server-tenant";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";
import type { RawReportType } from "../../../../../lib/raw-report-types";
import {
  contentSuggestsReportsRepositorySample,
  fileNameSuggestsReportsRepository,
} from "../../../../../lib/reports-repository-header";

export const runtime = "nodejs";

type Body = {
  headers?: unknown;
  actor_user_id?: unknown;
  /** Original file name — if it contains "Reports Repository", classification favors REPORTS_REPOSITORY. */
  file_name?: unknown;
  /** First ~64KB of file text — detects Reports Repository preamble / header line hints. */
  content_sample?: unknown;
};

/**
 * When report_type is SAFET_CLAIMS, fill any gaps in `existing` mapping by
 * fuzzy-matching the actual CSV headers against hard-coded SAFE-T alias lists.
 *
 * Normalization: lowercase + replace hyphens/underscores/punctuation with a
 * single space, then trim.  This makes "SAFE-T Claim ID", "safe_t_claim_id",
 * and "safe t claim id" all compare equal to the alias "safe t claim id".
 */
function normH(h: string): string {
  return h.toLowerCase().replace(/[-_]/g, " ").replace(/[^a-z0-9\s]/g, "").replace(/\s+/g, " ").trim();
}

const SAFET_FALLBACK_ALIASES: Record<string, string[]> = {
  // DB canonical  ← CSV alias variants (order matters — more specific aliases first)
  claim_id:     ["safe t claim id", "safet claim id", "safe-t claim id", "claim id", "claim-id"],
  // "Reimbursement Amount" or bare "Amount" both map to the `amount` column
  amount:       ["reimbursement amount", "reimbursement-amount", "reimburse amount", "amount"],
  asin:         ["asin"],
  order_id:     ["order id", "order-id", "amazon order id", "amazon-order-id"],
  // "Claim Status" OR bare "Status" map to `claim_status` — AI commonly skips these
  claim_status: ["claim status", "claim-status", "status"],
};

/**
 * Hard-coded mapping fallbacks for the TRANSACTIONS report (flat file TSV format).
 *
 * Amazon's Reports Repository Transactions .txt file uses abbreviated column names
 * that differ from the classic Fee Preview format:
 *
 *   "type"     → transaction_type  (instead of "transaction-type")
 *   "order-id" → order_id          (standard)
 *   "total"    → amount            (instead of "amount" or "total-product-charges")
 *   "date/time"→ posted_date
 */
const TRANSACTIONS_FALLBACK_ALIASES: Record<string, string[]> = {
  settlement_id:           ["settlement-id", "settlement id", "Settlement ID"],
  transaction_type:        ["transaction-type", "transaction type", "type"],
  order_id:                ["order-id", "order id", "amazon-order-id", "amazon order id"],
  amount:                  ["amount", "Amount", "total", "total-amount", "total amount", "price-amount", "price amount"],
  total_product_charges:   ["total-product-charges", "total product charges"],
  posted_date:             ["date/time", "date-time", "posted-date", "posted date"],
  sku:                     ["sku", "SKU"],
};

/** Gap-fill for Reports Repository transaction CSV (lowercase headers, column "type"). */
const REPORTS_REPOSITORY_FALLBACK_ALIASES: Record<string, string[]> = {
  date_time:         ["date/time", "date-time", "datetime", "posted-date", "posted date"],
  settlement_id:     ["settlement-id", "settlement id", "Settlement ID"],
  transaction_type:  ["type", "transaction-type", "transaction type"],
  order_id:          ["order-id", "order id", "amazon-order-id", "amazon order id"],
  sku:               ["sku", "SKU", "merchant-sku", "msku"],
  description:       ["description", "Description"],
  total_amount:      ["total", "Total", "total-amount", "total amount"],
};

const REMOVAL_SHIPMENT_FALLBACK_ALIASES: Record<string, string[]> = {
  order_id:      ["removal-order-id", "order-id", "order_id", "removal_order_id"],
  sku:           ["sku"],
  tracking_number: ["tracking-number", "tracking_number", "tracking-id", "tracking id"],
  carrier:       ["carrier", "carrier-name", "carrier name"],
  shipment_date: ["carrier-shipment-date", "shipment-date", "ship-date", "shipped-date", "shipment date"],
};

function applyRemovalShipmentFallbackMapping(
  headers: string[],
  existing: Record<string, string>,
): Record<string, string> {
  const result = { ...existing };
  for (const [canonical, aliases] of Object.entries(REMOVAL_SHIPMENT_FALLBACK_ALIASES)) {
    if (result[canonical]) continue;
    const normalizedAliases = aliases.map(normH);
    for (const header of headers) {
      if (normalizedAliases.includes(normH(header))) {
        result[canonical] = header;
        break;
      }
    }
  }
  return result;
}

/** Gap-fill for listing exports (seller-sku / asin1 column spellings). */
const LISTING_FALLBACK_ALIASES: Record<string, string[]> = {
  seller_sku: ["seller-sku", "seller sku", "seller_sku", "sku", "SKU"],
  asin: ["asin1", "asin1-value", "asin", "ASIN", "product-id", "product id"],
};

function applyListingFallbackMapping(
  headers: string[],
  existing: Record<string, string>,
): Record<string, string> {
  const result = { ...existing };
  for (const [canonical, aliases] of Object.entries(LISTING_FALLBACK_ALIASES)) {
    if (result[canonical]) continue;
    const normalizedAliases = aliases.map(normH);
    for (const header of headers) {
      if (normalizedAliases.includes(normH(header))) {
        result[canonical] = header;
        break;
      }
    }
  }
  return result;
}

const SETTLEMENT_FLAT_FALLBACK_ALIASES: Record<string, string[]> = {
  settlement_id:         ["settlement-id", "settlement id", "Settlement ID"],
  settlement_start_date: ["settlement-start-date", "settlement start date"],
  settlement_end_date:   ["settlement-end-date", "settlement end date"],
  deposit_date:          ["deposit-date", "deposit date"],
  total_amount:          ["total-amount", "total amount"],
  currency:              ["currency", "Currency"],
  transaction_status:    ["transaction-type", "transaction type", "transaction-status", "transaction status"],
  order_id:              ["order-id", "order id", "amazon-order-id", "amazon order id"],
};

function applySafeTFallbackMapping(
  headers: string[],
  existing: Record<string, string>,
): Record<string, string> {
  const result = { ...existing };
  for (const [canonical, aliases] of Object.entries(SAFET_FALLBACK_ALIASES)) {
    if (result[canonical]) continue; // already mapped — don't overwrite
    const normalizedAliases = aliases.map(normH);
    for (const header of headers) {
      if (normalizedAliases.includes(normH(header))) {
        result[canonical] = header;
        break;
      }
    }
  }
  return result;
}

/**
 * Fills mapping gaps for the TRANSACTIONS report using the flat-file alias table.
 * Runs after rule-based + AI mapping so it only fills what neither step resolved.
 */
function applyTransactionsFallbackMapping(
  headers: string[],
  existing: Record<string, string>,
): Record<string, string> {
  const result = { ...existing };
  for (const [canonical, aliases] of Object.entries(TRANSACTIONS_FALLBACK_ALIASES)) {
    if (result[canonical]) continue;
    const normalizedAliases = aliases.map(normH);
    for (const header of headers) {
      if (normalizedAliases.includes(normH(header))) {
        result[canonical] = header;
        break;
      }
    }
  }
  return result;
}

function applySettlementFlatFallbackMapping(
  headers: string[],
  existing: Record<string, string>,
): Record<string, string> {
  const result = { ...existing };
  for (const [canonical, aliases] of Object.entries(SETTLEMENT_FLAT_FALLBACK_ALIASES)) {
    if (result[canonical]) continue;
    const normalizedAliases = aliases.map(normH);
    for (const header of headers) {
      if (normalizedAliases.includes(normH(header))) {
        result[canonical] = header;
        break;
      }
    }
  }
  return result;
}

function applyReportsRepositoryFallbackMapping(
  headers: string[],
  existing: Record<string, string>,
): Record<string, string> {
  const result = { ...existing };
  for (const [canonical, aliases] of Object.entries(REPORTS_REPOSITORY_FALLBACK_ALIASES)) {
    if (result[canonical]) continue;
    const normalizedAliases = aliases.map(normH);
    for (const header of headers) {
      if (normalizedAliases.includes(normH(header))) {
        result[canonical] = header;
        break;
      }
    }
  }
  return result;
}

/**
 * POST /api/settings/imports/classify-headers
 *
 * Mapping pipeline — 4 steps in priority order:
 *
 * 0. MAPPING MEMORY: Look up a prior successful mapping for this exact set of
 *    headers (matched via sorted fingerprint). If found, reuse it instantly —
 *    no rule-based or AI pass needed.
 * 1. Rule-based classification from known Amazon header slugs.
 * 2. If rules return UNKNOWN, call OpenAI gpt-4o-mini which returns BOTH the
 *    report type AND a full column_mapping JSON.
 * 3. Merge rule-based alias mapping with AI mapping (AI wins on conflicts).
 * 4. Set needs_mapping=true if type is still UNKNOWN or required fields are missing.
 */
export async function POST(req: Request): Promise<Response> {
  try {
    const body = (await req.json()) as Body;
    const headersRaw = body.headers;
    const actor =
      typeof body.actor_user_id === "string" && isUuidString(body.actor_user_id.trim())
        ? body.actor_user_id.trim()
        : null;

    const headers = Array.isArray(headersRaw)
      ? headersRaw.map((h) => String(h ?? "").trim()).filter(Boolean)
      : [];

    if (headers.length === 0) {
      return NextResponse.json({ ok: false, error: "Missing headers array." }, { status: 400 });
    }

    const fileName = typeof body.file_name === "string" ? body.file_name.trim() : "";
    const contentSample = typeof body.content_sample === "string" ? body.content_sample.slice(0, 65536) : "";

    const orgId = await resolveWriteOrganizationId(actor, null);
    if (!isUuidString(orgId)) {
      return NextResponse.json({ ok: false, error: "Invalid organization scope." }, { status: 400 });
    }

    // Human-readable labels for rule-based matches (no GPT needed)
    const REPORT_TYPE_HUMAN_LABELS: Record<string, string> = {
      FBA_RETURNS:        "Amazon FBA Returns Report",
      REMOVAL_ORDER:      "Amazon Removal Order Detail",
      REMOVAL_SHIPMENT:   "Amazon Removal Shipment Detail",
      INVENTORY_LEDGER:   "Amazon Inventory Ledger",
      REIMBURSEMENTS:     "Amazon Reimbursements Report",
      SETTLEMENT:         "Amazon Settlement Report",
      SAFET_CLAIMS:       "Amazon SAFE-T Claims Report",
      TRANSACTIONS:       "Amazon Transactions Report",
      REPORTS_REPOSITORY: "Amazon Reports Repository Export",
      CATEGORY_LISTINGS:  "Amazon Category Listings Report",
      ALL_LISTINGS:       "Amazon All Listings Report",
      ACTIVE_LISTINGS:    "Amazon Active Listings Report",
    };

    // ── Step 1: Rule-based classification (ALWAYS runs first) ─────────────────
    // Rules are deterministic and must take priority over any cached mapping —
    // a past wrong mapping should never override a clear rule match.
    const rules = classifyCsvHeadersRuleBased(headers);
    let reportType: RawReportType = rules.reportType;
    let aiColumnMapping: Record<string, string> = {};
    let source: "memory" | "rules" | "gpt" | "rules+gpt" | "filename" | "content_sample" = "rules";
    let detectedFileType: string = REPORT_TYPE_HUMAN_LABELS[reportType] ?? reportType;
    let isSupported: boolean = (reportType as string) !== "UNKNOWN";
    let aiMessage: string = "";

    // ── Step 0: MAPPING MEMORY — only when rules say UNKNOWN ──────────────────
    // Compute a deterministic fingerprint from the sorted lowercase header list.
    // Query the org's history for a prior synced/mapped upload with the same
    // fingerprint — if found, reuse its report_type and column_mapping.
    if ((reportType as string) === "UNKNOWN") {
      const fingerprint = headers
        .map((h) => h.trim().toLowerCase())
        .sort()
        .join("|");

      const { data: memoryRow } = await supabaseServer
        .from("raw_report_uploads")
        .select("report_type, column_mapping")
        .eq("organization_id", orgId)
        .in("status", ["synced", "mapped"])
        .not("column_mapping", "is", null)
        .filter("metadata->>headers_fingerprint", "eq", fingerprint)
        .order("updated_at", { ascending: false })
        .limit(1)
        .maybeSingle();

      if (
        memoryRow?.column_mapping &&
        memoryRow?.report_type &&
        (memoryRow.report_type as string) !== "UNKNOWN"
      ) {
        console.info(
          "[classify-headers] Mapping Memory hit (rules=UNKNOWN) — pre-filling prior mapping for fingerprint:",
          fingerprint.slice(0, 60),
        );
        const memType = memoryRow.report_type as string;
        return NextResponse.json({
          ok: true,
          report_type: memType as RawReportType,
          column_mapping: memoryRow.column_mapping as Record<string, string>,
          needs_mapping: true,
          rule: "memory",
          source: "memory",
          detected_file_type: REPORT_TYPE_HUMAN_LABELS[memType] ?? memType,
          is_supported: memType !== "UNKNOWN",
          message: `Previously recognized as ${REPORT_TYPE_HUMAN_LABELS[memType] ?? memType}.`,
        });
      }
    }

    // ── Step 2: AI fallback when rules return UNKNOWN ─────────────────────────
    if ((reportType as string) === "UNKNOWN") {
      const gptResult = await classifyImportHeadersWithGpt({ organizationId: orgId, headers });
      reportType = gptResult.reportType;
      aiColumnMapping = gptResult.columnMapping;
      detectedFileType = gptResult.detectedFileType;
      isSupported = gptResult.isSupported;
      aiMessage = gptResult.message;
      source = "gpt";
    }

    // ── Step 3: Build rule-based alias mapping for the resolved type ───────────
    const ruleMapping: Record<string, string> =
      (reportType as string) !== "UNKNOWN"
        ? buildColumnMappingFromHeaders(headers, reportType)
        : {};

    // Merge: rule-based fills gaps; AI mapping wins when both have a key
    const merged: Record<string, string> = { ...ruleMapping, ...aiColumnMapping };
    if (rules.reportType !== "UNKNOWN" && Object.keys(aiColumnMapping).length > 0) {
      source = "rules+gpt";
    }

    // ── Step 3.5: Hard-coded fallback mappings per report type ────────────────
    // When required canonical fields are still unmapped after rule + AI passes,
    // scan the actual CSV headers against well-known alias lists and fill gaps.
    let column_mapping = merged;
    if ((reportType as string) === "REMOVAL_SHIPMENT") {
      column_mapping = applyRemovalShipmentFallbackMapping(headers, merged);
    } else if ((reportType as string) === "SAFET_CLAIMS") {
      column_mapping = applySafeTFallbackMapping(headers, merged);
    } else if ((reportType as string) === "SETTLEMENT") {
      column_mapping = applySettlementFlatFallbackMapping(headers, merged);
    } else if ((reportType as string) === "TRANSACTIONS") {
      column_mapping = applyTransactionsFallbackMapping(headers, merged);
    } else if ((reportType as string) === "REPORTS_REPOSITORY") {
      column_mapping = applyReportsRepositoryFallbackMapping(headers, merged);
    } else if (
      (reportType as string) === "CATEGORY_LISTINGS" ||
      (reportType as string) === "ALL_LISTINGS" ||
      (reportType as string) === "ACTIVE_LISTINGS"
    ) {
      column_mapping = applyListingFallbackMapping(headers, merged);
    }

    // ── Step 3.6: Filename / content — do not mis-file Reports Repository as Settlement ─
    if (fileNameSuggestsReportsRepository(fileName)) {
      reportType = "REPORTS_REPOSITORY";
      source = "filename";
      column_mapping = applyReportsRepositoryFallbackMapping(
        headers,
        { ...buildColumnMappingFromHeaders(headers, "REPORTS_REPOSITORY"), ...column_mapping },
      );
    } else if (
      contentSample.length > 0 &&
      contentSuggestsReportsRepositorySample(contentSample) &&
      (reportType === "SETTLEMENT" || reportType === "TRANSACTIONS" || reportType === "UNKNOWN")
    ) {
      reportType = "REPORTS_REPOSITORY";
      source = "content_sample";
      column_mapping = applyReportsRepositoryFallbackMapping(
        headers,
        { ...buildColumnMappingFromHeaders(headers, "REPORTS_REPOSITORY"), ...column_mapping },
      );
    }

    // Filename: "All+Listings+Report" / "All Listings" → prefer ALL_LISTINGS over ACTIVE when both match headers
    const fileNorm = fileName.replace(/\+/g, " ").toLowerCase();
    if (
      fileNorm.length > 0 &&
      /\ball\s+listings\b/.test(fileNorm) &&
      (reportType === "ACTIVE_LISTINGS" || reportType === "UNKNOWN")
    ) {
      const ds = new Set(
        headers.map((h) =>
          h
            .toLowerCase()
            .replace(/[-_]/g, " ")
            .replace(/\s+/g, " ")
            .trim(),
        ),
      );
      if (ds.has("seller sku") && (ds.has("asin1") || ds.has("asin"))) {
        reportType = "ALL_LISTINGS";
        source = "filename";
        isSupported = true;
        column_mapping = applyListingFallbackMapping(
          headers,
          {
            ...buildColumnMappingFromHeaders(headers, "ALL_LISTINGS"),
            ...column_mapping,
          },
        );
      }
    }

    // ── Step 4: Determine if manual intervention is required ───────────────────
    // Update is_supported + detectedFileType if reportType was remapped by filename/content rules
    if ((reportType as string) !== "UNKNOWN") {
      isSupported = true;
      if (!detectedFileType || detectedFileType === "Unknown File" || detectedFileType === "UNKNOWN") {
        detectedFileType = REPORT_TYPE_HUMAN_LABELS[reportType as string] ?? reportType;
      }
    }

    const needs_mapping =
      (reportType as string) === "UNKNOWN" ||
      ((reportType as string) !== "UNKNOWN" && mappingHasRequiredGaps(column_mapping, reportType));

    return NextResponse.json({
      ok: true,
      report_type: reportType,
      column_mapping,
      needs_mapping,
      rule: rules.matchedRule,
      source,
      detected_file_type: detectedFileType,
      is_supported: isSupported,
      message: aiMessage || (isSupported
        ? `Recognized as ${detectedFileType}.`
        : `This file was identified as "${detectedFileType}" but is not yet supported.`
      ),
    });
  } catch (e) {
    return NextResponse.json(
      { ok: false, error: e instanceof Error ? e.message : "Classification failed." },
      { status: 500 },
    );
  }
}
