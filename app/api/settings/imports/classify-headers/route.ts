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

export const runtime = "nodejs";

type Body = { headers?: unknown; actor_user_id?: unknown };

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
  transaction_type:        ["transaction-type", "transaction type", "type"],
  order_id:                ["order-id", "order id", "amazon-order-id", "amazon order id"],
  amount:                  ["amount", "Amount", "total"],
  total_product_charges:   ["total-product-charges", "total product charges"],
  posted_date:             ["date/time", "date-time", "posted-date", "posted date"],
  sku:                     ["sku", "SKU"],
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

    const orgId = await resolveWriteOrganizationId(actor, null);
    if (!isUuidString(orgId)) {
      return NextResponse.json({ ok: false, error: "Invalid organization scope." }, { status: 400 });
    }

    // ── Step 1: Rule-based classification (ALWAYS runs first) ─────────────────
    // Rules are deterministic and must take priority over any cached mapping —
    // a past wrong mapping should never override a clear rule match.
    const rules = classifyCsvHeadersRuleBased(headers);
    let reportType: RawReportType = rules.reportType;
    let aiColumnMapping: Record<string, string> = {};
    let source: "memory" | "rules" | "gpt" | "rules+gpt" = "rules";

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
        return NextResponse.json({
          ok: true,
          report_type: memoryRow.report_type as RawReportType,
          column_mapping: memoryRow.column_mapping as Record<string, string>,
          needs_mapping: true,
          rule: "memory",
          source: "memory",
        });
      }
    }

    // ── Step 2: AI fallback when rules return UNKNOWN ─────────────────────────
    if ((reportType as string) === "UNKNOWN") {
      const gptResult = await classifyImportHeadersWithGpt({ organizationId: orgId, headers });
      reportType = gptResult.reportType;
      aiColumnMapping = gptResult.columnMapping;
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
    if ((reportType as string) === "SAFET_CLAIMS") {
      column_mapping = applySafeTFallbackMapping(headers, merged);
    } else if ((reportType as string) === "TRANSACTIONS") {
      column_mapping = applyTransactionsFallbackMapping(headers, merged);
    }

    // ── Step 4: Determine if manual intervention is required ───────────────────
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
    });
  } catch (e) {
    return NextResponse.json(
      { ok: false, error: e instanceof Error ? e.message : "Classification failed." },
      { status: 500 },
    );
  }
}
