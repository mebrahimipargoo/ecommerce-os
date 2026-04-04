import "server-only";

import { getOrganizationOpenAIApiKey } from "./organization-openai-key";
import { parseGptReportType } from "./csv-import-detected-type";
import type { RawReportType } from "./raw-report-types";

const OPENAI_CHAT = "https://api.openai.com/v1/chat/completions";

/**
 * Full AI analysis: classify report type AND map CSV headers to canonical schema fields.
 *
 * The model returns a structured JSON object with two keys:
 *   report_type  — one of the canonical types listed in the system prompt
 *   mapping      — { "canonical_key": "exact CSV header string" }
 *
 * Returns UNKNOWN + empty mapping if the API key is missing, the request fails,
 * or the model cannot determine the type with confidence.
 */
export async function classifyImportHeadersWithGpt(input: {
  organizationId: string;
  headers: string[];
}): Promise<{ reportType: RawReportType; columnMapping: Record<string, string> }> {
  const key = await getOrganizationOpenAIApiKey(input.organizationId);
  if (!key) {
    return { reportType: "UNKNOWN", columnMapping: {} };
  }

  const hdr = input.headers.map((h) => h.trim()).filter(Boolean);
  if (hdr.length === 0) return { reportType: "UNKNOWN", columnMapping: {} };

  const systemPrompt = `You are an expert at analyzing Amazon seller CSV exports.
Your job:
1. Identify which Amazon report type these column headers belong to.
2. Map each relevant CSV header to its canonical schema field name.

Report types, definitive header signals, and canonical fields:

- FBA_RETURNS: signals = "license-plate-number" AND "detailed-disposition"
  canonical fields: lpn, detailed_disposition, asin, order_id, return_reason

- REMOVAL_ORDER: signals = "removal-order-id" OR ("requested-quantity" AND "disposed-quantity")
  canonical fields: order_id, sku, fnsku, disposition, shipped_quantity, cancelled_quantity, disposed_quantity, requested_quantity, status

- INVENTORY_LEDGER: signals = "fnsku" AND "ending warehouse balance"
  canonical fields: fnsku, ending_warehouse_balance, title, date, asin

- REIMBURSEMENTS: signals = "reimbursement-id" AND "quantity-reimbursed-total"
  canonical fields: reimbursement_id, quantity_reimbursed_total, order_id, asin, amount_per_unit, approval_date

- SETTLEMENT: signals = "settlement id" AND "transaction status"
  canonical fields: settlement_id, transaction_status, order_id, total, deposit_date

- SAFET_CLAIMS: signals = "safe-t claim id" AND "reimbursement amount"
  canonical fields: claim_id, amount, order_id, claim_status

- TRANSACTIONS: signals = "transaction type" AND "total product charges"
  canonical fields: transaction_type, total_product_charges, order_id, sku, posted_date

- UNKNOWN: none of the above patterns match with at least 85% confidence

Respond ONLY with a single valid JSON object — no markdown, no explanation:
{
  "report_type": "FBA_RETURNS" | "REMOVAL_ORDER" | "INVENTORY_LEDGER" | "REIMBURSEMENTS" | "SETTLEMENT" | "SAFET_CLAIMS" | "TRANSACTIONS" | "UNKNOWN",
  "mapping": {
    "<canonical_field>": "<exact CSV header string from the input>"
  }
}

Rules:
- Only include canonical fields that have a clear match in the provided headers.
- Use the exact header string from the input as the value (do not alter it).
- If report_type is UNKNOWN, mapping must be an empty object {}.
- Be conservative: choose UNKNOWN if you are not at least 85% confident.`;

  const userMessage = `CSV headers (comma-separated):\n${hdr.join(", ")}`;

  const body = {
    model: "gpt-4o-mini",
    temperature: 0,
    max_tokens: 256,
    response_format: { type: "json_object" },
    messages: [
      { role: "system" as const, content: systemPrompt },
      { role: "user" as const, content: userMessage },
    ],
  };

  try {
    const res = await fetch(OPENAI_CHAT, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${key}`,
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      return { reportType: "UNKNOWN", columnMapping: {} };
    }

    const json = (await res.json()) as {
      choices?: { message?: { content?: string } }[];
    };
    const raw = json.choices?.[0]?.message?.content?.trim() ?? "";

    let parsed: { report_type?: string; mapping?: Record<string, string> };
    try {
      parsed = JSON.parse(raw) as typeof parsed;
    } catch {
      return { reportType: "UNKNOWN", columnMapping: {} };
    }

    const reportType = parseGptReportType(parsed.report_type ?? "");
    const mapping = parsed.mapping && typeof parsed.mapping === "object" ? parsed.mapping : {};

    // Sanitize: only keep string → string entries matching actual CSV headers
    const headerSet = new Set(hdr);
    const sanitized: Record<string, string> = {};
    for (const [k, v] of Object.entries(mapping)) {
      if (typeof k === "string" && typeof v === "string" && headerSet.has(v)) {
        sanitized[k] = v;
      }
    }

    return { reportType, columnMapping: sanitized };
  } catch {
    return { reportType: "UNKNOWN", columnMapping: {} };
  }
}
