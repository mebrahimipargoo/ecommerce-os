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
}): Promise<{
  reportType: RawReportType;
  columnMapping: Record<string, string>;
  detectedFileType: string;
  isSupported: boolean;
  message: string;
}> {
  const UNSUPPORTED = (detectedFileType: string, message: string) => ({
    reportType: "UNKNOWN" as RawReportType,
    columnMapping: {},
    detectedFileType,
    isSupported: false,
    message,
  });

  const key = await getOrganizationOpenAIApiKey(input.organizationId);
  if (!key) {
    return UNSUPPORTED(
      "Unknown",
      "No OpenAI API key configured. Go to Settings → AI & OCR Engine to add one.",
    );
  }

  const hdr = input.headers.map((h) => h.trim()).filter(Boolean);
  if (hdr.length === 0) return UNSUPPORTED("Unknown", "No headers found in the file.");

  const systemPrompt = `You are an expert at analyzing CSV exports from e-commerce platforms.
Your job:
1. Identify what type of file these column headers belong to — from ANY platform (Amazon, Walmart, Shopify, eBay, custom, etc.).
2. If it is a supported Amazon report, map its headers to our canonical schema fields.

SUPPORTED Amazon report types (report_type must be one of these):
- FBA_RETURNS: signals = "license-plate-number" AND "detailed-disposition"
  canonical: lpn, detailed_disposition, asin, order_id, return_reason
- REMOVAL_ORDER: signals = "removal-order-id" OR ("requested-quantity" AND "disposed-quantity")
  canonical: order_id, sku, fnsku, disposition, shipped_quantity, cancelled_quantity, disposed_quantity, requested_quantity, status
- REMOVAL_SHIPMENT: signals = "tracking-number" AND ("carrier" OR "shipment-date" OR "carrier-shipment-date")
  canonical: order_id, sku, tracking_number, carrier, shipment_date
- INVENTORY_LEDGER: signals = "fnsku" AND "ending warehouse balance"
  canonical: fnsku, ending_warehouse_balance, title, date, asin
- REIMBURSEMENTS: signals = "reimbursement-id" AND "quantity-reimbursed-total"
  canonical: reimbursement_id, quantity_reimbursed_total, order_id, asin, amount_per_unit, approval_date
- SETTLEMENT: signals = "settlement id" AND "transaction status"
  canonical: settlement_id, transaction_status, order_id, total, deposit_date
- SAFET_CLAIMS: signals = "safe-t claim id" AND "reimbursement amount"
  canonical: claim_id, amount, order_id, claim_status
- TRANSACTIONS: signals = "transaction type" AND "total product charges"
  canonical: transaction_type, total_product_charges, order_id, sku, posted_date
- REPORTS_REPOSITORY: signals = "date/time" AND "settlement id" AND "type" AND "order id" AND "sku" AND "description" AND "total"
  canonical: date_time, settlement_id, transaction_type, order_id, sku, description, total_amount
- UNKNOWN: use when none of the above match with 85%+ confidence

Respond ONLY with a single valid JSON object:
{
  "report_type": "FBA_RETURNS" | "REMOVAL_ORDER" | "REMOVAL_SHIPMENT" | "INVENTORY_LEDGER" | "REIMBURSEMENTS" | "SETTLEMENT" | "SAFET_CLAIMS" | "TRANSACTIONS" | "REPORTS_REPOSITORY" | "UNKNOWN",
  "detected_file_type": "Human-readable name of the file, e.g. 'Amazon Removal Shipment Detail', 'Walmart Sales Report', 'Shopify Inventory Export', 'Unknown File'",
  "is_supported": true or false (true ONLY if report_type is not UNKNOWN),
  "mapping": { "<canonical_field>": "<exact CSV header string>" },
  "message": "One concise sentence for the user describing what was found."
}

Rules:
- detected_file_type must always be filled in — even for UNKNOWN files, guess the platform/type from header patterns.
- If report_type is UNKNOWN, mapping must be {}, is_supported must be false.
- Only include mapping entries where the value is an exact header string from the input.
- Be conservative: use UNKNOWN if less than 85% confident about the report type.`;

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
      return UNSUPPORTED("Unknown", `AI API returned status ${res.status}. Check your API key.`);
    }

    const json = (await res.json()) as {
      choices?: { message?: { content?: string } }[];
    };
    const raw = json.choices?.[0]?.message?.content?.trim() ?? "";

    let parsed: {
      report_type?: string;
      detected_file_type?: string;
      is_supported?: boolean;
      mapping?: Record<string, string>;
      message?: string;
    };
    try {
      parsed = JSON.parse(raw) as typeof parsed;
    } catch {
      return UNSUPPORTED("Unknown", "AI returned an unreadable response.");
    }

    const reportType = parseGptReportType(parsed.report_type ?? "");
    const mapping = parsed.mapping && typeof parsed.mapping === "object" ? parsed.mapping : {};
    const detectedFileType = typeof parsed.detected_file_type === "string" && parsed.detected_file_type.trim()
      ? parsed.detected_file_type.trim()
      : reportType !== "UNKNOWN" ? reportType : "Unknown File";
    const isSupported = reportType !== "UNKNOWN";
    const message = typeof parsed.message === "string" ? parsed.message.trim() : "";

    // Sanitize: only keep string → string entries matching actual CSV headers
    const headerSet = new Set(hdr);
    const sanitized: Record<string, string> = {};
    for (const [k, v] of Object.entries(mapping)) {
      if (typeof k === "string" && typeof v === "string" && headerSet.has(v)) {
        sanitized[k] = v;
      }
    }

    return { reportType, columnMapping: sanitized, detectedFileType, isSupported, message };
  } catch {
    return UNSUPPORTED("Unknown", "AI classification failed due to a network or parsing error.");
  }
}
