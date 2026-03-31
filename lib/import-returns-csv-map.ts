import { CLAIM_CONDITIONS } from "@/app/returns/claim-queue-helpers";

function normKey(s: string): string {
  return s.replace(/^\uFEFF/, "").trim().toLowerCase().replace(/[\s_]+/g, " ");
}

function pickFromRow(
  row: Record<string, string>,
  columnMapping: Record<string, string> | null,
  canonicalKey: string,
  headerAliases: string[],
): string {
  const mapped = columnMapping?.[canonicalKey]?.trim();
  if (mapped && Object.prototype.hasOwnProperty.call(row, mapped)) {
    return String(row[mapped] ?? "").trim();
  }
  const aliasNorm = headerAliases.map(normKey);
  for (const key of Object.keys(row)) {
    const nk = normKey(key);
    if (aliasNorm.some((a) => nk === a || nk.includes(a) || a.includes(nk))) {
      return String(row[key] ?? "").trim();
    }
  }
  return "";
}

const ORDER_ALIASES = ["order id", "order-id", "amazon order id", "amazon-order-id"];
const LPN_ALIASES = ["license plate number", "license-plate-number", "lpn"];
const SKU_ALIASES = ["merchant sku", "merchant-sku", "sku", "seller sku", "seller-sku"];
const REASON_ALIASES = ["reason", "return reason", "return-reason", "customer comments"];
const DISP_ALIASES = ["disposition", "detailed disposition", "condition", "item condition"];
const TITLE_ALIASES = ["product name", "item name", "title", "product title", "asin"];

/** Map Amazon disposition / condition text to `returns.conditions` (canonical claim codes where possible). */
export function dispositionToConditions(raw: string): string[] {
  const t = raw.trim().toLowerCase();
  if (!t) return [];
  if (t.includes("sellable") || t === "new" || t.includes("new sellable")) return [];
  if (t.includes("wrong") && (t.includes("item") || t.includes("product"))) {
    return t.includes("junk") ? ["wrong_item_junk"] : ["wrong_item_different"];
  }
  if (t.includes("empty") && t.includes("box")) return ["empty_box"];
  if (t.includes("missing") || t.includes("no item")) return ["missing_item"];
  if (t.includes("expir") || t.includes("best before")) return ["expired"];
  if (t.includes("damage") || t.includes("defect")) return ["damaged_customer"];
  if (t.includes("carrier")) return ["damaged_carrier"];
  if (t.includes("warehouse") || t.includes("fulfillment")) return ["damaged_warehouse"];
  if (t.includes("scratch")) return ["scratched"];
  if (t.includes("parts")) return ["missing_parts"];
  return [];
}

export function deriveImportStatus(conditions: string[]): string {
  const needsClaim = conditions.some((c) => CLAIM_CONDITIONS.has(c));
  return needsClaim ? "pending_evidence" : "received";
}

export type MappedReturnRow = {
  order_id: string | null;
  lpn: string | null;
  sku: string | null;
  notes: string | null;
  conditions: string[];
  item_name: string;
};

export function mapCsvRowToReturnFields(
  row: Record<string, string>,
  columnMapping: Record<string, string> | null,
): MappedReturnRow | null {
  const orderId =
    pickFromRow(row, columnMapping, "order-id", ORDER_ALIASES) ||
    pickFromRow(row, columnMapping, "order_id", ORDER_ALIASES);
  const lpn = pickFromRow(row, columnMapping, "license-plate-number", LPN_ALIASES);
  const sku =
    pickFromRow(row, columnMapping, "merchant-sku", SKU_ALIASES) ||
    pickFromRow(row, columnMapping, "sku", SKU_ALIASES);
  const reason = pickFromRow(row, columnMapping, "reason", REASON_ALIASES);
  const disposition =
    pickFromRow(row, columnMapping, "disposition", DISP_ALIASES) ||
    pickFromRow(row, columnMapping, "condition", DISP_ALIASES);
  const title = pickFromRow(row, columnMapping, "item-name", TITLE_ALIASES);

  const conditions = dispositionToConditions(disposition);
  const itemName =
    title.trim() ||
    sku.trim() ||
    lpn.trim() ||
    orderId.trim() ||
    "Amazon import";

  if (!orderId && !lpn && !sku && !reason && !disposition) {
    return null;
  }

  return {
    order_id: orderId.trim() || null,
    lpn: lpn.trim() || null,
    sku: sku.trim() || null,
    notes: reason.trim() || null,
    conditions,
    item_name: itemName.slice(0, 2000),
  };
}
