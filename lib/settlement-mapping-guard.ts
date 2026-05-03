/**
 * Pre-sync guard for SETTLEMENT imports: mapping synopsis + block rules.
 *
 * Blocks when the mapper rejects all sampled rows, or when a financial-like
 * column is not covered by native aliases, explicit raw_data policy labels, or
 * the shared SETTLEMENT_GUARD_COVERED_NORM_KEYS set from import-sync-mappers.
 */

import {
  applyColumnMappingToRow,
  mapRowToAmazonSettlement,
  normalizeAmazonReportRowKeys,
  normImportHeaderKey,
  SETTLEMENT_GUARD_COVERED_NORM_KEYS,
} from "./import-sync-mappers";

export type SettlementMappingReportEntry = {
  staging_source_column: string;
  target_amazon_settlements_column: string;
  data_type: string;
  transformation: string;
  required_or_new: "required" | "optional" | "new_column_needed";
  confidence: "high" | "medium" | "low";
};

/** Headers that map to typed columns or raw_data via existing mapper heuristics. */
const KNOWN_SETTLEMENT_KEY_PATTERN =
  /^(settlement[-_]?id|settlement[-_]?start[-_]?date|settlement[-_]?end[-_]?date|deposit[-_]?date|total[-_]?amount|currency|posted[-_]?date|order[-_]?id|sku|transaction[-_]?type|amount|description|quantity|marketplace|account[-_]?type|fulfillment|product[-_]?sales|selling[-_]?fees|fba[-_]?fees|gift|shipping|regulatory|promotional|rebate|withheld|other[-_]?transaction|other[-_]?amount|transaction[-_]?status|transaction[-_]?release)/i;

const FINANCIAL_LIKE =
  /(amount|fee|fees|tax|total|price|cost|credit|rebate|currency|quantity|proceeds|balance|charge|payment|refund)/i;

function classifyNormalizedKey(
  normalizedKey: string,
): Omit<SettlementMappingReportEntry, "staging_source_column"> {
  const k = normalizedKey;
  if (SETTLEMENT_GUARD_COVERED_NORM_KEYS.has(k)) {
    return {
      target_amazon_settlements_column: "mapped_native_or_raw_data",
      data_type: "text|numeric|timestamptz|jsonb",
      transformation:
        "mapper pickT / parseNum / parseIsoDateTime / raw_data passthrough (policy labels e.g. tax collection model)",
      required_or_new: "optional",
      confidence: "high",
    };
  }
  if (KNOWN_SETTLEMENT_KEY_PATTERN.test(k)) {
    return {
      target_amazon_settlements_column: "mapped_native_or_raw_data",
      data_type: "text|numeric|timestamptz",
      transformation: "mapper pickT / parseNum / parseIsoDateTime",
      required_or_new: /settlement[-_]?id/i.test(k) ? "required" : "optional",
      confidence: "high",
    };
  }
  if (FINANCIAL_LIKE.test(k)) {
    return {
      target_amazon_settlements_column: "raw_data (unmatched header)",
      data_type: "jsonb fragment",
      transformation: "passthrough to raw_data",
      required_or_new: "new_column_needed",
      confidence: "low",
    };
  }
  return {
    target_amazon_settlements_column: "raw_data",
    data_type: "jsonb fragment",
    transformation: "passthrough to raw_data",
    required_or_new: "optional",
    confidence: "medium",
  };
}

export type SettlementMappingGuardResult = {
  blocked: boolean;
  blockReason: string | null;
  mappingReport: SettlementMappingReportEntry[];
  mapperAcceptedSample: number;
  mapperRejectedSample: number;
  lowConfidenceFinancialKeys: string[];
};

/**
 * Samples staged rows, builds a mapping synopsis, and decides whether sync
 * should be blocked for operator review.
 */
export function evaluateSettlementMappingGuard(opts: {
  stagingSamples: { row_number: number; raw_row: Record<string, string> }[];
  columnMapping: Record<string, string> | null | undefined;
  organizationId: string;
  uploadId: string;
}): SettlementMappingGuardResult {
  const { stagingSamples, columnMapping, organizationId, uploadId } = opts;

  let mapperAcceptedSample = 0;
  let mapperRejectedSample = 0;
  for (const s of stagingSamples) {
    const mapped = applyColumnMappingToRow(
      normalizeAmazonReportRowKeys(s.raw_row ?? {}),
      columnMapping ?? null,
    );
    const row = mapRowToAmazonSettlement(mapped, organizationId, uploadId);
    if (row) mapperAcceptedSample += 1;
    else mapperRejectedSample += 1;
  }

  const keySet = new Set<string>();
  for (const s of stagingSamples) {
    const mapped = applyColumnMappingToRow(
      normalizeAmazonReportRowKeys(s.raw_row ?? {}),
      columnMapping ?? null,
    );
    for (const k of Object.keys(mapped)) {
      if (String(mapped[k] ?? "").trim() !== "") keySet.add(k);
    }
  }

  const mappingReport: SettlementMappingReportEntry[] = [...keySet]
    .sort()
    .map((staging_source_column) => ({
      staging_source_column,
      ...classifyNormalizedKey(normImportHeaderKey(staging_source_column)),
    }));

  const lowConfidenceFinancialKeys = mappingReport
    .filter((e) => e.confidence === "low")
    .map((e) => e.staging_source_column);

  const allRejected =
    stagingSamples.length > 0 && mapperAcceptedSample === 0 && mapperRejectedSample > 0;

  let blocked = false;
  let blockReason: string | null = null;

  if (allRejected) {
    blocked = true;
    blockReason =
      "Settlement mapper rejected every sampled row (missing settlement anchor such as settlement_id / Settlement ID, or wrong file shape). " +
      "Fix column mapping or report type, then retry Sync.";
  } else if (lowConfidenceFinancialKeys.length > 0) {
    blocked = true;
    blockReason =
      "Financial-like column(s) not covered by settlement mapper aliases or raw_data policy: " +
      lowConfidenceFinancialKeys.join(", ") +
      ". Extend aliases in import-sync-mappers or column_mapping, then retry Sync.";
  }

  return {
    blocked,
    blockReason,
    mappingReport,
    mapperAcceptedSample,
    mapperRejectedSample,
    lowConfidenceFinancialKeys,
  };
}
