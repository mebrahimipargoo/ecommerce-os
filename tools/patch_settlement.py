import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent

def patch_mappers():
    p = ROOT / "lib" / "import-sync-mappers.ts"
    t = p.read_text(encoding="utf-8")

    old = """function normKey(s: string): string {
  return s.replace(/^\\uFEFF/, "").trim().toLowerCase().replace(/[\\s_]+/g, "-");
}

/** Resolve a cell when CSV headers use hyphens but keys were normalized to underscores (or vice versa). */"""
    new = """function normKey(s: string): string {
  return s.replace(/^\\uFEFF/, "").trim().toLowerCase().replace(/[\\s_]+/g, "-");
}

/** Same normalization as `pickT` — pre-sync guard must use this for header keys. */
export function normImportHeaderKey(s: string): string {
  return normKey(s);
}

/** Resolve a cell when CSV headers use hyphens but keys were normalized to underscores (or vice versa). */"""
    if old not in t:
        raise SystemExit("mappers: normKey block not found")
    t = t.replace(old, new, 1)

    old = 'const AMOUNT_TOTAL_ALIASES     = ["total", "amount", "net-proceeds", "net proceeds", "amount-total", "amount total"];'
    new = """const AMOUNT_TOTAL_ALIASES = [
  "total",
  "Total",
  "amount",
  "net-proceeds",
  "net proceeds",
  "amount-total",
  "amount total",
  "Total Amount",
  "total amount",
  "Net Amount",
  "net amount",
  "Amount (Total)",
  "amount (total)",
];"""
    if old not in t:
        raise SystemExit("mappers: AMOUNT_TOTAL not found")
    t = t.replace(old, new, 1)

    old = """const SETTLEMENT_PRODUCT_SALES_ALIASES   = ["product sales", "product-sales", "product_sales"];
const SETTLEMENT_PRODUCT_SALES_TAX_ALIASES = ["product sales tax", "product-sales-tax"];"""
    new = """const SETTLEMENT_PRODUCT_SALES_ALIASES = ["product sales", "product-sales", "product_sales", "Product Sales"];
const SETTLEMENT_PRODUCT_SALES_TAX_ALIASES = [
  "product sales tax",
  "product-sales-tax",
  "Product Sales Tax",
  "product sales Tax",
  "Product tax",
  "product tax",
  "Tax: Product Sales",
  "tax: product sales",
];"""
    if old not in t:
        raise SystemExit("mappers: product sales tax block not found")
    t = t.replace(old, new, 1)

    old = """const SETTLEMENT_TAX_ON_REG_FEE_ALIASES  = [
  "Tax On Regulatory Fee", "tax on regulatory fee", "tax-on-regulatory-fee",
];"""
    new = """const SETTLEMENT_TAX_ON_REG_FEE_ALIASES = [
  "Tax On Regulatory Fee",
  "tax on regulatory fee",
  "tax-on-regulatory-fee",
  "Tax on Regulatory Fee",
  "Tax-On Regulatory Fee",
  "Regulatory fee tax",
  "regulatory fee tax",
  "regulatory-fee-tax",
];"""
    if old not in t:
        raise SystemExit("mappers: tax on reg fee not found")
    t = t.replace(old, new, 1)

    old = """const SETTLEMENT_SELLING_FEES_ALIASES    = ["selling fees", "selling-fees"];
const SETTLEMENT_FBA_FEES_ALIASES        = ["fba fees", "fba-fees"];
const SETTLEMENT_OTHER_TX_FEES_ALIASES   = ["other transaction fees", "other-transaction-fees"];"""
    new = """const SETTLEMENT_SELLING_FEES_ALIASES = [
  "selling fees",
  "selling-fees",
  "Selling Fees",
  "Selling Fee",
  "selling fee",
  "Selling Fees (Amazon)",
  "Referral Fee",
  "referral fee",
];
const SETTLEMENT_FBA_FEES_ALIASES = [
  "fba fees",
  "fba-fees",
  "FBA Fees",
  "FBA Fee",
  "fba fee",
  "FBA Transaction Fees",
  "fba transaction fees",
  "Amazon FBA fees",
  "amazon fba fees",
];
const SETTLEMENT_OTHER_TX_FEES_ALIASES = [
  "other transaction fees",
  "other-transaction-fees",
  "Other transaction fees",
  "Other Fees",
  "other fees",
  "Other transaction fee",
  "Misc Transaction Fees",
  "misc transaction fees",
];"""
    if old not in t:
        raise SystemExit("mappers: selling/fba/other fees not found")
    t = t.replace(old, new, 1)

    old = """const SETTLEMENT_TXT_PHYSICAL_KEYS = new Set([
  "settlement_id",
  "settlement_start_date",
  "settlement_end_date",
  "deposit_date",
  "total_amount",
  "currency",
]);"""
    new = old + """

/** No native column — values stay in raw_data JSONB; mapping guard still treats header as covered. */
const SETTLEMENT_RAW_DATA_POLICY_LABELS: string[] = [
  "tax collection model",
  "Tax Collection Model",
  "TAX COLLECTION MODEL",
  "marketplace facilitator tax",
  "Marketplace Facilitator Tax",
];

/** All strings whose normKey is considered "covered" by the settlement mapping guard. */
const SETTLEMENT_GUARD_ALIAS_SOURCE_STRINGS: string[] = [
  ...SETTLEMENT_RAW_DATA_POLICY_LABELS,
  ...SETTLEMENT_ID_ALIASES,
  ...TX_TYPE_ALIASES,
  ...DEPOSIT_DATE_ALIASES,
  ...AMOUNT_TOTAL_ALIASES,
  ...ORDER_ALIASES,
  ...SKU_ALIASES,
  ...CURRENCY_ALIASES,
  ...SETTLEMENT_QUANTITY_ALIASES,
  ...SETTLEMENT_MARKETPLACE_ALIASES,
  ...SETTLEMENT_ACCOUNT_TYPE_ALIASES,
  ...SETTLEMENT_FULFILLMENT_ALIASES,
  ...SETTLEMENT_PRODUCT_SALES_ALIASES,
  ...SETTLEMENT_PRODUCT_SALES_TAX_ALIASES,
  ...SETTLEMENT_SHIPPING_CREDITS_ALIASES,
  ...SETTLEMENT_SHIPPING_CREDITS_TAX_ALIASES,
  ...SETTLEMENT_GIFT_WRAP_CREDITS_ALIASES,
  ...SETTLEMENT_GIFTWRAP_CREDITS_TAX_ALIASES,
  ...SETTLEMENT_REGULATORY_FEE_ALIASES,
  ...SETTLEMENT_TAX_ON_REG_FEE_ALIASES,
  ...SETTLEMENT_PROMOTIONAL_REBATES_ALIASES,
  ...SETTLEMENT_PROMO_REBATES_TAX_ALIASES,
  ...SETTLEMENT_MARKETPLACE_WITHHELD_TAX_ALIASES,
  ...SETTLEMENT_SELLING_FEES_ALIASES,
  ...SETTLEMENT_FBA_FEES_ALIASES,
  ...SETTLEMENT_OTHER_TX_FEES_ALIASES,
  ...SETTLEMENT_OTHER_AMOUNT_ALIASES,
  ...SETTLEMENT_TX_STATUS_ALIASES,
  ...SETTLEMENT_TX_RELEASE_DATE_ALIASES,
  ...SETTLEMENT_DESCRIPTION_ALIASES,
];

/** Normalized keys: mapper aliases + flat-file keys + raw_data-only policy labels (tax collection model, …). */
export const SETTLEMENT_GUARD_COVERED_NORM_KEYS: ReadonlySet<string> = (() => {
  const out = new Set<string>();
  for (const x of SETTLEMENT_GUARD_ALIAS_SOURCE_STRINGS) out.add(normImportHeaderKey(x));
  for (const k of SETTLEMENT_TXT_PHYSICAL_KEYS) out.add(normImportHeaderKey(k));
  return out;
})();"""
    if old + "\n\n/** Deterministic line id" not in t and old not in t:
        # allow already patched
        if "SETTLEMENT_GUARD_COVERED_NORM_KEYS" in t:
            print("mappers: already has SETTLEMENT_GUARD_COVERED_NORM_KEYS")
        else:
            raise SystemExit("mappers: SETTLEMENT_TXT_PHYSICAL_KEYS anchor not found")
    else:
        if "SETTLEMENT_GUARD_COVERED_NORM_KEYS" not in t:
            t = t.replace(old, new, 1)

    p.write_text(t, encoding="utf-8")
    print("OK import-sync-mappers.ts")


def patch_guard():
    p = ROOT / "lib" / "settlement-mapping-guard.ts"
    p.write_text(GUARD_TS, encoding="utf-8")
    print("OK settlement-mapping-guard.ts")


GUARD_TS = r'''/**
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
'''


def patch_sync_route():
    p = ROOT / "app" / "api" / "settings" / "imports" / "sync" / "route.ts"
    t = p.read_text(encoding="utf-8")

    t = t.replace(
        """ * SETTLEMENT: optional env `SETTLEMENT_SYNC_SKIP_MAPPING_GUARD=1` bypasses the
 * pre-sync mapping guard (422 + mappingReport) when operators must force sync.
 */
""",
        " */\n",
        1,
    )

    old_guard = """    if (
      kind === "SETTLEMENT" &&
      totalStagingRows > 0 &&
      process.env.SETTLEMENT_SYNC_SKIP_MAPPING_GUARD !== "1" &&
      process.env.SETTLEMENT_SYNC_SKIP_MAPPING_GUARD !== "true"
    ) {"""
    new_guard = """    if (kind === "SETTLEMENT" && totalStagingRows > 0) {"""
    if old_guard not in t:
        if "SETTLEMENT_SYNC_SKIP_MAPPING_GUARD" in t:
            raise SystemExit("sync: guard block pattern mismatch")
    else:
        t = t.replace(old_guard, new_guard, 1)

    old_imp = """import {
  FPS_KEY_COMPLETE,
  FPS_KEY_FAILED,
  FPS_KEY_SYNC,
  FPS_LABEL_COMPLETE,
  FPS_NEXT_ACTION_LABEL_GENERIC,
  fpsLabelSync,
  fpsNextAfterSync,
  fpsPctPhase3,
} from "../../../../../lib/pipeline/file-processing-status-contract";"""
    new_imp = """import {
  FPS_KEY_COMPLETE,
  FPS_KEY_FAILED,
  FPS_KEY_PROCESS,
  FPS_KEY_SYNC,
  FPS_LABEL_COMPLETE,
  FPS_LABEL_PROCESS,
  FPS_NEXT_ACTION_LABEL_GENERIC,
  FPS_NEXT_ACTION_LABEL_SYNC,
  fpsLabelSync,
  fpsNextAfterSync,
  fpsPctPhase3,
} from "../../../../../lib/pipeline/file-processing-status-contract";"""
    if old_imp not in t:
        raise SystemExit("sync: import block not found")
    t = t.replace(old_imp, new_imp, 1)

    old_rel = """/** Revert optimistic sync lock — upload returns to `staged` after a pre-flight block (no domain writes). */
async function releaseRawReportSyncLockToStaged(
  uploadId: string,
  orgId: string,
  metaExtras: Record<string, unknown>,
): Promise<void> {
  const { data: prevRow } = await supabaseServer
    .from("raw_report_uploads")
    .select("metadata")
    .eq("id", uploadId)
    .maybeSingle();
  await supabaseServer
    .from("raw_report_uploads")
    .update({
      status: "staged",
      metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
        etl_phase: "staging",
        sync_progress: 0,
        error_message: "",
        ...metaExtras,
      }),
      updated_at: new Date().toISOString(),
    })
    .eq("id", uploadId)
    .eq("organization_id", orgId);
}"""
    new_rel = """type FpsRevertAfterSyncPreflight = {
  engine: AmazonImportEngineConfig;
  phase2StagedWatermark: number;
  fileRowTotal: number | null;
};

/** Revert optimistic sync lock — upload returns to `staged` after a pre-flight block (no domain writes). */
async function releaseRawReportSyncLockToStaged(
  uploadId: string,
  orgId: string,
  metaExtras: Record<string, unknown>,
  fpsRevert?: FpsRevertAfterSyncPreflight,
): Promise<void> {
  const { data: prevRow } = await supabaseServer
    .from("raw_report_uploads")
    .select("metadata")
    .eq("id", uploadId)
    .maybeSingle();
  await supabaseServer
    .from("raw_report_uploads")
    .update({
      status: "staged",
      metadata: mergeUploadMetadata((prevRow as { metadata?: unknown } | null)?.metadata, {
        etl_phase: "staging",
        sync_progress: 0,
        error_message: "",
        ...metaExtras,
      }),
      updated_at: new Date().toISOString(),
    })
    .eq("id", uploadId)
    .eq("organization_id", orgId);

  if (fpsRevert) {
    const e = fpsRevert.engine;
    const stageLabel = e.stage_target_table ?? "amazon_staging";
    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: uploadId,
        organization_id: orgId,
        status: "pending",
        current_phase: "staged",
        phase_key: FPS_KEY_PROCESS,
        phase_label: FPS_LABEL_PROCESS,
        current_phase_label: "Ready for Sync",
        next_action_key: "sync",
        next_action_label: FPS_NEXT_ACTION_LABEL_SYNC,
        next_action_key: "sync",
        stage_target_table: e.stage_target_table,
        sync_target_table: e.sync_target_table,
        generic_target_table: e.generic_target_table,
        current_target_table: stageLabel,
        upload_pct: 100,
        process_pct: 100,
        sync_pct: 0,
        phase1_upload_pct: 100,
        phase2_stage_pct: 100,
        phase3_raw_sync_pct: 0,
        phase3_status: "pending",
        processed_rows: fpsRevert.phase2StagedWatermark,
        staged_rows_written: fpsRevert.phase2StagedWatermark,
        raw_rows_written: 0,
        raw_rows_skipped_existing: 0,
        duplicate_rows_skipped: 0,
        ...(fpsRevert.fileRowTotal != null && fpsRevert.fileRowTotal > 0
          ? { total_rows: fpsRevert.fileRowTotal }
          : {}),
        import_metrics: { current_phase: "staged", rows_synced: 0 },
        error_message: null,
      },
      { onConflict: "upload_id" },
    );
  }
}"""
    if old_rel not in t:
        raise SystemExit("sync: releaseRawReportSyncLockToStaged not found")
    t = t.replace(old_rel, new_rel, 1)

    old_call = """        await releaseRawReportSyncLockToStaged(uploadId, orgId, {
          settlement_mapping_guard_blocked: true,
          settlement_mapping_guard_reason: guard.blockReason ?? "",
          settlement_mapping_guard_summary: {
            mapperAcceptedSample: guard.mapperAcceptedSample,
            mapperRejectedSample: guard.mapperRejectedSample,
            lowConfidenceFinancialKeys: guard.lowConfidenceFinancialKeys,
          },
        });"""
    new_call = """        await releaseRawReportSyncLockToStaged(
          uploadId,
          orgId,
          {
            settlement_mapping_guard_blocked: true,
            settlement_mapping_guard_reason: guard.blockReason ?? "",
            settlement_mapping_guard_summary: {
              mapperAcceptedSample: guard.mapperAcceptedSample,
              mapperRejectedSample: guard.mapperRejectedSample,
              lowConfidenceFinancialKeys: guard.lowConfidenceFinancialKeys,
            },
          },
          { engine, phase2StagedWatermark, fileRowTotal },
        );"""
    if old_call not in t:
        raise SystemExit("sync: release call not found")
    t = t.replace(old_call, new_call, 1)

    # bumpSyncProgressMetadata: use cumulative chunk rows for non-removal progress
    old_bump = """  let rawW: number;
  let rawSkip: number;
  if (removalShipment) {
    rawW = removalShipment.rawRowsWritten;
    rawSkip = removalShipment.rawRowsSkippedCrossUpload;
  } else if (opts.metricTotals) {
    const m = opts.metricTotals;
    rawW = m.rows_synced_new + m.rows_synced_updated + m.rows_synced_unchanged;
    rawSkip = m.rows_duplicate_against_existing;
  } else {
    rawW = opts.upserted.value;
    rawSkip = 0;
  }
  const dupBatch = opts.duplicateInBatchTotal?.value ?? 0;
  const phase3Denom =
    opts.fileRowTotal != null && opts.fileRowTotal > 0
      ? opts.fileRowTotal
      : Math.max(1, opts.totalStagingRows);
  const phase3Pct = fpsPctPhase3(rawW, rawSkip, phase3Denom);"""
    new_bump = """  let rawW: number;
  let rawSkip: number;
  let phase3Pct: number;
  const cumWritten = opts.upserted.value;
  const phase3Denom =
    opts.fileRowTotal != null && opts.fileRowTotal > 0
      ? opts.fileRowTotal
      : Math.max(1, opts.totalStagingRows);
  if (removalShipment) {
    rawW = removalShipment.rawRowsWritten;
    rawSkip = removalShipment.rawRowsSkippedCrossUpload;
    phase3Pct = fpsPctPhase3(rawW, rawSkip, phase3Denom);
  } else {
    rawW = cumWritten;
    rawSkip = opts.metricTotals?.rows_duplicate_against_existing ?? 0;
    phase3Pct = Math.min(100, Math.round((cumWritten / phase3Denom) * 100));
  }
  const dupBatch = opts.duplicateInBatchTotal?.value ?? 0;"""
    if old_bump not in t:
        raise SystemExit("sync: bumpSyncProgressMetadata block not found")
    t = t.replace(old_bump, new_bump, 1)

    # import_metrics merge in metadata update
    old_im = """        import_metrics: {
          ...prevIm,
          current_phase: "sync",
          rows_synced: opts.upserted.value,
          total_staging_rows: opts.totalStagingRows,
          ...(opts.fileRowTotal != null && opts.fileRowTotal > 0 ? { file_row_total_plan: opts.fileRowTotal } : {}),
          ...(opts.syncCountVerifyPending ? { sync_count_verification_pending: true } : {}),
          ...(removalShipment
            ? {
                removal_shipment_raw_rows_written: removalShipment.rawRowsWritten,
                removal_shipment_skipped_cross_upload: removalShipment.rawRowsSkippedCrossUpload,
              }
            : {}),
        },"""
    mt = """        import_metrics: {
          ...prevIm,
          current_phase: "sync",
          rows_synced: opts.upserted.value,
          total_staging_rows: opts.totalStagingRows,
          ...(opts.fileRowTotal != null && opts.fileRowTotal > 0 ? { file_row_total_plan: opts.fileRowTotal } : {}),
          ...(opts.syncCountVerifyPending ? { sync_count_verification_pending: true } : {}),
          ...(opts.metricTotals && !removalShipment
            ? {
                rows_synced_new: opts.metricTotals.rows_synced_new,
                rows_synced_updated: opts.metricTotals.rows_synced_updated,
                rows_synced_unchanged: opts.metricTotals.rows_synced_unchanged,
                rows_duplicate_against_existing: opts.metricTotals.rows_duplicate_against_existing,
                sync_rows_attempted:
                  opts.metricTotals.rows_synced_new +
                  opts.metricTotals.rows_synced_updated +
                  opts.metricTotals.rows_synced_unchanged +
                  opts.metricTotals.rows_duplicate_against_existing,
              }
            : {}),
          ...(removalShipment
            ? {
                removal_shipment_raw_rows_written: removalShipment.rawRowsWritten,
                removal_shipment_skipped_cross_upload: removalShipment.rawRowsSkippedCrossUpload,
              }
            : {}),
        },"""
    if old_im not in t:
        raise SystemExit("sync: import_metrics metadata block not found")
    t = t.replace(old_im, mt, 1)

    old_fps_im = """        import_metrics: {
          current_phase: "sync",
          rows_synced: opts.upserted.value,
          total_staging_rows: opts.totalStagingRows,
          ...(opts.fileRowTotal != null && opts.fileRowTotal > 0 ? { file_row_total_plan: opts.fileRowTotal } : {}),
          ...(opts.syncCountVerifyPending ? { sync_count_verification_pending: true } : {}),
          ...(removalShipment
            ? {
                removal_shipment_raw_rows_written: removalShipment.rawRowsWritten,
                removal_shipment_skipped_cross_upload: removalShipment.rawRowsSkippedCrossUpload,
              }
            : {}),
        },
      },
      { onConflict: "upload_id" },
    );
}"""
    new_fps_im = """        import_metrics: {
          current_phase: "sync",
          rows_synced: opts.upserted.value,
          total_staging_rows: opts.totalStagingRows,
          ...(opts.fileRowTotal != null && opts.fileRowTotal > 0 ? { file_row_total_plan: opts.fileRowTotal } : {}),
          ...(opts.syncCountVerifyPending ? { sync_count_verification_pending: true } : {}),
          ...(opts.metricTotals && !removalShipment
            ? {
                rows_synced_new: opts.metricTotals.rows_synced_new,
                rows_synced_updated: opts.metricTotals.rows_synced_updated,
                rows_synced_unchanged: opts.metricTotals.rows_synced_unchanged,
                rows_duplicate_against_existing: opts.metricTotals.rows_duplicate_against_existing,
                sync_rows_attempted:
                  opts.metricTotals.rows_synced_new +
                  opts.metricTotals.rows_synced_updated +
                  opts.metricTotals.rows_synced_unchanged +
                  opts.metricTotals.rows_duplicate_against_existing,
              }
            : {}),
          ...(removalShipment
            ? {
                removal_shipment_raw_rows_written: removalShipment.rawRowsWritten,
                removal_shipment_skipped_cross_upload: removalShipment.rawRowsSkippedCrossUpload,
              }
            : {}),
        },
      },
      { onConflict: "upload_id" },
    );
}"""
    if old_fps_im not in t:
        raise SystemExit("sync: fps import_metrics block not found")
    t = t.replace(old_fps_im, new_fps_im, 1)

    p.write_text(t, encoding="utf-8")
    print("OK sync route.ts")


def patch_flush_log():
    p = ROOT / "app" / "api" / "settings" / "imports" / "sync" / "route.ts"
    t = p.read_text(encoding="utf-8")
    needle = "  return { flushed: deduped.length, collapsedInBatch };\n}"
    if needle not in t:
        raise SystemExit("flush return not found")
    rep = """  console.log(
    JSON.stringify({
      event: "sync_batch_completed",
      kind,
      table,
      rows_deduped: deduped.length,
      rows_collapsed_in_batch: collapsedInBatch,
    }),
  );
  return { flushed: deduped.length, collapsedInBatch };
}"""
    t = t.replace(needle, rep, 1)
    p.write_text(t, encoding="utf-8")
    print("OK flushDomainBatch log")


def patch_universal_importer():
    p = ROOT / "app" / "(admin)" / "imports" / "UniversalImporter.tsx"
    t = p.read_text(encoding="utf-8")
    old = """    const pollDelayMs = () => {
      const p = phaseRef.current;
      return p === "processing" || p === "syncing" || p === "genericing" || p === "worklisting"
        ? 6200
        : 2800;
    };"""
    new = """    const pollDelayMs = () => {
      const p = phaseRef.current;
      if (p === "syncing") return 500;
      return p === "processing" || p === "genericing" || p === "worklisting" ? 6200 : 2800;
    };"""
    if old not in t:
        raise SystemExit("UniversalImporter pollDelayMs not found")
    t = t.replace(old, new, 1)

    old_sync = """    const uploadIdSnap = sessionUploadId;
    if (pollRef2.current) clearInterval(pollRef2.current);
    pollRef2.current = setInterval(() => {
      void Promise.all([
        supabase.from("raw_report_uploads").select("metadata").eq("id", uploadIdSnap).maybeSingle(),
        supabase.from("file_processing_status").select("*").eq("upload_id", uploadIdSnap).maybeSingle(),
      ]).then(([rpu, fps]) => {
        const fpsRow = fps.data as Record<string, unknown> | null | undefined;
        if (fpsRow && typeof fpsRow.sync_pct === "number") {
          setSyncPct(Math.min(100, Math.max(0, Number(fpsRow.sync_pct))));
        } else {
          const m = rpu.data?.metadata as Record<string, unknown> | null;
          if (m && typeof m.sync_progress === "number") setSyncPct(m.sync_progress);
        }
        if (fpsRow && typeof fpsRow.current_phase === "string") {
          setPhaseLabel(formatImportPhaseLabel(fpsRow.current_phase));
        }
        const im = fpsRow?.import_metrics as { rows_synced?: number } | undefined;
        const mSync = rpu.data?.metadata as Record<string, unknown> | null | undefined;
        const plan = resolveImportFileRowTotal({
          fps: (fpsRow as Record<string, unknown> | null | undefined) ?? undefined,
          metadata: mSync ?? undefined,
        });
        const pr =
          typeof im?.rows_synced === "number"
            ? im.rows_synced
            : fpsRow && typeof fpsRow.processed_rows === "number"
              ? Number(fpsRow.processed_rows)
              : 0;
        const pend = plan.verificationPending ? " · verification pending" : "";
        if (plan.total != null && plan.total > 0 && pr >= 0) {
          setProgressMsg(`Syncing… ${pr.toLocaleString()} / ${plan.total.toLocaleString()} rows${pend}`);
        } else if (pr > 0) {
          setProgressMsg(`Syncing… ${pr.toLocaleString()} rows${pend}`);
        }
      });
    }, 3000);"""
    new_sync = """    const uploadIdSnap = sessionUploadId;
    const pollSyncProgress = () => {
      void Promise.all([
        supabase.from("raw_report_uploads").select("metadata").eq("id", uploadIdSnap).maybeSingle(),
        supabase.from("file_processing_status").select("*").eq("upload_id", uploadIdSnap).maybeSingle(),
      ]).then(([rpu, fps]) => {
        const fpsRow = fps.data as Record<string, unknown> | null | undefined;
        if (fpsRow && typeof fpsRow.sync_pct === "number") {
          setSyncPct(Math.min(100, Math.max(0, Number(fpsRow.sync_pct))));
        } else {
          const m = rpu.data?.metadata as Record<string, unknown> | null;
          if (m && typeof m.sync_progress === "number") setSyncPct(m.sync_progress);
        }
        if (fpsRow && typeof fpsRow.current_phase === "string") {
          setPhaseLabel(formatImportPhaseLabel(fpsRow.current_phase));
        }
        const im = fpsRow?.import_metrics as { rows_synced?: number } | undefined;
        const mSync = rpu.data?.metadata as Record<string, unknown> | null | undefined;
        const plan = resolveImportFileRowTotal({
          fps: (fpsRow as Record<string, unknown> | null | undefined) ?? undefined,
          metadata: mSync ?? undefined,
        });
        const pr =
          typeof im?.rows_synced === "number"
            ? im.rows_synced
            : fpsRow && typeof fpsRow.processed_rows === "number"
              ? Number(fpsRow.processed_rows)
              : 0;
        const pend = plan.verificationPending ? " · verification pending" : "";
        if (plan.total != null && plan.total > 0 && pr >= 0) {
          setProgressMsg(`Syncing… ${pr.toLocaleString()} / ${plan.total.toLocaleString()} rows${pend}`);
        } else if (pr > 0) {
          setProgressMsg(`Syncing… ${pr.toLocaleString()} rows${pend}`);
        }
      });
    };
    pollSyncProgress();
    if (pollRef2.current) clearInterval(pollRef2.current);
    pollRef2.current = setInterval(pollSyncProgress, 500);"""
    if old_sync not in t:
        raise SystemExit("UniversalImporter runSync poll block not found")
    t = t.replace(old_sync, new_sync, 1)

    old_err = """      if (!res.ok || !json.ok) {
        if (json.settlement_mapping_guard) {
          const keys = json.lowConfidenceFinancialKeys?.length
            ? ` Unmapped financial-like headers: ${json.lowConfidenceFinancialKeys.join(", ")}.`
            : "";
          throw new Error(
            `${json.details || json.error || "Settlement mapping guard blocked sync."}${keys}` +
              " See Network response JSON for full mappingReport.",
          );
        }
        throw new Error(json.details || json.error || "Sync failed.");
      }"""
    new_err = """      if (!res.ok || !json.ok) {
        if (json.settlement_mapping_guard) {
          setSyncPct(0);
          const keys = json.lowConfidenceFinancialKeys?.length
            ? ` Unmapped financial-like headers: ${json.lowConfidenceFinancialKeys.join(", ")}.`
            : "";
          throw new Error(
            `${json.details || json.error || "Settlement mapping guard blocked sync."}${keys}` +
              " See Network response JSON for full mappingReport.",
          );
        }
        throw new Error(json.details || json.error || "Sync failed.");
      }"""
    if old_err not in t:
        raise SystemExit("UniversalImporter error block not found")
    t = t.replace(old_err, new_err, 1)

    p.write_text(t, encoding="utf-8")
    print("OK UniversalImporter.tsx")


def patch_validation_sql():
    p = ROOT / "supabase" / "scripts" / "settlement_recovery_validation.sql"
    t = p.read_text(encoding="utf-8")
    old = """-- SELECT organization_id, source_file_sha256, source_physical_row_number, COUNT(*) AS n
-- FROM public.amazon_settlements
-- WHERE organization_id = '<ORG_UUID>'::uuid
-- GROUP BY 1, 2, 3
-- HAVING COUNT(*) > 1;"""
    new = """-- SELECT organization_id, source_file_sha256, source_physical_row_number, COUNT(*) AS n
-- FROM public.amazon_settlements
-- WHERE organization_id = '<ORG_UUID>'::uuid
--   AND upload_id = '<UPLOAD_UUID>'::uuid
-- GROUP BY 1, 2, 3
-- HAVING COUNT(*) > 1;"""
    if old in t:
        t = t.replace(old, new, 1)
        p.write_text(t, encoding="utf-8")
        print("OK settlement_recovery_validation.sql")
    else:
        print("skip validation sql (pattern changed)")


if __name__ == "__main__":
    patch_mappers()
    patch_guard()
    patch_sync_route()
    patch_flush_log()
    patch_universal_importer()
    patch_validation_sql()
