/**
 * Technical tracking for `raw_report_uploads.metadata` JSONB.
 * Do not rely on legacy columns (total_bytes, upload_progress, storage_prefix, etc.).
 */

/** Client ledger sessions — must live outside `"use server"` modules (only async exports allowed there). */
export const AMAZON_LEDGER_UPLOAD_SOURCE = "amazon_ledger_uploader" as const;

/** Per-run counters surfaced in Import History (written by stage/sync routes). */
export type ImportRunMetrics = {
  physical_lines_seen?: number;
  data_rows_seen?: number;
  rows_staged?: number;
  rows_synced_upserted?: number;
  rows_mapper_invalid?: number;
  /** Lines merged in JS before upsert (duplicate conflict key in same batch). */
  rows_duplicate_in_file?: number;
  rows_net_collapsed_vs_staging?: number;
  rows_synced_new?: number;
  rows_synced_updated?: number;
  rows_synced_unchanged?: number;
  rows_duplicate_against_existing?: number;
  /** Live Phase 3 flush progress (sync route bump). */
  rows_synced?: number;
  total_staging_rows?: number;
  /** File plan total (parsed / upload metadata), not staging row count. */
  file_row_total_plan?: number;
  /** Staging row count query failed or totals could not be reconciled. */
  sync_count_verification_pending?: boolean;
  rows_invalid?: number;
  rows_skipped_empty?: number;
  rows_skipped_malformed?: number;
  current_phase?:
    | "upload"
    | "staging"
    | "process"
    | "staged"
    | "sync"
    | "raw_synced"
    | "generic"
    | "complete"
    | "failed";
  failure_reason?: string;
  // ── Product Identity validation metrics (mirrors metadata.product_identity_validation).
  // Surfaced here so the unified `import_metrics` blob the UI/SQL inspects has the same shape
  // for every report type that exposes parsed-vs-synced counters.
  detected_headers?: string[];
  detected_report_type?: string;
  rows_parsed?: number;
  products_upserted?: number;
  catalog_products_upserted?: number;
  identifiers_upserted?: number;
  invalid_identifier_counts?: {
    asin?: number;
    fnsku?: number;
    upc?: number;
    total?: number;
  };
  /** Product Identity intra-batch dedupe counters (see lib/product-identity-import.ts). */
  normalized_rows_count?: number;
  unique_product_sku_count?: number;
  duplicate_sku_count?: number;
  duplicate_sku_conflict_count?: number;
  catalog_unique_count?: number;
  identifier_unique_count?: number;
  /** Per-row CSV diagnostics (Product Identity). */
  rows_missing_seller_sku?: number;
  rows_invalid_seller_sku?: number;
  rows_skipped?: number;
  skipped_reason_counts?: {
    missing_seller_sku?: number;
    invalid_seller_sku?: number;
  };
  invalid_sku_examples?: { rowNumber: number; rawValue: string; reason: string }[];
  /** Informational note surfaced in import_metrics for debugging. */
  note?: string;
  /** Phase 2 operator-facing line (Import History / importer card). */
  phase2_operator_line?: string;
  /**
   * Phase 2 UI state — Nano retries, deferred DB count verification, batch exhaustion.
   */
  phase2_operator_state?:
    | "processing"
    | "retrying_batch"
    | "waiting_before_retry"
    | "count_verification_delayed"
    | "final_verification_pending"
    | "failed_after_retries"
    | "resume_available"
    | "completed";
  phase2_operator_batch?: number;
  phase2_operator_row_range?: string;
  phase2_operator_batch_attempts?: number;
  /** True when end-of-run DB count could not be confirmed; parser counters trusted. */
  staging_final_count_verify_pending?: boolean;
  /** Target staging table for the current phase (Product Identity pipeline). */
  stage_target_table?: string;
};

export type RawReportUploadMetadata = {
  /** Structured counters for the operator-facing import summary. */
  import_metrics?: ImportRunMetrics;
  /** Lines seen in the CSV (including blanks); see stage route. */
  physical_lines_seen?: number;
  /** Lines that passed date filter and were staged (Phase 2). */
  data_rows_seen?: number;
  sync_duplicate_in_batch_rows?: number;
  total_bytes?: number;
  storage_prefix?: string;
  upload_progress?: number;
  uploaded_bytes?: number;
  process_progress?: number;
  /** Phase 3 (Sync): 0–100 while domain upserts run in chunks. */
  sync_progress?: number;
  /** Phase 4 (Removal imports): 0–100 while expected_packages worklist is built via FastAPI. */
  worklist_progress?: number;
  /** Set when POST /etl/generate-worklist (or Next proxy) finishes successfully. */
  worklist_completed?: boolean;
  /** Which ETL phase last wrote progress (upload | staging | sync | worklist). */
  etl_phase?:
    | "upload"
    | "staging"
    | "sync"
    | "raw_synced"
    | "generic"
    | "complete"
    | "worklist";
  row_count?: number;
  /** Rows in amazon_staging before Phase 3 (for UI: staged vs synced). */
  staging_row_count?: number;
  /** Phase 2 CSV stream: contiguous-prefix watermark (not a verified DB row count). */
  staging_contiguous_watermark?: number;
  /** Phase 3: staging count vs file plan could not be verified (see sync route). */
  sync_count_verification_pending?: boolean;
  /** Rows written in Phase 3 after dedupe / shipment archive (see sync route). */
  sync_row_count?: number;
  /** Staging rows where the domain mapper returned null (Phase 3). */
  sync_mapper_null_count?: number;
  /**
   * Staging lines merged into another row because they shared the same DB conflict key
   * (after mapper). staging ≈ sync_row_count + sync_mapper_null_count + this.
   */
  sync_collapsed_by_dedupe?: number;
  /** Original extension, e.g. `.csv` */
  file_extension?: string;
  /** Lowercase hex MD5 of full file bytes (legacy; may be first 32 hex of SHA-256 for compat) */
  md5_hash?: string;
  /** Lowercase hex SHA-256 of entire file — used to replace prior REMOVAL_ORDER imports when re-uploading the same file */
  content_sha256?: string;
  /** Mirror of `raw_report_uploads.file_name` for JSON-only consumers */
  file_name?: string;
  file_size_bytes?: number;
  /** Planned chunk count for this upload */
  upload_chunks_count?: number;
  /** Chunk pipeline: last completed part index (0-based) and total parts for this file */
  last_part_index?: number;
  total_parts?: number;
  /** Optional error detail for failed uploads (stored in metadata only — no error_log column) */
  error_message?: string;
  /**
   * Set by the stage/sync route when they write status='failed'.
   * "process" = Phase 2 (Stage) failed.
   * "sync"    = Phase 3 (Sync) failed.
   * Used by the History table to show the correct "Retry" button.
   */
  failed_phase?: "process" | "sync" | "generic";
  /** Amazon Inventory Ledger: single object path under `raw-reports` bucket */
  ledger_storage_path?: string;
  /** Target `stores.id` for ledger uploads (tenant scope remains `organization_id` on the row) */
  ledger_store_id?: string;
  /** Imports Target Store (Wave 1) — propagated to removal domain tables */
  import_store_id?: string;
  /** e.g. `amazon_ledger_uploader` — distinguishes client-only ledger sessions */
  source?: string;
  /** Original CSV header row captured on upload — used to populate manual mapping dropdowns. */
  csv_headers?: string[];
  /**
   * Total data rows in the CSV file (excluding header rows), counted during Phase 1.
   * Used to display "X / Y rows" progress during Phase 2.
   */
  total_rows?: number;
  /** Listing import: raw_archive → canonical_sync → done (POST /imports/process). */
  catalog_listing_import_phase?:
    | "staged"
    | "raw_archive"
    | "raw_archived"
    | "canonical_sync"
    | "done";
  /** Physical data lines after header (pass 1). */
  catalog_listing_file_rows_seen?: number;
  /** Non-empty data lines after header (physical lines minus empty lines). User-facing row count. */
  catalog_listing_data_rows_seen?: number;
  /** Rows inserted into amazon_listing_report_rows_raw. */
  catalog_listing_raw_rows_stored?: number;
  catalog_listing_raw_rows_skipped_empty?: number;
  catalog_listing_raw_rows_skipped_malformed?: number;
  /** True new catalog rows this run (same as legacy inserted when re-import unchanged). */
  catalog_listing_canonical_rows_new?: number;
  catalog_listing_canonical_rows_updated?: number;
  catalog_listing_canonical_rows_unchanged?: number;
  catalog_listing_canonical_rows_invalid_for_merge?: number;
  /** @deprecated Use catalog_listing_canonical_rows_new */
  catalog_listing_canonical_rows_inserted?: number;
  catalog_listing_canonical_rows_unchanged_or_merged?: number;
  /** @deprecated Use catalog_listing_file_rows_seen */
  catalog_listing_total_rows_seen?: number;
  /** Non-fatal Phase 4 note when `product_identifier_map` sync failed after `catalog_products` upsert. */
  catalog_listing_identifier_map_sync_error?: string | null;
  /**
   * Rows that passed the date filter and were actually inserted into staging during Phase 2.
   * May be less than total_rows when a date range is applied.
   */
  processed_rows?: number;
  /** Date range filter applied during Phase 2 — ISO date string (YYYY-MM-DD). */
  start_date?: string | null;
  end_date?: string | null;
  /** Whether the full file was imported ignoring any date range. */
  import_full_file?: boolean;
  /**
   * Stable fingerprint of sorted CSV headers (e.g. "asin|fnsku|quantity|...").
   * Used by Mapping Memory to find a prior successful mapping for the same file format.
   */
  headers_fingerprint?: string;
  /**
   * Full Storage path for a single-object upload (Phase 1 non-chunked flow).
   * When present, the stage route reads this file directly instead of reading
   * concatenated part-N files.
   * Example: `{orgId}/{timestamp}-{random}/original.csv`
   */
  raw_file_path?: string;
  /** REMOVAL_SHIPMENT Phase 3: rows upserted into amazon_removal_shipments. */
  removal_shipment_phase3_raw_written?: number;
  /** REMOVAL_SHIPMENT Phase 3: logical lines skipped (already archived from another upload). */
  removal_shipment_phase3_skipped_cross_upload?: number;
  /** REMOVAL_SHIPMENT: archive rows for this upload after Phase 3 (Phase 4 denominator). */
  removal_shipment_lines_for_generic?: number;
  /** REMOVAL_SHIPMENT Phase 4: shipment lines used as generic progress denominator. */
  removal_shipment_phase4_generic_rows_written?: number;
  /**
   * Product Identity validation block — written by the process / sync routes
   * for `report_type = 'PRODUCT_IDENTITY'`. Mirrors the same keys exposed
   * inside `metadata.import_metrics` so the validation SQL can read either.
   */
  product_identity_validation?: {
    detected_headers: string[];
    detected_report_type: "PRODUCT_IDENTITY";
    rows_parsed: number;
    rows_synced: number;
    products_upserted: number;
    catalog_products_upserted: number;
    identifiers_upserted: number;
    invalid_identifier_counts: {
      asin: number;
      fnsku: number;
      upc: number;
      total: number;
    };
    /** Intra-batch dedupe stats (see lib/product-identity-import.ts). */
    normalized_rows_count?: number;
    unique_product_sku_count?: number;
    duplicate_sku_count?: number;
    duplicate_sku_conflict_count?: number;
    catalog_unique_count?: number;
    identifier_unique_count?: number;
    /** Per-row CSV diagnostics (Product Identity). */
    rows_missing_seller_sku?: number;
    rows_invalid_seller_sku?: number;
    rows_skipped?: number;
    skipped_reason_counts?: {
      missing_seller_sku?: number;
      invalid_seller_sku?: number;
    };
    invalid_sku_examples?: { rowNumber: number; rawValue: string; reason: string }[];
  };
  /** SETTLEMENT Phase 3 pre-flight: mapping guard blocked sync (operator reviews mapping report). */
  settlement_mapping_guard_blocked?: boolean;
  settlement_mapping_guard_reason?: string;
  settlement_mapping_guard_summary?: {
    mapperAcceptedSample?: number;
    mapperRejectedSample?: number;
    lowConfidenceFinancialKeys?: string[];
  };
};

function num(v: unknown, fallback = 0): number {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return fallback;
}

export function parseRawReportMetadata(raw: unknown): {
  storagePrefix: string | null;
  totalBytes: number;
  uploadProgress: number;
  processProgress: number;
  syncProgress: number;
  uploadedBytes: number;
  errorMessage: string | null;
  rowCount: number | null;
} {
  const m = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : {};
  const rc = m.row_count;
  const tr = m.total_rows;
  const rowCountFromRowCount =
    rc != null && (typeof rc === "number" || typeof rc === "string") ? num(rc, NaN) : NaN;
  const rowCountFromTotal =
    tr != null && (typeof tr === "number" || typeof tr === "string") ? num(tr, NaN) : NaN;
  const rowCount =
    Number.isFinite(rowCountFromRowCount) && rowCountFromRowCount >= 0
      ? rowCountFromRowCount
      : Number.isFinite(rowCountFromTotal) && rowCountFromTotal >= 0
        ? rowCountFromTotal
        : NaN;
  return {
    storagePrefix: typeof m.storage_prefix === "string" ? m.storage_prefix : null,
    totalBytes: num(m.total_bytes, 0),
    uploadProgress: Math.min(100, Math.max(0, num(m.upload_progress, 0))),
    processProgress: Math.min(100, Math.max(0, num(m.process_progress, 0))),
    syncProgress: Math.min(100, Math.max(0, num(m.sync_progress, 0))),
    uploadedBytes: num(m.uploaded_bytes, 0),
    errorMessage: typeof m.error_message === "string" ? m.error_message : null,
    rowCount: Number.isFinite(rowCount) ? rowCount : null,
  };
}

export function mergeUploadMetadata(
  prev: unknown,
  patch: Partial<RawReportUploadMetadata>,
): RawReportUploadMetadata {
  const base =
    prev && typeof prev === "object" && !Array.isArray(prev)
      ? { ...(prev as Record<string, unknown>) }
      : {};
  return { ...base, ...patch } as RawReportUploadMetadata;
}
