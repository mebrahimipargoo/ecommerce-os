/**
 * Technical tracking for `raw_report_uploads.metadata` JSONB.
 * Do not rely on legacy columns (total_bytes, upload_progress, storage_prefix, etc.).
 */

/** Client ledger sessions — must live outside `"use server"` modules (only async exports allowed there). */
export const AMAZON_LEDGER_UPLOAD_SOURCE = "amazon_ledger_uploader" as const;

export type RawReportUploadMetadata = {
  total_bytes?: number;
  storage_prefix?: string;
  upload_progress?: number;
  uploaded_bytes?: number;
  process_progress?: number;
  row_count?: number;
  /** Original extension, e.g. `.csv` */
  file_extension?: string;
  /** Lowercase hex MD5 of full file bytes */
  md5_hash?: string;
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
  failed_phase?: "process" | "sync";
  /** Amazon Inventory Ledger: single object path under `raw-reports` bucket */
  ledger_storage_path?: string;
  /** Target `stores.id` for ledger uploads (tenant scope remains `organization_id` on the row) */
  ledger_store_id?: string;
  /** e.g. `amazon_ledger_uploader` — distinguishes client-only ledger sessions */
  source?: string;
  /** Original CSV header row captured on upload — used to populate manual mapping dropdowns. */
  csv_headers?: string[];
  /**
   * Total data rows in the CSV file (excluding header rows), counted during Phase 1.
   * Used to display "X / Y rows" progress during Phase 2.
   */
  total_rows?: number;
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
  uploadedBytes: number;
  errorMessage: string | null;
  rowCount: number | null;
} {
  const m = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : {};
  const rc = m.row_count;
  const rowCount =
    rc != null && (typeof rc === "number" || typeof rc === "string") ? num(rc, NaN) : NaN;
  return {
    storagePrefix: typeof m.storage_prefix === "string" ? m.storage_prefix : null,
    totalBytes: num(m.total_bytes, 0),
    uploadProgress: Math.min(100, Math.max(0, num(m.upload_progress, 0))),
    processProgress: Math.min(100, Math.max(0, num(m.process_progress, 0))),
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
