/**
 * Technical tracking for `raw_report_uploads.metadata` JSONB.
 * Do not rely on legacy columns (total_bytes, upload_progress, storage_prefix, etc.).
 */

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
