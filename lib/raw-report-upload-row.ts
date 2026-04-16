import type { ImportFpsSnapshot } from "./import-ui-action-state";

/**
 * Shape of `raw_report_uploads` rows for admin import UI (not exported from `"use server"` files).
 */
export type RawReportUploadRow = {
  id: string;
  organization_id: string;
  file_name: string;
  /** Smart-import canonical (FBA_RETURNS, …) or legacy Amazon slug — `raw_report_uploads.report_type`. */
  report_type: string;
  /** Parsed from `metadata.storage_prefix`. */
  storage_prefix: string | null;
  /** Lifecycle + sync: pending | uploading | processing | synced | failed | … */
  status: string;
  /** Parsed from `metadata.upload_progress`. */
  upload_progress: number;
  /** Parsed from `metadata.process_progress`. */
  process_progress: number;
  /** Parsed from `metadata.uploaded_bytes`. */
  uploaded_bytes: number;
  /** Parsed from `metadata.total_bytes`. */
  total_bytes: number;
  row_count: number | null;
  column_mapping: Record<string, string> | null;
  /** Parsed from `metadata.error_message` only. */
  errorMessage: string | null;
  /** Raw JSONB for advanced UI. */
  metadata: Record<string, unknown> | null;
  /** FK to `profiles.id` — who created this upload row. */
  created_by: string | null;
  created_at: string;
  updated_at: string;
  /** Optional client-only label — not resolved from DB joins. */
  created_by_name?: string | null;
  /** Joined from `file_processing_status` (phase columns) when listing uploads. */
  file_processing_status?: ImportFpsSnapshot | null;
};
