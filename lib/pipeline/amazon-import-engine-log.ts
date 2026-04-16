/**
 * One JSON line per phase transition (stdout).
 * Fields: report_type, upload_id, phase_key, target_table, rows written, skipped, duplicates.
 */
export type AmazonImportPhaseLog = {
  report_type: string;
  upload_id: string;
  /** Alias `phase` — prefer `phase_key` for new callers. */
  phase: string;
  phase_key?: string;
  rows_processed?: number;
  rows_written?: number;
  rows_skipped_existing?: number;
  duplicates_skipped?: number;
  target_table: string | null;
};

export function logImportPhase(fields: AmazonImportPhaseLog): void {
  const line: Record<string, unknown> = {
    engine: "amazon_import",
    report_type: fields.report_type,
    upload_id: fields.upload_id,
    phase_key: fields.phase_key ?? fields.phase,
    phase: fields.phase,
    target_table: fields.target_table,
  };
  if (fields.rows_processed !== undefined) line.rows_processed = fields.rows_processed;
  if (fields.rows_written !== undefined) line.rows_written = fields.rows_written;
  if (fields.rows_skipped_existing !== undefined) line.rows_skipped_existing = fields.rows_skipped_existing;
  if (fields.duplicates_skipped !== undefined) line.duplicates_skipped = fields.duplicates_skipped;
  console.log(JSON.stringify(line));
}

/** Legacy name — forwards to {@link logImportPhase}. */
export function logAmazonImportEngineEvent(fields: {
  report_type: string;
  upload_id: string;
  phase: string;
  target_table: string | null;
  rows_processed?: number;
  generic_rows_written?: number;
}): void {
  logImportPhase({
    report_type: fields.report_type,
    upload_id: fields.upload_id,
    phase: fields.phase,
    target_table: fields.target_table,
    rows_processed: fields.rows_processed ?? fields.generic_rows_written,
  });
}
