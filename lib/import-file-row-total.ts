/**
 * Canonical "file row total" for import progress denominators.
 *
 * Never uses staging table counts, `import_metrics.total_staging_rows`, or
 * `metadata.staging_row_count` as the file total. Avoids treating incremental
 * `data_rows_total` / `data_rows_seen` as the plan total while Phase 2 is still running.
 */

function norm(s: unknown): string {
  return String(s ?? "")
    .trim()
    .toLowerCase();
}

function numPos(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v) && v > 0) return Math.floor(v);
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    if (Number.isFinite(n) && n > 0) return Math.floor(n);
  }
  return null;
}

export type ImportFileRowTotalResult = {
  /** Parsed / upload-metadata plan total; null if unknown mid-run. */
  total: number | null;
  verificationPending: boolean;
};

/**
 * Resolve the fixed file row total for `processed / total` style progress.
 *
 * Priority: `fps.total_rows`, `metadata.total_rows`, `fps.file_rows_total`,
 * listing physical-line hints — then, only after Phase 2 is complete, incremental
 * counters (`data_rows_total`, `data_rows_seen`) as a last-resort fallback.
 */
export function resolveImportFileRowTotal(input: {
  fps?: Record<string, unknown> | null;
  metadata?: Record<string, unknown> | null;
}): ImportFileRowTotalResult {
  const f = input.fps && typeof input.fps === "object" && !Array.isArray(input.fps) ? input.fps : {};
  const m =
    input.metadata && typeof input.metadata === "object" && !Array.isArray(input.metadata)
      ? input.metadata
      : {};
  const im = m.import_metrics as Record<string, unknown> | undefined;

  const verificationPending =
    m.staging_final_count_verify_pending === true ||
    im?.staging_final_count_verify_pending === true ||
    m.sync_count_verification_pending === true ||
    im?.sync_count_verification_pending === true;

  const stableCandidates = [
    numPos(f.total_rows),
    numPos(m.total_rows),
    numPos(f.file_rows_total),
    numPos(m.catalog_listing_file_rows_seen),
    numPos(m.catalog_listing_total_rows_seen),
  ];
  const stable = stableCandidates.find((x) => x != null);
  if (stable != null) {
    return { total: stable, verificationPending };
  }

  const p2 = norm(f.phase2_status);
  const cur = norm(f.current_phase ?? m.etl_phase);
  const phase2Done =
    p2 === "complete" ||
    cur === "staged" ||
    cur === "sync" ||
    cur === "raw_synced" ||
    cur === "complete" ||
    cur === "generic";

  if (phase2Done) {
    const tail =
      numPos(f.data_rows_total) ?? numPos(m.data_rows_seen) ?? numPos(m.catalog_listing_data_rows_seen);
    if (tail != null) return { total: tail, verificationPending };
  }

  return { total: null, verificationPending };
}
