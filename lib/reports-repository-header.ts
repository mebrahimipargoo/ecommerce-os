/**
 * Amazon "Reports Repository" CSV: preamble lines before the real header row.
 * Header row is detected dynamically (keywords in first N lines), not only slice(9).
 */

/** @deprecated Prefer {@link findReportsRepositoryHeaderLineIndex} — kept for migrations/tests. */
export const REPORTS_REPOSITORY_PREAMBLE_LINE_COUNT = 9;

const MAX_HEADER_SCAN = 20;

/** Filename hint (case-insensitive). */
export function fileNameSuggestsReportsRepository(fileName: string): boolean {
  const n = fileName.trim().toLowerCase();
  return n.includes("reports repository") || n.includes("reports-repository");
}

function normLine(s: string): string {
  return s
    .replace(/^\uFEFF/, "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

/** Same token normalization as `normForDetection` in csv-import-detected-type (subset). */
function normHeaderCellForDetection(cell: string): string {
  return (cell ?? "")
    .replace(/^\uFEFF/, "")
    .toLowerCase()
    .replace(/[-_]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * True when a split header row contains the seven Amazon Reports Repository
 * anchor columns (after normalization). Avoids treating preamble prose as data.
 */
export function headerRowHasReportsRepositoryRequiredTokens(line: string): boolean {
  const sep = line.includes("\t") ? "\t" : ",";
  const cells = line.split(sep).map((p) => p.replace(/^"|"$/g, "").trim()).filter(Boolean);
  if (cells.length < 7) return false;
  const ds = new Set(cells.map(normHeaderCellForDetection));
  return (
    ds.has("date/time") &&
    ds.has("settlement id") &&
    ds.has("type") &&
    ds.has("order id") &&
    ds.has("sku") &&
    ds.has("description") &&
    ds.has("total")
  );
}

/**
 * True if this physical line looks like the Reports Repository header row:
 * contains both a date/time column token and settlement id.
 */
export function lineLooksLikeReportsRepositoryHeader(line: string): boolean {
  const lower = line.toLowerCase();
  const hasDateTime =
    lower.includes("date/time") ||
    lower.includes("date-time") ||
    /\bdate\s*,\s*time\b/i.test(line) ||
    (lower.includes("date") && lower.includes("time") && (line.includes("/") || lower.includes("date-time")));
  const hasSettlementId =
    lower.includes("settlement id") ||
    lower.includes("settlement-id") ||
    /\bsettlement\s+id\b/i.test(lower);
  return hasDateTime && hasSettlementId;
}

/** Heuristic: many comma-separated tokens, includes settlement + (date or type or order). */
function regexFallbackHeaderLine(line: string): boolean {
  const t = line.trim();
  if (t.length < 20) return false;
  const sep = t.includes("\t") ? "\t" : ",";
  const parts = t.split(sep).map((p) => p.replace(/^"|"$/g, "").trim()).filter(Boolean);
  if (parts.length < 6) return false;
  const joined = normLine(parts.join(" "));
  return (
    joined.includes("settlement") &&
    joined.includes("id") &&
    (joined.includes("date") || joined.includes("order") || joined.includes("type"))
  );
}

/**
 * Classic Amazon export: first ~9 lines are prose/metadata; line 10 is CSV header.
 */
export function looksLikeClassicNineLinePreamble(lines: string[]): boolean {
  if (lines.length < 10) return false;
  const commaCounts = lines.slice(0, 10).map((ln) => (ln.match(/,/g) ?? []).length);
  const headAvg = commaCounts.slice(0, 8).reduce((a, b) => a + b, 0) / 8;
  const line9 = commaCounts[9] ?? 0;
  return headAvg < 3 && line9 >= 5;
}

export type ReportsRepoHeaderDetection = {
  /** Zero-based line index in the full file where the CSV header row starts. */
  index: number;
  method: "keyword" | "regex" | "classic_preamble_9" | "fallback_zero";
};

/**
 * Scan the first {@link MAX_HEADER_SCAN} lines for a Reports Repository header row.
 * Fallback order: keyword match → regex → classic 9-line preamble → 0.
 */
export function findReportsRepositoryHeaderLineIndex(rawText: string): ReportsRepoHeaderDetection {
  const lines = rawText.split(/\r?\n/);
  const n = Math.min(lines.length, MAX_HEADER_SCAN);

  for (let i = 0; i < n; i++) {
    const ln = lines[i];
    if (lineLooksLikeReportsRepositoryHeader(ln) && headerRowHasReportsRepositoryRequiredTokens(ln)) {
      return { index: i, method: "keyword" };
    }
  }
  for (let i = 0; i < n; i++) {
    if (regexFallbackHeaderLine(lines[i])) {
      return { index: i, method: "regex" };
    }
  }
  if (looksLikeClassicNineLinePreamble(lines)) {
    return { index: REPORTS_REPOSITORY_PREAMBLE_LINE_COUNT, method: "classic_preamble_9" };
  }
  return { index: 0, method: "fallback_zero" };
}

/** Drop all lines before the header row and return CSV text starting at the header. */
export function sliceCsvFromHeaderLine(rawText: string, headerLineIndex: number): string {
  const lines = rawText.split(/\r?\n/);
  const idx = Math.max(0, Math.min(lines.length, Math.floor(headerLineIndex)));
  return lines.slice(idx).join("\n");
}

/** @deprecated Use {@link sliceCsvFromHeaderLine} with {@link findReportsRepositoryHeaderLineIndex}. */
export function stripReportsRepositoryPreamble(rawText: string): string {
  return sliceCsvFromHeaderLine(rawText, REPORTS_REPOSITORY_PREAMBLE_LINE_COUNT);
}

/**
 * True if any of the first ~20 lines looks like a Reports Repository header row
 * (date/time + settlement id), without needing the full file.
 */
export function contentSuggestsReportsRepositorySample(textSample: string): boolean {
  const lines = textSample.split(/\r?\n/).slice(0, 20);
  if (
    lines.some(
      (ln) => lineLooksLikeReportsRepositoryHeader(ln) && headerRowHasReportsRepositoryRequiredTokens(ln),
    )
  ) {
    return true;
  }
  return looksLikeClassicNineLinePreamble(textSample.split(/\r?\n/));
}
