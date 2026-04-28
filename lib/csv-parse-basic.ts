/**
 * Minimal RFC4180-style CSV parser (handles quoted fields and embedded commas).
 * Used client-side for large Amazon ledger files without adding dependencies.
 */

/**
 * Heuristic: scan common Amazon date/time column names and return the first
 * parseable value as an ISO date string (YYYY-MM-DD), or null.
 * Pure function — safe to call in both client and server contexts.
 */
export function guessLedgerSnapshotDate(row: Record<string, string>): string | null {
  const candidates: string[] = [];
  for (const [k, v] of Object.entries(row)) {
    const lk = k.toLowerCase();
    if (!v?.trim()) continue;
    if (
      lk.includes("date") ||
      lk.includes("time") ||
      lk === "snapshot" ||
      lk.includes("event")
    ) {
      candidates.push(v.trim());
    }
  }
  for (const raw of candidates) {
    const d = new Date(raw);
    if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  }
  return null;
}

/**
 * Probes the first few non-empty lines of a text file to decide whether the
 * delimiter is a tab (\t) or a comma (,).
 *
 * Amazon Flat File reports (e.g. Transactions from Reports Repository) are
 * distributed as .txt TSV files.  Naively parsing them with a comma delimiter
 * collapses every row into a single cell and breaks header detection entirely.
 *
 * Strategy: count tab occurrences vs comma occurrences across the first 5
 * non-empty lines. Whichever character appears more frequently is the delimiter.
 * Ties default to comma (standard CSV).
 *
 * Exported so callers can display the detected delimiter or pass it to third-
 * party parsers (e.g. csv-parser on the server side).
 */
export function detectDelimiter(text: string): "\t" | "," {
  const s = text.replace(/^\uFEFF/, "");
  const lines = s.split("\n").filter((l) => l.trim().length > 0).slice(0, 5);
  let tabs = 0;
  let commas = 0;
  for (const line of lines) {
    tabs   += (line.match(/\t/g)  ?? []).length;
    commas += (line.match(/,/g)   ?? []).length;
  }
  return tabs > commas ? "\t" : ",";
}

/**
 * Minimal RFC4180-style CSV / TSV parser (handles quoted fields and embedded
 * separators).  The delimiter is auto-detected via detectDelimiter unless an
 * explicit value is passed.
 *
 * Passing `delimiter` explicitly lets callers override auto-detection when the
 * file extension is known (e.g. ".txt" → "\t").
 */
export function parseCsvToMatrix(text: string, delimiter?: "\t" | ","): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let cur = "";
  let inQuotes = false;
  const s = text.replace(/^\uFEFF/, "");
  // Use the supplied delimiter or auto-detect from the content.
  const sep = delimiter ?? detectDelimiter(s);
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (inQuotes) {
      if (c === '"') {
        if (s[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += c;
      }
    } else if (c === '"') {
      inQuotes = true;
    } else if (c === sep) {
      row.push(cur);
      cur = "";
    } else if (c === "\n") {
      row.push(cur);
      rows.push(row);
      row = [];
      cur = "";
    } else if (c !== "\r") {
      cur += c;
    }
  }
  row.push(cur);
  if (row.some((cell) => cell.length > 0)) rows.push(row);
  return rows;
}

/**
 * A scored set of well-known Amazon CSV column keywords.
 * Used by findHeaderRowIndex to identify the real header row even when
 * several junk/metadata rows precede it (e.g. Settlement reports have 9 such rows).
 */
// Space-based keyword set — matches regardless of hyphens/underscores in actual headers.
// "removal-order-id", "Removal Order ID", and "removal_order_id" all → "removal order id".
const HEADER_KEYWORD_SET = new Set([
  // Universal identifiers / dates
  "order id", "amazon order id", "sku", "msku", "asin", "fnsku",
  "date", "event date", "snapshot date", "posted date", "date time", "date/time",
  "time", "currency", "status", "type", "marketplace", "condition",
  "title", "description", "quantity", "price", "amount", "total",
  "seller sku", "product name", "vendor", "upc", "mfg #", "mfg",
  // FBA Returns
  "license plate number", "return reason code", "detailed disposition",
  // Removal Orders
  "removal order id", "shipped quantity", "requested quantity", "disposed quantity",
  "tracking number",
  // Inventory Ledger
  "ending warehouse balance",
  // Reimbursements
  "reimbursement id", "quantity reimbursed total", "approval date",
  // Settlement
  "settlement id", "transaction status", "deposit date", "net proceeds",
  // SAFE-T Claims — multiple alias forms so score ≥ 2 is reachable with just 2 columns
  "safe t claim id", "safe t", "claim id", "reimbursement amount", "claim status",
  // Transactions / Fee Preview
  "transaction type", "total product charges",
]);

/** Normalize to space-based form — same algorithm as normForDetection in csv-import-detected-type. */
function normForHeaderDetection(h: string): string {
  return (h ?? "")
    .replace(/^\uFEFF/, "")
    .toLowerCase()
    .replace(/[-_]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Aggressive normalization: strips ALL non-alphanumeric characters (including hyphens and underscores).
 * Used for SAFE-T priority detection so every Amazon variant collapses correctly:
 *   "SAFE-T Claim ID" → "safet claim id"
 *   "safe_t_claim_id" → "safet claim id"
 *   "SafeT-ClaimId"   → "safet claimid"
 */
function normAlphanumOnly(h: string): string {
  return (h ?? "")
    .replace(/^\uFEFF/, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Scans up to the first 20 rows of a parsed CSV matrix to find the real header row.
 *
 * Problem: Some Amazon reports (e.g. Settlement) prepend several lines of metadata
 * before the actual column headers, which breaks naive `matrix[0]` header extraction.
 *
 * Strategy: score each row by counting how many of its cells match known Amazon
 * header keywords. The row with the highest score (≥ 2 matches) is the header row.
 * Falls back to row 0 if no row scores high enough.
 *
 * @param matrix - Output of parseCsvToMatrix
 * @returns Zero-based index of the detected header row
 */
export function findHeaderRowIndex(matrix: string[][]): number {
  // ── Priority scan: first 15 rows for SAFE-T / claim markers ──────────────
  //
  // Amazon SAFE-T reports often prepend preamble metadata rows before the real
  // column headers.  We scan the first 15 rows and return the index of the
  // FIRST row whose cells match any SAFE-T header keyword.
  //
  // Two normalization passes are applied in parallel so every Amazon variant
  // is caught regardless of punctuation style:
  //
  //   Pass A — hyphen/underscore → space:
  //     "SAFE-T Claim ID"   → "safe t claim id"
  //     "safe_t_claim_id"   → "safe t claim id"
  //
  //   Pass B — strip ALL non-alphanumeric chars (aggressive):
  //     "SAFE-T Claim ID"   → "safet claim id"
  //     "SafeT-ClaimId"     → "safet claimid"
  //
  // A row is treated as the SAFE-T header row if any of its cells match:
  //   • exact   "safe t claim id"   (Pass A)
  //   • exact   "safet claim id"    (Pass B)
  //   • contains "safe t claim"     (Pass A)
  //   • contains "safet claim"      (Pass B)
  //   • contains both "safe" AND "claim" in the same cell (Pass A or B)
  //   • exact   "claim id"          (Pass A)
  const SAFE_T_LIMIT = Math.min(matrix.length, 15);
  for (let i = 0; i < SAFE_T_LIMIT; i++) {
    const row = matrix[i];
    if (!row || row.length < 2) continue;
    const isSafeTHeaderRow = row.some((cell) => {
      const a = normForHeaderDetection(cell);  // Pass A
      const b = normAlphanumOnly(cell);         // Pass B
      return (
        a === "safe t claim id"    || b === "safet claim id"    ||
        a.includes("safe t claim") || b.includes("safet claim") ||
        (a.includes("safe") && a.includes("claim"))             ||
        (b.includes("safe") && b.includes("claim"))             ||
        a === "claim id"
      );
    });
    if (isSafeTHeaderRow) return i;
  }

  // ── General scoring for all other report types ────────────────────────────
  // Reports Repository files (Transactions, Settlements, …) also prepend metadata
  // rows, so the scan window is capped at 15 to match the SAFE-T priority pass above.
  const limit = Math.min(matrix.length, 15);
  let bestIdx = 0;
  let bestScore = 0;

  for (let i = 0; i < limit; i++) {
    const row = matrix[i];
    if (!row || row.length < 2) continue;
    const score = row.filter((cell) =>
      HEADER_KEYWORD_SET.has(normForHeaderDetection(cell)),
    ).length;
    if (score > bestScore) {
      bestScore = score;
      bestIdx = i;
    }
  }

  return bestScore >= 2 ? bestIdx : 0;
}

export function parseCsvToRecords(text: string): Record<string, string>[] {
  const matrix = parseCsvToMatrix(text.trim());
  if (matrix.length < 2) return [];
  // Use findHeaderRowIndex so Reports-Repository files (SAFE-T, Transactions, etc.)
  // that prepend metadata rows are parsed from the correct header row, not row 0.
  const headerIdx = findHeaderRowIndex(matrix);
  const headers = (matrix[headerIdx] ?? []).map((h) => h.trim());
  const out: Record<string, string>[] = [];
  for (let r = headerIdx + 1; r < matrix.length; r++) {
    const cells = matrix[r];
    if (cells.every((c) => !String(c).trim())) continue;
    const row: Record<string, string> = {};
    for (let j = 0; j < headers.length; j++) {
      const key = headers[j] || `col_${j}`;
      row[key] = cells[j] ?? "";
    }
    out.push(row);
  }
  return out;
}
