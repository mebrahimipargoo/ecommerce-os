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
export function parseCsvToMatrix(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let cur = "";
  let inQuotes = false;
  const s = text.replace(/^\uFEFF/, "");
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
    } else if (c === ",") {
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

export function parseCsvToRecords(text: string): Record<string, string>[] {
  const matrix = parseCsvToMatrix(text.trim());
  if (matrix.length < 2) return [];
  const headers = matrix[0].map((h) => h.trim());
  const out: Record<string, string>[] = [];
  for (let r = 1; r < matrix.length; r++) {
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
