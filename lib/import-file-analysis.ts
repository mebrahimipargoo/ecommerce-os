/**
 * Client-side file analysis: MD5, headers, and data row counts for import uploads.
 * Supports .csv, .txt (line-oriented like CSV), and .xlsx (first sheet).
 */

import SparkMD5 from "spark-md5";
import * as XLSX from "xlsx";
import { parseCsvHeaderLine } from "./csv-import-mapping";

const TEXT_CHUNK = 4 * 1024 * 1024;

export function getImportFileExtension(fileName: string): string {
  const lower = fileName.toLowerCase();
  if (lower.endsWith(".xlsx")) return ".xlsx";
  if (lower.endsWith(".csv")) return ".csv";
  if (lower.endsWith(".txt")) return ".txt";
  const dot = fileName.lastIndexOf(".");
  return dot >= 0 ? fileName.slice(dot).toLowerCase() : "";
}

export function isAllowedImportExtension(ext: string): boolean {
  return ext === ".csv" || ext === ".txt" || ext === ".xlsx";
}

/** Full-file MD5 (hex, lowercase). */
export async function computeFileMd5Hex(file: File): Promise<string> {
  const spark = new SparkMD5.ArrayBuffer();
  for (let offset = 0; offset < file.size; offset += TEXT_CHUNK) {
    const slice = file.slice(offset, offset + TEXT_CHUNK);
    const buf = await slice.arrayBuffer();
    spark.append(buf);
  }
  return spark.end();
}

/** Strip trailing empty lines; lines are split on \n (handles \r\n). */
function trimTrailingEmptyLines(lines: string[]): void {
  while (lines.length > 0 && lines[lines.length - 1].trim() === "") {
    lines.pop();
  }
}

/**
 * Data rows only: excludes header row, ignores trailing blank lines.
 * Header-only file → 0 rows.
 */
export function countDataRowsFromDelimitedText(text: string): number {
  const normalized = text.replace(/^\uFEFF/, "");
  const lines = normalized.split(/\r?\n/);
  trimTrailingEmptyLines(lines);
  if (lines.length <= 1) return 0;
  let count = 0;
  for (let i = 1; i < lines.length; i++) {
    if (lines[i].trim() !== "") count++;
  }
  return count;
}

function rowIsEmptyExcel(row: unknown[]): boolean {
  return row.every((c) => {
    if (c == null || c === "") return true;
    if (typeof c === "string") return c.trim() === "";
    return false;
  });
}

/**
 * First sheet only. Row count = non-empty data rows after header row;
 * trailing blank rows skipped.
 */
export function countDataRowsFromXlsxArrayBuffer(buf: ArrayBuffer): number {
  const wb = XLSX.read(buf, { type: "array" });
  const name = wb.SheetNames[0];
  if (!name) return 0;
  const sheet = wb.Sheets[name];
  const rows = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
    raw: false,
  }) as unknown[][];
  while (rows.length > 0 && rowIsEmptyExcel(rows[rows.length - 1] as unknown[])) {
    rows.pop();
  }
  if (rows.length <= 1) return 0;
  let count = 0;
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i] as unknown[];
    if (!rowIsEmptyExcel(r)) count++;
  }
  return count;
}

export async function countDataRowsForFile(file: File, ext: string): Promise<number> {
  if (ext === ".xlsx") {
    const buf = await file.arrayBuffer();
    return countDataRowsFromXlsxArrayBuffer(buf);
  }
  const text = await file.text();
  return countDataRowsFromDelimitedText(text);
}

export async function peekImportFileHeaders(file: File, ext: string): Promise<string[]> {
  if (ext === ".xlsx") {
    const buf = await file.arrayBuffer();
    const wb = XLSX.read(buf, { type: "array" });
    const name = wb.SheetNames[0];
    if (!name) return [];
    const sheet = wb.Sheets[name];
    const first = XLSX.utils.sheet_to_json(sheet, {
      header: 1,
      defval: "",
      raw: false,
    }) as unknown[][];
    const row0 = (first[0] ?? []) as unknown[];
    return row0.map((c) => (c == null ? "" : String(c).trim())).filter((s) => s.length > 0);
  }
  const maxBytes = Math.min(TEXT_CHUNK, file.size);
  const blob = file.slice(0, maxBytes);
  const raw = await blob.text();
  const text = raw.replace(/^\uFEFF/, "");
  const lineEnd = text.indexOf("\n");
  const first = (lineEnd === -1 ? text : text.slice(0, lineEnd)).replace(/\r$/, "");
  return parseCsvHeaderLine(first);
}
