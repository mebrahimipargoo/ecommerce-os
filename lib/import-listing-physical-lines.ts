/**
 * Listing raw archive: one DB row per physical file line (after header).
 * This avoids csv-parser merging quoted newlines into fewer "logical" rows.
 */

import { createHash } from "node:crypto";
import type { Readable } from "node:stream";

import {
  applyColumnMappingToRow,
  extractListingIdentifiersForRawRow,
  listingMappedRowToRawPayload,
  normalizeAmazonReportRowKeys,
} from "./import-sync-mappers";

export type ListingPhysicalPass1Metrics = {
  file_rows_seen: number;
  raw_rows_stored: number;
  raw_rows_skipped_empty: number;
  raw_rows_skipped_malformed: number;
};

export async function streamToBuffer(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

/** Normalize newlines; strip leading BOM from first line only (caller). */
export function splitPhysicalLines(body: string): string[] {
  const t = body.replace(/^\uFEFF/, "");
  return t.split(/\r?\n/);
}

export function splitDataLineIntoCells(line: string, separator: "\t" | ","): string[] {
  if (separator === "\t") {
    return line.split("\t");
  }
  const out: string[] = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      inQuotes = !inQuotes;
      continue;
    }
    if (c === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += c;
  }
  out.push(cur);
  return out;
}

function sha256Hex(s: string): string {
  return createHash("sha256").update(s, "utf8").digest("hex");
}

function trimHeaderCell(h: string): string {
  return h.replace(/^\uFEFF/, "").trim();
}

export type BuildRawRowsParams = {
  lines: string[];
  separator: "\t" | ",";
  columnMapping: Record<string, string> | null;
  organizationId: string;
  storeId: string | null;
  sourceUploadId: string;
  sourceReportType: string;
};

/**
 * For each physical data line (lines[1..]), produce one insert payload for amazon_listing_report_rows_raw.
 * Header is lines[0]. Empty lines: no row (caller increments skipped_empty).
 */
export function buildRawRowInsertForPhysicalLine(
  params: BuildRawRowsParams & {
    lineIdx: number;
    rawLine: string;
  },
): Record<string, unknown> | null {
  const { lines, separator, columnMapping, organizationId, storeId, sourceUploadId, sourceReportType, lineIdx, rawLine } =
    params;

  if (lineIdx < 1 || lineIdx >= lines.length) return null;

  const trimmed = rawLine.trim();
  if (trimmed === "") {
    return null;
  }

  const headerCells = splitDataLineIntoCells(trimHeaderCell(lines[0]), separator).map(trimHeaderCell);
  const dataCells = splitDataLineIntoCells(rawLine, separator);
  const fileLineNumber = lineIdx + 1;

  let parseStatus: "parsed" | "skipped_malformed" = "parsed";
  let parseError: string | null = null;

  if (headerCells.length === 0) {
    return {
      organization_id: organizationId,
      store_id: storeId,
      source_upload_id: sourceUploadId,
      source_report_type: sourceReportType,
      row_number: fileLineNumber,
      seller_sku: null,
      asin: null,
      listing_id: null,
      raw_payload: { _unparsed_line: rawLine },
      source_line_hash: sha256Hex(rawLine),
      parse_status: "skipped_malformed",
      parse_error: "Missing header row",
    };
  }

  const row: Record<string, string> = {};
  for (let j = 0; j < headerCells.length; j++) {
    const key = headerCells[j] ?? `column_${j}`;
    row[key] = dataCells[j] ?? "";
  }

  if (dataCells.length === 0 && trimmed.length > 0) {
    parseStatus = "skipped_malformed";
    parseError = "No cells parsed for non-empty line";
  }

  const mappedRow = applyColumnMappingToRow(normalizeAmazonReportRowKeys(row), columnMapping);
  const ids = extractListingIdentifiersForRawRow(mappedRow);

  return {
    organization_id: organizationId,
    store_id: storeId,
    source_upload_id: sourceUploadId,
    source_report_type: sourceReportType,
    row_number: fileLineNumber,
    seller_sku: ids.seller_sku?.trim() || null,
    asin: ids.asin?.trim() || null,
    listing_id: ids.listing_id?.trim() || null,
    raw_payload: listingMappedRowToRawPayload(mappedRow),
    source_line_hash: sha256Hex(rawLine),
    parse_status: parseStatus,
    parse_error: parseError,
  };
}
