import "server-only";

import csv from "csv-parser";
import { Readable } from "node:stream";
import type { SupabaseClient } from "@supabase/supabase-js";

import { mergeUploadMetadata } from "./raw-report-upload-metadata";
import { isUuidString } from "./uuid";

type CsvRow = Record<string, string>;

type NormalizedRow = {
  rowNumber: number;
  original: CsvRow;
  sku: string;
  productName: string;
  vendorName: string | null;
  mfgPartNumber: string | null;
  asin: string | null;
  fnsku: string | null;
  upc: string | null;
};

/**
 * One canonical row per (organization_id, store_id, sku). Built by
 * `dedupeNormalizedRowsBySku` so the products upsert never receives two
 * rows that would conflict against `products_organization_store_sku_key`
 * inside a single Postgres command (the cause of error 21000:
 * "ON CONFLICT DO UPDATE command cannot affect row a second time").
 *
 * Conflict tracking:
 *   * `mergedRows` records every original CSV row number that contributed.
 *   * `conflictingFields` lists the fields that disagreed across duplicates.
 *   * `alternativeIdentifiers` keeps the rejected ASIN/FNSKU/UPC values so
 *     the identifier map and the backlog can still surface them.
 */
type DedupedProductRow = NormalizedRow & {
  duplicateCount: number;
  mergedRows: { rowNumber: number; original: CsvRow }[];
  conflictingFields: string[];
  alternativeIdentifiers: {
    asin: string[];
    fnsku: string[];
    upc: string[];
  };
};

type ProductRecord = {
  id: string;
  sku: string;
  product_name?: string | null;
  vendor_name?: string | null;
  mfg_part_number?: string | null;
  upc_code?: string | null;
  asin?: string | null;
  fnsku?: string | null;
  metadata?: unknown;
  /**
   * Per-field provenance map written by the enrichment merge SQL functions
   * (see migration `20260635_product_identity_enrichment_priority.sql`).
   * Used to refuse downgrade overwrites of trusted ASIN/FNSKU values that
   * came from a higher-priority source (e.g. All Listings owns ASIN at 100,
   * Product Identity ships ASIN at 90 — we will not overwrite it).
   */
  field_provenance?: Record<string, FieldProvenanceEntry> | null;
};

/**
 * One field's provenance record. Mirrors the JSONB shape produced by
 * `identity_merge_build_provenance(...)` in SQL.
 */
type FieldProvenanceEntry = {
  source?: string | null;
  priority?: number | null;
  confidence?: number | null;
  written_at?: string | null;
  upload_id?: string | null;
};

/**
 * Per-field priority used by Product Identity CSV writes. Higher beats lower.
 * Mirrors the priorities defined in the SQL migration so the TS path and the
 * SQL merge functions stay in lockstep.
 */
const PRODUCT_IDENTITY_PRIORITY = {
  product_name: 100,
  vendor_name: 100,
  mfg_part_number: 100,
  upc_code: 100,
  asin: 90,
  fnsku: 60,
} as const;

const PRODUCT_IDENTITY_SOURCE = "product_identity";

type CatalogRecord = {
  id: string;
  seller_sku: string | null;
  asin: string | null;
  fnsku?: string | null;
  item_name?: string | null;
  raw_payload?: unknown;
};

type IdentifierType = "SKU" | "ASIN" | "FNSKU" | "UPC";

type IdentifierMapInsert = {
  organization_id: string;
  product_id: string;
  catalog_product_id: string | null;
  store_id: string;
  seller_sku: string | null;
  asin: string | null;
  fnsku: string | null;
  upc_code: string | null;
  msku: string | null;
  title: string | null;
  external_listing_id: string;
  source_upload_id: string;
  source_report_type: string;
  source_file_sha256: string;
  source_physical_row_number: number;
  match_source: string;
  inventory_source: null;
  confidence_score: number;
  linked_from_report_family: string;
  linked_from_target_table: string;
  first_seen_at?: string;
  last_seen_at: string;
  is_primary: boolean;
};

type ExistingIdentifierMapRow = {
  id: string;
  product_id: string | null;
  store_id: string | null;
  seller_sku: string | null;
  asin: string | null;
  fnsku: string | null;
  upc_code: string | null;
  external_listing_id: string | null;
};

export type ProductIdentityImportStats = {
  rowsRead: number;
  productsInserted: number;
  productsUpdated: number;
  catalogProductsInserted: number;
  catalogProductsUpdated: number;
  identifiersInserted: number;
  invalidAsinCount: number;
  invalidFnskuCount: number;
  invalidUpcCount: number;
  invalidIdentifierCount: number;
  ambiguousIdentifierCount: number;
  unresolvedRows: number;
  /** Number of normalized CSV rows produced (before any dedupe). */
  normalizedRowsCount: number;
  /** Distinct (org, store, sku) groups after intra-batch dedupe — what is actually upserted into products. */
  uniqueProductSkuCount: number;
  /** Normalized rows that collapsed because another row shared the same (org, store, sku). */
  duplicateSkuCount: number;
  /** Of duplicate SKUs, how many had inconsistent ASIN/FNSKU/UPC values across the duplicates. */
  duplicateSkuConflictCount: number;
  /** Distinct (org, store, seller_sku, asin) catalog rows after dedupe. */
  catalogUniqueCount: number;
  /** Distinct external_listing_id rows actually written to product_identifier_map. */
  identifierUniqueCount: number;
  // ── Per-row CSV diagnostics (added 2026-04-29 to explain "rows_parsed - 1") ──
  /** CSV rows where Seller SKU was empty / missing entirely. */
  rowsMissingSellerSku: number;
  /**
   * CSV rows where Seller SKU was present but rejected by `normalizeIdentifier`
   * (placeholder values like "x", "0", "fbm", "unknown", "null", or Excel
   * error tokens like "#NAME?", "#REF!", "#VALUE!", "#DIV/0!", "#N/A").
   */
  rowsInvalidSellerSku: number;
  /** rowsMissingSellerSku + rowsInvalidSellerSku — rows that did not become a NormalizedRow. */
  rowsSkipped: number;
  /** Per-reason breakdown of skipped rows. */
  skippedReasonCounts: {
    missing_seller_sku: number;
    invalid_seller_sku: number;
  };
  /**
   * Up to MAX_INVALID_SKU_EXAMPLES rejected SKU values with their CSV row
   * numbers. Surfaced into metadata so an operator can fix the source file.
   */
  invalidSkuExamples: { rowNumber: number; rawValue: string; reason: string }[];
};

export type ProductIdentityColumnMapping = Partial<Record<
  "upc" | "vendor" | "seller_sku" | "mfg_part_number" | "fnsku" | "asin" | "product_name",
  string
>>;

export const PRODUCT_IDENTITY_REPORT_TYPE = "PRODUCT_IDENTITY";

const SOURCE_REPORT_TYPE = "PRODUCT_IDENTITY_IMPORT";
const IDENTIFIER_IGNORE_VALUES = new Set([
  "",
  "x",
  "0",
  "fbm",
  "this one is good",
  "unknown",
  "null",
  // Excel formula-error tokens that show up when a CSV is exported from a
  // workbook with broken references / lookups. They are NOT valid SKUs.
  "#name?",
  "#ref!",
  "#value!",
  "#div/0!",
  "#n/a",
  "#null!",
  "#num!",
]);
const ASIN_RE = /^B[0-9A-Z]{9}$/;
const FNSKU_RE = /^X[0-9A-Z]{9}$/;
const UPC_RE = /^[0-9]{8,14}$/;
const PRODUCT_UPSERT_CONFLICT = "organization_id,store_id,sku";
const CATALOG_UPSERT_CONFLICT = "organization_id,store_id,seller_sku,asin";
const BACKLOG_UPSERT_CONFLICT = "organization_id,store_id,identifier_type,identifier_value,reason,seller_sku";
/**
 * Matches the partial unique index `uq_product_identifier_map_product_identity`
 * created in migration 20260636. The partial WHERE clause restricts uniqueness
 * to product_identity rows, so listings/ledger bridge rows are never affected
 * by this upsert path.
 */
const CHUNK_SIZE = 250;

const COLUMN_ALIASES: Record<keyof ProductIdentityColumnMapping, string[]> = {
  upc: ["UPC", "UPC Code", "upc_code", "barcode", "gtin"],
  vendor: ["Vendor", "vendor_name", "vendor name", "brand"],
  seller_sku: ["Seller SKU", "seller-sku", "seller sku", "sku", "MSKU", "msku"],
  mfg_part_number: [
    "Mfg #",
    "Mfg#",
    "Mfg No",
    "Mfg Number",
    "Manufacturer Part Number",
    "manufacturer-part-number",
    "mfg_part_number",
  ],
  fnsku: ["FNSKU", "fulfillment-network-sku", "fulfillment channel sku"],
  asin: ["ASIN", "asin1", "product-id", "product id"],
  product_name: ["Product Name", "product-name", "item-name", "item name", "title", "description"],
};

export const MAX_INVALID_SKU_EXAMPLES = 10;

export function emptyProductIdentityImportStats(): ProductIdentityImportStats {
  return {
    rowsRead: 0,
    productsInserted: 0,
    productsUpdated: 0,
    catalogProductsInserted: 0,
    catalogProductsUpdated: 0,
    identifiersInserted: 0,
    invalidAsinCount: 0,
    invalidFnskuCount: 0,
    invalidUpcCount: 0,
    invalidIdentifierCount: 0,
    ambiguousIdentifierCount: 0,
    unresolvedRows: 0,
    normalizedRowsCount: 0,
    uniqueProductSkuCount: 0,
    duplicateSkuCount: 0,
    duplicateSkuConflictCount: 0,
    catalogUniqueCount: 0,
    identifierUniqueCount: 0,
    rowsMissingSellerSku: 0,
    rowsInvalidSellerSku: 0,
    rowsSkipped: 0,
    skippedReasonCounts: {
      missing_seller_sku: 0,
      invalid_seller_sku: 0,
    },
    invalidSkuExamples: [],
  };
}

function stripBom(value: string): string {
  return value.replace(/^\uFEFF/, "");
}

function normHeader(value: string): string {
  return stripBom(value)
    .trim()
    .toLowerCase()
    .replace(/[-_]/g, " ")
    .replace(/[^a-z0-9#\s]/g, "")
    .replace(/\s+/g, " ");
}

function cell(row: CsvRow, header: string): string {
  if (row[header] != null) return String(row[header]);

  const wanted = normHeader(header);
  const foundKey = Object.keys(row).find((key) => normHeader(key) === wanted);
  return foundKey ? String(row[foundKey] ?? "") : "";
}

function mappedCell(
  row: CsvRow,
  mapping: ProductIdentityColumnMapping | null | undefined,
  key: keyof ProductIdentityColumnMapping,
): string {
  const mapped = mapping?.[key]?.trim();
  if (mapped) {
    const value = cell(row, mapped);
    if (value !== "") return value;
  }

  for (const alias of COLUMN_ALIASES[key]) {
    const value = cell(row, alias);
    if (value !== "") return value;
  }

  return "";
}

function normalizeIdentifier(raw: string): string | null {
  const trimmed = String(raw ?? "").trim();
  if (IDENTIFIER_IGNORE_VALUES.has(trimmed.toLowerCase())) return null;
  return trimmed;
}

function normalizeAsin(raw: string, stats: ProductIdentityImportStats): string | null {
  const value = normalizeIdentifier(raw);
  if (value == null) return null;

  const normalized = value.toUpperCase();
  if (!ASIN_RE.test(normalized)) {
    stats.invalidAsinCount += 1;
    return null;
  }
  return normalized;
}

function normalizeFnsku(raw: string, stats: ProductIdentityImportStats): string | null {
  const value = normalizeIdentifier(raw);
  if (value == null) return null;

  const normalized = value.toUpperCase();
  if (!FNSKU_RE.test(normalized)) {
    stats.invalidFnskuCount += 1;
    return null;
  }
  return normalized;
}

function normalizeUpc(raw: string, stats: ProductIdentityImportStats): string | null {
  const value = normalizeIdentifier(raw);
  if (value == null) return null;

  if (!UPC_RE.test(value)) {
    stats.invalidUpcCount += 1;
    return null;
  }
  return value;
}

function asPlainRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

/**
 * Result of normalizing a single CSV row.
 *
 * The previous implementation returned `NormalizedRow | null` and only
 * incremented `stats.unresolvedRows`, which made it impossible for the
 * operator to tell the difference between:
 *   * "rows that had no Seller SKU at all" (most common — empty cells), and
 *   * "rows that had a Seller SKU value but it was a placeholder / Excel
 *     error token" (rare; these should be fixed in the source file).
 *
 * The diagnostic version below returns a discriminated result so the caller
 * can record both buckets and capture a few examples of bad SKU values.
 */
type NormalizedRowResult =
  | { kind: "ok"; row: NormalizedRow }
  | { kind: "missing_seller_sku" }
  | { kind: "invalid_seller_sku"; rawValue: string };

function normalizeRow(
  row: CsvRow,
  rowNumber: number,
  stats: ProductIdentityImportStats,
  mapping?: ProductIdentityColumnMapping | null,
): NormalizedRowResult {
  const rawSku = mappedCell(row, mapping, "seller_sku");
  const trimmedSku = String(rawSku ?? "").trim();
  if (trimmedSku === "") {
    return { kind: "missing_seller_sku" };
  }

  const sku = normalizeIdentifier(rawSku);
  if (!sku) {
    return { kind: "invalid_seller_sku", rawValue: trimmedSku };
  }

  const productName = normalizeIdentifier(mappedCell(row, mapping, "product_name")) ?? sku;

  return {
    kind: "ok",
    row: {
      rowNumber,
      original: row,
      sku,
      productName,
      vendorName: normalizeIdentifier(mappedCell(row, mapping, "vendor")),
      mfgPartNumber: normalizeIdentifier(mappedCell(row, mapping, "mfg_part_number")),
      asin: normalizeAsin(mappedCell(row, mapping, "asin"), stats),
      fnsku: normalizeFnsku(mappedCell(row, mapping, "fnsku"), stats),
      upc: normalizeUpc(mappedCell(row, mapping, "upc"), stats),
    },
  };
}

function chunk<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

function productMetadata(
  existing: unknown,
  row: NormalizedRow | DedupedProductRow,
  uploadId: string,
  sourceFileSha256: string,
): Record<string, unknown> {
  const dedupedRow = row as Partial<DedupedProductRow>;
  return {
    ...asPlainRecord(existing),
    product_identity_import: {
      source_upload_id: uploadId,
      source_file_sha256: sourceFileSha256,
      source_physical_row_number: row.rowNumber,
      imported_at: new Date().toISOString(),
      original_row: row.original,
      normalized_identifiers: {
        sku: row.sku,
        asin: row.asin,
        fnsku: row.fnsku,
        upc: row.upc,
      },
      duplicate_count: dedupedRow.duplicateCount ?? 1,
      merged_row_numbers: dedupedRow.mergedRows?.map((m) => m.rowNumber) ?? [row.rowNumber],
      conflicting_fields: dedupedRow.conflictingFields ?? [],
      alternative_identifiers: dedupedRow.alternativeIdentifiers ?? { asin: [], fnsku: [], upc: [] },
    },
  };
}

function catalogKey(sku: string | null, asin: string | null): string {
  return `${sku ?? ""}\x1f${asin ?? ""}`;
}

function identifierMapKey(row: {
  store_id: string | null;
  seller_sku: string | null;
  asin: string | null;
  fnsku: string | null;
  upc_code: string | null;
  external_listing_id: string | null;
}): string {
  return [
    row.store_id ?? "",
    row.seller_sku ?? "",
    row.asin ?? "",
    row.fnsku ?? "",
    row.upc_code ?? "",
    row.external_listing_id ?? "",
  ].join("\x1f");
}

export async function readProductIdentityCsvRowsFromStream(
  source: Readable,
  options?: { skipLines?: number; separator?: "," | "\t" },
): Promise<CsvRow[]> {
  const rows: CsvRow[] = [];

  await new Promise<void>((resolve, reject) => {
    source
      .pipe(
        csv({
          mapHeaders: ({ header }) => stripBom(String(header ?? "")).trim(),
          skipLines: options?.skipLines ?? 0,
          separator: options?.separator ?? ",",
        }),
      )
      .on("data", (row: Record<string, unknown>) => {
        const normalized: CsvRow = {};
        for (const [key, value] of Object.entries(row)) {
          normalized[String(key)] = value == null ? "" : String(value);
        }
        rows.push(normalized);
      })
      .on("error", reject)
      .on("end", resolve);
  });

  return rows;
}

async function upsertBacklog(
  supabase: SupabaseClient,
  params: {
    organizationId: string;
    storeId: string;
    sourceUploadId: string;
    row: NormalizedRow | null;
    identifierType: IdentifierType;
    identifierValue: string | null;
    reason: string;
    candidateProductIds?: string[];
    rawPayload?: Record<string, unknown>;
  },
): Promise<void> {
  const { organizationId, storeId, sourceUploadId, row, identifierType, identifierValue, reason } = params;

  const payload = {
    organization_id: organizationId,
    store_id: storeId,
    source_upload_id: sourceUploadId,
    seller_sku: row?.sku ?? null,
    asin: row?.asin ?? null,
    current_catalog_fnsku: row?.fnsku ?? null,
    item_name: row?.productName ?? null,
    source_report_type: SOURCE_REPORT_TYPE,
    identifier_type: identifierType,
    identifier_value: identifierValue,
    reason,
    candidate_product_ids: params.candidateProductIds ?? [],
    raw_payload: params.rawPayload ?? row?.original ?? {},
    updated_at: new Date().toISOString(),
  };

  const { error } = await supabase
    .from("catalog_identity_unresolved_backlog")
    .upsert(payload, { onConflict: BACKLOG_UPSERT_CONFLICT });

  if (error) {
    console.warn(`Backlog write failed for ${identifierType}:${identifierValue ?? ""}: ${error.message}`);
  }
}

async function prefetchProducts(
  supabase: SupabaseClient,
  organizationId: string,
  storeId: string,
  skus: string[],
): Promise<Map<string, ProductRecord>> {
  const bySku = new Map<string, ProductRecord>();

  for (const skuChunk of chunk([...new Set(skus)], CHUNK_SIZE)) {
    // Try the modern column set first (post-20260635 migration). If
    // `field_provenance` is missing on older databases, fall back to the
    // legacy SELECT so the import keeps working — the priority guard then
    // degrades to "fill nulls only", which is still safe.
    const { data, error } = await supabase
      .from("products")
      .select("id, sku, product_name, vendor_name, mfg_part_number, upc_code, asin, fnsku, metadata, field_provenance")
      .eq("organization_id", organizationId)
      .eq("store_id", storeId)
      .in("sku", skuChunk);

    if (error) {
      const msg = (error.message ?? "").toLowerCase();
      const missingProvenance =
        msg.includes("field_provenance") &&
        (msg.includes("does not exist") || msg.includes("schema cache") || msg.includes("column"));

      if (!missingProvenance) {
        throw new Error(`products prefetch failed: ${error.message}`);
      }

      const retry = await supabase
        .from("products")
        .select("id, sku, product_name, vendor_name, mfg_part_number, upc_code, asin, fnsku, metadata")
        .eq("organization_id", organizationId)
        .eq("store_id", storeId)
        .in("sku", skuChunk);
      if (retry.error) throw new Error(`products prefetch failed: ${retry.error.message}`);
      for (const row of (retry.data ?? []) as ProductRecord[]) {
        bySku.set(row.sku, row);
      }
      continue;
    }

    for (const row of (data ?? []) as ProductRecord[]) {
      bySku.set(row.sku, row);
    }
  }

  return bySku;
}

/**
 * True when the incoming Product Identity value should overwrite the existing
 * column. Mirrors `identity_merge_should_overwrite(...)` in SQL:
 *   1. Never overwrite anything with NULL/empty (preserve trusted data).
 *   2. Always allow filling a NULL/empty existing value.
 *   3. Otherwise allow only when this writer's priority is strictly greater
 *      than the priority recorded in field_provenance for the same field.
 */
function shouldProductIdentityOverwrite(
  existingValue: string | null | undefined,
  existingProvenance: Record<string, FieldProvenanceEntry> | null | undefined,
  fieldKey: keyof typeof PRODUCT_IDENTITY_PRIORITY,
  incomingValue: string | null | undefined,
): boolean {
  if (incomingValue == null || incomingValue === "") return false;
  if (existingValue == null || existingValue === "") return true;
  const recordedPriority = Number(existingProvenance?.[fieldKey]?.priority ?? 0);
  return PRODUCT_IDENTITY_PRIORITY[fieldKey] > recordedPriority;
}

/**
 * Builds the field_provenance JSON entries for the columns this Product
 * Identity row actually wrote. We only emit entries for the fields where
 * the row supplied a non-null value AND `shouldProductIdentityOverwrite`
 * returned true — otherwise the existing higher-priority record stands.
 */
function buildProductIdentityFieldProvenance(params: {
  existing: ProductRecord | undefined;
  payload: {
    product_name: string | null;
    vendor_name: string | null;
    mfg_part_number: string | null;
    upc_code: string | null;
    asin: string | null;
    fnsku: string | null;
  };
  written: Record<keyof typeof PRODUCT_IDENTITY_PRIORITY, boolean>;
  uploadId: string;
}): Record<string, FieldProvenanceEntry> {
  const now = new Date().toISOString();
  const carryover: Record<string, FieldProvenanceEntry> = {
    ...(params.existing?.field_provenance ?? {}),
  };
  for (const key of Object.keys(PRODUCT_IDENTITY_PRIORITY) as (keyof typeof PRODUCT_IDENTITY_PRIORITY)[]) {
    if (!params.written[key]) continue;
    carryover[key] = {
      source: PRODUCT_IDENTITY_SOURCE,
      priority: PRODUCT_IDENTITY_PRIORITY[key],
      confidence: 1,
      written_at: now,
      upload_id: params.uploadId,
    };
  }
  return carryover;
}

async function upsertProducts(params: {
  supabase: SupabaseClient;
  organizationId: string;
  storeId: string;
  rows: DedupedProductRow[];
  sourceUploadId: string;
  sourceFileSha256: string;
  stats: ProductIdentityImportStats;
}): Promise<Map<string, string>> {
  const { supabase, organizationId, storeId, rows, sourceUploadId, sourceFileSha256, stats } = params;
  const existingBySku = await prefetchProducts(
    supabase,
    organizationId,
    storeId,
    rows.map((row) => row.sku),
  );
  const productIdBySku = new Map<string, string>();
  // Tracks whether the live table actually has the `field_provenance` column.
  // Set to false on the first PostgREST "schema cache" / "column does not
  // exist" error so subsequent chunks omit the column without retrying.
  let provenanceSupported = true;

  for (const rowChunk of chunk(rows, CHUNK_SIZE)) {
    const now = new Date().toISOString();
    const payload = rowChunk.map((row) => {
      const existing = existingBySku.get(row.sku);
      const existingProv = existing?.field_provenance ?? null;

      // Per-field priority guard. Mirrors merge_product_identity_into_products(...)
      // in SQL: never overwrite a higher-priority trusted value, never
      // overwrite a non-null value with NULL.
      const writeProductName = shouldProductIdentityOverwrite(existing?.product_name, existingProv, "product_name", row.productName);
      const writeVendor      = shouldProductIdentityOverwrite(existing?.vendor_name, existingProv, "vendor_name", row.vendorName);
      const writeMfg         = shouldProductIdentityOverwrite(existing?.mfg_part_number, existingProv, "mfg_part_number", row.mfgPartNumber);
      const writeUpc         = shouldProductIdentityOverwrite(existing?.upc_code, existingProv, "upc_code", row.upc);
      const writeAsin        = shouldProductIdentityOverwrite(existing?.asin, existingProv, "asin", row.asin);
      const writeFnsku       = shouldProductIdentityOverwrite(existing?.fnsku, existingProv, "fnsku", row.fnsku);

      const product_name = writeProductName ? row.productName : (existing?.product_name ?? row.productName ?? row.sku);
      const vendor_name  = writeVendor      ? row.vendorName  : (existing?.vendor_name ?? null);
      const mfg_part_number = writeMfg      ? row.mfgPartNumber : (existing?.mfg_part_number ?? null);
      const upc_code     = writeUpc         ? row.upc         : (existing?.upc_code ?? null);
      const asin         = writeAsin        ? row.asin        : (existing?.asin ?? null);
      const fnsku        = writeFnsku       ? row.fnsku       : (existing?.fnsku ?? null);

      const fieldProvenance = buildProductIdentityFieldProvenance({
        existing,
        payload: { product_name, vendor_name, mfg_part_number, upc_code, asin, fnsku },
        written: {
          product_name: writeProductName,
          vendor_name: writeVendor,
          mfg_part_number: writeMfg,
          upc_code: writeUpc,
          asin: writeAsin,
          fnsku: writeFnsku,
        },
        uploadId: sourceUploadId,
      });

      const base: Record<string, unknown> = {
        organization_id: organizationId,
        store_id: storeId,
        sku: row.sku,
        product_name,
        vendor_name,
        mfg_part_number,
        upc_code,
        asin,
        fnsku,
        metadata: productMetadata(existing?.metadata, row, sourceUploadId, sourceFileSha256),
        last_seen_at: now,
        last_catalog_sync_at: now,
      };
      if (provenanceSupported) base.field_provenance = fieldProvenance;
      return base;
    });

    const upsertOnce = async (rows: Record<string, unknown>[]) =>
      supabase.from("products").upsert(rows, { onConflict: PRODUCT_UPSERT_CONFLICT }).select("id, sku");

    let { data, error } = await upsertOnce(payload);

    if (error && provenanceSupported) {
      const msg = (error.message ?? "").toLowerCase();
      const missingProvenance =
        msg.includes("field_provenance") &&
        (msg.includes("does not exist") || msg.includes("schema cache") || msg.includes("column"));
      if (missingProvenance) {
        provenanceSupported = false;
        const fallback = payload.map((row) => {
          // Drop the unknown column and try again — keeps the priority guard
          // (the values themselves were already chosen correctly above) but
          // skips the provenance write until the migration runs.
          const { field_provenance: _omit, ...rest } = row as { field_provenance?: unknown } & Record<string, unknown>;
          return rest;
        });
        const retry = await upsertOnce(fallback);
        data = retry.data;
        error = retry.error;
      }
    }

    if (error) {
      throw new Error(
        [
          `products upsert failed: ${error.message}`,
          "Expected unique constraint/index: products_organization_store_sku_key on (organization_id, store_id, sku).",
          error.details ? `details: ${error.details}` : "",
          error.hint ? `hint: ${error.hint}` : "",
          error.code ? `code: ${error.code}` : "",
        ].filter(Boolean).join(" "),
      );
    }

    for (const product of (data ?? []) as ProductRecord[]) {
      productIdBySku.set(product.sku, product.id);
      if (existingBySku.has(product.sku)) {
        stats.productsUpdated += 1;
      } else {
        stats.productsInserted += 1;
      }
    }
  }

  return productIdBySku;
}

async function prefetchCatalogProducts(
  supabase: SupabaseClient,
  organizationId: string,
  storeId: string,
  rows: DedupedProductRow[],
): Promise<Map<string, CatalogRecord>> {
  const byKey = new Map<string, CatalogRecord>();

  for (const skuChunk of chunk([...new Set(rows.map((row) => row.sku))], CHUNK_SIZE)) {
    const { data, error } = await supabase
      .from("catalog_products")
      .select("id, seller_sku, asin, fnsku, item_name, raw_payload")
      .eq("organization_id", organizationId)
      .eq("store_id", storeId)
      .in("seller_sku", skuChunk);

    if (error) throw new Error(`catalog_products prefetch failed: ${error.message}`);
    for (const row of (data ?? []) as CatalogRecord[]) {
      byKey.set(catalogKey(row.seller_sku, row.asin), row);
    }
  }

  return byKey;
}

async function upsertCatalogProducts(params: {
  supabase: SupabaseClient;
  organizationId: string;
  storeId: string;
  rows: DedupedProductRow[];
  sourceUploadId: string;
  stats: ProductIdentityImportStats;
}): Promise<Map<string, string>> {
  const { supabase, organizationId, storeId, rows, sourceUploadId, stats } = params;
  // Catalog identity is per (sku, asin) — collapse rows that already share
  // both axes so the same upsert command never sees a duplicate conflict tuple.
  const dedupedForCatalog = dedupeForCatalogUpsert(rows);
  stats.catalogUniqueCount = dedupedForCatalog.length;
  const existingByKey = await prefetchCatalogProducts(supabase, organizationId, storeId, dedupedForCatalog);
  const catalogIdBySku = new Map<string, string>();

  for (const rowChunk of chunk(dedupedForCatalog, CHUNK_SIZE)) {
    const payload = rowChunk.map((row) => {
      const existing = existingByKey.get(catalogKey(row.sku, row.asin));
      return {
        organization_id: organizationId,
        store_id: storeId,
        source_report_type: SOURCE_REPORT_TYPE,
        source_upload_id: sourceUploadId,
        seller_sku: row.sku,
        asin: row.asin,
        fnsku: row.fnsku ?? existing?.fnsku ?? null,
        item_name: row.productName || existing?.item_name || row.sku,
        raw_payload: {
          ...asPlainRecord(existing?.raw_payload),
          ...row.original,
          _product_identity_import: {
            source_upload_id: sourceUploadId,
            source_physical_row_number: row.rowNumber,
            duplicate_count: row.duplicateCount,
            merged_row_numbers: row.mergedRows.map((m) => m.rowNumber),
            normalized_identifiers: {
              sku: row.sku,
              asin: row.asin,
              fnsku: row.fnsku,
              upc: row.upc,
            },
          },
        },
      };
    });

    const { data, error } = await supabase
      .from("catalog_products")
      .upsert(payload, { onConflict: CATALOG_UPSERT_CONFLICT })
      .select("id, seller_sku, asin");

    if (error) {
      throw new Error(
        [
          `catalog_products upsert failed: ${error.message}`,
          "Expected unique index: uq_catalog_products_canonical_identity on (organization_id, store_id, seller_sku, asin).",
          error.details ? `details: ${error.details}` : "",
          error.hint ? `hint: ${error.hint}` : "",
          error.code ? `code: ${error.code}` : "",
        ].filter(Boolean).join(" "),
      );
    }

    for (const catalog of (data ?? []) as CatalogRecord[]) {
      const sku = catalog.seller_sku ?? "";
      if (!sku) continue;

      catalogIdBySku.set(sku, catalog.id);
      if (existingByKey.has(catalogKey(catalog.seller_sku, catalog.asin))) {
        stats.catalogProductsUpdated += 1;
      } else {
        stats.catalogProductsInserted += 1;
      }
    }
  }

  return catalogIdBySku;
}

function identifierRowsForProduct(params: {
  organizationId: string;
  storeId: string;
  row: DedupedProductRow;
  productId: string;
  catalogProductId: string | null;
  sourceUploadId: string;
  sourceFileSha256: string;
}): IdentifierMapInsert[] {
  const { organizationId, storeId, row, productId, catalogProductId, sourceUploadId, sourceFileSha256 } = params;
  const now = new Date().toISOString();
  const identifiers: { type: IdentifierType; value: string; asin: string | null; fnsku: string | null; upc: string | null }[] = [
    { type: "SKU", value: row.sku, asin: null, fnsku: null, upc: null },
  ];

  // Primary identifiers chosen by the dedupe step.
  if (row.asin) identifiers.push({ type: "ASIN", value: row.asin, asin: row.asin, fnsku: null, upc: null });
  if (row.fnsku) identifiers.push({ type: "FNSKU", value: row.fnsku, asin: null, fnsku: row.fnsku, upc: null });
  if (row.upc) identifiers.push({ type: "UPC", value: row.upc, asin: null, fnsku: null, upc: row.upc });

  // Alternative identifiers from collapsed duplicate CSV rows. Recording them
  // here keeps the identifier map honest when the same SKU was uploaded with
  // disagreeing ASIN/FNSKU/UPC values — operators can review the conflict in
  // metadata.product_identity_import.alternative_identifiers on the product row.
  for (const value of row.alternativeIdentifiers.asin) {
    identifiers.push({ type: "ASIN", value, asin: value, fnsku: null, upc: null });
  }
  for (const value of row.alternativeIdentifiers.fnsku) {
    identifiers.push({ type: "FNSKU", value, asin: null, fnsku: value, upc: null });
  }
  for (const value of row.alternativeIdentifiers.upc) {
    identifiers.push({ type: "UPC", value, asin: null, fnsku: null, upc: value });
  }

  return identifiers.map((identifier) => ({
    organization_id: organizationId,
    product_id: productId,
    catalog_product_id: catalogProductId,
    store_id: storeId,
    seller_sku: row.sku,
    asin: identifier.asin,
    fnsku: identifier.fnsku,
    upc_code: identifier.upc,
    msku: row.sku,
    title: row.productName,
    external_listing_id: `product_identity:${identifier.type}:${identifier.value}`,
    source_upload_id: sourceUploadId,
    source_report_type: SOURCE_REPORT_TYPE,
    source_file_sha256: sourceFileSha256,
    source_physical_row_number: row.rowNumber,
    match_source: `product_identity_${identifier.type.toLowerCase()}`,
    inventory_source: null,
    confidence_score: 1,
    linked_from_report_family: "product_identity",
    linked_from_target_table: "products",
    first_seen_at: now,
    last_seen_at: now,
    is_primary: identifier.type === "SKU",
  }));
}

async function prefetchIdentifierMapRows(
  supabase: SupabaseClient,
  organizationId: string,
  storeId: string,
  skus: string[],
): Promise<Map<string, { id: string; product_id: string | null }>> {
  const byKey = new Map<string, { id: string; product_id: string | null }>();

  for (const skuChunk of chunk([...new Set(skus)], 100)) {
    const { data, error } = await supabase
      .from("product_identifier_map")
      .select("id, product_id, store_id, seller_sku, asin, fnsku, upc_code, external_listing_id")
      .eq("organization_id", organizationId)
      .eq("store_id", storeId)
      .in("seller_sku", skuChunk);

    if (error) throw new Error(`product_identifier_map prefetch failed: ${error.message}`);

    for (const row of (data ?? []) as ExistingIdentifierMapRow[]) {
      byKey.set(identifierMapKey(row), {
        id: row.id,
        product_id: row.product_id != null ? String(row.product_id) : null,
      });
    }
  }

  return byKey;
}

async function upsertIdentifierMap(params: {
  supabase: SupabaseClient;
  organizationId: string;
  storeId: string;
  rows: DedupedProductRow[];
  productIdBySku: Map<string, string>;
  catalogIdBySku: Map<string, string>;
  sourceUploadId: string;
  sourceFileSha256: string;
  stats: ProductIdentityImportStats;
}): Promise<void> {
  const {
    supabase,
    organizationId,
    storeId,
    rows,
    productIdBySku,
    catalogIdBySku,
    sourceUploadId,
    sourceFileSha256,
    stats,
  } = params;
  const existingByKey = await prefetchIdentifierMapRows(
    supabase,
    organizationId,
    storeId,
    rows.map((row) => row.sku),
  );

  const toInsert: IdentifierMapInsert[] = [];
  const toUpdate: { id: string; patch: Partial<IdentifierMapInsert> }[] = [];
  /**
   * Deduplicate generated map rows inside this batch by their deterministic
   * `external_listing_id`. Two CSV rows that share the same sku/asin/fnsku/upc
   * tuple would otherwise produce identical bridge rows, and the partial
   * unique index `uq_product_identifier_map_product_identity` would reject
   * the second one as a duplicate-key violation.
   */
  const insertSeen = new Set<string>();

  for (const row of rows) {
    const productId = productIdBySku.get(row.sku);
    if (!productId) {
      stats.unresolvedRows += 1;
      await upsertBacklog(supabase, {
        organizationId,
        storeId,
        sourceUploadId,
        row,
        identifierType: "SKU",
        identifierValue: row.sku,
        reason: "missing_product_after_upsert",
      });
      continue;
    }

    const mapRows = identifierRowsForProduct({
      organizationId,
      storeId,
      row,
      productId,
      catalogProductId: catalogIdBySku.get(row.sku) ?? null,
      sourceUploadId,
      sourceFileSha256,
    });

    for (const mapRow of mapRows) {
      const key = identifierMapKey(mapRow);
      const existing = existingByKey.get(key);
      if (existing) {
        const newPid = mapRow.product_id != null ? String(mapRow.product_id) : "";
        const oldPid = existing.product_id != null ? String(existing.product_id) : "";
        if (oldPid && newPid && oldPid !== newPid) {
          stats.unresolvedRows += 1;
          await upsertBacklog(supabase, {
            organizationId,
            storeId,
            sourceUploadId,
            row,
            identifierType: "SKU",
            identifierValue: row.sku,
            reason: "identifier_map_product_id_conflict",
            candidateProductIds: [oldPid, newPid],
            rawPayload: {
              external_listing_id: mapRow.external_listing_id,
              existing_map_id: existing.id,
            },
          });
          continue;
        }
        toUpdate.push({
          id: existing.id,
          patch: {
            product_id: mapRow.product_id,
            catalog_product_id: mapRow.catalog_product_id,
            title: mapRow.title,
            source_upload_id: mapRow.source_upload_id,
            source_report_type: mapRow.source_report_type,
            source_file_sha256: mapRow.source_file_sha256,
            source_physical_row_number: mapRow.source_physical_row_number,
            match_source: mapRow.match_source,
            confidence_score: mapRow.confidence_score,
            linked_from_report_family: mapRow.linked_from_report_family,
            linked_from_target_table: mapRow.linked_from_target_table,
            last_seen_at: mapRow.last_seen_at,
          },
        });
        continue;
      }
      if (insertSeen.has(mapRow.external_listing_id)) continue;
      insertSeen.add(mapRow.external_listing_id);
      toInsert.push(mapRow);
    }
  }
  stats.identifierUniqueCount = toInsert.length + toUpdate.length;

  for (const insertChunk of chunk(toInsert, CHUNK_SIZE)) {
    // Existing rows were pre-fetched by deterministic external_listing_id and
    // are patched in `toUpdate`; new rows are inserted. We intentionally avoid
    // PostgREST `upsert(... onConflict: organization_id,store_id,external_listing_id)`
    // here because the database invariant is a partial unique index for
    // product_identity rows only, and PostgREST cannot express the required
    // partial-index predicate in `on_conflict`.
    const { error } = await supabase
      .from("product_identifier_map")
      .insert(insertChunk);
    if (!error) {
      stats.identifiersInserted += insertChunk.length;
      continue;
    }

    // Per-row fallback: useful when a single bad row in the chunk would otherwise
    // poison the whole batch. Records the failure into the backlog for triage.
    for (const row of insertChunk) {
      const { error: rowError } = await supabase
        .from("product_identifier_map")
        .insert(row);
      if (!rowError) {
        stats.identifiersInserted += 1;
        continue;
      }

      stats.unresolvedRows += 1;
      await upsertBacklog(supabase, {
        organizationId,
        storeId,
        sourceUploadId,
        row: rows.find((sourceRow) => sourceRow.sku === row.seller_sku) ?? null,
        identifierType: row.upc_code ? "UPC" : row.fnsku ? "FNSKU" : row.asin ? "ASIN" : "SKU",
        identifierValue: row.upc_code ?? row.fnsku ?? row.asin ?? row.seller_sku,
        reason: "identifier_map_insert_conflict",
        candidateProductIds: [row.product_id],
        rawPayload: { failed_row: row, error: rowError.message },
      });
    }
  }

  for (const updateChunk of chunk(toUpdate, 75)) {
    for (const update of updateChunk) {
      const { error } = await supabase.from("product_identifier_map").update(update.patch).eq("id", update.id);
      if (error) throw new Error(`product_identifier_map update failed: ${error.message}`);
    }
  }
}

/**
 * Collapse normalized rows by (organization_id, store_id, sku) BEFORE any
 * upsert. Postgres rejects an ON CONFLICT batch where two rows resolve to the
 * same conflict tuple ("ON CONFLICT DO UPDATE command cannot affect row a
 * second time", SQLSTATE 21000), so the importer must dedupe in JS first.
 *
 * Merge rules per duplicate group:
 *   * The first row defines the canonical row number / original record.
 *   * Non-null fields (productName, vendorName, mfgPartNumber, asin, fnsku,
 *     upc) take the FIRST non-null value seen — never silently overwritten.
 *   * If a later row provides a different non-null ASIN/FNSKU/UPC, the
 *     conflict is recorded (`conflictingFields`, `alternativeIdentifiers`)
 *     and surfaced to the unresolved backlog so an operator can review.
 */
function dedupeNormalizedRowsBySku(
  organizationId: string,
  storeId: string,
  rows: NormalizedRow[],
  stats: ProductIdentityImportStats,
): DedupedProductRow[] {
  const grouped = new Map<string, DedupedProductRow>();

  for (const row of rows) {
    const key = `${organizationId}\x1f${storeId}\x1f${row.sku}`;
    const existing = grouped.get(key);
    if (!existing) {
      grouped.set(key, {
        ...row,
        duplicateCount: 1,
        mergedRows: [{ rowNumber: row.rowNumber, original: row.original }],
        conflictingFields: [],
        alternativeIdentifiers: { asin: [], fnsku: [], upc: [] },
      });
      continue;
    }

    existing.duplicateCount += 1;
    existing.mergedRows.push({ rowNumber: row.rowNumber, original: row.original });

    // Fill empty fields with the first non-null value.
    if (!existing.productName && row.productName) existing.productName = row.productName;
    if (!existing.vendorName && row.vendorName) existing.vendorName = row.vendorName;
    if (!existing.mfgPartNumber && row.mfgPartNumber) existing.mfgPartNumber = row.mfgPartNumber;

    const recordIdentifierConflict = (
      field: "asin" | "fnsku" | "upc",
      currentValue: string | null,
      incomingValue: string | null,
    ): string | null => {
      if (!incomingValue) return currentValue;
      if (!currentValue) return incomingValue;
      if (currentValue === incomingValue) return currentValue;
      if (!existing.conflictingFields.includes(field)) {
        existing.conflictingFields.push(field);
      }
      // Keep the rejected value so identifier map and backlog still see it.
      const bucket = existing.alternativeIdentifiers[field];
      if (!bucket.includes(incomingValue)) bucket.push(incomingValue);
      return currentValue;
    };

    existing.asin = recordIdentifierConflict("asin", existing.asin, row.asin);
    existing.fnsku = recordIdentifierConflict("fnsku", existing.fnsku, row.fnsku);
    existing.upc = recordIdentifierConflict("upc", existing.upc, row.upc);
  }

  const deduped = [...grouped.values()];
  stats.normalizedRowsCount = rows.length;
  stats.uniqueProductSkuCount = deduped.length;
  stats.duplicateSkuCount = Math.max(0, rows.length - deduped.length);
  stats.duplicateSkuConflictCount = deduped.filter((d) => d.conflictingFields.length > 0).length;
  return deduped;
}

/**
 * Collapse deduped product rows by (organization_id, store_id, seller_sku, asin)
 * before catalog_products upsert. Catalog identity is per (sku, asin), so a
 * single SKU may legitimately produce multiple catalog rows when the seller
 * lists it under multiple ASINs.
 */
function dedupeForCatalogUpsert(rows: DedupedProductRow[]): DedupedProductRow[] {
  const seen = new Map<string, DedupedProductRow>();
  for (const row of rows) {
    const key = `${row.sku}\x1f${row.asin ?? ""}`;
    if (!seen.has(key)) seen.set(key, row);
  }
  return [...seen.values()];
}

function countAmbiguousIdentifiers(rows: DedupedProductRow[], productIdBySku: Map<string, string>): number {
  const groups = new Map<string, Set<string>>();

  for (const row of rows) {
    const productId = productIdBySku.get(row.sku);
    if (!productId) continue;

    for (const [type, value] of [
      ["ASIN", row.asin],
      ["FNSKU", row.fnsku],
      ["UPC", row.upc],
    ] as const) {
      if (!value) continue;
      const key = `${type}:${value}`;
      const productIds = groups.get(key) ?? new Set<string>();
      productIds.add(productId);
      groups.set(key, productIds);
    }
  }

  return [...groups.values()].filter((productIds) => productIds.size > 1).length;
}

async function finalizeUpload(params: {
  supabase: SupabaseClient;
  organizationId: string;
  uploadId: string;
  stats: ProductIdentityImportStats;
  rows: DedupedProductRow[];
  sourceFileSha256: string;
}): Promise<void> {
  const { supabase, organizationId, uploadId, stats, rows, sourceFileSha256 } = params;
  const now = new Date().toISOString();

  const { data: upload, error: fetchErr } = await supabase
    .from("raw_report_uploads")
    .select("metadata")
    .eq("id", uploadId)
    .eq("organization_id", organizationId)
    .maybeSingle();

  if (fetchErr) throw new Error(`raw_report_uploads final lookup failed: ${fetchErr.message}`);

  const metadata = asPlainRecord((upload as { metadata?: unknown } | null)?.metadata);
  const productIdentityImport = asPlainRecord(metadata.product_identity_import);
  const productsUpserted = stats.productsInserted + stats.productsUpdated;
  const catalogProductsUpserted = stats.catalogProductsInserted + stats.catalogProductsUpdated;
  const identifiersUpserted = stats.identifiersInserted;
  const rowsSynced = productsUpserted + catalogProductsUpserted + identifiersUpserted;
  const detectedHeaders =
    Array.isArray(metadata.csv_headers)
      ? (metadata.csv_headers as unknown[]).map((h) => String(h ?? "")).filter(Boolean)
      : [];

  const validation = {
    detected_headers: detectedHeaders,
    detected_report_type: "PRODUCT_IDENTITY" as const,
    rows_parsed: stats.rowsRead,
    rows_synced: rowsSynced,
    products_upserted: productsUpserted,
    catalog_products_upserted: catalogProductsUpserted,
    identifiers_upserted: identifiersUpserted,
    invalid_identifier_counts: {
      asin: stats.invalidAsinCount,
      fnsku: stats.invalidFnskuCount,
      upc: stats.invalidUpcCount,
      total: stats.invalidIdentifierCount,
    },
    normalized_rows_count: stats.normalizedRowsCount,
    unique_product_sku_count: stats.uniqueProductSkuCount,
    duplicate_sku_count: stats.duplicateSkuCount,
    duplicate_sku_conflict_count: stats.duplicateSkuConflictCount,
    catalog_unique_count: stats.catalogUniqueCount,
    identifier_unique_count: stats.identifierUniqueCount,
    // Per-row CSV diagnostics (added 2026-04-29 to explain off-by-one
    // mysteries between rowsRead and normalized_rows_count).
    rows_missing_seller_sku: stats.rowsMissingSellerSku,
    rows_invalid_seller_sku: stats.rowsInvalidSellerSku,
    rows_skipped: stats.rowsSkipped,
    skipped_reason_counts: stats.skippedReasonCounts,
    invalid_sku_examples: stats.invalidSkuExamples,
  };

  // NOTE: `row_count` / `total_rows` here are JSONB metadata keys, NOT the
  // legacy `raw_report_uploads.row_count` column (which is no longer relied
  // on by the UI or the Product Identity pipeline). Progress is sourced from
  // `metadata.process_progress` / `metadata.sync_progress` and from
  // `file_processing_status` (see processProductIdentityUpload below).
  const metadataPatch = {
    total_rows: stats.rowsRead,
    processed_rows: rows.length,
    process_progress: 100,
    sync_progress: 100,
    content_sha256: sourceFileSha256,
    etl_phase: "complete",
    product_identity_import: {
      ...productIdentityImport,
      completed_at: now,
      normalized_rows: rows.length,
      stats,
      validation,
    } as Record<string, unknown>,
    product_identity_validation: validation,
    import_metrics: {
      current_phase: "complete",
      data_rows_seen: stats.rowsRead,
      rows_synced_upserted: rowsSynced,
      rows_invalid: stats.invalidIdentifierCount,
      detected_headers: detectedHeaders,
      detected_report_type: "PRODUCT_IDENTITY",
      rows_parsed: stats.rowsRead,
      rows_synced: rowsSynced,
      products_upserted: productsUpserted,
      catalog_products_upserted: catalogProductsUpserted,
      identifiers_upserted: identifiersUpserted,
      invalid_identifier_counts: validation.invalid_identifier_counts,
      normalized_rows_count: stats.normalizedRowsCount,
      unique_product_sku_count: stats.uniqueProductSkuCount,
      duplicate_sku_count: stats.duplicateSkuCount,
      duplicate_sku_conflict_count: stats.duplicateSkuConflictCount,
      catalog_unique_count: stats.catalogUniqueCount,
      identifier_unique_count: stats.identifierUniqueCount,
      rows_missing_seller_sku: stats.rowsMissingSellerSku,
      rows_invalid_seller_sku: stats.rowsInvalidSellerSku,
      rows_skipped: stats.rowsSkipped,
      skipped_reason_counts: stats.skippedReasonCounts,
      invalid_sku_examples: stats.invalidSkuExamples,
    },
  } as unknown as Parameters<typeof mergeUploadMetadata>[1];

  const mergedMetadata = mergeUploadMetadata(metadata, metadataPatch);

  const { error } = await supabase
    .from("raw_report_uploads")
    .update({
      status: "synced",
      import_pipeline_completed_at: now,
      metadata: mergedMetadata,
      updated_at: now,
    })
    .eq("id", uploadId)
    .eq("organization_id", organizationId);

  if (error) throw new Error(`raw_report_uploads finalize failed: ${error.message}`);
}

/** Shared error wrapper used by route helpers so the API can map cleanly to HTTP status codes. */
export type ProductIdentityPipelineError = {
  ok: false;
  status: number;
  error: string;
};

export type ProductIdentityPipelineSuccess = {
  ok: true;
  stats: ProductIdentityImportStats;
  storeId: string;
  contentSha256: string;
  rowsParsed: number;
  detectedHeaders: string[];
};

export type ProductIdentityPipelineResult =
  | ProductIdentityPipelineSuccess
  | ProductIdentityPipelineError;

function productIdentityColumnMappingFromAny(value: unknown): ProductIdentityColumnMapping | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const result: ProductIdentityColumnMapping = {};
  for (const key of ["upc", "vendor", "seller_sku", "mfg_part_number", "fnsku", "asin", "product_name"] as const) {
    const mapped = (value as Record<string, unknown>)[key];
    if (typeof mapped === "string" && mapped.trim()) result[key] = mapped.trim();
  }
  return Object.keys(result).length > 0 ? result : null;
}

/**
 * Load the raw CSV/TSV from Supabase Storage, parse it, and run the full
 * Product Identity import (products + catalog_products + product_identifier_map).
 *
 * Idempotent: re-running for the same `(organization_id, store_id, content_sha256)`
 * is safe because of the partial unique index `uq_product_identifier_map_product_identity`
 * (migration 20260636) and the per-field priority guard inside `upsertProducts`.
 *
 * Both `/api/settings/imports/process` and `/api/settings/imports/sync` call
 * this helper for `report_type = 'PRODUCT_IDENTITY'` so the work is unified
 * regardless of which button the user clicked. The wrapper routes are responsible
 * for status transitions, file_processing_status updates, and HTTP responses.
 */
export async function runProductIdentityImportFromUpload(params: {
  supabase: SupabaseClient;
  organizationId: string;
  uploadId: string;
  uploadRow: Record<string, unknown>;
  metadata: Record<string, unknown>;
}): Promise<ProductIdentityPipelineResult> {
  const { supabase, organizationId, uploadId, uploadRow, metadata } = params;

  const storeId =
    typeof metadata.import_store_id === "string" && isUuidString(metadata.import_store_id.trim())
      ? metadata.import_store_id.trim()
      : typeof metadata.ledger_store_id === "string" && isUuidString(metadata.ledger_store_id.trim())
        ? metadata.ledger_store_id.trim()
        : "";

  if (!isUuidString(storeId)) {
    return {
      ok: false,
      status: 400,
      error: "Product Identity import requires a selected target store.",
    };
  }

  const rawFilePath =
    typeof metadata.raw_file_path === "string" ? metadata.raw_file_path.trim() : "";
  if (!rawFilePath) {
    return {
      ok: false,
      status: 400,
      error: "Missing raw_file_path in upload metadata for Product Identity import.",
    };
  }

  const contentSha256 =
    typeof metadata.content_sha256 === "string" && /^[a-f0-9]{64}$/i.test(metadata.content_sha256.trim())
      ? metadata.content_sha256.trim().toLowerCase()
      : "";
  if (!contentSha256) {
    return {
      ok: false,
      status: 400,
      error: "Missing content SHA-256 for Product Identity import.",
    };
  }

  const rawFileExt = rawFilePath.split(".").pop()?.toLowerCase() ?? "";
  const metaFileExt =
    typeof metadata.file_extension === "string"
      ? metadata.file_extension.replace(/^\./, "").toLowerCase()
      : "";
  const fileExt = metaFileExt || rawFileExt || "csv";
  if (fileExt === "xlsx" || fileExt === "xls") {
    return {
      ok: false,
      status: 415,
      error:
        "Product Identity import currently requires CSV or TXT. Export Excel as CSV and re-upload.",
    };
  }
  if (fileExt !== "csv" && fileExt !== "txt") {
    return {
      ok: false,
      status: 415,
      error: `Unsupported Product Identity file type: .${fileExt}`,
    };
  }

  const { data: blob, error: dlErr } = await supabase.storage.from("raw-reports").download(rawFilePath);
  if (dlErr || !blob) {
    return {
      ok: false,
      status: 500,
      error: dlErr?.message ?? `Could not download file from storage: ${rawFilePath}`,
    };
  }

  const webStream = blob.stream() as unknown as ReadableStream<Uint8Array>;
  const source = Readable.fromWeb(webStream as Parameters<typeof Readable.fromWeb>[0]);
  const headerRowIndex =
    typeof metadata.header_row_index === "number" && metadata.header_row_index > 0
      ? Math.floor(metadata.header_row_index)
      : 0;

  const csvRows = await readProductIdentityCsvRowsFromStream(source, {
    skipLines: headerRowIndex,
    separator: fileExt === "txt" ? "\t" : ",",
  });

  const detectedHeaders = Array.isArray(metadata.csv_headers)
    ? (metadata.csv_headers as unknown[]).map((h) => String(h ?? "")).filter(Boolean)
    : Object.keys(csvRows[0] ?? {});

  const stats = await runProductIdentityImport({
    supabase,
    organizationId,
    storeId,
    uploadId,
    csvRows,
    columnMapping: productIdentityColumnMappingFromAny(uploadRow.column_mapping),
    sourceFileSha256: contentSha256,
  });

  return {
    ok: true,
    stats,
    storeId,
    contentSha256,
    rowsParsed: csvRows.length,
    detectedHeaders,
  };
}

export async function runProductIdentityImport(params: {
  supabase: SupabaseClient;
  organizationId: string;
  storeId: string;
  uploadId: string;
  csvRows: CsvRow[];
  columnMapping?: ProductIdentityColumnMapping | null;
  sourceFileSha256: string;
}): Promise<ProductIdentityImportStats> {
  const { supabase, organizationId, storeId, uploadId, csvRows, columnMapping, sourceFileSha256 } = params;
  const stats = emptyProductIdentityImportStats();
  stats.rowsRead = csvRows.length;

  const normalizedRows: NormalizedRow[] = [];
  for (const [index, row] of csvRows.entries()) {
    const result = normalizeRow(row, index + 1, stats, columnMapping);
    if (result.kind === "ok") {
      normalizedRows.push(result.row);
      continue;
    }

    // Per-reason accounting (diagnostic counters surfaced into metadata).
    if (result.kind === "missing_seller_sku") {
      stats.rowsMissingSellerSku += 1;
      stats.skippedReasonCounts.missing_seller_sku += 1;
    } else {
      stats.rowsInvalidSellerSku += 1;
      stats.skippedReasonCounts.invalid_seller_sku += 1;
      if (stats.invalidSkuExamples.length < MAX_INVALID_SKU_EXAMPLES) {
        stats.invalidSkuExamples.push({
          rowNumber: index + 1,
          rawValue: result.rawValue,
          reason: "matches_identifier_ignore_value_or_excel_error_token",
        });
      }
    }
    stats.rowsSkipped += 1;
    // Keep legacy field for backward-compat readers.
    stats.unresolvedRows += 1;

    await upsertBacklog(supabase, {
      organizationId,
      storeId,
      sourceUploadId: uploadId,
      row: null,
      identifierType: "SKU",
      identifierValue: result.kind === "invalid_seller_sku" ? result.rawValue : null,
      reason:
        result.kind === "missing_seller_sku"
          ? "missing_required_seller_sku"
          : "invalid_seller_sku_value",
      rawPayload: {
        ...row,
        _product_identity_import: {
          source_upload_id: uploadId,
          source_physical_row_number: index + 1,
          skipped_reason: result.kind,
          ...(result.kind === "invalid_seller_sku" ? { raw_value: result.rawValue } : {}),
        },
      },
    });
  }

  stats.invalidIdentifierCount = stats.invalidAsinCount + stats.invalidFnskuCount + stats.invalidUpcCount;

  // ──────────────────────────────────────────────────────────────────────
  // Dedupe BEFORE products upsert. The bulk Postgres upsert into
  //   products(organization_id, store_id, sku)
  // rejects any batch with two rows that share the conflict tuple
  // ("ON CONFLICT DO UPDATE command cannot affect row a second time",
  // SQLSTATE 21000). A Product Identity CSV may legitimately list the same
  // Seller SKU multiple times (e.g. one row per ASIN), so collapse first.
  // ──────────────────────────────────────────────────────────────────────
  const dedupedRows = dedupeNormalizedRowsBySku(organizationId, storeId, normalizedRows, stats);

  // Surface duplicate-conflict groups to the unresolved backlog so the
  // operator sees which SKUs disagreed on ASIN/FNSKU/UPC across CSV rows.
  for (const row of dedupedRows) {
    if (row.conflictingFields.length === 0) continue;
    await upsertBacklog(supabase, {
      organizationId,
      storeId,
      sourceUploadId: uploadId,
      row,
      identifierType: "SKU",
      identifierValue: row.sku,
      reason: "duplicate_seller_sku_with_identifier_conflict",
      rawPayload: {
        sku: row.sku,
        canonical_row_number: row.rowNumber,
        merged_row_numbers: row.mergedRows.map((m) => m.rowNumber),
        conflicting_fields: row.conflictingFields,
        chosen_identifiers: { asin: row.asin, fnsku: row.fnsku, upc: row.upc },
        alternative_identifiers: row.alternativeIdentifiers,
      },
    });
  }

  const productIdBySku = await upsertProducts({
    supabase,
    organizationId,
    storeId,
    rows: dedupedRows,
    sourceUploadId: uploadId,
    sourceFileSha256,
    stats,
  });

  const catalogIdBySku = await upsertCatalogProducts({
    supabase,
    organizationId,
    storeId,
    rows: dedupedRows,
    sourceUploadId: uploadId,
    stats,
  });

  stats.ambiguousIdentifierCount = countAmbiguousIdentifiers(dedupedRows, productIdBySku);

  await upsertIdentifierMap({
    supabase,
    organizationId,
    storeId,
    rows: dedupedRows,
    productIdBySku,
    catalogIdBySku,
    sourceUploadId: uploadId,
    sourceFileSha256,
    stats,
  });

  await finalizeUpload({
    supabase,
    organizationId,
    uploadId,
    stats,
    rows: dedupedRows,
    sourceFileSha256,
  });

  return stats;
}

// ══════════════════════════════════════════════════════════════════════════════
// NEW Phase 2 / Phase 3 split  (2026-04-29)
// ══════════════════════════════════════════════════════════════════════════════

const PI_STAGING_TABLE = "product_identity_staging_rows";
const PI_STAGING_CHUNK = 250;
const PI_STAGING_UPSERT_CONFLICT = "upload_id,source_physical_row_number";

/**
 * Phase 2 — Process: parse the raw CSV from Storage, validate each row,
 * and write ONLY to `product_identity_staging_rows`.
 *
 * This function does NOT write to:
 *   - products
 *   - catalog_products
 *   - product_identifier_map
 *
 * After completion the upload is moved to `staged` so the UI can show the
 * Sync button. Progress is persisted to `file_processing_status` after every
 * chunk so polling gives real advancement.
 */
export async function processProductIdentityToStaging(params: {
  supabase: SupabaseClient;
  organizationId: string;
  uploadId: string;
  uploadRow: Record<string, unknown>;
  metadata: Record<string, unknown>;
  /** Callback invoked after each staging batch so the caller can update FPS. */
  onChunkProgress?: (params: { staged: number; total: number }) => Promise<void>;
}): Promise<ProductIdentityPipelineResult> {
  const { supabase, organizationId, uploadId, uploadRow, metadata } = params;

  // ── Resolve store ─────────────────────────────────────────────────────────
  const storeId =
    typeof metadata.import_store_id === "string" && isUuidString(metadata.import_store_id.trim())
      ? metadata.import_store_id.trim()
      : typeof metadata.ledger_store_id === "string" && isUuidString(metadata.ledger_store_id.trim())
        ? metadata.ledger_store_id.trim()
        : "";
  if (!isUuidString(storeId)) {
    return { ok: false, status: 400, error: "Product Identity import requires a selected target store." };
  }

  // ── Resolve file ──────────────────────────────────────────────────────────
  const rawFilePath = typeof metadata.raw_file_path === "string" ? metadata.raw_file_path.trim() : "";
  if (!rawFilePath) {
    return { ok: false, status: 400, error: "Missing raw_file_path in upload metadata for Product Identity import." };
  }
  const contentSha256 =
    typeof metadata.content_sha256 === "string" && /^[a-f0-9]{64}$/i.test(metadata.content_sha256.trim())
      ? metadata.content_sha256.trim().toLowerCase()
      : "";
  if (!contentSha256) {
    return { ok: false, status: 400, error: "Missing content SHA-256 for Product Identity import." };
  }
  const rawFileExt = rawFilePath.split(".").pop()?.toLowerCase() ?? "";
  const metaFileExt =
    typeof metadata.file_extension === "string"
      ? metadata.file_extension.replace(/^\./, "").toLowerCase()
      : "";
  const fileExt = metaFileExt || rawFileExt || "csv";
  if (fileExt === "xlsx" || fileExt === "xls") {
    return { ok: false, status: 415, error: "Product Identity import currently requires CSV or TXT. Export Excel as CSV and re-upload." };
  }
  if (fileExt !== "csv" && fileExt !== "txt") {
    return { ok: false, status: 415, error: `Unsupported Product Identity file type: .${fileExt}` };
  }

  // ── Download ──────────────────────────────────────────────────────────────
  const { data: blob, error: dlErr } = await supabase.storage.from("raw-reports").download(rawFilePath);
  if (dlErr || !blob) {
    return { ok: false, status: 500, error: dlErr?.message ?? `Could not download file: ${rawFilePath}` };
  }

  const webStream = blob.stream() as unknown as ReadableStream<Uint8Array>;
  const source = Readable.fromWeb(webStream as Parameters<typeof Readable.fromWeb>[0]);
  const headerRowIndex =
    typeof metadata.header_row_index === "number" && metadata.header_row_index > 0
      ? Math.floor(metadata.header_row_index)
      : 0;

  const csvRows = await readProductIdentityCsvRowsFromStream(source, {
    skipLines: headerRowIndex,
    separator: fileExt === "txt" ? "\t" : ",",
  });

  const columnMapping = productIdentityColumnMappingFromAny(uploadRow.column_mapping);
  const detectedHeaders = Array.isArray(metadata.csv_headers)
    ? (metadata.csv_headers as unknown[]).map((h) => String(h ?? "")).filter(Boolean)
    : Object.keys(csvRows[0] ?? {});

  // ── Parse / validate each CSV row → staging payload ───────────────────────
  const stats = emptyProductIdentityImportStats();
  stats.rowsRead = csvRows.length;

  const stagedPayloads: Record<string, unknown>[] = [];

  for (const [index, row] of csvRows.entries()) {
    const rowNumber = index + 1;
    const result = normalizeRow(row, rowNumber, stats, columnMapping);

    // Compute a deterministic line hash for idempotency lookups.
    const lineHashInput = JSON.stringify(row);
    // Use a simple djb2 fingerprint — no crypto needed for staging dedup.
    let h = 5381;
    for (let i = 0; i < lineHashInput.length; i++) {
      h = ((h << 5) + h) ^ lineHashInput.charCodeAt(i);
    }
    const sourceLineHash = (h >>> 0).toString(16).padStart(8, "0");

    const validationErrors: Record<string, string> = {};
    let normalizedData: Record<string, unknown> = {};

    if (result.kind === "missing_seller_sku") {
      stats.rowsMissingSellerSku += 1;
      stats.skippedReasonCounts.missing_seller_sku += 1;
      stats.rowsSkipped += 1;
      stats.unresolvedRows += 1;
      validationErrors.seller_sku = "missing";
    } else if (result.kind === "invalid_seller_sku") {
      stats.rowsInvalidSellerSku += 1;
      stats.skippedReasonCounts.invalid_seller_sku += 1;
      stats.rowsSkipped += 1;
      stats.unresolvedRows += 1;
      if (stats.invalidSkuExamples.length < MAX_INVALID_SKU_EXAMPLES) {
        stats.invalidSkuExamples.push({
          rowNumber,
          rawValue: result.rawValue,
          reason: "matches_identifier_ignore_value_or_excel_error_token",
        });
      }
      validationErrors.seller_sku = `invalid_value:${result.rawValue}`;
    } else {
      const nr = result.row;
      normalizedData = {
        seller_sku: nr.sku,
        product_name: nr.productName,
        vendor_name: nr.vendorName,
        mfg_part_number: nr.mfgPartNumber,
        asin: nr.asin,
        fnsku: nr.fnsku,
        upc_code: nr.upc,
      };
    }

    const normalizedRow = result.kind === "ok" ? result.row : null;

    stagedPayloads.push({
      upload_id: uploadId,
      organization_id: organizationId,
      store_id: storeId,
      source_file_sha256: contentSha256,
      source_physical_row_number: rowNumber,
      source_line_hash: sourceLineHash,
      seller_sku: normalizedRow?.sku ?? null,
      asin: normalizedRow?.asin ?? null,
      fnsku: normalizedRow?.fnsku ?? null,
      upc_code: normalizedRow?.upc ?? null,
      vendor_name: normalizedRow?.vendorName ?? null,
      mfg_part_number: normalizedRow?.mfgPartNumber ?? null,
      product_name: normalizedRow?.productName ?? null,
      raw_data: row,
      normalized_data: normalizedData,
      validation_errors: validationErrors,
    });
  }

  stats.invalidIdentifierCount = stats.invalidAsinCount + stats.invalidFnskuCount + stats.invalidUpcCount;

  // ── Write to staging in chunks, updating progress after each ─────────────
  // We write in smaller chunks (100 rows) and report progress after EACH
  // chunk so the polling UI sees real advancement instead of a stuck bar.
  const WRITE_CHUNK = 100;
  const totalRows = stagedPayloads.length;
  let stagedCount = 0;

  for (let i = 0; i < stagedPayloads.length; i += WRITE_CHUNK) {
    const chunkPayload = stagedPayloads.slice(i, i + WRITE_CHUNK);
    const { error: insErr } = await supabase
      .from(PI_STAGING_TABLE)
      .upsert(chunkPayload, { onConflict: PI_STAGING_UPSERT_CONFLICT, ignoreDuplicates: false });
    if (insErr) {
      return {
        ok: false,
        status: 500,
        error: `product_identity_staging_rows write failed: ${insErr.message}`,
      };
    }
    stagedCount += chunkPayload.length;
    // Report progress on every chunk so the polling bar advances.
    await params.onChunkProgress?.({ staged: stagedCount, total: totalRows });
  }

  // ── Finalise Phase 2: set upload to 'staged', transition FPS ─────────────
  const now = new Date().toISOString();
  const metaPatch = {
    process_progress: 100,
    total_rows: stats.rowsRead,
    import_metrics: {
      current_phase: "staged",
      data_rows_seen: stats.rowsRead,
      rows_staged: stagedCount,
      rows_missing_seller_sku: stats.rowsMissingSellerSku,
      rows_invalid_seller_sku: stats.rowsInvalidSellerSku,
      rows_skipped: stats.rowsSkipped,
      detected_headers: detectedHeaders,
      detected_report_type: "PRODUCT_IDENTITY",
    },
  };
  // Try with import_pipeline_staged_at first; fall back if the column does not
  // exist yet (pre-migration 20260641 databases).
  let finalizeErr: { message?: string } | null = null;
  {
    const { error } = await supabase
      .from("raw_report_uploads")
      .update({
        status: "staged",
        import_pipeline_staged_at: now,
        metadata: mergeUploadMetadata(metadata, metaPatch as Parameters<typeof mergeUploadMetadata>[1]),
        updated_at: now,
      })
      .eq("id", uploadId)
      .eq("organization_id", organizationId);
    finalizeErr = error;
  }
  if (finalizeErr) {
    const msg = (finalizeErr.message ?? "").toLowerCase();
    const unknownCol = msg.includes("import_pipeline_staged_at") &&
      (msg.includes("does not exist") || msg.includes("schema cache") || msg.includes("column"));
    if (!unknownCol) {
      throw new Error(`raw_report_uploads staged finalise failed: ${finalizeErr.message}`);
    }
    // Retry without the missing column.
    await supabase
      .from("raw_report_uploads")
      .update({
        status: "staged",
        metadata: mergeUploadMetadata(metadata, metaPatch as Parameters<typeof mergeUploadMetadata>[1]),
        updated_at: now,
      })
      .eq("id", uploadId)
      .eq("organization_id", organizationId);
  }

  return {
    ok: true,
    stats,
    storeId,
    contentSha256,
    rowsParsed: csvRows.length,
    detectedHeaders,
  };
}

/**
 * Phase 3 — Sync: read from `product_identity_staging_rows`, deduplicate,
 * then upsert final tables (products, catalog_products, product_identifier_map).
 *
 * Progress is persisted after every chunk via the optional `onChunkProgress`
 * callback so polling gives real advancement.
 */
export async function syncProductIdentityFromStaging(params: {
  supabase: SupabaseClient;
  organizationId: string;
  uploadId: string;
  uploadRow: Record<string, unknown>;
  metadata: Record<string, unknown>;
  onChunkProgress?: (params: { synced: number; total: number }) => Promise<void>;
}): Promise<ProductIdentityPipelineResult> {
  const { supabase, organizationId, uploadId, metadata } = params;

  const storeId =
    typeof metadata.import_store_id === "string" && isUuidString(metadata.import_store_id.trim())
      ? metadata.import_store_id.trim()
      : typeof metadata.ledger_store_id === "string" && isUuidString(metadata.ledger_store_id.trim())
        ? metadata.ledger_store_id.trim()
        : "";
  if (!isUuidString(storeId)) {
    return { ok: false, status: 400, error: "Product Identity sync requires a target store in upload metadata." };
  }

  const contentSha256 =
    typeof metadata.content_sha256 === "string" && /^[a-f0-9]{64}$/i.test(metadata.content_sha256.trim())
      ? metadata.content_sha256.trim().toLowerCase()
      : "";
  if (!contentSha256) {
    return { ok: false, status: 400, error: "Missing content SHA-256 in upload metadata." };
  }

  const detectedHeaders = Array.isArray(metadata.csv_headers)
    ? (metadata.csv_headers as unknown[]).map((h) => String(h ?? "")).filter(Boolean)
    : [];

  // ── Read all staging rows for this upload ─────────────────────────────────
  const READ_CHUNK = 1000;
  let offset = 0;
  const allStagingRows: Record<string, unknown>[] = [];
  for (;;) {
    const { data, error } = await supabase
      .from(PI_STAGING_TABLE)
      .select("*")
      .eq("upload_id", uploadId)
      .eq("organization_id", organizationId)
      .order("source_physical_row_number", { ascending: true })
      .range(offset, offset + READ_CHUNK - 1);
    if (error) {
      return { ok: false, status: 500, error: `product_identity_staging_rows read failed: ${error.message}` };
    }
    if (!data || data.length === 0) break;
    allStagingRows.push(...(data as Record<string, unknown>[]));
    if (data.length < READ_CHUNK) break;
    offset += READ_CHUNK;
  }

  if (allStagingRows.length === 0) {
    return { ok: false, status: 409, error: "No staging rows found for this upload. Run Process first." };
  }

  // ── Convert staging rows into NormalizedRows ──────────────────────────────
  const stats = emptyProductIdentityImportStats();
  stats.rowsRead = allStagingRows.length;

  const rawNormalizedRows: NormalizedRow[] = [];
  for (const sr of allStagingRows) {
    const nd = sr.normalized_data && typeof sr.normalized_data === "object" && !Array.isArray(sr.normalized_data)
      ? (sr.normalized_data as Record<string, unknown>)
      : {};
    const sku = typeof sr.seller_sku === "string" ? sr.seller_sku.trim() : "";
    if (!sku) {
      stats.rowsSkipped += 1;
      stats.unresolvedRows += 1;
      continue;
    }
    const rowNumber = typeof sr.source_physical_row_number === "number" ? sr.source_physical_row_number : 0;
    const original = sr.raw_data && typeof sr.raw_data === "object" && !Array.isArray(sr.raw_data)
      ? (sr.raw_data as Record<string, string>)
      : {};

    // Validate identifiers (ASIN/FNSKU/UPC) against the same regex rules, so
    // the sync phase has the same quality gates as the combined approach.
    const rawAsin = typeof nd.asin === "string" ? nd.asin : "";
    const rawFnsku = typeof nd.fnsku === "string" ? nd.fnsku : "";
    const rawUpc = typeof nd.upc_code === "string" ? nd.upc_code : "";
    const asin = normalizeAsin(rawAsin, stats);
    const fnsku = normalizeFnsku(rawFnsku, stats);
    const upc = normalizeUpc(rawUpc, stats);

    rawNormalizedRows.push({
      rowNumber,
      original,
      sku,
      productName: typeof nd.product_name === "string" ? nd.product_name || sku : sku,
      vendorName: typeof nd.vendor_name === "string" ? nd.vendor_name || null : null,
      mfgPartNumber: typeof nd.mfg_part_number === "string" ? nd.mfg_part_number || null : null,
      asin,
      fnsku,
      upc,
    });
  }

  stats.invalidIdentifierCount = stats.invalidAsinCount + stats.invalidFnskuCount + stats.invalidUpcCount;

  // ── Dedupe by (org, store, sku) ───────────────────────────────────────────
  const dedupedRows = dedupeNormalizedRowsBySku(organizationId, storeId, rawNormalizedRows, stats);

  // Conflict groups → backlog
  for (const row of dedupedRows) {
    if (row.conflictingFields.length === 0) continue;
    await upsertBacklog(supabase, {
      organizationId,
      storeId,
      sourceUploadId: uploadId,
      row,
      identifierType: "SKU",
      identifierValue: row.sku,
      reason: "duplicate_seller_sku_with_identifier_conflict",
      rawPayload: {
        sku: row.sku,
        canonical_row_number: row.rowNumber,
        conflicting_fields: row.conflictingFields,
        chosen_identifiers: { asin: row.asin, fnsku: row.fnsku, upc: row.upc },
        alternative_identifiers: row.alternativeIdentifiers,
      },
    });
  }

  // ── Upsert final tables with per-chunk progress ───────────────────────────
  const totalSync = dedupedRows.length;
  let syncedCount = 0;
  const SYNC_CHUNK = 250;

  // Process products in chunks and report progress.
  const productIdBySku = new Map<string, string>();
  {
    const existingBySku = await prefetchProducts(supabase, organizationId, storeId, dedupedRows.map((r) => r.sku));
    let provenanceSupported = true;
    for (const rowChunk of chunk(dedupedRows, SYNC_CHUNK)) {
      const now = new Date().toISOString();
      const payload = rowChunk.map((row) => {
        const existing = existingBySku.get(row.sku);
        const existingProv = existing?.field_provenance ?? null;
        const writeProductName = shouldProductIdentityOverwrite(existing?.product_name, existingProv, "product_name", row.productName);
        const writeVendor = shouldProductIdentityOverwrite(existing?.vendor_name, existingProv, "vendor_name", row.vendorName);
        const writeMfg = shouldProductIdentityOverwrite(existing?.mfg_part_number, existingProv, "mfg_part_number", row.mfgPartNumber);
        const writeUpc = shouldProductIdentityOverwrite(existing?.upc_code, existingProv, "upc_code", row.upc);
        const writeAsin = shouldProductIdentityOverwrite(existing?.asin, existingProv, "asin", row.asin);
        const writeFnsku = shouldProductIdentityOverwrite(existing?.fnsku, existingProv, "fnsku", row.fnsku);
        const product_name = writeProductName ? row.productName : (existing?.product_name ?? row.productName ?? row.sku);
        const vendor_name = writeVendor ? row.vendorName : (existing?.vendor_name ?? null);
        const mfg_part_number = writeMfg ? row.mfgPartNumber : (existing?.mfg_part_number ?? null);
        const upc_code = writeUpc ? row.upc : (existing?.upc_code ?? null);
        const asin = writeAsin ? row.asin : (existing?.asin ?? null);
        const fnsku = writeFnsku ? row.fnsku : (existing?.fnsku ?? null);
        const fieldProvenance = buildProductIdentityFieldProvenance({
          existing,
          payload: { product_name, vendor_name, mfg_part_number, upc_code, asin, fnsku },
          written: {
            product_name: writeProductName,
            vendor_name: writeVendor,
            mfg_part_number: writeMfg,
            upc_code: writeUpc,
            asin: writeAsin,
            fnsku: writeFnsku,
          },
          uploadId,
        });
        const base: Record<string, unknown> = {
          organization_id: organizationId,
          store_id: storeId,
          sku: row.sku,
          product_name, vendor_name, mfg_part_number, upc_code, asin, fnsku,
          metadata: productMetadata(existing?.metadata, row, uploadId, contentSha256),
          last_seen_at: now,
          last_catalog_sync_at: now,
        };
        if (provenanceSupported) base.field_provenance = fieldProvenance;
        return base;
      });
      const upsertOnce = async (rows: Record<string, unknown>[]) =>
        supabase.from("products").upsert(rows, { onConflict: "organization_id,store_id,sku" }).select("id, sku");
      let { data, error } = await upsertOnce(payload);
      if (error && provenanceSupported) {
        const msg = (error.message ?? "").toLowerCase();
        if (msg.includes("field_provenance") && (msg.includes("does not exist") || msg.includes("schema cache") || msg.includes("column"))) {
          provenanceSupported = false;
          const fallback = payload.map(({ field_provenance: _omit, ...rest }: Record<string, unknown> & { field_provenance?: unknown }) => rest);
          const retry = await upsertOnce(fallback);
          data = retry.data; error = retry.error;
        }
      }
      if (error) throw new Error(`products upsert failed: ${error.message}`);
      for (const product of (data ?? []) as ProductRecord[]) {
        productIdBySku.set(product.sku, product.id);
        if (existingBySku.has(product.sku)) stats.productsUpdated += 1;
        else stats.productsInserted += 1;
      }
      syncedCount += rowChunk.length;
      await params.onChunkProgress?.({ synced: syncedCount, total: totalSync });
    }
  }

  // Catalog products.
  const catalogIdBySku = await upsertCatalogProducts({
    supabase, organizationId, storeId,
    rows: dedupedRows, sourceUploadId: uploadId, stats,
  });
  stats.catalogUniqueCount = (stats.catalogProductsInserted + stats.catalogProductsUpdated);

  stats.ambiguousIdentifierCount = countAmbiguousIdentifiers(dedupedRows, productIdBySku);

  await upsertIdentifierMap({
    supabase, organizationId, storeId,
    rows: dedupedRows, productIdBySku, catalogIdBySku,
    sourceUploadId: uploadId, sourceFileSha256: contentSha256, stats,
  });

  // Finalise in raw_report_uploads.
  await finalizeUpload({ supabase, organizationId, uploadId, stats, rows: dedupedRows, sourceFileSha256: contentSha256 });

  return {
    ok: true,
    stats,
    storeId,
    contentSha256,
    rowsParsed: allStagingRows.length,
    detectedHeaders,
  };
}
