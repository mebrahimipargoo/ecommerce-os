import "server-only";

import csv from "csv-parser";
import type { Readable } from "node:stream";
import type { SupabaseClient } from "@supabase/supabase-js";

import { mergeUploadMetadata } from "./raw-report-upload-metadata";

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
};

export type ProductIdentityColumnMapping = Partial<Record<
  "upc" | "vendor" | "seller_sku" | "mfg_part_number" | "fnsku" | "asin" | "product_name",
  string
>>;

export const PRODUCT_IDENTITY_REPORT_TYPE = "PRODUCT_IDENTITY";

const SOURCE_REPORT_TYPE = "PRODUCT_IDENTITY_IMPORT";
const IDENTIFIER_IGNORE_VALUES = new Set(["", "x", "0", "fbm", "this one is good", "unknown", "null"]);
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
const IDENTIFIER_MAP_UPSERT_CONFLICT = "organization_id,store_id,external_listing_id";
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

function normalizeRow(
  row: CsvRow,
  rowNumber: number,
  stats: ProductIdentityImportStats,
  mapping?: ProductIdentityColumnMapping | null,
): NormalizedRow | null {
  const sku = normalizeIdentifier(mappedCell(row, mapping, "seller_sku"));
  if (!sku) {
    stats.unresolvedRows += 1;
    return null;
  }

  const productName = normalizeIdentifier(mappedCell(row, mapping, "product_name")) ?? sku;

  return {
    rowNumber,
    original: row,
    sku,
    productName,
    vendorName: normalizeIdentifier(mappedCell(row, mapping, "vendor")),
    mfgPartNumber: normalizeIdentifier(mappedCell(row, mapping, "mfg_part_number")),
    asin: normalizeAsin(mappedCell(row, mapping, "asin"), stats),
    fnsku: normalizeFnsku(mappedCell(row, mapping, "fnsku"), stats),
    upc: normalizeUpc(mappedCell(row, mapping, "upc"), stats),
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
  row: NormalizedRow,
  uploadId: string,
  sourceFileSha256: string,
): Record<string, unknown> {
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
  rows: NormalizedRow[];
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

    if (error) throw new Error(`products upsert failed: ${error.message}`);

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
  rows: NormalizedRow[],
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
  rows: NormalizedRow[];
  sourceUploadId: string;
  stats: ProductIdentityImportStats;
}): Promise<Map<string, string>> {
  const { supabase, organizationId, storeId, rows, sourceUploadId, stats } = params;
  const existingByKey = await prefetchCatalogProducts(supabase, organizationId, storeId, rows);
  const catalogIdBySku = new Map<string, string>();

  for (const rowChunk of chunk(rows, CHUNK_SIZE)) {
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

    if (error) throw new Error(`catalog_products upsert failed: ${error.message}`);

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
  row: NormalizedRow;
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

  if (row.asin) identifiers.push({ type: "ASIN", value: row.asin, asin: row.asin, fnsku: null, upc: null });
  if (row.fnsku) identifiers.push({ type: "FNSKU", value: row.fnsku, asin: null, fnsku: row.fnsku, upc: null });
  if (row.upc) identifiers.push({ type: "UPC", value: row.upc, asin: null, fnsku: null, upc: row.upc });

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
): Promise<Map<string, string>> {
  const byKey = new Map<string, string>();

  for (const skuChunk of chunk([...new Set(skus)], 100)) {
    const { data, error } = await supabase
      .from("product_identifier_map")
      .select("id, store_id, seller_sku, asin, fnsku, upc_code, external_listing_id")
      .eq("organization_id", organizationId)
      .eq("store_id", storeId)
      .in("seller_sku", skuChunk);

    if (error) throw new Error(`product_identifier_map prefetch failed: ${error.message}`);

    for (const row of (data ?? []) as ExistingIdentifierMapRow[]) {
      byKey.set(identifierMapKey(row), row.id);
    }
  }

  return byKey;
}

async function upsertIdentifierMap(params: {
  supabase: SupabaseClient;
  organizationId: string;
  storeId: string;
  rows: NormalizedRow[];
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
      const existingId = existingByKey.get(key);
      if (existingId) {
        toUpdate.push({
          id: existingId,
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
      } else {
        toInsert.push(mapRow);
      }
    }
  }

  for (const insertChunk of chunk(toInsert, CHUNK_SIZE)) {
    // Upsert (not bare insert) so a re-import of the same Product Identity CSV
    // is idempotent at the database level. The partial unique index
    // `uq_product_identifier_map_product_identity` covers product_identity
    // rows by (organization_id, store_id, external_listing_id) — the same
    // tuple this code already produces deterministically.
    const { error } = await supabase
      .from("product_identifier_map")
      .upsert(insertChunk, { onConflict: IDENTIFIER_MAP_UPSERT_CONFLICT, ignoreDuplicates: false });
    if (!error) {
      stats.identifiersInserted += insertChunk.length;
      continue;
    }

    // Per-row fallback: useful when a single bad row in the chunk would otherwise
    // poison the whole batch. Records the failure into the backlog for triage.
    for (const row of insertChunk) {
      const { error: rowError } = await supabase
        .from("product_identifier_map")
        .upsert(row, { onConflict: IDENTIFIER_MAP_UPSERT_CONFLICT, ignoreDuplicates: false });
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

function countAmbiguousIdentifiers(rows: NormalizedRow[], productIdBySku: Map<string, string>): number {
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
  rows: NormalizedRow[];
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

  const metadataPatch = {
    row_count: stats.rowsRead,
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
    } as Record<string, unknown>,
    import_metrics: {
      current_phase: "complete",
      data_rows_seen: stats.rowsRead,
      rows_synced_upserted:
        stats.productsInserted +
        stats.productsUpdated +
        stats.catalogProductsInserted +
        stats.catalogProductsUpdated +
        stats.identifiersInserted,
      rows_invalid: stats.invalidIdentifierCount,
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
    const normalized = normalizeRow(row, index + 1, stats, columnMapping);
    if (normalized) {
      normalizedRows.push(normalized);
      continue;
    }

    await upsertBacklog(supabase, {
      organizationId,
      storeId,
      sourceUploadId: uploadId,
      row: null,
      identifierType: "SKU",
      identifierValue: null,
      reason: "missing_required_seller_sku",
      rawPayload: {
        ...row,
        _product_identity_import: {
          source_upload_id: uploadId,
          source_physical_row_number: index + 1,
        },
      },
    });
  }

  stats.invalidIdentifierCount = stats.invalidAsinCount + stats.invalidFnskuCount + stats.invalidUpcCount;

  const productIdBySku = await upsertProducts({
    supabase,
    organizationId,
    storeId,
    rows: normalizedRows,
    sourceUploadId: uploadId,
    sourceFileSha256,
    stats,
  });

  const catalogIdBySku = await upsertCatalogProducts({
    supabase,
    organizationId,
    storeId,
    rows: normalizedRows,
    sourceUploadId: uploadId,
    stats,
  });

  stats.ambiguousIdentifierCount = countAmbiguousIdentifiers(normalizedRows, productIdBySku);

  await upsertIdentifierMap({
    supabase,
    organizationId,
    storeId,
    rows: normalizedRows,
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
    rows: normalizedRows,
    sourceFileSha256,
  });

  return stats;
}
