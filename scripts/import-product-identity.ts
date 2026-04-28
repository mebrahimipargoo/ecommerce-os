/**
 * One-off product identity import for:
 *   All Items - Database - For Maysam - 2026.csv
 *
 * Requires:
 *   NEXT_PUBLIC_SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 *
 * Usage:
 *   npx tsx scripts/import-product-identity.ts \
 *     --file "All Items - Database - For Maysam - 2026.csv" \
 *     --organization-id <uuid> \
 *     --store-id <uuid>
 */

import { createHash } from "node:crypto";
import { createReadStream, existsSync, readFileSync } from "node:fs";
import { basename, isAbsolute, join, resolve } from "node:path";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import csv from "csv-parser";

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
};

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

type ImportStats = {
  rowsRead: number;
  productsInserted: number;
  productsUpdated: number;
  catalogProductsInserted: number;
  catalogProductsUpdated: number;
  identifiersInserted: number;
  invalidAsinCount: number;
  invalidFnskuCount: number;
  invalidUpcCount: number;
  ambiguousIdentifierCount: number;
  unresolvedRows: number;
};

const REPORT_TYPE = "UNKNOWN";
const SOURCE_REPORT_TYPE = "PRODUCT_IDENTITY_IMPORT";
const IDENTIFIER_IGNORE_VALUES = new Set(["", "x", "0", "fbm", "this one is good", "unknown", "null"]);
const ASIN_RE = /^B[0-9A-Z]{9}$/;
const FNSKU_RE = /^X[0-9A-Z]{9}$/;
const UPC_RE = /^[0-9]{8,14}$/;
const PRODUCT_UPSERT_CONFLICT = "organization_id,store_id,sku";
const CATALOG_UPSERT_CONFLICT = "organization_id,store_id,seller_sku,asin";
const BACKLOG_UPSERT_CONFLICT = "organization_id,store_id,identifier_type,identifier_value,reason,seller_sku";
const CHUNK_SIZE = 250;

function loadEnvLocal(): void {
  const p = join(process.cwd(), ".env.local");
  if (!existsSync(p)) return;

  for (const line of readFileSync(p, "utf8").split("\n")) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;

    const i = t.indexOf("=");
    if (i === -1) continue;

    const k = t.slice(0, i).trim();
    let v = t.slice(i + 1).trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
      v = v.slice(1, -1);
    }
    if (process.env[k] == null || process.env[k] === "") {
      process.env[k] = v;
    }
  }
}

function parseArgs(argv: string[]): { file: string; organizationId: string; storeId: string } {
  const args = new Map<string, string>();
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith("--")) continue;
    const key = arg.slice(2);
    const value = argv[i + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`Missing value for --${key}`);
    }
    args.set(key, value);
    i += 1;
  }

  const file = args.get("file");
  const organizationId = args.get("organization-id");
  const storeId = args.get("store-id");

  if (!file || !organizationId || !storeId) {
    throw new Error(
      'Usage: npx tsx scripts/import-product-identity.ts --file "All Items - Database - For Maysam - 2026.csv" --organization-id <uuid> --store-id <uuid>',
    );
  }

  return { file, organizationId, storeId };
}

function stripBom(value: string): string {
  return value.replace(/^\uFEFF/, "");
}

function cell(row: CsvRow, header: string): string {
  if (row[header] != null) return String(row[header]);

  const wanted = header.trim().toLowerCase();
  const foundKey = Object.keys(row).find((key) => key.trim().toLowerCase() === wanted);
  return foundKey ? String(row[foundKey] ?? "") : "";
}

function normalizeIdentifier(raw: string): string | null {
  const trimmed = String(raw ?? "").trim();
  if (IDENTIFIER_IGNORE_VALUES.has(trimmed.toLowerCase())) return null;
  return trimmed;
}

function normalizeAsin(raw: string, stats: ImportStats): string | null {
  const value = normalizeIdentifier(raw);
  if (value == null) return null;

  const normalized = value.toUpperCase();
  if (!ASIN_RE.test(normalized)) {
    stats.invalidAsinCount += 1;
    return null;
  }
  return normalized;
}

function normalizeFnsku(raw: string, stats: ImportStats): string | null {
  const value = normalizeIdentifier(raw);
  if (value == null) return null;

  const normalized = value.toUpperCase();
  if (!FNSKU_RE.test(normalized)) {
    stats.invalidFnskuCount += 1;
    return null;
  }
  return normalized;
}

function normalizeUpc(raw: string, stats: ImportStats): string | null {
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

function normalizeRow(row: CsvRow, rowNumber: number, stats: ImportStats): NormalizedRow | null {
  const sku = normalizeIdentifier(cell(row, "Seller SKU"));
  if (!sku) {
    stats.unresolvedRows += 1;
    return null;
  }

  const productName = normalizeIdentifier(cell(row, "Product Name")) ?? sku;

  return {
    rowNumber,
    original: row,
    sku,
    productName,
    vendorName: normalizeIdentifier(cell(row, "Vendor")),
    mfgPartNumber: normalizeIdentifier(cell(row, "Mfg #")),
    asin: normalizeAsin(cell(row, "ASIN"), stats),
    fnsku: normalizeFnsku(cell(row, "FNSKU"), stats),
    upc: normalizeUpc(cell(row, "UPC"), stats),
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

async function hashFile(filePath: string): Promise<{ sha256: string; bytes: number }> {
  const hash = createHash("sha256");
  let bytes = 0;

  await new Promise<void>((resolvePromise, reject) => {
    const stream = createReadStream(filePath);
    stream.on("data", (part: string | Buffer) => {
      const buffer = Buffer.isBuffer(part) ? part : Buffer.from(part);
      bytes += buffer.length;
      hash.update(buffer);
    });
    stream.on("error", reject);
    stream.on("end", resolvePromise);
  });

  return { sha256: hash.digest("hex"), bytes };
}

async function readCsv(filePath: string): Promise<CsvRow[]> {
  const rows: CsvRow[] = [];

  await new Promise<void>((resolvePromise, reject) => {
    createReadStream(filePath)
      .pipe(
        csv({
          mapHeaders: ({ header }) => stripBom(String(header ?? "")).trim(),
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
      .on("end", resolvePromise);
  });

  return rows;
}

async function ensureUploadRow(params: {
  supabase: SupabaseClient;
  organizationId: string;
  storeId: string;
  filePath: string;
  sourceFileSha256: string;
  fileSizeBytes: number;
}): Promise<string> {
  const { supabase, organizationId, storeId, filePath, sourceFileSha256, fileSizeBytes } = params;
  const fileName = basename(filePath);

  const { data: candidates, error: findErr } = await supabase
    .from("raw_report_uploads")
    .select("id, metadata")
    .eq("organization_id", organizationId)
    .eq("file_name", fileName)
    .eq("report_type", REPORT_TYPE)
    .order("created_at", { ascending: false })
    .limit(50);

  if (findErr) throw new Error(`raw_report_uploads lookup failed: ${findErr.message}`);

  const existing = (candidates ?? []).find((row: Record<string, unknown>) => {
    const metadata = asPlainRecord(row.metadata);
    const nested = asPlainRecord(metadata.product_identity_import);
    return metadata.content_sha256 === sourceFileSha256 || nested.content_sha256 === sourceFileSha256;
  }) as { id: string; metadata?: unknown } | undefined;

  const now = new Date().toISOString();
  const metadataPatch = {
    content_sha256: sourceFileSha256,
    file_name: fileName,
    file_size_bytes: fileSizeBytes,
    import_store_id: storeId,
    product_identity_import: {
      content_sha256: sourceFileSha256,
      source: "scripts/import-product-identity.ts",
      file_name: fileName,
      store_id: storeId,
      started_at: now,
    },
  };

  if (existing?.id) {
    const { error } = await supabase
      .from("raw_report_uploads")
      .update({
        status: "processing",
        metadata: { ...asPlainRecord(existing.metadata), ...metadataPatch },
        import_pipeline_started_at: now,
        import_pipeline_completed_at: null,
        import_pipeline_failed_at: null,
        updated_at: now,
      })
      .eq("id", existing.id);

    if (error) throw new Error(`raw_report_uploads update failed: ${error.message}`);
    return existing.id;
  }

  const { data, error } = await supabase
    .from("raw_report_uploads")
    .insert({
      organization_id: organizationId,
      file_name: fileName,
      report_type: REPORT_TYPE,
      status: "processing",
      metadata: metadataPatch,
      import_pipeline_started_at: now,
      updated_at: now,
    })
    .select("id")
    .single();

  if (error) throw new Error(`raw_report_uploads insert failed: ${error.message}`);
  return String((data as { id: string }).id);
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
    const { data, error } = await supabase
      .from("products")
      .select("id, sku, product_name, vendor_name, mfg_part_number, upc_code, asin, fnsku, metadata")
      .eq("organization_id", organizationId)
      .eq("store_id", storeId)
      .in("sku", skuChunk);

    if (error) throw new Error(`products prefetch failed: ${error.message}`);
    for (const row of (data ?? []) as ProductRecord[]) {
      bySku.set(row.sku, row);
    }
  }

  return bySku;
}

async function upsertProducts(params: {
  supabase: SupabaseClient;
  organizationId: string;
  storeId: string;
  rows: NormalizedRow[];
  sourceUploadId: string;
  sourceFileSha256: string;
  stats: ImportStats;
}): Promise<Map<string, string>> {
  const { supabase, organizationId, storeId, rows, sourceUploadId, sourceFileSha256, stats } = params;
  const existingBySku = await prefetchProducts(
    supabase,
    organizationId,
    storeId,
    rows.map((row) => row.sku),
  );
  const productIdBySku = new Map<string, string>();

  for (const rowChunk of chunk(rows, CHUNK_SIZE)) {
    const payload = rowChunk.map((row) => {
      const existing = existingBySku.get(row.sku);
      return {
        organization_id: organizationId,
        store_id: storeId,
        sku: row.sku,
        product_name: row.productName || existing?.product_name || row.sku,
        vendor_name: row.vendorName ?? existing?.vendor_name ?? null,
        mfg_part_number: row.mfgPartNumber ?? existing?.mfg_part_number ?? null,
        upc_code: row.upc ?? existing?.upc_code ?? null,
        asin: row.asin ?? existing?.asin ?? null,
        fnsku: row.fnsku ?? existing?.fnsku ?? null,
        metadata: productMetadata(existing?.metadata, row, sourceUploadId, sourceFileSha256),
        last_seen_at: new Date().toISOString(),
        last_catalog_sync_at: new Date().toISOString(),
      };
    });

    const { data, error } = await supabase
      .from("products")
      .upsert(payload, { onConflict: PRODUCT_UPSERT_CONFLICT })
      .select("id, sku");

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
  stats: ImportStats;
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
  stats: ImportStats;
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
    const { error } = await supabase.from("product_identifier_map").insert(insertChunk);
    if (!error) {
      stats.identifiersInserted += insertChunk.length;
      continue;
    }

    for (const row of insertChunk) {
      const { error: rowError } = await supabase.from("product_identifier_map").insert(row);
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
  uploadId: string;
  stats: ImportStats;
  rows: NormalizedRow[];
  sourceFileSha256: string;
}): Promise<void> {
  const { supabase, uploadId, stats, rows, sourceFileSha256 } = params;
  const now = new Date().toISOString();

  const { data: upload, error: fetchErr } = await supabase
    .from("raw_report_uploads")
    .select("metadata")
    .eq("id", uploadId)
    .maybeSingle();

  if (fetchErr) throw new Error(`raw_report_uploads final lookup failed: ${fetchErr.message}`);

  const metadata = asPlainRecord((upload as { metadata?: unknown } | null)?.metadata);
  const productIdentityImport = asPlainRecord(metadata.product_identity_import);

  const { error } = await supabase
    .from("raw_report_uploads")
    .update({
      status: "synced",
      import_pipeline_completed_at: now,
      metadata: {
        ...metadata,
        row_count: stats.rowsRead,
        total_rows: stats.rowsRead,
        content_sha256: sourceFileSha256,
        product_identity_import: {
          ...productIdentityImport,
          completed_at: now,
          normalized_rows: rows.length,
          stats,
        },
        import_metrics: {
          current_phase: "complete",
          rows_synced_upserted: stats.productsInserted + stats.productsUpdated,
          rows_invalid: stats.invalidAsinCount + stats.invalidFnskuCount + stats.invalidUpcCount,
        },
        etl_phase: "complete",
      },
      updated_at: now,
    })
    .eq("id", uploadId);

  if (error) throw new Error(`raw_report_uploads finalize failed: ${error.message}`);
}

function printStats(stats: ImportStats): void {
  console.log("Product identity import complete:");
  console.log(`rows read: ${stats.rowsRead}`);
  console.log(`products inserted/updated: ${stats.productsInserted}/${stats.productsUpdated}`);
  console.log(`catalog_products inserted/updated: ${stats.catalogProductsInserted}/${stats.catalogProductsUpdated}`);
  console.log(`identifiers inserted: ${stats.identifiersInserted}`);
  console.log(`invalid ASIN count: ${stats.invalidAsinCount}`);
  console.log(`invalid FNSKU count: ${stats.invalidFnskuCount}`);
  console.log(`invalid UPC count: ${stats.invalidUpcCount}`);
  console.log(`ambiguous identifier count: ${stats.ambiguousIdentifierCount}`);
  console.log(`unresolved rows: ${stats.unresolvedRows}`);
}

async function main(): Promise<void> {
  loadEnvLocal();

  const { file, organizationId, storeId } = parseArgs(process.argv.slice(2));
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!supabaseUrl || !serviceRoleKey) {
    throw new Error("Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
  }

  const filePath = isAbsolute(file) ? file : resolve(process.cwd(), file);
  if (!existsSync(filePath)) {
    throw new Error(`CSV file not found: ${filePath}`);
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });
  const stats: ImportStats = {
    rowsRead: 0,
    productsInserted: 0,
    productsUpdated: 0,
    catalogProductsInserted: 0,
    catalogProductsUpdated: 0,
    identifiersInserted: 0,
    invalidAsinCount: 0,
    invalidFnskuCount: 0,
    invalidUpcCount: 0,
    ambiguousIdentifierCount: 0,
    unresolvedRows: 0,
  };

  const [{ sha256, bytes }, csvRows] = await Promise.all([hashFile(filePath), readCsv(filePath)]);
  stats.rowsRead = csvRows.length;

  const uploadId = await ensureUploadRow({
    supabase,
    organizationId,
    storeId,
    filePath,
    sourceFileSha256: sha256,
    fileSizeBytes: bytes,
  });

  const normalizedRows: NormalizedRow[] = [];
  for (const [index, row] of csvRows.entries()) {
    const normalized = normalizeRow(row, index + 1, stats);
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

  const productIdBySku = await upsertProducts({
    supabase,
    organizationId,
    storeId,
    rows: normalizedRows,
    sourceUploadId: uploadId,
    sourceFileSha256: sha256,
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
    sourceFileSha256: sha256,
    stats,
  });

  await finalizeUpload({
    supabase,
    uploadId,
    stats,
    rows: normalizedRows,
    sourceFileSha256: sha256,
  });

  printStats(stats);
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
