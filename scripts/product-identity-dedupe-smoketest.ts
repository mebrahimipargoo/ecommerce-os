/**
 * Stand-alone smoke test for the Product Identity intra-batch dedupe.
 *
 * Run:
 *   npx tsx scripts/product-identity-dedupe-smoketest.ts
 *
 * Asserts:
 *   * Duplicate Seller SKU rows in a single CSV collapse to one row before
 *     the products upsert (no Postgres error 21000).
 *   * Catalog upsert is deduped by (seller_sku, asin).
 *   * product_identifier_map insert payload has no duplicate
 *     external_listing_id values.
 */
import "node:path";

const Module = require("module");
Module._cache[require.resolve("server-only")] = { exports: {} };

const { runProductIdentityImport } = require("../lib/product-identity-import") as typeof import("../lib/product-identity-import");

type CapturedBatch = Record<string, unknown>[];

function makeChain<T>(value: T) {
  // Dual-promise / chain object that supports `.eq().eq().in()` and
  // `await ...` patterns used by the importer.
  const result: any = {
    data: value,
    error: null,
    eq() {
      return makeChain(value);
    },
    in() {
      return makeChain(value);
    },
    select() {
      return makeChain(value);
    },
    maybeSingle: async () => ({ data: null, error: null }),
    then(onFulfilled: (v: { data: T; error: null }) => unknown) {
      return Promise.resolve({ data: value, error: null }).then(onFulfilled);
    },
  };
  return result;
}

const captured = {
  products: [] as CapturedBatch[],
  catalog: [] as CapturedBatch[],
  identifierInserts: [] as CapturedBatch[],
  identifierUpdates: [] as CapturedBatch[],
};

const supabase: any = {
  from(table: string) {
    return {
      select() {
        return {
          eq() {
            return {
              eq() {
                return {
                  in: async () => ({ data: [], error: null }),
                  maybeSingle: async () => ({ data: { metadata: null }, error: null }),
                };
              },
              in: async () => ({ data: [], error: null }),
              maybeSingle: async () => ({ data: { metadata: null }, error: null }),
            };
          },
        };
      },
      insert: async (rows: CapturedBatch | Record<string, unknown>) => {
        const arr = Array.isArray(rows) ? rows : [rows];
        captured.identifierInserts.push(arr);
        return { data: arr, error: null };
      },
      upsert(rows: CapturedBatch | Record<string, unknown>, _opts?: unknown) {
        const arr = Array.isArray(rows) ? rows : [rows];
        if (table === "products") captured.products.push(arr);
        if (table === "catalog_products") captured.catalog.push(arr);
        return {
          select: async () => ({
            data: arr.map((row, i) => ({
              id: `${table}_${i}`,
              sku: (row as any).sku,
              seller_sku: (row as any).seller_sku,
              asin: (row as any).asin,
            })),
            error: null,
          }),
        };
      },
      update(patch: Record<string, unknown>) {
        captured.identifierUpdates.push([patch]);
        return {
          eq() {
            return {
              eq: async () => ({ data: null, error: null }),
            };
          },
        };
      },
    };
  },
};

const csvRows = [
  // Same SKU twice with different ASIN/FNSKU → should be deduped + recorded as conflict.
  { "Seller SKU": "SKU-A", "Product Name": "Acme A", Vendor: "V1", "Mfg #": "M1", UPC: "012345678905", ASIN: "B000000001", FNSKU: "X000000001" },
  { "Seller SKU": "SKU-A", "Product Name": "Acme A",  Vendor: "V1", "Mfg #": "M1", UPC: "012345678905", ASIN: "B000000099", FNSKU: "X000000099" },
  { "Seller SKU": "SKU-B", "Product Name": "Acme B", Vendor: "V2", "Mfg #": "M2", UPC: "012345678912", ASIN: "B000000002", FNSKU: "X000000002" },
  // Exact duplicate of SKU-A row 1.
  { "Seller SKU": "SKU-A", "Product Name": "Acme A", Vendor: "V1", "Mfg #": "M1", UPC: "012345678905", ASIN: "B000000001", FNSKU: "X000000001" },
  // Missing Seller SKU → rowsMissingSellerSku += 1.
  { "Seller SKU": "", "Product Name": "Orphan", Vendor: "V3" },
  // Invalid Seller SKU (Excel error token) → rowsInvalidSellerSku += 1, captured in invalid_sku_examples.
  { "Seller SKU": "#NAME?", "Product Name": "BrokenLookup", Vendor: "V4" },
  // Invalid Seller SKU (placeholder) → rowsInvalidSellerSku += 1.
  { "Seller SKU": "0", "Product Name": "Placeholder", Vendor: "V5" },
];

(async () => {
  const stats = await runProductIdentityImport({
    supabase,
    organizationId: "00000000-0000-0000-0000-000000000001",
    storeId: "00000000-0000-0000-0000-000000000002",
    uploadId: "00000000-0000-0000-0000-000000000099",
    csvRows: csvRows as unknown as Record<string, string>[],
    columnMapping: null,
    sourceFileSha256: "a".repeat(64),
  });

  const productPayload = captured.products[0] ?? [];
  const productSkus = productPayload.map((row) => (row as any).sku);
  const catalogPayload = captured.catalog[0] ?? [];
  const identifierInserted = (captured.identifierInserts.flat() ?? []) as Record<string, unknown>[];
  const identifierIds = identifierInserted.map((row) => row.external_listing_id as string);

  const summary = {
    csv_rows: csvRows.length,
    rows_read: stats.rowsRead,
    rows_missing_seller_sku: stats.rowsMissingSellerSku,
    rows_invalid_seller_sku: stats.rowsInvalidSellerSku,
    rows_skipped: stats.rowsSkipped,
    skipped_reason_counts: stats.skippedReasonCounts,
    invalid_sku_examples: stats.invalidSkuExamples,
    normalized_rows_count: stats.normalizedRowsCount,
    unique_product_sku_count: stats.uniqueProductSkuCount,
    duplicate_sku_count: stats.duplicateSkuCount,
    duplicate_sku_conflict_count: stats.duplicateSkuConflictCount,
    catalog_unique_count: stats.catalogUniqueCount,
    identifier_unique_count: stats.identifierUniqueCount,
    invalid_identifier_count: stats.invalidIdentifierCount,
    products_in_batch: productPayload.length,
    products_unique_in_batch: new Set(productSkus).size,
    catalog_rows_in_batch: catalogPayload.length,
    identifier_inserted_count: identifierIds.length,
    identifier_inserted_unique: new Set(identifierIds).size,
  };
  console.log(JSON.stringify(summary, null, 2));

  const assertions: { name: string; pass: boolean }[] = [
    { name: "products batch has no duplicate sku", pass: productPayload.length === new Set(productSkus).size },
    { name: "catalog batch has no duplicate (sku,asin)", pass: catalogPayload.length === new Set(catalogPayload.map((r) => `${(r as any).seller_sku}|${(r as any).asin}`)).size },
    { name: "identifier insert has no duplicate external_listing_id", pass: identifierIds.length === new Set(identifierIds).size },
    { name: "duplicate_sku_count > 0 (collapsed CSV duplicates)", pass: stats.duplicateSkuCount > 0 },
    { name: "duplicate_sku_conflict_count tracks ASIN/FNSKU disagreement", pass: stats.duplicateSkuConflictCount >= 1 },
    { name: "rows_missing_seller_sku counts empty Seller SKU rows", pass: stats.rowsMissingSellerSku === 1 },
    { name: "rows_invalid_seller_sku counts placeholder/#NAME? rows", pass: stats.rowsInvalidSellerSku === 2 },
    { name: "invalid_sku_examples captures the bad SKU values", pass: stats.invalidSkuExamples.some((e) => e.rawValue === "#NAME?") && stats.invalidSkuExamples.some((e) => e.rawValue === "0") },
    { name: "rows_skipped = missing + invalid", pass: stats.rowsSkipped === stats.rowsMissingSellerSku + stats.rowsInvalidSellerSku },
  ];
  let failed = 0;
  for (const a of assertions) {
    const tag = a.pass ? "PASS" : "FAIL";
    console.log(`${tag}: ${a.name}`);
    if (!a.pass) failed += 1;
  }
  if (failed > 0) {
    console.error(`${failed} assertion(s) failed.`);
    process.exit(1);
  }
})();
