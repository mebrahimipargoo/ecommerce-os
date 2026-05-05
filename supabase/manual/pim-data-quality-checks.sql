-- PIM data quality — read-only diagnostics (run in SQL editor or CI).
-- Adjust :org_id filters or comment sections as needed.

-- ── 1) Null store_id counts (core PIM + Amazon tables touched by stabilization migration) ──

SELECT 'products' AS tbl, COUNT(*) AS null_store_rows
FROM public.products
WHERE store_id IS NULL;

SELECT 'product_identifier_map' AS tbl, COUNT(*) AS null_store_rows
FROM public.product_identifier_map
WHERE store_id IS NULL;

SELECT 'catalog_products' AS tbl, COUNT(*) AS null_store_rows
FROM public.catalog_products
WHERE store_id IS NULL;

SELECT 'amazon_listing_report_rows_raw' AS tbl, COUNT(*) AS null_store_rows
FROM public.amazon_listing_report_rows_raw
WHERE store_id IS NULL;

SELECT 'amazon_transactions' AS tbl, COUNT(*) AS null_store_rows
FROM public.amazon_transactions
WHERE store_id IS NULL;

SELECT 'amazon_reimbursements' AS tbl, COUNT(*) AS null_store_rows
FROM public.amazon_reimbursements
WHERE store_id IS NULL;

SELECT 'amazon_safet_claims' AS tbl, COUNT(*) AS null_store_rows
FROM public.amazon_safet_claims
WHERE store_id IS NULL;

SELECT 'amazon_returns' AS tbl, COUNT(*) AS null_store_rows
FROM public.amazon_returns
WHERE store_id IS NULL;

SELECT 'amazon_inventory_ledger' AS tbl, COUNT(*) AS null_store_rows
FROM public.amazon_inventory_ledger
WHERE store_id IS NULL;

-- ── 2) Store / organization mismatch (row.store_id set but store belongs to another org) ──

SELECT 'products' AS tbl, p.id, p.organization_id, p.store_id, s.organization_id AS store_org
FROM public.products p
JOIN public.stores s ON s.id = p.store_id
WHERE p.store_id IS NOT NULL
  AND p.organization_id IS DISTINCT FROM s.organization_id
LIMIT 500;

SELECT 'product_identifier_map' AS tbl, m.id, m.organization_id, m.store_id, s.organization_id AS store_org
FROM public.product_identifier_map m
JOIN public.stores s ON s.id = m.store_id
WHERE m.store_id IS NOT NULL
  AND m.organization_id IS DISTINCT FROM s.organization_id
LIMIT 500;

-- ── 3) product_identifier_map: null product_id or broken FK ──

SELECT COUNT(*) AS map_rows_null_product_id
FROM public.product_identifier_map
WHERE product_id IS NULL;

SELECT m.id, m.organization_id, m.store_id, m.product_id
FROM public.product_identifier_map m
LEFT JOIN public.products p ON p.id = m.product_id
WHERE m.product_id IS NOT NULL
  AND p.id IS NULL
LIMIT 500;

-- ── 4) Products without any identifier-map row (informational; backlog-only products may be valid) ──

SELECT COUNT(*) AS products_without_identifier_map
FROM public.products pr
WHERE NOT EXISTS (
  SELECT 1
  FROM public.product_identifier_map m
  WHERE m.product_id = pr.id
);

-- Sample (trim in UI if noisy)
SELECT pr.id, pr.organization_id, pr.store_id, pr.sku, pr.product_name
FROM public.products pr
WHERE NOT EXISTS (
  SELECT 1
  FROM public.product_identifier_map m
  WHERE m.product_id = pr.id
)
LIMIT 200;

-- ── 5) Duplicate (organization_id, store_id, sku) on products (should be empty if unique index holds) ──

SELECT organization_id, store_id, sku, COUNT(*) AS cnt
FROM public.products
WHERE store_id IS NOT NULL
  AND sku IS NOT NULL
  AND btrim(sku) <> ''
GROUP BY organization_id, store_id, sku
HAVING COUNT(*) > 1;

-- ── 6) Identifier conflicts: same org+store+seller_sku → multiple product_id ──

SELECT organization_id, store_id, seller_sku, COUNT(DISTINCT product_id) AS distinct_products, ARRAY_AGG(DISTINCT product_id) AS product_ids
FROM public.product_identifier_map
WHERE store_id IS NOT NULL
  AND product_id IS NOT NULL
  AND seller_sku IS NOT NULL
  AND btrim(seller_sku) <> ''
GROUP BY organization_id, store_id, seller_sku
HAVING COUNT(DISTINCT product_id) > 1
LIMIT 200;

-- Same for ASIN
SELECT organization_id, store_id, asin, COUNT(DISTINCT product_id) AS distinct_products, ARRAY_AGG(DISTINCT product_id) AS product_ids
FROM public.product_identifier_map
WHERE store_id IS NOT NULL
  AND product_id IS NOT NULL
  AND asin IS NOT NULL
  AND btrim(asin) <> ''
GROUP BY organization_id, store_id, asin
HAVING COUNT(DISTINCT product_id) > 1
LIMIT 200;

-- UPC bucket (normalize empty to excluded)
SELECT organization_id, store_id, upc_code, COUNT(DISTINCT product_id) AS distinct_products, ARRAY_AGG(DISTINCT product_id) AS product_ids
FROM public.product_identifier_map
WHERE store_id IS NOT NULL
  AND product_id IS NOT NULL
  AND upc_code IS NOT NULL
  AND btrim(upc_code) <> ''
GROUP BY organization_id, store_id, upc_code
HAVING COUNT(DISTINCT product_id) > 1
LIMIT 200;
