-- Validation checks for scripts/import-product-identity.ts.
-- Replace the UUIDs in each params CTE before running.
-- Leave source_upload_id as NULL to validate all product identity imports for the org/store.

-- 1) Imported products missing required product identity fields.
WITH params AS (
  SELECT
    '00000000-0000-0000-0000-000000000000'::uuid AS organization_id,
    '00000000-0000-0000-0000-000000000000'::uuid AS store_id,
    NULL::uuid AS source_upload_id
)
SELECT
  p.id,
  p.sku,
  p.product_name,
  p.asin,
  p.fnsku,
  p.upc_code,
  p.metadata->'product_identity_import'->>'source_upload_id' AS source_upload_id
FROM public.products p
CROSS JOIN params prm
WHERE p.organization_id = prm.organization_id
  AND p.store_id = prm.store_id
  AND (
    prm.source_upload_id IS NULL
    OR p.metadata->'product_identity_import'->>'source_upload_id' = prm.source_upload_id::text
  )
  AND (
    NULLIF(BTRIM(p.sku), '') IS NULL
    OR NULLIF(BTRIM(p.product_name), '') IS NULL
  )
ORDER BY p.sku;

-- 2) Duplicate product keys. This should return zero rows after the migration.
WITH params AS (
  SELECT
    '00000000-0000-0000-0000-000000000000'::uuid AS organization_id,
    '00000000-0000-0000-0000-000000000000'::uuid AS store_id
)
SELECT
  p.organization_id,
  p.store_id,
  p.sku,
  COUNT(*) AS product_rows,
  ARRAY_AGG(p.id ORDER BY p.created_at, p.id) AS product_ids
FROM public.products p
JOIN params prm
  ON prm.organization_id = p.organization_id
 AND prm.store_id = p.store_id
GROUP BY p.organization_id, p.store_id, p.sku
HAVING COUNT(*) > 1
ORDER BY product_rows DESC, p.sku;

-- 3) Invalid identifiers that slipped into physical identity columns.
WITH params AS (
  SELECT
    '00000000-0000-0000-0000-000000000000'::uuid AS organization_id,
    '00000000-0000-0000-0000-000000000000'::uuid AS store_id,
    NULL::uuid AS source_upload_id
),
invalids AS (
  SELECT
    'products'::text AS source_table,
    p.id::text AS row_id,
    'ASIN'::text AS identifier_type,
    p.asin AS identifier_value,
    p.sku AS seller_sku
  FROM public.products p
  CROSS JOIN params prm
  WHERE p.organization_id = prm.organization_id
    AND p.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR p.metadata->'product_identity_import'->>'source_upload_id' = prm.source_upload_id::text)
    AND p.asin IS NOT NULL
    AND p.asin !~ '^B[0-9A-Z]{9}$'

  UNION ALL
  SELECT 'products', p.id::text, 'FNSKU', p.fnsku, p.sku
  FROM public.products p
  CROSS JOIN params prm
  WHERE p.organization_id = prm.organization_id
    AND p.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR p.metadata->'product_identity_import'->>'source_upload_id' = prm.source_upload_id::text)
    AND p.fnsku IS NOT NULL
    AND p.fnsku !~ '^X[0-9A-Z]{9}$'

  UNION ALL
  SELECT 'products', p.id::text, 'UPC', p.upc_code, p.sku
  FROM public.products p
  CROSS JOIN params prm
  WHERE p.organization_id = prm.organization_id
    AND p.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR p.metadata->'product_identity_import'->>'source_upload_id' = prm.source_upload_id::text)
    AND p.upc_code IS NOT NULL
    AND p.upc_code !~ '^[0-9]{8,14}$'

  UNION ALL
  SELECT 'catalog_products', cp.id::text, 'ASIN', cp.asin, cp.seller_sku
  FROM public.catalog_products cp
  CROSS JOIN params prm
  WHERE cp.organization_id = prm.organization_id
    AND cp.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR cp.source_upload_id = prm.source_upload_id)
    AND cp.asin IS NOT NULL
    AND cp.asin !~ '^B[0-9A-Z]{9}$'

  UNION ALL
  SELECT 'catalog_products', cp.id::text, 'FNSKU', cp.fnsku, cp.seller_sku
  FROM public.catalog_products cp
  CROSS JOIN params prm
  WHERE cp.organization_id = prm.organization_id
    AND cp.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR cp.source_upload_id = prm.source_upload_id)
    AND cp.fnsku IS NOT NULL
    AND cp.fnsku !~ '^X[0-9A-Z]{9}$'

  UNION ALL
  SELECT 'product_identifier_map', pim.id::text, 'ASIN', pim.asin, pim.seller_sku
  FROM public.product_identifier_map pim
  CROSS JOIN params prm
  WHERE pim.organization_id = prm.organization_id
    AND pim.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR pim.source_upload_id = prm.source_upload_id)
    AND pim.asin IS NOT NULL
    AND pim.asin !~ '^B[0-9A-Z]{9}$'

  UNION ALL
  SELECT 'product_identifier_map', pim.id::text, 'FNSKU', pim.fnsku, pim.seller_sku
  FROM public.product_identifier_map pim
  CROSS JOIN params prm
  WHERE pim.organization_id = prm.organization_id
    AND pim.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR pim.source_upload_id = prm.source_upload_id)
    AND pim.fnsku IS NOT NULL
    AND pim.fnsku !~ '^X[0-9A-Z]{9}$'

  UNION ALL
  SELECT 'product_identifier_map', pim.id::text, 'UPC', pim.upc_code, pim.seller_sku
  FROM public.product_identifier_map pim
  CROSS JOIN params prm
  WHERE pim.organization_id = prm.organization_id
    AND pim.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR pim.source_upload_id = prm.source_upload_id)
    AND pim.upc_code IS NOT NULL
    AND pim.upc_code !~ '^[0-9]{8,14}$'
)
SELECT *
FROM invalids
ORDER BY source_table, identifier_type, seller_sku;

-- 4) Informational ambiguity report. ASIN and UPC are allowed to map to more
-- than one product; this query shows where that happened.
WITH params AS (
  SELECT
    '00000000-0000-0000-0000-000000000000'::uuid AS organization_id,
    '00000000-0000-0000-0000-000000000000'::uuid AS store_id,
    NULL::uuid AS source_upload_id
),
identifier_edges AS (
  SELECT 'ASIN'::text AS identifier_type, asin AS identifier_value, product_id
  FROM public.product_identifier_map pim
  CROSS JOIN params prm
  WHERE pim.organization_id = prm.organization_id
    AND pim.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR pim.source_upload_id = prm.source_upload_id)
    AND pim.asin IS NOT NULL
    AND pim.product_id IS NOT NULL

  UNION ALL
  SELECT 'UPC', upc_code, product_id
  FROM public.product_identifier_map pim
  CROSS JOIN params prm
  WHERE pim.organization_id = prm.organization_id
    AND pim.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR pim.source_upload_id = prm.source_upload_id)
    AND pim.upc_code IS NOT NULL
    AND pim.product_id IS NOT NULL
)
SELECT
  identifier_type,
  identifier_value,
  COUNT(DISTINCT product_id) AS product_count,
  ARRAY_AGG(DISTINCT product_id) AS product_ids
FROM identifier_edges
GROUP BY identifier_type, identifier_value
HAVING COUNT(DISTINCT product_id) > 1
ORDER BY product_count DESC, identifier_type, identifier_value;

-- 5) Unresolved backlog counts grouped by reason and identifier type.
WITH params AS (
  SELECT
    '00000000-0000-0000-0000-000000000000'::uuid AS organization_id,
    '00000000-0000-0000-0000-000000000000'::uuid AS store_id,
    NULL::uuid AS source_upload_id
)
SELECT
  b.reason,
  b.identifier_type,
  COUNT(*) AS unresolved_rows
FROM public.catalog_identity_unresolved_backlog b
CROSS JOIN params prm
WHERE b.organization_id = prm.organization_id
  AND b.store_id = prm.store_id
  AND (prm.source_upload_id IS NULL OR b.source_upload_id = prm.source_upload_id)
GROUP BY b.reason, b.identifier_type
ORDER BY unresolved_rows DESC, b.reason, b.identifier_type;

-- 6) Raw provenance coverage for imported products, catalog rows, and identifier map rows.
WITH params AS (
  SELECT
    '00000000-0000-0000-0000-000000000000'::uuid AS organization_id,
    '00000000-0000-0000-0000-000000000000'::uuid AS store_id,
    NULL::uuid AS source_upload_id
),
coverage AS (
  SELECT
    'products'::text AS source_table,
    COUNT(*) AS rows_seen,
    COUNT(*) FILTER (
      WHERE p.metadata ? 'product_identity_import'
        AND p.metadata->'product_identity_import' ? 'original_row'
    ) AS rows_with_raw_payload,
    COUNT(*) FILTER (
      WHERE p.metadata->'product_identity_import'->>'source_upload_id' IS NOT NULL
    ) AS rows_with_source_upload_id
  FROM public.products p
  CROSS JOIN params prm
  WHERE p.organization_id = prm.organization_id
    AND p.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR p.metadata->'product_identity_import'->>'source_upload_id' = prm.source_upload_id::text)

  UNION ALL
  SELECT
    'catalog_products',
    COUNT(*),
    COUNT(*) FILTER (WHERE cp.raw_payload <> '{}'::jsonb),
    COUNT(*) FILTER (WHERE cp.source_upload_id IS NOT NULL)
  FROM public.catalog_products cp
  CROSS JOIN params prm
  WHERE cp.organization_id = prm.organization_id
    AND cp.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR cp.source_upload_id = prm.source_upload_id)

  UNION ALL
  SELECT
    'product_identifier_map',
    COUNT(*),
    COUNT(*) FILTER (WHERE pim.source_file_sha256 IS NOT NULL AND pim.source_physical_row_number IS NOT NULL),
    COUNT(*) FILTER (WHERE pim.source_upload_id IS NOT NULL)
  FROM public.product_identifier_map pim
  CROSS JOIN params prm
  WHERE pim.organization_id = prm.organization_id
    AND pim.store_id = prm.store_id
    AND (prm.source_upload_id IS NULL OR pim.source_upload_id = prm.source_upload_id)
)
SELECT *
FROM coverage
ORDER BY source_table;
