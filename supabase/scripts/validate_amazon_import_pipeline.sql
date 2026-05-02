-- =============================================================================
-- Amazon Import Pipeline Validation SQL
--
-- Read-only diagnostic queries for the 6 file types fixed in 20260642:
--   1) amazon_all_orders                    (Fulfilled Shipments)
--   2) amazon_settlements                   (Transaction / Payment Detail)
--   3) amazon_transactions                  (Simple Transactions Summary)
--   4) amazon_inventory_ledger              (Headerless Inventory Ledger)
--   5) amazon_manage_fba_inventory          (Restock Inventory)
--   6) amazon_amazon_fulfilled_inventory    (Amazon Fulfilled Inventory)
--
-- Usage:
--   • Replace :org with the active organization_id (uuid) before running.
--   • Optional: replace :upload with a single raw_report_uploads.id to scope
--     coverage to that one upload, or remove the AND clauses to scan all rows.
--   • All queries are read-only; safe to run in production.
-- =============================================================================

-- ── 0) Active import context — confirm the organization + store ─────────────
SELECT
  'organization' AS scope,
  id             AS organization_id,
  name
FROM public.organizations
WHERE id = :'org'::uuid;

SELECT
  'store' AS scope,
  s.id    AS store_id,
  s.name,
  s.organization_id,
  s.is_default,
  s.is_active
FROM public.stores s
WHERE s.organization_id = :'org'::uuid
ORDER BY s.is_default DESC, s.name ASC;

-- ── 1) amazon_all_orders — row count + product coverage ─────────────────────
SELECT
  'amazon_all_orders' AS table_name,
  COUNT(*)                                                    AS rows_total,
  COUNT(*) FILTER (WHERE amazon_order_id IS NOT NULL)         AS rows_with_amazon_order_id,
  COUNT(*) FILTER (WHERE sku IS NOT NULL)                     AS rows_with_sku,
  COUNT(*) FILTER (WHERE quantity IS NOT NULL AND quantity > 0) AS rows_with_quantity,
  COUNT(*) FILTER (WHERE resolved_product_id IS NOT NULL)     AS rows_resolved_product,
  COUNT(*) FILTER (WHERE identifier_resolution_status = 'resolved') AS rows_status_resolved,
  COUNT(*) FILTER (WHERE identifier_resolution_status = 'matched')  AS rows_status_matched,
  COUNT(*) FILTER (WHERE identifier_resolution_status = 'ambiguous') AS rows_status_ambiguous,
  COUNT(*) FILTER (WHERE identifier_resolution_status = 'unresolved') AS rows_status_unresolved
FROM public.amazon_all_orders
WHERE organization_id = :'org'::uuid;

-- Top unresolved Fulfilled Shipments SKUs (largest impact first).
SELECT
  sku,
  COUNT(*)                AS lines,
  SUM(COALESCE(quantity, 0)) AS units,
  SUM(COALESCE(item_price, 0)) AS gross_item_price
FROM public.amazon_all_orders
WHERE organization_id = :'org'::uuid
  AND resolved_product_id IS NULL
  AND sku IS NOT NULL
GROUP BY sku
ORDER BY lines DESC
LIMIT 25;

-- ── 2) amazon_settlements — row count, sku/order_id non-null, totals ────────
SELECT
  'amazon_settlements' AS table_name,
  COUNT(*)                                              AS rows_total,
  COUNT(*) FILTER (WHERE settlement_id IS NOT NULL)     AS rows_with_settlement_id,
  COUNT(*) FILTER (WHERE order_id IS NOT NULL)          AS rows_with_order_id,
  COUNT(*) FILTER (WHERE sku IS NOT NULL)               AS rows_with_sku,
  COUNT(*) FILTER (WHERE order_id IS NOT NULL OR sku IS NOT NULL) AS rows_with_order_or_sku,
  COUNT(*) FILTER (WHERE order_id IS NULL AND sku IS NULL AND COALESCE(amount_total, 0) = 0) AS rows_broken_legacy,
  COUNT(*) FILTER (WHERE transaction_status IS NOT NULL) AS rows_with_transaction_status,
  COUNT(*) FILTER (WHERE transaction_release_date IS NOT NULL) AS rows_with_release_date,
  ROUND(SUM(COALESCE(amount_total, 0))::numeric, 2)     AS amount_total_sum,
  ROUND(SUM(COALESCE(product_sales, 0))::numeric, 2)    AS product_sales_sum,
  ROUND(SUM(COALESCE(selling_fees, 0))::numeric, 2)     AS selling_fees_sum,
  ROUND(SUM(COALESCE(fba_fees, 0))::numeric, 2)         AS fba_fees_sum,
  COUNT(*) FILTER (WHERE resolved_product_id IS NOT NULL) AS rows_resolved_product
FROM public.amazon_settlements
WHERE organization_id = :'org'::uuid;

-- Per-upload settlement health, including legacy broken rows.
SELECT
  s.upload_id,
  rru.report_type,
  COUNT(*)                                                              AS rows,
  COUNT(*) FILTER (WHERE s.order_id IS NULL AND s.sku IS NULL
                     AND COALESCE(s.amount_total, 0) = 0)               AS legacy_broken_rows,
  COUNT(*) FILTER (WHERE s.transaction_status IS NOT NULL)              AS rows_with_status,
  ROUND(SUM(COALESCE(s.amount_total, 0))::numeric, 2)                   AS amount_total_sum
FROM public.amazon_settlements s
LEFT JOIN public.raw_report_uploads rru ON rru.id = s.upload_id
WHERE s.organization_id = :'org'::uuid
GROUP BY s.upload_id, rru.report_type
ORDER BY rows DESC;

-- ── 3) amazon_transactions — order_id join coverage to amazon_all_orders ───
WITH txns AS (
  SELECT t.id, t.order_id, t.amount, t.resolved_product_id
  FROM public.amazon_transactions t
  WHERE t.organization_id = :'org'::uuid
)
SELECT
  'amazon_transactions' AS table_name,
  COUNT(*)                                              AS rows_total,
  COUNT(*) FILTER (WHERE order_id IS NOT NULL)          AS rows_with_order_id,
  COUNT(*) FILTER (
    WHERE order_id IN (
      SELECT amazon_order_id FROM public.amazon_all_orders
       WHERE organization_id = :'org'::uuid
       UNION ALL
      SELECT order_id FROM public.amazon_all_orders
       WHERE organization_id = :'org'::uuid
    )
  )                                                     AS rows_match_all_orders,
  COUNT(*) FILTER (WHERE resolved_product_id IS NOT NULL) AS rows_resolved_product,
  ROUND(SUM(COALESCE(amount, 0))::numeric, 2)           AS amount_sum
FROM txns;

-- ── 4) amazon_inventory_ledger — event_date / event_type / quantity ────────
SELECT
  'amazon_inventory_ledger' AS table_name,
  COUNT(*)                                                AS rows_total,
  COUNT(*) FILTER (WHERE event_date IS NOT NULL)          AS rows_with_event_date,
  COUNT(*) FILTER (WHERE event_timestamp IS NOT NULL)     AS rows_with_event_timestamp,
  COUNT(*) FILTER (WHERE event_type IS NOT NULL)          AS rows_with_event_type,
  COUNT(*) FILTER (WHERE quantity IS NOT NULL)            AS rows_with_quantity,
  COUNT(*) FILTER (WHERE fnsku IS NOT NULL)               AS rows_with_fnsku,
  COUNT(*) FILTER (WHERE sku IS NOT NULL)                 AS rows_with_sku,
  COUNT(*) FILTER (WHERE asin IS NOT NULL)                AS rows_with_asin,
  COUNT(*) FILTER (WHERE country IS NOT NULL)             AS rows_with_country,
  COUNT(*) FILTER (WHERE resolved_product_id IS NOT NULL) AS rows_resolved_product,
  COUNT(*) FILTER (WHERE identifier_resolution_status IS NOT NULL) AS rows_with_resolution_status
FROM public.amazon_inventory_ledger
WHERE organization_id = :'org'::uuid;

-- Event-type histogram (sanity-check the headerless export populates `event_type`).
SELECT event_type, COUNT(*) AS lines, SUM(COALESCE(quantity, 0)) AS qty_sum
FROM public.amazon_inventory_ledger
WHERE organization_id = :'org'::uuid
GROUP BY event_type
ORDER BY lines DESC
LIMIT 20;

-- ── 5) amazon_manage_fba_inventory — Restock typed columns coverage ─────────
SELECT
  'amazon_manage_fba_inventory' AS table_name,
  COUNT(*)                                              AS rows_total,
  COUNT(*) FILTER (WHERE sku IS NOT NULL)               AS rows_with_sku,
  COUNT(*) FILTER (WHERE fnsku IS NOT NULL)             AS rows_with_fnsku,
  COUNT(*) FILTER (WHERE afn_total_quantity IS NOT NULL) AS rows_with_total_units,
  COUNT(*) FILTER (WHERE inbound_quantity IS NOT NULL)   AS rows_with_inbound,
  COUNT(*) FILTER (WHERE recommended_replenishment_qty IS NOT NULL) AS rows_with_recommended,
  COUNT(*) FILTER (WHERE resolved_product_id IS NOT NULL) AS rows_resolved_product
FROM public.amazon_manage_fba_inventory
WHERE organization_id = :'org'::uuid;

-- ── 6) amazon_amazon_fulfilled_inventory — coverage ─────────────────────────
SELECT
  'amazon_amazon_fulfilled_inventory' AS table_name,
  COUNT(*)                                              AS rows_total,
  COUNT(*) FILTER (WHERE seller_sku IS NOT NULL)        AS rows_with_seller_sku,
  COUNT(*) FILTER (WHERE fulfillment_channel_sku IS NOT NULL) AS rows_with_fulfillment_channel_sku,
  COUNT(*) FILTER (WHERE asin IS NOT NULL)              AS rows_with_asin,
  COUNT(*) FILTER (WHERE quantity_available IS NOT NULL) AS rows_with_qty_available,
  COUNT(*) FILTER (WHERE warehouse_condition_code = 'SELLABLE'
                     AND condition_type = 'NewItem')    AS rows_sellable_new,
  COUNT(*) FILTER (WHERE resolved_product_id IS NOT NULL) AS rows_resolved_product
FROM public.amazon_amazon_fulfilled_inventory
WHERE organization_id = :'org'::uuid;

-- ── 7) Idempotency / duplicate checks ────────────────────────────────────────
-- Each table is upserted by (organization_id, source_file_sha256, source_physical_row_number)
-- — except amazon_settlements which uses (organization_id, upload_id, amazon_line_key)
-- and amazon_transactions which keys on source_line_hash. The queries below
-- surface any rows that should be unique under those rules but aren't.

-- amazon_all_orders (org + file_sha + row_number must be unique).
SELECT 'amazon_all_orders dup' AS chk, COUNT(*) AS dup_rows
FROM (
  SELECT 1
  FROM public.amazon_all_orders
  WHERE organization_id = :'org'::uuid
  GROUP BY organization_id, source_file_sha256, source_physical_row_number
  HAVING COUNT(*) > 1
) d;

-- amazon_settlements (org + upload_id + amazon_line_key must be unique).
SELECT 'amazon_settlements dup' AS chk, COUNT(*) AS dup_rows
FROM (
  SELECT 1
  FROM public.amazon_settlements
  WHERE organization_id = :'org'::uuid
  GROUP BY organization_id, upload_id, amazon_line_key
  HAVING COUNT(*) > 1
) d;

-- amazon_transactions (org + source_line_hash must be unique on legacy index;
-- org + file_sha + row_number on the physical-line index).
SELECT 'amazon_transactions hash dup' AS chk, COUNT(*) AS dup_rows
FROM (
  SELECT 1
  FROM public.amazon_transactions
  WHERE organization_id = :'org'::uuid
  GROUP BY organization_id, source_line_hash
  HAVING COUNT(*) > 1
) d;

-- amazon_inventory_ledger (org + file_sha + row_number).
SELECT 'amazon_inventory_ledger dup' AS chk, COUNT(*) AS dup_rows
FROM (
  SELECT 1
  FROM public.amazon_inventory_ledger
  WHERE organization_id = :'org'::uuid
  GROUP BY organization_id, source_file_sha256, source_physical_row_number
  HAVING COUNT(*) > 1
) d;

-- amazon_manage_fba_inventory (org + file_sha + row_number).
SELECT 'amazon_manage_fba_inventory dup' AS chk, COUNT(*) AS dup_rows
FROM (
  SELECT 1
  FROM public.amazon_manage_fba_inventory
  WHERE organization_id = :'org'::uuid
  GROUP BY organization_id, source_file_sha256, source_physical_row_number
  HAVING COUNT(*) > 1
) d;

-- amazon_amazon_fulfilled_inventory (org + file_sha + row_number).
SELECT 'amazon_amazon_fulfilled_inventory dup' AS chk, COUNT(*) AS dup_rows
FROM (
  SELECT 1
  FROM public.amazon_amazon_fulfilled_inventory
  WHERE organization_id = :'org'::uuid
  GROUP BY organization_id, source_file_sha256, source_physical_row_number
  HAVING COUNT(*) > 1
) d;

-- ── 8) Per-upload row-count audit (cross-checks Phase 2/3 metrics) ──────────
SELECT
  rru.id           AS upload_id,
  rru.report_type,
  rru.status,
  rru.metadata->>'row_count'           AS metadata_row_count,
  rru.metadata->>'staging_row_count'   AS metadata_staging_count,
  rru.metadata->>'sync_row_count'      AS metadata_sync_count,
  COALESCE((SELECT COUNT(*) FROM public.amazon_all_orders ao
              WHERE ao.organization_id = rru.organization_id
                AND ao.source_upload_id = rru.id), 0)  AS all_orders_rows,
  COALESCE((SELECT COUNT(*) FROM public.amazon_settlements s
              WHERE s.organization_id = rru.organization_id
                AND s.upload_id = rru.id), 0)         AS settlement_rows,
  COALESCE((SELECT COUNT(*) FROM public.amazon_transactions t
              WHERE t.organization_id = rru.organization_id
                AND t.upload_id = rru.id), 0)         AS transaction_rows,
  COALESCE((SELECT COUNT(*) FROM public.amazon_inventory_ledger l
              WHERE l.organization_id = rru.organization_id
                AND l.upload_id = rru.id), 0)         AS ledger_rows,
  COALESCE((SELECT COUNT(*) FROM public.amazon_manage_fba_inventory m
              WHERE m.organization_id = rru.organization_id
                AND m.source_upload_id = rru.id), 0)  AS restock_rows,
  COALESCE((SELECT COUNT(*) FROM public.amazon_amazon_fulfilled_inventory afi
              WHERE afi.organization_id = rru.organization_id
                AND afi.source_upload_id = rru.id), 0) AS afi_rows
FROM public.raw_report_uploads rru
WHERE rru.organization_id = :'org'::uuid
ORDER BY rru.created_at DESC
LIMIT 50;
