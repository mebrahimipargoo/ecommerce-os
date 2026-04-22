-- Part A — Unified product identity read model.
--
-- ONE read-only view, no new storage tables.
--
-- Identity carrier:
--   amazon_amazon_fulfilled_inventory  (sku, fnsku, asin)
--
-- product_name fallback chain (first non-null wins):
--   1. catalog_products.item_name   matched by (sku, asin)
--   2. catalog_products.item_name   matched by sku
--   3. catalog_products.item_name   matched by asin
--   4. catalog_products.item_name   matched by fnsku
--   5. product_identifier_map.title matched by fnsku
--   6. product_identifier_map.title matched by sku
--   7. amazon_inventory_ledger.title  matched by fnsku
--   8. amazon_manage_fba_inventory.product_name matched by fnsku
--   9. amazon_fba_inventory.product_name        matched by fnsku
--
-- confidence_score:
--   product_identifier_map.confidence_score from the bridge row that fnsku-matches
--   the AFI row (preferred) or sku+asin-matches it; 1.0000 default if no bridge
--   row exists (AFI itself is the primary clean source).

BEGIN;

DROP VIEW IF EXISTS public.v_product_identity;

CREATE VIEW public.v_product_identity AS
WITH afi AS (
  SELECT
    organization_id,
    nullif(btrim(seller_sku), '')              AS sku,
    nullif(btrim(asin), '')                    AS asin,
    nullif(btrim(fulfillment_channel_sku), '') AS fnsku,
    max(updated_at)                            AS last_seen_at
  FROM public.amazon_amazon_fulfilled_inventory
  GROUP BY 1, 2, 3, 4
)
SELECT
  a.organization_id,
  COALESCE(
    (SELECT cp.item_name
     FROM public.catalog_products cp
     WHERE cp.organization_id = a.organization_id
       AND cp.seller_sku IS NOT DISTINCT FROM a.sku
       AND cp.asin       IS NOT DISTINCT FROM a.asin
       AND cp.item_name IS NOT NULL
     ORDER BY cp.last_seen_at DESC NULLS LAST LIMIT 1),
    (SELECT cp.item_name
     FROM public.catalog_products cp
     WHERE cp.organization_id = a.organization_id
       AND cp.seller_sku IS NOT DISTINCT FROM a.sku
       AND a.sku IS NOT NULL
       AND cp.item_name IS NOT NULL
     ORDER BY cp.last_seen_at DESC NULLS LAST LIMIT 1),
    (SELECT cp.item_name
     FROM public.catalog_products cp
     WHERE cp.organization_id = a.organization_id
       AND cp.asin IS NOT DISTINCT FROM a.asin
       AND a.asin IS NOT NULL
       AND cp.item_name IS NOT NULL
     ORDER BY cp.last_seen_at DESC NULLS LAST LIMIT 1),
    (SELECT cp.item_name
     FROM public.catalog_products cp
     WHERE cp.organization_id = a.organization_id
       AND cp.fnsku IS NOT DISTINCT FROM a.fnsku
       AND a.fnsku IS NOT NULL
       AND cp.item_name IS NOT NULL
     ORDER BY cp.last_seen_at DESC NULLS LAST LIMIT 1),
    (SELECT pim.title
     FROM public.product_identifier_map pim
     WHERE pim.organization_id = a.organization_id
       AND pim.fnsku IS NOT DISTINCT FROM a.fnsku
       AND a.fnsku IS NOT NULL
       AND pim.title IS NOT NULL
     ORDER BY pim.last_seen_at DESC NULLS LAST LIMIT 1),
    (SELECT pim.title
     FROM public.product_identifier_map pim
     WHERE pim.organization_id = a.organization_id
       AND pim.seller_sku IS NOT DISTINCT FROM a.sku
       AND a.sku IS NOT NULL
       AND pim.title IS NOT NULL
     ORDER BY pim.last_seen_at DESC NULLS LAST LIMIT 1),
    (SELECT l.title
     FROM public.amazon_inventory_ledger l
     WHERE l.organization_id = a.organization_id
       AND l.fnsku IS NOT DISTINCT FROM a.fnsku
       AND a.fnsku IS NOT NULL
       AND l.title IS NOT NULL
     LIMIT 1),
    (SELECT m.product_name
     FROM public.amazon_manage_fba_inventory m
     WHERE m.organization_id = a.organization_id
       AND nullif(btrim(m.fnsku), '') IS NOT DISTINCT FROM a.fnsku
       AND a.fnsku IS NOT NULL
       AND m.product_name IS NOT NULL
     LIMIT 1),
    (SELECT f.product_name
     FROM public.amazon_fba_inventory f
     WHERE f.organization_id = a.organization_id
       AND nullif(btrim(f.fnsku), '') IS NOT DISTINCT FROM a.fnsku
       AND a.fnsku IS NOT NULL
       AND f.product_name IS NOT NULL
     LIMIT 1)
  )                                                                 AS product_name,
  a.sku                                                             AS sku,
  a.asin                                                            AS asin,
  a.fnsku                                                           AS fnsku,
  'amazon_amazon_fulfilled_inventory'::text                         AS source_table,
  COALESCE(
    (SELECT pim.confidence_score
     FROM public.product_identifier_map pim
     WHERE pim.organization_id = a.organization_id
       AND (
            (pim.fnsku IS NOT DISTINCT FROM a.fnsku AND a.fnsku IS NOT NULL)
         OR (pim.seller_sku IS NOT DISTINCT FROM a.sku
             AND pim.asin   IS NOT DISTINCT FROM a.asin
             AND a.sku IS NOT NULL AND a.asin IS NOT NULL)
       )
     ORDER BY
       CASE WHEN pim.fnsku IS NOT DISTINCT FROM a.fnsku AND a.fnsku IS NOT NULL THEN 1 ELSE 2 END,
       pim.last_seen_at DESC NULLS LAST
     LIMIT 1),
    1.0000
  )::numeric(10,4)                                                  AS confidence_score
FROM afi a;

COMMENT ON VIEW public.v_product_identity IS
  'Read-only product identity surface. Identity carrier = amazon_amazon_fulfilled_inventory. '
  'product_name resolved from catalog_products → product_identifier_map → amazon_inventory_ledger → '
  'amazon_manage_fba_inventory → amazon_fba_inventory. confidence_score from product_identifier_map '
  '(fnsku-bridge preferred), defaults to 1.0000 when AFI alone defines the row.';

GRANT SELECT ON public.v_product_identity TO authenticated;
GRANT SELECT ON public.v_product_identity TO service_role;

NOTIFY pgrst, 'reload schema';

COMMIT;
