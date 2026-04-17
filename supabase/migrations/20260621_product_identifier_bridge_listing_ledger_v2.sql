-- Listing provenance + ledger sku/asin/title columns + backfill + resolver view.
-- Additive / idempotent.

BEGIN;

ALTER TABLE public.product_identifier_map
  ADD COLUMN IF NOT EXISTS linked_from_report_family text,
  ADD COLUMN IF NOT EXISTS linked_from_target_table text;

COMMENT ON COLUMN public.product_identifier_map.linked_from_report_family IS
  'High-level import family: listing | inventory | …';
COMMENT ON COLUMN public.product_identifier_map.linked_from_target_table IS
  'Primary table that seeded this bridge row (e.g. catalog_products, amazon_inventory_ledger).';

-- Optional physical columns for ledger lines (still duplicated in raw_data for legacy rows).
ALTER TABLE public.amazon_inventory_ledger
  ADD COLUMN IF NOT EXISTS sku text,
  ADD COLUMN IF NOT EXISTS asin text,
  ADD COLUMN IF NOT EXISTS title text;

CREATE INDEX IF NOT EXISTS idx_product_identifier_map_org_fnsku
  ON public.product_identifier_map (organization_id, fnsku)
  WHERE fnsku IS NOT NULL;

-- Backfill listing-derived bridge rows (historical NULL match_source / provenance).
UPDATE public.product_identifier_map AS pim
SET
  match_source = 'listing_catalog',
  inventory_source = NULL,
  confidence_score = COALESCE(pim.confidence_score, 1.0000),
  linked_from_report_family = 'listing',
  linked_from_target_table = 'catalog_products',
  first_seen_at = COALESCE(pim.first_seen_at, pim.last_seen_at, now()),
  last_seen_at = COALESCE(pim.last_seen_at, now()),
  source_report_type = COALESCE(pim.source_report_type, cp.source_report_type)
FROM public.catalog_products AS cp
WHERE pim.catalog_product_id = cp.id
  AND pim.organization_id = cp.organization_id
  AND pim.catalog_product_id IS NOT NULL
  AND (
    pim.match_source IS NULL
    OR pim.linked_from_report_family IS NULL
    OR pim.linked_from_target_table IS NULL
  );

-- ── Resolver: ledger line → bridge match priority (1 best … 99 none) ───────────
DROP VIEW IF EXISTS public.v_inventory_ledger_identifier_candidates;

CREATE VIEW public.v_inventory_ledger_identifier_candidates AS
WITH ledger AS (
  SELECT
    l.id,
    l.organization_id,
    l.upload_id,
    l.fnsku,
    COALESCE(
      NULLIF(btrim(l.sku::text), ''),
      NULLIF(btrim((coalesce(l.raw_data, '{}'::jsonb))->>'sku'), ''),
      NULLIF(btrim((coalesce(l.raw_data, '{}'::jsonb))->>'SKU'), ''),
      NULLIF(btrim((coalesce(l.raw_data, '{}'::jsonb))->>'MSKU'), ''),
      NULLIF(btrim((coalesce(l.raw_data, '{}'::jsonb))->>'seller_sku'), '')
    ) AS resolved_sku,
    COALESCE(
      NULLIF(btrim(l.asin::text), ''),
      NULLIF(btrim((coalesce(l.raw_data, '{}'::jsonb))->>'asin'), ''),
      NULLIF(btrim((coalesce(l.raw_data, '{}'::jsonb))->>'ASIN'), ''),
      NULLIF(btrim((coalesce(l.raw_data, '{}'::jsonb))->>'asin1'), '')
    ) AS resolved_asin
  FROM public.amazon_inventory_ledger AS l
)
SELECT
  le.id AS ledger_id,
  le.organization_id,
  le.upload_id,
  le.fnsku,
  le.resolved_sku,
  le.resolved_asin,
  (
    SELECT p.id
    FROM public.product_identifier_map AS p
    WHERE p.organization_id = le.organization_id
      AND p.fnsku IS NOT NULL
      AND le.fnsku IS NOT NULL
      AND btrim(p.fnsku) = btrim(le.fnsku)
    LIMIT 1
  ) AS mapped_identifier_map_id,
  (
    SELECT p.product_id
    FROM public.product_identifier_map AS p
    WHERE p.organization_id = le.organization_id
      AND p.fnsku IS NOT NULL
      AND le.fnsku IS NOT NULL
      AND btrim(p.fnsku) = btrim(le.fnsku)
    LIMIT 1
  ) AS mapped_product_id,
  (
    SELECT p.catalog_product_id
    FROM public.product_identifier_map AS p
    WHERE p.organization_id = le.organization_id
      AND p.fnsku IS NOT NULL
      AND le.fnsku IS NOT NULL
      AND btrim(p.fnsku) = btrim(le.fnsku)
    LIMIT 1
  ) AS mapped_catalog_product_id,
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM public.product_identifier_map AS p
      WHERE p.organization_id = le.organization_id
        AND p.fnsku IS NOT NULL
        AND le.fnsku IS NOT NULL
        AND btrim(p.fnsku) = btrim(le.fnsku)
    ) THEN 1
    WHEN le.resolved_sku IS NOT NULL
      AND le.resolved_asin IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM public.product_identifier_map AS p
        WHERE p.organization_id = le.organization_id
          AND p.seller_sku IS NOT NULL
          AND p.asin IS NOT NULL
          AND btrim(p.seller_sku) = btrim(le.resolved_sku)
          AND btrim(p.asin) = btrim(le.resolved_asin)
      ) THEN 2
    WHEN le.resolved_sku IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM public.product_identifier_map AS p
        WHERE p.organization_id = le.organization_id
          AND p.seller_sku IS NOT NULL
          AND btrim(p.seller_sku) = btrim(le.resolved_sku)
      ) THEN 3
    WHEN le.resolved_asin IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM public.product_identifier_map AS p
        WHERE p.organization_id = le.organization_id
          AND p.asin IS NOT NULL
          AND btrim(p.asin) = btrim(le.resolved_asin)
      ) THEN 4
    ELSE 99
  END AS match_priority
FROM ledger AS le;

COMMENT ON VIEW public.v_inventory_ledger_identifier_candidates IS
  'Per-ledger-line match tier vs product_identifier_map (1=fnsku … 4=asin … 99=none). FNSKU-only lines become priority 1 once a bridge row exists for that FNSKU.';

COMMIT;
