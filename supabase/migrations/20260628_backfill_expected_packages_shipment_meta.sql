-- Restore the direct path:
--   amazon_removal_shipments  →  expected_packages
-- (typed shipment metadata: tracking_number, carrier, shipment_date)
--
-- The existing enrich_expected_packages_from_shipment_allocations only fills
-- expected_packages when EXACTLY one shipment_container path can be reached
-- through removal_item_allocations. That requires amazon_removals demand to
-- match shipment_box_items supply on the canonical 7-column key. Whenever
-- amazon_removals is empty for the upload (REMOVAL_SHIPMENT-only sessions)
-- or the canonical key disagrees on disposition / order_type, the enricher
-- silently returns 0 and expected_packages.tracking_number / carrier /
-- shipment_date stay null.
--
-- This function reads typed columns directly from amazon_removal_shipments
-- (already populated by the Phase 3 sync route since 20260521) and fills the
-- three shipment-derived fields on expected_packages, fill-null only,
-- ambiguity-aware, in 4 priority tiers:
--
--   1. organization_id + upload_id + order_id + sku + fnsku
--   2. organization_id + upload_id + order_id + sku
--   3. organization_id + order_id + sku + fnsku
--   4. organization_id + order_id + sku
--
-- Within the lowest tier that matches at least one shipment row, if the
-- distinct (tracking_number, carrier, shipment_date) tuples > 1 the row is
-- skipped and counted as ambiguous (no overwrite, no fill).
--
-- No new tables, views, or columns. Existing rows preserved.

BEGIN;

CREATE OR REPLACE FUNCTION public.backfill_expected_packages_shipment_meta(
  p_organization_id uuid,
  p_upload_id uuid DEFAULT NULL
)
RETURNS TABLE (
  rows_filled bigint,
  rows_skipped_ambiguous bigint,
  rows_no_match bigint
)
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, public
AS $function$
DECLARE
  v_filled bigint := 0;
  v_ambig  bigint := 0;
  v_none   bigint := 0;
BEGIN
  WITH ep_scope AS (
    SELECT
      ep.id,
      ep.organization_id,
      ep.upload_id,
      ep.order_id,
      nullif(trim(both from ep.sku), '')   AS sku,
      nullif(trim(both from ep.fnsku), '') AS fnsku,
      ep.tracking_number,
      ep.carrier,
      ep.shipment_date
    FROM public.expected_packages ep
    WHERE ep.organization_id = p_organization_id
      AND (p_upload_id IS NULL OR ep.upload_id = p_upload_id)
      AND (
        ep.tracking_number IS NULL
        OR ep.carrier IS NULL
        OR ep.shipment_date IS NULL
      )
  ),
  ars_scope AS (
    SELECT
      ars.organization_id,
      ars.upload_id,
      ars.order_id,
      nullif(trim(both from ars.sku), '')   AS sku,
      nullif(trim(both from ars.fnsku), '') AS fnsku,
      nullif(trim(both from ars.tracking_number), '') AS tn,
      nullif(trim(both from ars.carrier), '')         AS car,
      ars.shipment_date AS sd
    FROM public.amazon_removal_shipments ars
    WHERE ars.organization_id = p_organization_id
      AND (
        ars.tracking_number IS NOT NULL
        OR ars.carrier IS NOT NULL
        OR ars.shipment_date IS NOT NULL
      )
  ),
  t1 AS (
    SELECT ep.id AS ep_id, 1 AS tier, ars.tn, ars.car, ars.sd
    FROM ep_scope ep
    JOIN ars_scope ars
      ON ars.organization_id = ep.organization_id
     AND ep.upload_id IS NOT NULL
     AND ars.upload_id IS NOT NULL
     AND ars.upload_id = ep.upload_id
     AND ars.order_id  = ep.order_id
     AND ars.sku   IS NOT DISTINCT FROM ep.sku
     AND ars.fnsku IS NOT DISTINCT FROM ep.fnsku
  ),
  t2 AS (
    SELECT ep.id AS ep_id, 2 AS tier, ars.tn, ars.car, ars.sd
    FROM ep_scope ep
    JOIN ars_scope ars
      ON ars.organization_id = ep.organization_id
     AND ep.upload_id IS NOT NULL
     AND ars.upload_id IS NOT NULL
     AND ars.upload_id = ep.upload_id
     AND ars.order_id  = ep.order_id
     AND ars.sku IS NOT DISTINCT FROM ep.sku
  ),
  t3 AS (
    SELECT ep.id AS ep_id, 3 AS tier, ars.tn, ars.car, ars.sd
    FROM ep_scope ep
    JOIN ars_scope ars
      ON ars.organization_id = ep.organization_id
     AND ars.order_id = ep.order_id
     AND ars.sku   IS NOT DISTINCT FROM ep.sku
     AND ars.fnsku IS NOT DISTINCT FROM ep.fnsku
  ),
  t4 AS (
    SELECT ep.id AS ep_id, 4 AS tier, ars.tn, ars.car, ars.sd
    FROM ep_scope ep
    JOIN ars_scope ars
      ON ars.organization_id = ep.organization_id
     AND ars.order_id = ep.order_id
     AND ars.sku IS NOT DISTINCT FROM ep.sku
  ),
  all_matches AS (
    SELECT * FROM t1
    UNION ALL SELECT * FROM t2
    UNION ALL SELECT * FROM t3
    UNION ALL SELECT * FROM t4
  ),
  tier_summary AS (
    SELECT
      ep_id,
      tier,
      count(DISTINCT (tn, car, sd))::bigint AS distinct_tuples,
      max(tn)  AS tn,
      max(car) AS car,
      max(sd)  AS sd
    FROM all_matches
    GROUP BY ep_id, tier
  ),
  chosen AS (
    -- Lowest tier wins. If that tier is ambiguous we still pick it and skip;
    -- we do NOT bypass a more-specific tier with a less-specific one.
    SELECT DISTINCT ON (ep_id)
      ep_id, tier, distinct_tuples, tn, car, sd
    FROM tier_summary
    ORDER BY ep_id, tier
  ),
  ep_status AS (
    SELECT
      ep.id,
      CASE
        WHEN c.ep_id IS NULL          THEN 'nomatch'
        WHEN c.distinct_tuples = 1    THEN 'fill'
        ELSE                                'ambiguous'
      END AS status,
      c.tn  AS tn,
      c.car AS car,
      c.sd  AS sd
    FROM ep_scope ep
    LEFT JOIN chosen c ON c.ep_id = ep.id
  ),
  upd AS (
    UPDATE public.expected_packages ep
    SET
      tracking_number = COALESCE(ep.tracking_number, es.tn),
      carrier         = COALESCE(ep.carrier,         es.car),
      shipment_date   = COALESCE(ep.shipment_date,   es.sd),
      updated_at      = now()
    FROM ep_status es
    WHERE ep.id = es.id
      AND es.status = 'fill'
      AND (
        (ep.tracking_number IS NULL AND es.tn  IS NOT NULL)
        OR (ep.carrier      IS NULL AND es.car IS NOT NULL)
        OR (ep.shipment_date IS NULL AND es.sd  IS NOT NULL)
      )
    RETURNING ep.id
  )
  SELECT
    (SELECT count(*)::bigint FROM upd),
    (SELECT count(*)::bigint FROM ep_status WHERE status = 'ambiguous'),
    (SELECT count(*)::bigint FROM ep_status WHERE status = 'nomatch')
  INTO v_filled, v_ambig, v_none;

  RETURN QUERY SELECT v_filled, v_ambig, v_none;
END;
$function$;

COMMENT ON FUNCTION public.backfill_expected_packages_shipment_meta(uuid, uuid) IS
  'Fill expected_packages.tracking_number / carrier / shipment_date (fill-null only) directly from '
  'amazon_removal_shipments. 4-tier priority match (org+upload+order+sku+fnsku → org+upload+order+sku → '
  'org+order+sku+fnsku → org+order+sku); ambiguous tiers (>1 distinct tracking/carrier/date tuple) are skipped.';

GRANT EXECUTE ON FUNCTION public.backfill_expected_packages_shipment_meta(uuid, uuid) TO service_role;

NOTIFY pgrst, 'reload schema';

COMMIT;
