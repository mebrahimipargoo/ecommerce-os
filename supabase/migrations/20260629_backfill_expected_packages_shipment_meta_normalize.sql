-- Patch backfill_expected_packages_shipment_meta to:
--   1. canonicalize tracking_number / carrier per row (CSV-split, trim, dedupe, sort)
--      so "2221311171, 2221311171, 2221311171" counts as ONE value, not three;
--   2. detect ambiguity per FIELD (not per tuple) — a field is ambiguous only when
--      >= 2 distinct non-null normalized values exist at the chosen tier;
--   3. ignore NULL candidates when counting distinct values, so a row that
--      partially fills (e.g. carrier null on one source row, populated on another)
--      no longer causes false ambiguity.
--
-- Same signature, same return type, same caller contract — pure CREATE OR REPLACE.
-- No new tables, views, or columns. Non-destructive. Idempotent.

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
      nullif(btrim(ep.sku), '')   AS sku,
      nullif(btrim(ep.fnsku), '') AS fnsku,
      ep.tracking_number          AS ep_tn,
      ep.carrier                  AS ep_car,
      ep.shipment_date            AS ep_sd
    FROM public.expected_packages ep
    WHERE ep.organization_id = p_organization_id
      AND (p_upload_id IS NULL OR ep.upload_id = p_upload_id)
      AND (
        ep.tracking_number IS NULL
        OR ep.carrier      IS NULL
        OR ep.shipment_date IS NULL
      )
  ),
  ars_scope AS (
    SELECT
      ars.organization_id,
      ars.upload_id,
      ars.order_id,
      nullif(btrim(ars.sku), '')   AS sku,
      nullif(btrim(ars.fnsku), '') AS fnsku,
      -- Canonicalize tracking_number per row:
      -- split on comma, trim each token, drop empties, dedupe, sort, rejoin.
      -- Examples:
      --   '2221311171, 2221311171, 2221311171' -> '2221311171'
      --   'TRK1, TRK2'                         -> 'TRK1, TRK2'   (1 canonical string)
      --   ''  / NULL                            -> NULL
      (
        SELECT string_agg(v, ', ' ORDER BY v)
        FROM (
          SELECT DISTINCT nullif(btrim(t), '') AS v
          FROM unnest(string_to_array(coalesce(ars.tracking_number, ''), ',')) AS u(t)
        ) s
        WHERE v IS NOT NULL
      ) AS tn,
      (
        SELECT string_agg(v, ', ' ORDER BY v)
        FROM (
          SELECT DISTINCT nullif(btrim(t), '') AS v
          FROM unnest(string_to_array(coalesce(ars.carrier, ''), ',')) AS u(t)
        ) s
        WHERE v IS NOT NULL
      ) AS car,
      ars.shipment_date AS sd
    FROM public.amazon_removal_shipments ars
    WHERE ars.organization_id = p_organization_id
  ),
  t1 AS (
    SELECT ep.id AS ep_id, 1 AS tier, ars.tn, ars.car, ars.sd
    FROM ep_scope ep
    JOIN ars_scope ars
      ON ars.organization_id = ep.organization_id
     AND ep.upload_id  IS NOT NULL
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
     AND ep.upload_id  IS NOT NULL
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
  -- Per (ep_id, tier): per-field distinct count over NON-NULL canonical values.
  -- Each field is independently safe iff its distinct non-null count <= 1.
  -- Two source rows that canonicalize to the SAME tracking string contribute 1.
  -- A NULL value on one row never conflicts with a populated value on another.
  tier_summary AS (
    SELECT
      ep_id,
      tier,
      count(*) FILTER (
        WHERE tn IS NOT NULL OR car IS NOT NULL OR sd IS NOT NULL
      )                                                   AS n_rows,
      count(DISTINCT tn)  FILTER (WHERE tn  IS NOT NULL)  AS n_tn,
      count(DISTINCT car) FILTER (WHERE car IS NOT NULL)  AS n_car,
      count(DISTINCT sd)  FILTER (WHERE sd  IS NOT NULL)  AS n_sd,
      max(tn)  AS tn,
      max(car) AS car,
      max(sd)  AS sd
    FROM all_matches
    GROUP BY ep_id, tier
  ),
  chosen AS (
    SELECT DISTINCT ON (ep_id)
      ep_id, tier, n_rows, n_tn, n_car, n_sd, tn, car, sd
    FROM tier_summary
    WHERE n_rows > 0
    ORDER BY ep_id, tier
  ),
  ep_status AS (
    SELECT
      ep.id,
      ep.ep_tn,
      ep.ep_car,
      ep.ep_sd,
      c.ep_id IS NOT NULL                       AS has_match,
      c.n_tn,
      c.n_car,
      c.n_sd,
      CASE WHEN c.n_tn  = 1 THEN c.tn  END      AS pick_tn,
      CASE WHEN c.n_car = 1 THEN c.car END      AS pick_car,
      CASE WHEN c.n_sd  = 1 THEN c.sd  END      AS pick_sd
    FROM ep_scope ep
    LEFT JOIN chosen c ON c.ep_id = ep.id
  ),
  upd AS (
    UPDATE public.expected_packages ep
    SET
      tracking_number = COALESCE(ep.tracking_number, es.pick_tn),
      carrier         = COALESCE(ep.carrier,         es.pick_car),
      shipment_date   = COALESCE(ep.shipment_date,   es.pick_sd),
      updated_at      = now()
    FROM ep_status es
    WHERE ep.id = es.id
      AND es.has_match
      AND (
           (ep.tracking_number IS NULL AND es.pick_tn  IS NOT NULL)
        OR (ep.carrier         IS NULL AND es.pick_car IS NOT NULL)
        OR (ep.shipment_date   IS NULL AND es.pick_sd  IS NOT NULL)
      )
    RETURNING ep.id
  )
  SELECT
    (SELECT count(*)::bigint FROM upd),
    (
      -- Ambiguous = had candidate rows but did not update, AND at least one
      -- still-null field at the chosen tier had >= 2 distinct candidate values.
      SELECT count(*)::bigint
      FROM ep_status es
      WHERE es.has_match
        AND es.id NOT IN (SELECT id FROM upd)
        AND (
             (es.ep_tn  IS NULL AND COALESCE(es.n_tn,  0) >= 2)
          OR (es.ep_car IS NULL AND COALESCE(es.n_car, 0) >= 2)
          OR (es.ep_sd  IS NULL AND COALESCE(es.n_sd,  0) >= 2)
        )
    ),
    (SELECT count(*)::bigint FROM ep_status WHERE NOT has_match)
  INTO v_filled, v_ambig, v_none;

  RETURN QUERY SELECT v_filled, v_ambig, v_none;
END;
$function$;

COMMENT ON FUNCTION public.backfill_expected_packages_shipment_meta(uuid, uuid) IS
  'Fill expected_packages.tracking_number / carrier / shipment_date (fill-null only) directly from '
  'amazon_removal_shipments. 4-tier priority match (org+upload+order+sku+fnsku → org+upload+order+sku → '
  'org+order+sku+fnsku → org+order+sku). Per-row CSV canonicalization (split, trim, dedupe, sort) before '
  'distinct counting; per-field independent ambiguity (>= 2 distinct non-null canonical values blocks ONLY '
  'that field). NULL candidates never conflict with populated candidates.';

NOTIFY pgrst, 'reload schema';

COMMIT;
