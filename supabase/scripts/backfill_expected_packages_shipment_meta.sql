-- One-shot backfill: expected_packages.tracking_number / carrier / shipment_date
-- ← amazon_removal_shipments (typed columns), org-wide, fill-null only,
-- ambiguity-aware per FIELD (not per tuple), with per-row CSV normalization.
--
-- Safe to run repeatedly. Does NOT overwrite non-null fields. Does NOT delete
-- or insert any expected_packages rows. No schema changes.
--
-- Tiers (lowest tier with any candidate wins; per-field ambiguity inside that
-- tier blocks ONLY the ambiguous field, not the whole row):
--
--   1. organization_id + upload_id + order_id + sku + fnsku
--   2. organization_id + upload_id + order_id + sku
--   3. organization_id + order_id + sku + fnsku
--   4. organization_id + order_id + sku
--
-- Per-row CSV canonicalization for tracking_number and carrier:
--   '2221311171, 2221311171, 2221311171' -> '2221311171'  (single value)
--   'TRK1, TRK2'                         -> 'TRK1, TRK2'  (single canonical string)
--
-- Field is filled when its distinct non-null canonical value count = 1.
-- Field is skipped (ambiguous) when distinct non-null count >= 2.
-- NULL candidates never conflict with populated ones.
--
-- Final SELECT returns three counts: rows_filled, rows_skipped_ambiguous,
-- rows_no_match.

BEGIN;

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
  WHERE ep.tracking_number IS NULL
     OR ep.carrier         IS NULL
     OR ep.shipment_date   IS NULL
),
ars_scope AS (
  SELECT
    ars.organization_id,
    ars.upload_id,
    ars.order_id,
    nullif(btrim(ars.sku), '')   AS sku,
    nullif(btrim(ars.fnsku), '') AS fnsku,
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
  (SELECT count(*)::bigint FROM upd) AS rows_filled,
  (
    SELECT count(*)::bigint
    FROM ep_status es
    WHERE es.has_match
      AND es.id NOT IN (SELECT id FROM upd)
      AND (
           (es.ep_tn  IS NULL AND COALESCE(es.n_tn,  0) >= 2)
        OR (es.ep_car IS NULL AND COALESCE(es.n_car, 0) >= 2)
        OR (es.ep_sd  IS NULL AND COALESCE(es.n_sd,  0) >= 2)
      )
  ) AS rows_skipped_ambiguous,
  (SELECT count(*)::bigint FROM ep_status WHERE NOT has_match) AS rows_no_match;

COMMIT;
