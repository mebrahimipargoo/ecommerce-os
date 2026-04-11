-- Operational eligibility rule layer over canonical expected_packages (+ removal scan totals).
-- Derived only from existing tables; no new base tables.

BEGIN;

CREATE OR REPLACE VIEW public.v_ops_case_lines AS
SELECT
  ep.organization_id,
  ep.store_id,
  ep.upload_id,
  ep.order_id,
  ep.order_type,
  ep.sku,
  ep.fnsku,
  ep.disposition,
  ep.tracking_number,
  ep.carrier,
  ep.shipment_date,
  ep.allocation_box_code,
  COALESCE(
    (
      SELECT SUM(ria.scanned_quantity)::numeric
      FROM public.removal_item_allocations AS ria
      WHERE ria.organization_id = ep.organization_id
        AND COALESCE(ar_staging.id, ar_business.id) IS NOT NULL
        AND ria.removal_id = COALESCE(ar_staging.id, ar_business.id)
    ),
    0::numeric
  ) AS actual_scanned_count,
  (
    lower(nullif(trim(both from ep.order_type), '')) = 'return'
  ) AS scan_eligible,
  true AS claim_eligible,
  (
    lower(nullif(trim(both from ep.order_type), '')) IS NULL
    OR lower(nullif(trim(both from ep.order_type), '')) NOT IN (
      'return',
      'disposal',
      'liquidation',
      'liquidations'
    )
  ) AS review_required,
  CASE
    WHEN lower(nullif(trim(both from ep.order_type), '')) = 'return' THEN 'return'::text
    WHEN lower(nullif(trim(both from ep.order_type), '')) IN ('disposal', 'liquidation', 'liquidations')
      THEN 'non_return_ops'::text
    ELSE 'review'::text
  END AS claim_domain,
  CASE
    WHEN lower(nullif(trim(both from ep.order_type), '')) = 'return'
      THEN 'Return shipment — inbound scan and claim eligible.'::text
    WHEN lower(nullif(trim(both from ep.order_type), '')) IN ('disposal', 'liquidation', 'liquidations')
      THEN 'Removal / liquidation line — claim-focused; not an inbound scan line.'::text
    ELSE 'Unclassified or missing order_type — confirm handling (claim allowed; scan rules TBD).'::text
  END AS claim_reason_hint,
  CASE
    WHEN lower(nullif(trim(both from ep.order_type), '')) = 'return' THEN
      CASE
        WHEN nullif(trim(both from ep.tracking_number), '') IS NOT NULL THEN 'covered'::text
        ELSE 'uncovered'::text
      END
    ELSE 'not_applicable'::text
  END AS coverage_status
FROM public.expected_packages AS ep
LEFT JOIN public.amazon_removals AS ar_staging
  ON ep.source_staging_id IS NOT NULL
  AND ar_staging.organization_id = ep.organization_id
  AND ar_staging.upload_id = ep.upload_id
  AND ar_staging.source_staging_id = ep.source_staging_id
LEFT JOIN LATERAL (
  SELECT ar.id
  FROM public.amazon_removals AS ar
  WHERE ar.organization_id = ep.organization_id
    AND ar_staging.id IS NULL
    AND ar.store_id IS NOT DISTINCT FROM ep.store_id
    AND ar.order_id IS NOT DISTINCT FROM ep.order_id
    AND ar.order_type IS NOT DISTINCT FROM ep.order_type
    AND ar.sku IS NOT DISTINCT FROM ep.sku
    AND ar.fnsku IS NOT DISTINCT FROM ep.fnsku
    AND ar.disposition IS NOT DISTINCT FROM ep.disposition
  ORDER BY ar.id
  LIMIT 1
) AS ar_business ON true;

COMMENT ON VIEW public.v_ops_case_lines IS
  'DB-first operational rules: scan vs claim eligibility, review flags, and coverage status over expected_packages; '
  'actual_scanned_count sums removal_item_allocations for the resolved amazon_removals row (staging match else canonical business key).';

GRANT SELECT ON public.v_ops_case_lines TO authenticated;
GRANT SELECT ON public.v_ops_case_lines TO service_role;

NOTIFY pgrst, 'reload schema';

COMMIT;

-- Verification (run manually after migrate):
-- SELECT order_type,
--        scan_eligible,
--        claim_eligible,
--        coverage_status,
--        count(*) AS n
--   FROM public.v_ops_case_lines
--  GROUP BY 1, 2, 3, 4
--  ORDER BY 1, 2, 3, 4;
