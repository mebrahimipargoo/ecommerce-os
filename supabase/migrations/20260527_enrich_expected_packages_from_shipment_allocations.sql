-- After worklist upsert, enrich expected_packages from removal_item_allocations →
-- shipment tree when exactly one shipment_container is involved (no fake tracking when multiple).

BEGIN;

ALTER TABLE public.expected_packages
  ADD COLUMN IF NOT EXISTS allocation_box_code text;

COMMENT ON COLUMN public.expected_packages.allocation_box_code IS
  'Populated from shipment_boxes.box_code when a single allocation path exists (see enrich_expected_packages_from_shipment_allocations).';

CREATE OR REPLACE FUNCTION public.enrich_expected_packages_from_shipment_allocations(
  p_organization_id uuid,
  p_upload_id uuid,
  p_store_id uuid DEFAULT NULL
)
RETURNS TABLE (
  expected_rows_with_single_allocation_path bigint,
  expected_rows_with_multiple_allocation_paths bigint,
  expected_rows_with_no_allocation_path bigint,
  expected_rows_enriched_from_allocation bigint
)
LANGUAGE sql
VOLATILE
SET search_path = pg_catalog, public
AS $function$
  WITH ep_scope AS (
    SELECT
      ep.id AS ep_id,
      COALESCE(ar_staging.id, ar_business.id) AS removal_id
    FROM public.expected_packages ep
    LEFT JOIN public.amazon_removals ar_staging
      ON ar_staging.organization_id = ep.organization_id
      AND ar_staging.upload_id = ep.upload_id
      AND ar_staging.source_staging_id IS NOT DISTINCT FROM ep.source_staging_id
    LEFT JOIN LATERAL (
      SELECT arb.id
      FROM public.amazon_removals arb
      WHERE arb.organization_id = ep.organization_id
        AND ar_staging.id IS NULL
        AND arb.store_id IS NOT DISTINCT FROM ep.store_id
        AND arb.order_id IS NOT DISTINCT FROM ep.order_id
        AND arb.sku IS NOT DISTINCT FROM ep.sku
        AND arb.fnsku IS NOT DISTINCT FROM ep.fnsku
        AND arb.disposition IS NOT DISTINCT FROM ep.disposition
        AND (p_store_id IS NULL OR arb.store_id IS NOT DISTINCT FROM p_store_id)
      ORDER BY arb.id
      LIMIT 1
    ) AS ar_business ON true
    WHERE ep.organization_id = p_organization_id
      AND ep.upload_id = p_upload_id
  ),
  paths AS (
    SELECT
      s.ep_id,
      sc.id AS container_id,
      trim(both from sc.tracking_number) AS trk,
      sc.carrier,
      sc.shipment_date,
      nullif(trim(both from sbi.fnsku::text), '') AS sbi_fnsku,
      nullif(trim(both from sb.box_code::text), '') AS box_code
    FROM ep_scope s
    INNER JOIN public.removal_item_allocations ria
      ON ria.removal_id = s.removal_id
      AND ria.organization_id = p_organization_id
      AND (p_store_id IS NULL OR ria.store_id IS NOT DISTINCT FROM p_store_id)
    INNER JOIN public.shipment_box_items sbi ON sbi.id = ria.shipment_box_item_id
    INNER JOIN public.shipment_boxes sb ON sb.id = sbi.shipment_box_id
    INNER JOIN public.shipment_containers sc ON sc.id = sb.shipment_container_id
    WHERE s.removal_id IS NOT NULL
  ),
  agg AS (
    SELECT
      s.ep_id,
      COALESCE((
        SELECT COUNT(DISTINCT p.container_id)
        FROM paths p
        WHERE p.ep_id = s.ep_id
      ), 0)::bigint AS n_cont
    FROM ep_scope s
  ),
  stats AS (
    SELECT
      COALESCE((SELECT COUNT(*)::bigint FROM agg a WHERE a.n_cont = 1), 0)::bigint AS sgl,
      COALESCE((SELECT COUNT(*)::bigint FROM agg a WHERE a.n_cont > 1), 0)::bigint AS mul,
      COALESCE((SELECT COUNT(*)::bigint FROM agg a WHERE a.n_cont = 0), 0)::bigint AS non
  ),
  single_payload AS (
    SELECT
      p.ep_id,
      MAX(p.trk) AS tracking_number,
      MAX(p.carrier) AS carrier,
      MAX(p.shipment_date) AS shipment_ts,
      MIN(p.sbi_fnsku) AS fnsku_fill,
      MIN(p.box_code) AS box_fill
    FROM paths p
    INNER JOIN agg a ON a.ep_id = p.ep_id AND a.n_cont = 1
    GROUP BY p.ep_id
  ),
  upd AS (
    UPDATE public.expected_packages ep
    SET
      tracking_number = COALESCE(nullif(trim(both from ep.tracking_number), ''), sp.tracking_number),
      carrier = COALESCE(nullif(trim(both from ep.carrier), ''), sp.carrier),
      shipment_date = COALESCE(
        ep.shipment_date,
        CASE WHEN sp.shipment_ts IS NOT NULL THEN (sp.shipment_ts AT TIME ZONE 'UTC')::date ELSE NULL END
      ),
      fnsku = COALESCE(nullif(trim(both from ep.fnsku), ''), sp.fnsku_fill),
      allocation_box_code = COALESCE(nullif(trim(both from ep.allocation_box_code), ''), sp.box_fill),
      updated_at = now()
    FROM single_payload sp
    WHERE ep.id = sp.ep_id
    RETURNING ep.id
  )
  SELECT
    st.sgl,
    st.mul,
    st.non,
    (SELECT COUNT(*)::bigint FROM upd)
  FROM stats st;
$function$;

COMMENT ON FUNCTION public.enrich_expected_packages_from_shipment_allocations(uuid, uuid, uuid) IS
  'Post–worklist: resolve amazon_removals by upload+source_staging_id when present, else by org/store/order/sku/fnsku/disposition; '
  'then fill expected_packages from allocation + shipment tree when exactly one container path exists.';

GRANT EXECUTE ON FUNCTION public.enrich_expected_packages_from_shipment_allocations(uuid, uuid, uuid) TO service_role;

NOTIFY pgrst, 'reload schema';

COMMIT;
