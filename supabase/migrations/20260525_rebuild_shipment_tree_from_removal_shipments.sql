-- Materialize shipment_containers / shipment_boxes / shipment_box_items from amazon_removal_shipments
-- (scoped rebuild; synthetic one box per container until real box data exists).

BEGIN;

CREATE OR REPLACE FUNCTION public.rebuild_shipment_tree_from_removal_shipments(
  p_organization_id uuid,
  p_store_id uuid DEFAULT NULL
)
RETURNS TABLE (
  n_containers bigint,
  n_boxes bigint,
  n_items bigint
)
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, public
AS $function$
BEGIN
  DELETE FROM public.shipment_containers AS c
  WHERE c.organization_id = p_organization_id
    AND (
      p_store_id IS NULL
      OR c.store_id IS NOT DISTINCT FROM p_store_id
    );

  RETURN QUERY
  WITH
  scope AS (
    SELECT
      ars.*
    FROM public.amazon_removal_shipments AS ars
    WHERE ars.organization_id = p_organization_id
      AND (
        p_store_id IS NULL
        OR ars.store_id IS NOT DISTINCT FROM p_store_id
      )
  ),
  cont_src AS (
    SELECT
      s.organization_id,
      s.store_id,
      trim(both from s.tracking_number) AS tracking_number,
      max(nullif(trim(s.carrier), '')) AS carrier,
      min(s.shipment_date) AS shipment_date_d,
      min(s.order_date) AS order_date_d
    FROM scope AS s
    WHERE s.tracking_number IS NOT NULL
      AND length(trim(both from s.tracking_number)) > 0
    GROUP BY
      s.organization_id,
      s.store_id,
      trim(both from s.tracking_number)
  ),
  ins_cont AS (
    INSERT INTO public.shipment_containers (
      organization_id,
      store_id,
      tracking_number,
      carrier,
      shipment_date,
      order_date,
      status
    )
    SELECT
      cs.organization_id,
      cs.store_id,
      cs.tracking_number,
      cs.carrier,
      CASE
        WHEN cs.shipment_date_d IS NOT NULL
          THEN (cs.shipment_date_d AT TIME ZONE 'UTC')
        ELSE NULL
      END,
      CASE
        WHEN cs.order_date_d IS NOT NULL
          THEN (cs.order_date_d AT TIME ZONE 'UTC')
        ELSE NULL
      END,
      'pending'
    FROM cont_src AS cs
    RETURNING
      id,
      organization_id,
      store_id,
      tracking_number
  ),
  ins_box AS (
    INSERT INTO public.shipment_boxes (
      organization_id,
      store_id,
      shipment_container_id,
      box_code,
      box_label,
      packing_slip_code,
      status
    )
    SELECT
      ic.organization_id,
      ic.store_id,
      ic.id,
      ic.tracking_number,
      ic.tracking_number,
      NULL::text,
      'pending'
    FROM ins_cont AS ic
    RETURNING
      id,
      shipment_container_id
  ),
  line_agg AS (
    SELECT
      s.organization_id,
      s.store_id,
      trim(both from s.tracking_number) AS tracking_number,
      s.order_id,
      s.sku,
      s.fnsku,
      s.disposition,
      sum(
        coalesce(
          nullif(s.shipped_quantity, 0),
          nullif(s.requested_quantity, 0),
          0
        )::numeric
      ) AS expected_quantity
    FROM scope AS s
    WHERE s.tracking_number IS NOT NULL
      AND length(trim(both from s.tracking_number)) > 0
      AND s.order_id IS NOT NULL
      AND s.sku IS NOT NULL
      AND s.fnsku IS NOT NULL
      AND s.disposition IS NOT NULL
    GROUP BY
      s.organization_id,
      s.store_id,
      trim(both from s.tracking_number),
      s.order_id,
      s.sku,
      s.fnsku,
      s.disposition
  ),
  ins_items AS (
    INSERT INTO public.shipment_box_items (
      organization_id,
      store_id,
      shipment_box_id,
      order_id,
      sku,
      fnsku,
      disposition,
      expected_quantity,
      scanned_quantity,
      status
    )
    SELECT
      la.organization_id,
      la.store_id,
      ib.id,
      la.order_id,
      la.sku,
      la.fnsku,
      la.disposition,
      la.expected_quantity,
      0::numeric,
      'pending'
    FROM line_agg AS la
    INNER JOIN ins_cont AS ic
      ON la.organization_id = ic.organization_id
      AND la.store_id IS NOT DISTINCT FROM ic.store_id
      AND la.tracking_number = ic.tracking_number
    INNER JOIN ins_box AS ib
      ON ib.shipment_container_id = ic.id
    RETURNING
      id
  )
  SELECT
    (SELECT count(*)::bigint FROM ins_cont),
    (SELECT count(*)::bigint FROM ins_box),
    (SELECT count(*)::bigint FROM ins_items);
END;
$function$;

COMMENT ON FUNCTION public.rebuild_shipment_tree_from_removal_shipments(uuid, uuid) IS
  'Deletes shipment tree rows for the given org (optional store), then loads from amazon_removal_shipments. '
  'Containers: one per (organization_id, store_id, tracking_number) with carrier and earliest shipment_date/order_date. '
  'Boxes: one synthetic row per container (box_code/box_label = tracking). '
  'Items: one line per (org, store, box, order_id, sku, fnsku, disposition); '
  'expected_quantity = sum of shipped_quantity per row, else requested_quantity when shipped is null or zero.';

GRANT EXECUTE ON FUNCTION public.rebuild_shipment_tree_from_removal_shipments(uuid, uuid) TO service_role;

NOTIFY pgrst, 'reload schema';

COMMIT;
