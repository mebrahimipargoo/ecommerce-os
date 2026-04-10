-- DB-side rematerialization of removal_item_allocations: greedy FIFO from removal demand
-- (amazon_removals.shipped_quantity) to shipment supply (shipment_box_items.expected_quantity).
-- Scoped by organization_id and optional store_id only; no global rebuild.

BEGIN;

CREATE OR REPLACE FUNCTION public.rebuild_removal_item_allocations(
  p_organization_id uuid,
  p_store_id uuid DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, public
AS $function$
DECLARE
  v_inserted bigint := 0;
  v_need numeric;
  v_avail numeric;
  v_take numeric;
  r_key RECORD;
  r_rem RECORD;
  r_sup RECORD;
BEGIN
  DELETE FROM public.removal_item_allocations AS ria
  WHERE ria.organization_id = p_organization_id
    AND (
      p_store_id IS NULL
      OR ria.store_id IS NOT DISTINCT FROM p_store_id
    );

  FOR r_key IN
    SELECT DISTINCT
      ar.organization_id,
      ar.store_id,
      ar.order_id,
      ar.sku,
      ar.fnsku,
      ar.disposition
    FROM public.amazon_removals AS ar
    WHERE ar.organization_id = p_organization_id
      AND (p_store_id IS NULL OR ar.store_id IS NOT DISTINCT FROM p_store_id)
      AND ar.order_id IS NOT NULL
      AND ar.sku IS NOT NULL
      AND ar.fnsku IS NOT NULL
      AND ar.disposition IS NOT NULL
      AND COALESCE(ar.shipped_quantity, 0) > 0
  LOOP
    FOR r_rem IN
      SELECT
        ar.id AS removal_id,
        ar.organization_id,
        ar.store_id,
        ar.order_id,
        ar.sku,
        ar.fnsku,
        ar.disposition,
        COALESCE(ar.shipped_quantity, 0)::numeric AS need_qty
      FROM public.amazon_removals AS ar
      WHERE ar.organization_id = r_key.organization_id
        AND ar.store_id IS NOT DISTINCT FROM r_key.store_id
        AND ar.order_id IS NOT DISTINCT FROM r_key.order_id
        AND ar.sku IS NOT DISTINCT FROM r_key.sku
        AND ar.fnsku IS NOT DISTINCT FROM r_key.fnsku
        AND ar.disposition IS NOT DISTINCT FROM r_key.disposition
        AND COALESCE(ar.shipped_quantity, 0) > 0
      ORDER BY ar.id
    LOOP
      v_need := r_rem.need_qty;

      FOR r_sup IN
        SELECT
          sbi.id AS shipment_box_item_id,
          sc.tracking_number
        FROM public.shipment_box_items AS sbi
        INNER JOIN public.shipment_boxes AS sb ON sb.id = sbi.shipment_box_id
        INNER JOIN public.shipment_containers AS sc ON sc.id = sb.shipment_container_id
        WHERE sbi.organization_id = r_key.organization_id
          AND sbi.store_id IS NOT DISTINCT FROM r_key.store_id
          AND sbi.order_id IS NOT DISTINCT FROM r_key.order_id
          AND sbi.sku IS NOT DISTINCT FROM r_key.sku
          AND sbi.fnsku IS NOT DISTINCT FROM r_key.fnsku
          AND sbi.disposition IS NOT DISTINCT FROM r_key.disposition
          AND sbi.order_id IS NOT NULL
          AND sbi.sku IS NOT NULL
          AND sbi.fnsku IS NOT NULL
          AND sbi.disposition IS NOT NULL
          AND COALESCE(sbi.expected_quantity, 0) > 0
        ORDER BY
          sc.shipment_date ASC NULLS LAST,
          sc.tracking_number ASC NULLS LAST,
          sbi.id ASC
      LOOP
        EXIT WHEN v_need <= 0;

        SELECT
          COALESCE(sbi.expected_quantity, 0)::numeric
          - COALESCE((
              SELECT SUM(sub.allocated_quantity)
              FROM public.removal_item_allocations AS sub
              WHERE sub.shipment_box_item_id = r_sup.shipment_box_item_id
            ), 0)
        INTO v_avail
        FROM public.shipment_box_items AS sbi
        WHERE sbi.id = r_sup.shipment_box_item_id;

        IF v_avail <= 0 THEN
          CONTINUE;
        END IF;

        v_take := LEAST(v_need, v_avail);
        IF v_take <= 0 THEN
          CONTINUE;
        END IF;

        INSERT INTO public.removal_item_allocations (
          organization_id,
          store_id,
          removal_id,
          shipment_box_item_id,
          order_id,
          sku,
          fnsku,
          disposition,
          tracking_number,
          allocated_quantity,
          scanned_quantity,
          status
        )
        VALUES (
          r_rem.organization_id,
          r_rem.store_id,
          r_rem.removal_id,
          r_sup.shipment_box_item_id,
          r_rem.order_id,
          r_rem.sku,
          r_rem.fnsku,
          r_rem.disposition,
          r_sup.tracking_number,
          v_take,
          0,
          'pending'
        );

        v_need := v_need - v_take;
        v_inserted := v_inserted + 1;
      END LOOP;
    END LOOP;
  END LOOP;

  RETURN v_inserted;
END;
$function$;

COMMENT ON FUNCTION public.rebuild_removal_item_allocations(uuid, uuid) IS
  'Deletes removal_item_allocations for the given org (and optional store), then rebuilds rows by matching '
  'organization_id, store_id, order_id, sku, fnsku, disposition. '
  'Demand = shipped_quantity per amazon_removals row; supply = expected_quantity per shipment_box_item; '
  'FIFO on supply by shipment_containers.shipment_date, tracking_number, shipment_box_items.id; '
  'multiple removal rows for the same key consume supply in amazon_removals.id order.';

GRANT EXECUTE ON FUNCTION public.rebuild_removal_item_allocations(uuid, uuid) TO service_role;

NOTIFY pgrst, 'reload schema';

COMMIT;
