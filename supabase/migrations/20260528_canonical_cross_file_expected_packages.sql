-- Canonical cross-file identity for REMOVAL_ORDER ↔ REMOVAL_SHIPMENT reconciliation:
-- organization_id, store_id, order_id, order_type, sku, fnsku, disposition
-- (quantities and order_date remain row attributes, not match keys.)
--
-- 1) expected_packages: replace staging + quantity-heavy business uniques with one canonical unique.
-- 2) shipment_box_items: add order_type; shipment tree + allocation rebuild match on canonical key.

BEGIN;

-- ── shipment_box_items: order_type for allocation matching to amazon_removals.order_type ─────
ALTER TABLE public.shipment_box_items
  ADD COLUMN IF NOT EXISTS order_type text;

COMMENT ON COLUMN public.shipment_box_items.order_type IS
  'Mirrors removal/shipment line order_type; part of canonical cross-file match with amazon_removals.';

-- ── rebuild_shipment_tree_from_removal_shipments: aggregate by order_type in line_agg ────────
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
      cs.shipment_date_d,
      cs.order_date_d,
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
      s.order_type,
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
      s.order_type,
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
      order_type,
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
      la.order_type,
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
  'Materializes shipment tree from amazon_removal_shipments; box items keyed by order line incl. order_type.';

-- ── rebuild_removal_item_allocations: canonical match incl. order_type ─────────────────────
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
      ar.order_type,
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
        AND ar.order_type IS NOT DISTINCT FROM r_key.order_type
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
          AND sbi.order_type IS NOT DISTINCT FROM r_key.order_type
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
  'FIFO allocation: match demand/supply on org, store, order_id, order_type, sku, fnsku, disposition.';

-- ── expected_packages: dedupe then swap to canonical unique ─────────────────────────────────
DELETE FROM public.expected_packages AS a
USING public.expected_packages AS b
WHERE a.id > b.id
  AND a.organization_id = b.organization_id
  AND a.store_id IS NOT DISTINCT FROM b.store_id
  AND a.order_id IS NOT DISTINCT FROM b.order_id
  AND a.order_type IS NOT DISTINCT FROM b.order_type
  AND a.sku IS NOT DISTINCT FROM b.sku
  AND a.fnsku IS NOT DISTINCT FROM b.fnsku
  AND a.disposition IS NOT DISTINCT FROM b.disposition;

DROP INDEX IF EXISTS public.uq_expected_packages_business_line;
DROP INDEX IF EXISTS public.uq_expected_packages_org_upload_source_staging;

CREATE UNIQUE INDEX IF NOT EXISTS uq_expected_packages_canonical_cross_file
  ON public.expected_packages (
    organization_id,
    store_id,
    order_id,
    order_type,
    sku,
    fnsku,
    disposition
  )
  NULLS NOT DISTINCT;

COMMENT ON INDEX public.uq_expected_packages_canonical_cross_file IS
  'Cross-file worklist identity: same row for REMOVAL_ORDER + REMOVAL_SHIPMENT when these fields align.';

-- ── enrich_expected_packages_from_shipment_allocations: business fallback incl. order_type ─
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
        AND arb.order_type IS NOT DISTINCT FROM ep.order_type
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
  'Resolve removal_id by staging row or canonical (org, store, order, type, sku, fnsku, disposition); enrich when one container path.';

NOTIFY pgrst, 'reload schema';

COMMIT;
