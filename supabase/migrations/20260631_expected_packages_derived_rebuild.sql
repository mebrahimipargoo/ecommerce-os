-- Part B — Removal Expected Build redesigned around real Amazon truth:
--   amazon_removals          (Removal Order Detail)     — line-level truth
--   amazon_removal_shipments (Removal Shipment Detail)  — shipment-level split truth
--
-- expected_packages becomes the *derived* scan plan:
--   one row per matched (detail_line × shipment_line) pair, plus
--   one remainder row per detail line whose detail.shipped_quantity exceeds
--   the sum of matching shipment.shipped_quantity.
--   Conflict state when shipments overflow detail.shipped_quantity.
--
-- Reuses expected_packages (additive columns only) instead of creating a new
-- table. Existing rows are preserved and tagged build_source = 'legacy'; the
-- canonical-cross-file unique index is converted to a partial index over those
-- legacy rows so the old generate-worklist code path keeps working unchanged
-- alongside the new rebuild.
--
-- Destructive note (single change requiring justification):
--   The existing unique index uq_expected_packages_canonical_cross_file is
--   DROPPED and immediately re-created as a PARTIAL index restricted to
--   build_source IN (NULL, 'legacy'). Without this change the new model
--   (multiple ep rows per canonical 7-tuple, one per shipment row) cannot exist
--   at all, because the current index forbids it. No data is lost; the index
--   is recreated covering the same rows it covered before.
--
-- Wave 5 identity enrichment, every other importer, and every UI route are
-- untouched.

BEGIN;

-- ── 1) Additive columns on expected_packages ────────────────────────────────
ALTER TABLE public.expected_packages
  ADD COLUMN IF NOT EXISTS build_source                  text,
  ADD COLUMN IF NOT EXISTS build_status                  text,
  ADD COLUMN IF NOT EXISTS source_detail_row_id          uuid,
  ADD COLUMN IF NOT EXISTS source_shipment_row_id        uuid,
  ADD COLUMN IF NOT EXISTS detail_shipped_quantity_total integer,
  ADD COLUMN IF NOT EXISTS expected_scan_quantity        integer,
  ADD COLUMN IF NOT EXISTS shipment_row_quantity         integer,
  ADD COLUMN IF NOT EXISTS detail_grouping_key           text,
  ADD COLUMN IF NOT EXISTS rebuild_run_at                timestamptz;

COMMENT ON COLUMN public.expected_packages.build_source IS
  'How this row was created: legacy (pre-rebuild rows) | detail_shipment | detail_remainder.';
COMMENT ON COLUMN public.expected_packages.build_status IS
  'Operational state set by rebuild: matched | awaiting_shipment_match | shipment_overflow_conflict.';
COMMENT ON COLUMN public.expected_packages.source_detail_row_id IS
  'amazon_removals.id of the detail line that produced this expected row.';
COMMENT ON COLUMN public.expected_packages.source_shipment_row_id IS
  'amazon_removal_shipments.id of the shipment row that produced this expected row; NULL on remainder rows.';
COMMENT ON COLUMN public.expected_packages.detail_shipped_quantity_total IS
  'detail.shipped_quantity at the time this row was last rebuilt.';
COMMENT ON COLUMN public.expected_packages.expected_scan_quantity IS
  'Quantity the warehouse must scan against this row.';
COMMENT ON COLUMN public.expected_packages.shipment_row_quantity IS
  'Shipment row shipped_quantity feeding this row; NULL on remainder rows.';
COMMENT ON COLUMN public.expected_packages.detail_grouping_key IS
  'Stable canonical key for grouping rows produced by the same detail line: '
  'organization_id|store_id|order_id|order_type|order_date|sku|fnsku|disposition.';

-- Soft FK references — derived rows lose their pointer (not the row) when a
-- source row is deleted. The next rebuild reconciles them.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'expected_packages_source_detail_fk'
  ) THEN
    ALTER TABLE public.expected_packages
      ADD CONSTRAINT expected_packages_source_detail_fk
      FOREIGN KEY (source_detail_row_id)
      REFERENCES public.amazon_removals (id)
      ON DELETE SET NULL;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'expected_packages_source_shipment_fk'
  ) THEN
    ALTER TABLE public.expected_packages
      ADD CONSTRAINT expected_packages_source_shipment_fk
      FOREIGN KEY (source_shipment_row_id)
      REFERENCES public.amazon_removal_shipments (id)
      ON DELETE SET NULL;
  END IF;
END $$;

-- ── 2) Tag every pre-existing row as legacy so the partial indexes split cleanly
UPDATE public.expected_packages
SET build_source = 'legacy'
WHERE build_source IS NULL;

-- ── 3) Convert canonical-cross-file unique index to a PARTIAL index that
--      covers only legacy rows. Derived rows live under the new partial unique
--      below.
DROP INDEX IF EXISTS public.uq_expected_packages_canonical_cross_file;

CREATE UNIQUE INDEX IF NOT EXISTS uq_expected_packages_canonical_legacy
  ON public.expected_packages (
    organization_id, store_id, order_id, order_type, sku, fnsku, disposition
  )
  WHERE build_source = 'legacy'
  NULLS NOT DISTINCT;

COMMENT ON INDEX public.uq_expected_packages_canonical_legacy IS
  'Legacy canonical-cross-file unique (org, store, order_id, order_type, sku, fnsku, disposition) — '
  'preserves the contract used by the existing generate-worklist upsert. Applies only to '
  'build_source = ''legacy'' rows; derived rows have their own partial unique below.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_expected_packages_derived_pair
  ON public.expected_packages (
    organization_id, source_detail_row_id, source_shipment_row_id
  )
  WHERE build_source IN ('detail_shipment', 'detail_remainder')
  NULLS NOT DISTINCT;

COMMENT ON INDEX public.uq_expected_packages_derived_pair IS
  'Derived expected_packages identity: (org, source_detail_row_id, source_shipment_row_id). '
  'NULL source_shipment_row_id is the remainder row for the detail. Allows multiple ep rows per '
  'canonical 7-tuple, one per shipment.';

CREATE INDEX IF NOT EXISTS idx_expected_packages_derived_status
  ON public.expected_packages (organization_id, build_status)
  WHERE build_source IN ('detail_shipment', 'detail_remainder');

CREATE INDEX IF NOT EXISTS idx_expected_packages_derived_detail
  ON public.expected_packages (source_detail_row_id)
  WHERE source_detail_row_id IS NOT NULL;

-- ── 4) Rebuild function — dedicated DB-side action, idempotent ──────────────
--
-- For each amazon_removals row in scope:
--   * find amazon_removal_shipments rows where
--       organization_id, store_id, order_id, order_type,
--       order_date (request_date), sku, fnsku, disposition
--     match (NULL-safe);
--   * insert one ep row per matched shipment (build_source = 'detail_shipment');
--   * insert one remainder ep row when sum(shipment.shipped_quantity) <
--     detail.shipped_quantity (build_source = 'detail_remainder');
--   * stamp build_status = 'shipment_overflow_conflict' on every matched row
--     for that detail when sum(shipment.shipped_quantity) > detail.shipped_quantity.
--
-- Idempotent via UPSERT keyed on (org, source_detail_row_id, source_shipment_row_id);
-- obsolete derived rows in scope (whose pair no longer exists) are deleted.

CREATE OR REPLACE FUNCTION public.rebuild_expected_packages_from_removals(
  p_organization_id uuid,
  p_store_id uuid DEFAULT NULL
)
RETURNS TABLE (
  detail_lines_in_scope     bigint,
  matched_rows_upserted     bigint,
  remainder_rows_upserted   bigint,
  overflow_lines            bigint,
  obsolete_rows_deleted     bigint
)
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, public
AS $function$
DECLARE
  v_run_at        timestamptz := now();
  v_detail_lines  bigint := 0;
  v_matched       bigint := 0;
  v_remainder     bigint := 0;
  v_overflow      bigint := 0;
  v_deleted       bigint := 0;
BEGIN
  -- (a) Build target rows in a temp table so we can use it for both
  --     UPSERT and obsolete-cleanup against the same scope.
  CREATE TEMP TABLE _rebuild_target ON COMMIT DROP AS
  WITH detail AS (
    SELECT
      d.id                                        AS detail_id,
      d.organization_id,
      d.store_id,
      d.upload_id                                 AS detail_upload_id,
      d.order_id,
      d.order_type,
      d.order_date,
      nullif(btrim(d.sku), '')                    AS sku,
      nullif(btrim(d.fnsku), '')                  AS fnsku,
      nullif(btrim(d.disposition), '')            AS disposition,
      COALESCE(d.shipped_quantity, 0)             AS detail_shipped_qty,
      d.requested_quantity,
      d.disposed_quantity,
      d.cancelled_quantity,
      d.order_status
    FROM public.amazon_removals d
    WHERE d.organization_id = p_organization_id
      AND (p_store_id IS NULL OR d.store_id IS NOT DISTINCT FROM p_store_id)
      AND d.order_id IS NOT NULL
  ),
  shipment AS (
    SELECT
      s.id                                        AS shipment_id,
      s.organization_id,
      s.store_id,
      s.upload_id                                 AS shipment_upload_id,
      s.order_id,
      s.order_type,
      s.order_date,
      nullif(btrim(s.sku), '')                    AS sku,
      nullif(btrim(s.fnsku), '')                  AS fnsku,
      nullif(btrim(s.disposition), '')            AS disposition,
      COALESCE(s.shipped_quantity, 0)             AS shipment_shipped_qty,
      s.tracking_number,
      s.carrier,
      s.shipment_date
    FROM public.amazon_removal_shipments s
    WHERE s.organization_id = p_organization_id
      AND (p_store_id IS NULL OR s.store_id IS NOT DISTINCT FROM p_store_id)
  ),
  pair AS (
    SELECT
      d.detail_id,
      d.organization_id,
      d.store_id,
      d.detail_upload_id,
      d.order_id,
      d.order_type,
      d.order_date,
      d.sku,
      d.fnsku,
      d.disposition,
      d.detail_shipped_qty,
      d.requested_quantity,
      d.disposed_quantity,
      d.cancelled_quantity,
      d.order_status,
      s.shipment_id,
      s.shipment_upload_id,
      s.shipment_shipped_qty,
      s.tracking_number,
      s.carrier,
      s.shipment_date
    FROM detail d
    LEFT JOIN shipment s
      ON s.organization_id = d.organization_id
     AND s.store_id    IS NOT DISTINCT FROM d.store_id
     AND s.order_id    IS NOT DISTINCT FROM d.order_id
     AND s.order_type  IS NOT DISTINCT FROM d.order_type
     AND s.order_date  IS NOT DISTINCT FROM d.order_date
     AND s.sku         IS NOT DISTINCT FROM d.sku
     AND s.fnsku       IS NOT DISTINCT FROM d.fnsku
     AND s.disposition IS NOT DISTINCT FROM d.disposition
  ),
  agg AS (
    SELECT
      detail_id,
      sum(COALESCE(shipment_shipped_qty, 0)) FILTER (WHERE shipment_id IS NOT NULL) AS shipment_total,
      max(detail_shipped_qty)                                                          AS detail_total
    FROM pair
    GROUP BY detail_id
  ),
  matched_rows AS (
    SELECT
      p.organization_id,
      p.store_id,
      COALESCE(p.shipment_upload_id, p.detail_upload_id)              AS upload_id,
      p.order_id,
      p.order_type,
      p.order_date,
      p.sku,
      p.fnsku,
      p.disposition,
      p.requested_quantity,
      p.detail_shipped_qty                                            AS shipped_quantity,
      p.disposed_quantity,
      p.cancelled_quantity,
      p.order_status,
      p.tracking_number,
      p.carrier,
      p.shipment_date,
      p.detail_id                                                     AS source_detail_row_id,
      p.shipment_id                                                   AS source_shipment_row_id,
      p.detail_shipped_qty                                            AS detail_shipped_quantity_total,
      p.shipment_shipped_qty                                          AS shipment_row_quantity,
      p.shipment_shipped_qty                                          AS expected_scan_quantity,
      'detail_shipment'::text                                         AS build_source,
      CASE
        WHEN a.shipment_total > a.detail_total THEN 'shipment_overflow_conflict'
        ELSE 'matched'
      END                                                             AS build_status,
      concat_ws('|',
        p.organization_id::text,
        COALESCE(p.store_id::text, ''),
        p.order_id,
        COALESCE(p.order_type, ''),
        COALESCE(p.order_date::text, ''),
        COALESCE(p.sku, ''),
        COALESCE(p.fnsku, ''),
        COALESCE(p.disposition, '')
      )                                                               AS detail_grouping_key
    FROM pair p
    JOIN agg a USING (detail_id)
    WHERE p.shipment_id IS NOT NULL
  ),
  remainder_rows AS (
    SELECT DISTINCT ON (p.detail_id)
      p.organization_id,
      p.store_id,
      p.detail_upload_id                                              AS upload_id,
      p.order_id,
      p.order_type,
      p.order_date,
      p.sku,
      p.fnsku,
      p.disposition,
      p.requested_quantity,
      p.detail_shipped_qty                                            AS shipped_quantity,
      p.disposed_quantity,
      p.cancelled_quantity,
      p.order_status,
      NULL::text                                                      AS tracking_number,
      NULL::text                                                      AS carrier,
      NULL::date                                                      AS shipment_date,
      p.detail_id                                                     AS source_detail_row_id,
      NULL::uuid                                                      AS source_shipment_row_id,
      p.detail_shipped_qty                                            AS detail_shipped_quantity_total,
      NULL::integer                                                   AS shipment_row_quantity,
      (a.detail_total - COALESCE(a.shipment_total, 0))::integer       AS expected_scan_quantity,
      'detail_remainder'::text                                        AS build_source,
      'awaiting_shipment_match'::text                                 AS build_status,
      concat_ws('|',
        p.organization_id::text,
        COALESCE(p.store_id::text, ''),
        p.order_id,
        COALESCE(p.order_type, ''),
        COALESCE(p.order_date::text, ''),
        COALESCE(p.sku, ''),
        COALESCE(p.fnsku, ''),
        COALESCE(p.disposition, '')
      )                                                               AS detail_grouping_key
    FROM pair p
    JOIN agg a USING (detail_id)
    WHERE a.detail_total > COALESCE(a.shipment_total, 0)
    ORDER BY p.detail_id
  )
  SELECT * FROM matched_rows
  UNION ALL
  SELECT * FROM remainder_rows;

  SELECT count(DISTINCT source_detail_row_id) INTO v_detail_lines FROM _rebuild_target;
  SELECT count(*) INTO v_matched   FROM _rebuild_target WHERE build_source = 'detail_shipment';
  SELECT count(*) INTO v_remainder FROM _rebuild_target WHERE build_source = 'detail_remainder';
  SELECT count(DISTINCT source_detail_row_id) INTO v_overflow
    FROM _rebuild_target WHERE build_status = 'shipment_overflow_conflict';

  -- (b) UPSERT — fill-null only for tracking metadata, do NOT overwrite warehouse
  -- scan progress (e.g. actual_scanned_count) when present.
  INSERT INTO public.expected_packages AS ep (
    organization_id, store_id, upload_id,
    order_id, order_type, order_date,
    sku, fnsku, disposition,
    requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity,
    order_status, tracking_number, carrier, shipment_date,
    source_detail_row_id, source_shipment_row_id,
    detail_shipped_quantity_total, shipment_row_quantity, expected_scan_quantity,
    build_source, build_status, detail_grouping_key, rebuild_run_at
  )
  SELECT
    organization_id, store_id, upload_id,
    order_id, order_type, order_date,
    sku, fnsku, disposition,
    requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity,
    order_status, tracking_number, carrier, shipment_date,
    source_detail_row_id, source_shipment_row_id,
    detail_shipped_quantity_total, shipment_row_quantity, expected_scan_quantity,
    build_source, build_status, detail_grouping_key, v_run_at
  FROM _rebuild_target
  ON CONFLICT (organization_id, source_detail_row_id, source_shipment_row_id)
    WHERE build_source IN ('detail_shipment', 'detail_remainder')
  DO UPDATE SET
    store_id                       = excluded.store_id,
    upload_id                      = excluded.upload_id,
    order_id                       = excluded.order_id,
    order_type                     = excluded.order_type,
    order_date                     = excluded.order_date,
    sku                            = excluded.sku,
    fnsku                          = excluded.fnsku,
    disposition                    = excluded.disposition,
    requested_quantity             = excluded.requested_quantity,
    shipped_quantity               = excluded.shipped_quantity,
    disposed_quantity              = excluded.disposed_quantity,
    cancelled_quantity             = excluded.cancelled_quantity,
    order_status                   = COALESCE(ep.order_status, excluded.order_status),
    tracking_number                = COALESCE(ep.tracking_number, excluded.tracking_number),
    carrier                        = COALESCE(ep.carrier,         excluded.carrier),
    shipment_date                  = COALESCE(ep.shipment_date,   excluded.shipment_date),
    detail_shipped_quantity_total  = excluded.detail_shipped_quantity_total,
    shipment_row_quantity          = excluded.shipment_row_quantity,
    expected_scan_quantity         = excluded.expected_scan_quantity,
    build_source                   = excluded.build_source,
    build_status                   = excluded.build_status,
    detail_grouping_key            = excluded.detail_grouping_key,
    rebuild_run_at                 = excluded.rebuild_run_at,
    updated_at                     = now();

  -- (c) Delete derived rows in scope whose target pair no longer exists.
  WITH del AS (
    DELETE FROM public.expected_packages ep
    USING (
      SELECT ep.id
      FROM public.expected_packages ep
      LEFT JOIN _rebuild_target t
        ON t.organization_id        = ep.organization_id
       AND t.source_detail_row_id   IS NOT DISTINCT FROM ep.source_detail_row_id
       AND t.source_shipment_row_id IS NOT DISTINCT FROM ep.source_shipment_row_id
       AND t.build_source           = ep.build_source
      WHERE ep.organization_id = p_organization_id
        AND (p_store_id IS NULL OR ep.store_id IS NOT DISTINCT FROM p_store_id)
        AND ep.build_source IN ('detail_shipment', 'detail_remainder')
        AND t.organization_id IS NULL
    ) AS obs
    WHERE ep.id = obs.id
    RETURNING ep.id
  )
  SELECT count(*) INTO v_deleted FROM del;

  RETURN QUERY SELECT v_detail_lines, v_matched, v_remainder, v_overflow, v_deleted;
END;
$function$;

COMMENT ON FUNCTION public.rebuild_expected_packages_from_removals(uuid, uuid) IS
  'Idempotent rebuild of expected_packages derived rows from amazon_removals (detail) × '
  'amazon_removal_shipments (shipment). One ep row per matched shipment row; one remainder row '
  'per detail line whose detail.shipped_quantity exceeds matched shipment total. Stamps '
  'build_status = ''shipment_overflow_conflict'' on rows where shipment total > detail total. '
  'Safe to rerun after each removal-detail or removal-shipment import.';

GRANT EXECUTE ON FUNCTION public.rebuild_expected_packages_from_removals(uuid, uuid) TO service_role;

NOTIFY pgrst, 'reload schema';

COMMIT;
