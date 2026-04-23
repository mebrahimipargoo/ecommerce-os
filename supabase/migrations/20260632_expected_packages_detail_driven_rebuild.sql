-- Rebuild expected_packages must be DETAIL-DRIVEN, not SHIPMENT-DRIVEN.
--
-- Replaces public.rebuild_expected_packages_from_removals so the contract is:
--
--   1) Every amazon_removals row in scope MUST appear in expected_packages.
--      No detail row may disappear because shipment match is missing.
--
--   2) expected_packages preserves the full quantity snapshot from the detail:
--        requested_quantity
--        shipped_quantity
--        disposed_quantity
--        cancelled_quantity
--        in_process_quantity   <- new column on expected_packages
--        removal_fee           <- new column on expected_packages
--        currency              <- new column on expected_packages
--      The snapshot is denormalized onto every derived row produced from that
--      detail line, so it is queryable per row regardless of shipment state.
--
--   3) amazon_removal_shipments is used ONLY to SPLIT or ENRICH:
--        adds tracking_number, carrier, shipment_date
--        splits one detail row into multiple expected rows when multiple
--        shipment rows match.
--
--   4) If no shipment rows match a detail line:
--        keep ONE unsplit expected row
--        tracking_number / carrier / shipment_date stay NULL
--        the detail row still exists (build_source = 'detail_remainder',
--        build_status = 'awaiting_shipment_match' when shipped_quantity > 0,
--        build_status = 'no_shipment_expected'    when shipped_quantity = 0).
--
--   5) If shipment rows cover only part of the detail.shipped_quantity:
--        emit shipment-backed split rows for what they cover, plus ONE
--        remainder row for the unresolved part. Disposed / cancelled /
--        in_process quantities remain visible on every emitted row.
--
--   6) Disposed / cancelled / in_process quantities remain visible on
--      expected_packages even when no shipment rows exist.
--
--   7) No code path requires shipment rows for a detail row to exist in
--      expected_packages.
--
-- Reuses every existing column, constraint, FK, and partial unique index
-- introduced by 20260631_expected_packages_derived_rebuild.sql. Only additive
-- columns are added; the function is replaced via CREATE OR REPLACE.
--
-- Idempotent. Safe to re-run.

BEGIN;

-- ── 1) Additive columns to carry the full detail quantity snapshot ──────────
ALTER TABLE public.expected_packages
  ADD COLUMN IF NOT EXISTS in_process_quantity integer,
  ADD COLUMN IF NOT EXISTS removal_fee         numeric(18, 6),
  ADD COLUMN IF NOT EXISTS currency            text;

COMMENT ON COLUMN public.expected_packages.in_process_quantity IS
  'amazon_removals.in_process_quantity snapshot copied at rebuild time. '
  'Visible on every derived row produced from the detail line, even when no '
  'shipment rows exist.';
COMMENT ON COLUMN public.expected_packages.removal_fee IS
  'amazon_removals.removal_fee snapshot copied at rebuild time. Denormalized '
  'onto every derived row produced from the detail line.';
COMMENT ON COLUMN public.expected_packages.currency IS
  'amazon_removals.currency snapshot copied at rebuild time. Denormalized '
  'onto every derived row produced from the detail line.';

-- ── 2) Detail-driven rebuild ────────────────────────────────────────────────
--
-- Keys & contracts intentionally unchanged from 20260631:
--   * partial unique uq_expected_packages_derived_pair on
--     (organization_id, source_detail_row_id, source_shipment_row_id)
--     WHERE build_source IN ('detail_shipment','detail_remainder')
--   * legacy partial unique uq_expected_packages_canonical_legacy untouched
--   * FKs expected_packages_source_detail_fk / _source_shipment_fk untouched
--
-- Output enum:
--   build_source IN ('detail_shipment', 'detail_remainder')
--   build_status IN ('matched',
--                    'shipment_overflow_conflict',
--                    'awaiting_shipment_match',
--                    'no_shipment_expected')

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
  --
  --     CRITICAL: detail is the driver. shipment LEFT JOINs to detail.
  --     A detail line with NO matching shipment still produces a row
  --     (the remainder/no-shipment-expected row).
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
      d.in_process_quantity,
      d.removal_fee,
      d.currency,
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
      d.in_process_quantity,
      d.removal_fee,
      d.currency,
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
      count(*) FILTER (WHERE shipment_id IS NOT NULL)                AS shipment_count,
      sum(COALESCE(shipment_shipped_qty, 0))
        FILTER (WHERE shipment_id IS NOT NULL)                        AS shipment_total,
      max(detail_shipped_qty)                                          AS detail_total
    FROM pair
    GROUP BY detail_id
  ),
  matched_rows AS (
    -- One row per (detail × shipment) when shipment matched.
    -- Quantity snapshot from detail is denormalized onto every shipment row.
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
      p.in_process_quantity,
      p.removal_fee,
      p.currency,
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
    -- DETAIL-DRIVEN GUARANTEE: every detail line that is not fully covered
    -- by shipment-backed rows gets exactly one remainder row.
    --
    --   * No shipment matched at all  → one remainder row
    --     (expected_scan_quantity = detail.shipped_quantity, may be 0)
    --   * Shipments cover only part   → one remainder row for the rest
    --   * Shipments cover all/over    → no remainder row (matched rows suffice;
    --                                   overflow is flagged on matched rows)
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
      p.in_process_quantity,
      p.removal_fee,
      p.currency,
      p.order_status,
      NULL::text                                                      AS tracking_number,
      NULL::text                                                      AS carrier,
      NULL::date                                                      AS shipment_date,
      p.detail_id                                                     AS source_detail_row_id,
      NULL::uuid                                                      AS source_shipment_row_id,
      p.detail_shipped_qty                                            AS detail_shipped_quantity_total,
      NULL::integer                                                   AS shipment_row_quantity,
      GREATEST(
        a.detail_total - COALESCE(a.shipment_total, 0),
        0
      )::integer                                                      AS expected_scan_quantity,
      'detail_remainder'::text                                        AS build_source,
      CASE
        WHEN COALESCE(a.shipment_count, 0) = 0
         AND COALESCE(a.detail_total, 0) = 0
          THEN 'no_shipment_expected'
        ELSE 'awaiting_shipment_match'
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
    WHERE COALESCE(a.shipment_count, 0) = 0
       OR a.detail_total > COALESCE(a.shipment_total, 0)
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
    in_process_quantity, removal_fee, currency,
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
    in_process_quantity, removal_fee, currency,
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
    in_process_quantity            = excluded.in_process_quantity,
    removal_fee                    = excluded.removal_fee,
    currency                       = excluded.currency,
    order_status                   = COALESCE(ep.order_status,    excluded.order_status),
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
  --     Only touches build_source IN ('detail_shipment','detail_remainder').
  --     Legacy rows are never touched.
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
  'Detail-driven rebuild of expected_packages from amazon_removals (detail) × '
  'amazon_removal_shipments (shipment). Every detail row in scope produces at least '
  'one expected row; shipments only SPLIT (one ep row per matched shipment) or '
  'ENRICH (tracking_number, carrier, shipment_date). Detail quantity snapshot — '
  'requested, shipped, disposed, cancelled, in_process, removal_fee, currency — is '
  'denormalized onto every emitted row. build_status: matched | '
  'shipment_overflow_conflict | awaiting_shipment_match | no_shipment_expected. '
  'Idempotent. Safe to rerun after each detail or shipment import.';

GRANT EXECUTE ON FUNCTION public.rebuild_expected_packages_from_removals(uuid, uuid) TO service_role;

NOTIFY pgrst, 'reload schema';

COMMIT;
