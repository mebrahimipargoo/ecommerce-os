-- Cleanup ONLY derived removal expected_packages rows.
--
-- Targets: expected_packages.build_source IN ('detail_shipment','detail_remainder')
-- Spares:  expected_packages.build_source = 'legacy' (or NULL if any survived).
--
-- Use when you want to wipe the derived scan plan and re-run
-- public.rebuild_expected_packages_from_removals from a clean slate.
--
-- DOES NOT touch:
--   * legacy expected_packages rows
--   * amazon_removals (detail truth)
--   * amazon_removal_shipments (shipment truth)
--   * shipment_containers / shipment_boxes / shipment_box_items / removal_item_allocations
--   * any other report family
--
-- Scope: by default org-wide for the most-active org. Replace the WHERE
-- clause below to scope to a specific organization_id / store_id / upload.

BEGIN;

WITH cfg AS (
  SELECT organization_id
  FROM public.expected_packages
  WHERE build_source IN ('detail_shipment','detail_remainder')
  GROUP BY organization_id
  ORDER BY count(*) DESC
  LIMIT 1
),
del AS (
  DELETE FROM public.expected_packages ep
  USING cfg
  WHERE ep.organization_id = cfg.organization_id
    AND ep.build_source IN ('detail_shipment','detail_remainder')
  RETURNING ep.id, ep.build_source
)
SELECT
  count(*)                                                    AS rows_deleted_total,
  count(*) FILTER (WHERE build_source = 'detail_shipment')    AS rows_deleted_matched,
  count(*) FILTER (WHERE build_source = 'detail_remainder')   AS rows_deleted_remainder
FROM del;

COMMIT;
