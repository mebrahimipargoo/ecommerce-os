-- Claim-base view for removal-related claims.
--
-- SOURCE OF TRUTH: amazon_removals (one row per removal detail row).
--
-- Joined sources (all LEFT JOIN, all pre-aggregated to avoid duplication):
--   1) expected_packages       via expected_packages.source_detail_row_id = amazon_removals.id
--                              (1629 detail rows fan out to 6002 expected rows -> aggregate)
--   2) amazon_reimbursements   via (organization_id, order_id, sku)
--                              (current data: 0 matches with removal order_ids - reimbursements
--                               in this dataset are customer-order driven, not removal driven.
--                               Join is left in place so it lights up when removal-keyed
--                               reimbursements arrive; nullable today, NOT guessed.)
--   3) amazon_transactions     via (organization_id, order_id)
--                              (current data: 0 matches with removal order_ids - transactions
--                               here are customer-order based. Same nullable-today policy.
--                               Schema does not separate FBA fee vs commission, so those
--                               two fields are explicitly NULL until a separated source exists.)
--
-- amazon_returns is NOT joined: it is customer-return tracking and current data shows
-- no relevant linkage to removal detail rows. Per the brief, it is added "only if needed".
--
-- claim_reason_candidate priority (first hit wins):
--   1. disposed_without_reimbursement   - any disposed_quantity > 0 with no reimbursement money seen
--   2. shipped_not_fully_scanned        - status='Completed', shipped > 0, scans < shipped
--   3. other_removal_discrepancy        - status='Completed' but qty math (req vs ship+disp+canc+inproc)
--                                         doesn't reconcile

BEGIN;

DROP VIEW IF EXISTS public.v_claim_base_amazon_removals;

CREATE VIEW public.v_claim_base_amazon_removals AS
WITH ep_agg AS (
    SELECT
        ep.source_detail_row_id                            AS detail_id,
        TRUE                                               AS has_expected_rows,
        SUM(COALESCE(ep.expected_scan_quantity, 0))::int   AS expected_scan_quantity_total,
        SUM(COALESCE(ep.actual_scanned_count, 0))::int     AS scanned_quantity_total,
        COUNT(*)::int                                      AS expected_row_count
    FROM public.expected_packages ep
    WHERE ep.source_detail_row_id IS NOT NULL
    GROUP BY ep.source_detail_row_id
),
rb_agg AS (
    SELECT
        rb.organization_id,
        rb.order_id,
        rb.sku,
        SUM(COALESCE(rb.amount_reimbursed, 0))::numeric                                          AS reimbursement_amount_total,
        COUNT(*)::int                                                                            AS reimbursement_row_count,
        COUNT(*) FILTER (WHERE rb.raw_data->>'reason' = 'Reimbursement_Reversal')::int          AS reversal_row_count,
        SUM(COALESCE(rb.amount_reimbursed, 0))
            FILTER (WHERE rb.raw_data->>'reason' = 'Reimbursement_Reversal')::numeric           AS reversal_amount_total
    FROM public.amazon_reimbursements rb
    WHERE rb.order_id IS NOT NULL
      AND rb.sku      IS NOT NULL
    GROUP BY rb.organization_id, rb.order_id, rb.sku
),
tx_agg AS (
    SELECT
        tx.organization_id,
        tx.order_id,
        SUM(NULLIF(tx.raw_data->>'Total product charges','')::numeric) AS principal_amount_total,
        NULL::numeric                                                  AS fba_fee_total,
        NULL::numeric                                                  AS commission_fee_total
    FROM public.amazon_transactions tx
    WHERE tx.order_id IS NOT NULL
    GROUP BY tx.organization_id, tx.order_id
)
SELECT
    ar.organization_id,
    ar.store_id,
    ar.id                                                  AS source_detail_row_id,
    ar.order_id,
    ar.order_type,
    ar.order_date,
    ar.sku,
    ar.fnsku,
    ar.disposition,
    ar.requested_quantity,
    ar.shipped_quantity,
    ar.disposed_quantity,
    ar.cancelled_quantity,
    ar.in_process_quantity,
    ar.removal_fee,
    ar.currency,
    ar.status                                              AS removal_status,

    COALESCE(ep.has_expected_rows, FALSE)                  AS has_expected_rows,
    COALESCE(ep.expected_scan_quantity_total, 0)           AS expected_scan_quantity_total,
    ep.scanned_quantity_total,

    rb.reimbursement_amount_total,
    CASE
        WHEN rb.reimbursement_row_count IS NULL OR rb.reimbursement_row_count = 0
            THEN 'none'
        WHEN COALESCE(rb.reversal_row_count,0) > 0
             AND COALESCE(rb.reimbursement_amount_total,0) <= 0
            THEN 'reversed'
        WHEN COALESCE(rb.reimbursement_amount_total,0) > 0
            THEN 'reimbursed'
        ELSE 'partial'
    END                                                    AS reimbursement_status,

    tx.principal_amount_total,
    tx.fba_fee_total,
    tx.commission_fee_total,

    CASE
        WHEN COALESCE(ar.disposed_quantity,0) > 0
             AND COALESCE(rb.reimbursement_amount_total,0) <= 0
            THEN 'disposed_without_reimbursement'

        WHEN ar.status = 'Completed'
             AND COALESCE(ar.shipped_quantity,0) > 0
             AND COALESCE(ep.scanned_quantity_total,0) < COALESCE(ar.shipped_quantity,0)
            THEN 'shipped_not_fully_scanned'

        WHEN ar.status = 'Completed'
             AND COALESCE(ar.requested_quantity,0)
                 <> COALESCE(ar.shipped_quantity,0)
                  + COALESCE(ar.disposed_quantity,0)
                  + COALESCE(ar.cancelled_quantity,0)
                  + COALESCE(ar.in_process_quantity,0)
            THEN 'other_removal_discrepancy'

        ELSE NULL
    END                                                    AS claim_reason_candidate

FROM public.amazon_removals ar
LEFT JOIN ep_agg ep
       ON ep.detail_id = ar.id
LEFT JOIN rb_agg rb
       ON rb.organization_id = ar.organization_id
      AND rb.order_id        = ar.order_id
      AND rb.sku             = ar.sku
LEFT JOIN tx_agg tx
       ON tx.organization_id = ar.organization_id
      AND tx.order_id        = ar.order_id;

COMMENT ON VIEW public.v_claim_base_amazon_removals IS
  'One row per amazon_removals detail row. Pre-aggregates expected_packages by source_detail_row_id, '
  'amazon_reimbursements by (org,order_id,sku), and amazon_transactions by (org,order_id) to prevent '
  'fan-out duplication. Identifier candidate views and shipment prediction tables are intentionally '
  'NOT used. Fields with no reliable mapping in the current schema (fba_fee_total, commission_fee_total) '
  'are NULL by design.';

COMMIT;
