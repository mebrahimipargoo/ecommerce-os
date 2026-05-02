-- =============================================================================
-- Manual cleanup / supersede SQL for legacy broken Amazon import rows.
--
-- READ BEFORE RUNNING:
--   • This file does NOT execute any destructive operation.
--   • Every DELETE / UPDATE statement is wrapped in a `\if false … \endif`
--     guard. Replace `false` with `true` (and review the WHERE clause) only
--     after running the SELECT diagnostics above and confirming the row set.
--   • Run the pre-cleanup SELECTs first, then commit a transaction with the
--     enabled DELETEs explicitly. Never auto-run on production.
--
-- Targets:
--   • amazon_settlements rows where order_id, sku, and amount_total are all
--     null/zero (the symptom described by the user — placeholder rows that
--     never carried real data).
--   • amazon_transactions rows where order_id is null AND amount is null AND
--     transaction_type is empty (residual from an earlier mis-classification).
--
-- Supersede strategy:
--   1) Identify candidate rows.
--   2) Confirm a replacement upload exists for the same (organization_id,
--      report period). If yes → mark legacy rows with metadata note and
--      delete. If no → leave legacy rows in place and request a re-import.
-- =============================================================================

-- ── 0) Pre-cleanup selects (read-only) ─────────────────────────────────────

-- Settlement rows that look completely empty (legacy broken).
SELECT
  s.organization_id,
  s.upload_id,
  rru.report_type,
  rru.created_at,
  COUNT(*) AS broken_rows
FROM public.amazon_settlements s
LEFT JOIN public.raw_report_uploads rru ON rru.id = s.upload_id
WHERE s.organization_id = :'org'::uuid
  AND s.order_id IS NULL
  AND s.sku IS NULL
  AND COALESCE(s.amount_total, 0) = 0
GROUP BY s.organization_id, s.upload_id, rru.report_type, rru.created_at
ORDER BY rru.created_at DESC;

-- Transaction rows that look completely empty (no order, no amount).
SELECT
  t.organization_id,
  t.upload_id,
  rru.report_type,
  rru.created_at,
  COUNT(*) AS broken_rows
FROM public.amazon_transactions t
LEFT JOIN public.raw_report_uploads rru ON rru.id = t.upload_id
WHERE t.organization_id = :'org'::uuid
  AND t.order_id IS NULL
  AND COALESCE(t.amount, 0) = 0
  AND COALESCE(t.transaction_type, '') = ''
GROUP BY t.organization_id, t.upload_id, rru.report_type, rru.created_at
ORDER BY rru.created_at DESC;

-- ── 1) Identify candidate uploads to supersede ──────────────────────────────
-- Show the most recent successful settlement / transaction upload per org so
-- you can decide whether the legacy rows above are safely superseded.
WITH ranked AS (
  SELECT
    rru.id,
    rru.organization_id,
    rru.report_type,
    rru.status,
    rru.created_at,
    ROW_NUMBER() OVER (
      PARTITION BY rru.organization_id, rru.report_type
      ORDER BY rru.created_at DESC
    ) AS rn
  FROM public.raw_report_uploads rru
  WHERE rru.organization_id = :'org'::uuid
    AND rru.report_type IN ('SETTLEMENT', 'TRANSACTIONS')
    AND rru.status IN ('synced', 'complete')
)
SELECT id AS latest_synced_upload_id, organization_id, report_type, status, created_at
FROM ranked
WHERE rn = 1;

-- ── 2) GUARDED DELETEs ─────────────────────────────────────────────────────-
-- Replace `false` with `true` and review WHERE before running.

-- 2a) Delete legacy broken amazon_settlements rows.
\if false
BEGIN;

WITH targets AS (
  SELECT id FROM public.amazon_settlements
  WHERE organization_id = :'org'::uuid
    AND order_id IS NULL
    AND sku IS NULL
    AND COALESCE(amount_total, 0) = 0
)
DELETE FROM public.amazon_settlements
WHERE id IN (SELECT id FROM targets)
RETURNING id, upload_id;

COMMIT;
\endif

-- 2b) Delete legacy broken amazon_transactions rows.
\if false
BEGIN;

WITH targets AS (
  SELECT id FROM public.amazon_transactions
  WHERE organization_id = :'org'::uuid
    AND order_id IS NULL
    AND COALESCE(amount, 0) = 0
    AND COALESCE(transaction_type, '') = ''
)
DELETE FROM public.amazon_transactions
WHERE id IN (SELECT id FROM targets)
RETURNING id, upload_id;

COMMIT;
\endif

-- 2c) Mark superseded uploads in raw_report_uploads metadata so the History
--     view reflects the cleanup decision (does NOT delete rows).
\if false
UPDATE public.raw_report_uploads
SET
  status     = 'superseded',
  metadata   = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
    'superseded_at', now(),
    'superseded_reason', 'Legacy broken rows replaced by 20260642 typed import.'
  ),
  updated_at = now()
WHERE organization_id = :'org'::uuid
  AND id IN (
    -- Replace this list with the upload_ids surfaced by the section 0 SELECT.
    -- '00000000-0000-0000-0000-000000000000'::uuid,
  );
\endif

-- ── 3) Sanity recount after cleanup ────────────────────────────────────────-
-- Re-run after applying any DELETE above to confirm broken rows are gone.
SELECT
  'amazon_settlements broken rows after cleanup' AS chk,
  COUNT(*) AS broken_rows
FROM public.amazon_settlements
WHERE organization_id = :'org'::uuid
  AND order_id IS NULL
  AND sku IS NULL
  AND COALESCE(amount_total, 0) = 0;

SELECT
  'amazon_transactions broken rows after cleanup' AS chk,
  COUNT(*) AS broken_rows
FROM public.amazon_transactions
WHERE organization_id = :'org'::uuid
  AND order_id IS NULL
  AND COALESCE(amount, 0) = 0
  AND COALESCE(transaction_type, '') = '';
