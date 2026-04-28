-- 20260637_raw_report_uploads_check_product_identity.sql
--
-- Symptom this fixes
-- ──────────────────
-- Importer UI showed "AI Detected File Type: PRODUCT_IDENTITY" and reached
-- the Map & Classify step, but the Import History row's TYPE column read
-- "Unknown / other" and clicking Process returned:
--   "Could not determine import kind (FBA returns, removal order, inventory
--    ledger, or listing export). Set the Type in History or re-upload so
--    headers can be classified."
--
-- Root cause
-- ──────────
-- `raw_report_uploads_report_type_check` (last rebuilt in 20260604) does not
-- list 'PRODUCT_IDENTITY' as an allowed value. The classifier called
-- `UPDATE raw_report_uploads SET report_type = 'PRODUCT_IDENTITY' …` which
-- Postgres rejected with SQLSTATE 23514 (check_violation). The row therefore
-- kept its initial AUTO value of 'UNKNOWN', the importer moved to status
-- 'mapped' anyway, and the Process route's `resolveAmazonImportSyncKind`
-- returned 'UNKNOWN' for the row.
--
-- This migration rebuilds the CHECK so PRODUCT_IDENTITY (and a couple of
-- additional canonical types declared in lib/raw-report-types.ts but never
-- added to the DB CHECK) are accepted, and recovers any rows already stuck
-- at UNKNOWN whose metadata records the AI-detected type.

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────
-- 1. Recovery for rows already inserted with report_type='UNKNOWN' because
--    the prior CHECK rejected the classifier's UPDATE. We promote them ONLY
--    when their metadata or column_mapping carries clear evidence of the
--    detected type (so we never invent a type out of thin air).
--
--    Targets the specific symptom from the screenshot: a Product Identity
--    upload where:
--      * report_type = 'UNKNOWN'
--      * metadata stores `csv_headers` that include UPC/Vendor/Mfg #/SKU/
--        Product Name (these are the canonical Product Identity columns), OR
--      * `column_mapping` already has a `seller_sku` mapping written by the
--        Product Identity rule-based matcher.
-- ──────────────────────────────────────────────────────────────────────────
UPDATE public.raw_report_uploads ru
SET
  report_type = 'PRODUCT_IDENTITY',
  updated_at = now()
WHERE ru.report_type = 'UNKNOWN'
  AND ru.status IN ('mapped', 'ready', 'uploaded', 'pending', 'needs_mapping', 'failed')
  AND (
    -- Heuristic A: column_mapping already says it's a product-identity layout.
    (
      ru.column_mapping IS NOT NULL
      AND ru.column_mapping ? 'seller_sku'
      AND (ru.column_mapping ? 'upc' OR ru.column_mapping ? 'mfg_part_number' OR ru.column_mapping ? 'vendor')
    )
    OR
    -- Heuristic B: csv_headers in metadata look like a Product Identity export.
    (
      ru.metadata ? 'csv_headers'
      AND EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(ru.metadata -> 'csv_headers') AS h(value)
        WHERE lower(btrim(h.value)) IN ('upc', 'vendor', 'mfg #', 'mfg#', 'mfg no', 'manufacturer part number', 'fnsku')
        HAVING COUNT(*) >= 2
      )
    )
  );

-- ──────────────────────────────────────────────────────────────────────────
-- 2. Drop and re-create the CHECK constraint with the full canonical set
--    from lib/raw-report-types.ts. Adding entries is safe; existing rows
--    cannot violate the new constraint because every value present in the
--    table is already part of the new ARRAY (we just promoted a few above).
-- ──────────────────────────────────────────────────────────────────────────
ALTER TABLE public.raw_report_uploads
  DROP CONSTRAINT IF EXISTS raw_report_uploads_report_type_check;

ALTER TABLE public.raw_report_uploads
  ADD CONSTRAINT raw_report_uploads_report_type_check CHECK (
    report_type = ANY (ARRAY[
      -- Canonical smart-import types written by header classification.
      'FBA_RETURNS'::text,
      'REMOVAL_ORDER'::text,
      'REMOVAL_SHIPMENT'::text,
      'INVENTORY_LEDGER'::text,
      'REIMBURSEMENTS'::text,
      'SETTLEMENT'::text,
      'SAFET_CLAIMS'::text,
      'TRANSACTIONS'::text,
      'REPORTS_REPOSITORY'::text,
      'PRODUCT_IDENTITY'::text,            -- new: enables Product Identity CSV
      -- Additional canonical types known to the application.
      'ALL_ORDERS'::text,
      'REPLACEMENTS'::text,
      'FBA_GRADE_AND_RESELL'::text,
      'MANAGE_FBA_INVENTORY'::text,
      'FBA_INVENTORY'::text,
      'INBOUND_PERFORMANCE'::text,         -- new
      'AMAZON_FULFILLED_INVENTORY'::text,  -- new
      'RESERVED_INVENTORY'::text,
      'FEE_PREVIEW'::text,
      'MONTHLY_STORAGE_FEES'::text,
      'UNKNOWN'::text,
      'CATEGORY_LISTINGS'::text,
      'ALL_LISTINGS'::text,
      'ACTIVE_LISTINGS'::text,
      -- Legacy slugs kept for backward compatibility with old rows.
      'fba_customer_returns'::text,
      'reimbursements'::text,
      'inventory_ledger'::text,
      'safe_t_claims'::text,
      'transaction_view'::text,
      'settlement_repository'::text
    ])
  );

COMMENT ON CONSTRAINT raw_report_uploads_report_type_check ON public.raw_report_uploads IS
  'Allowed values for report_type. Mirrors lib/raw-report-types.ts. Adding a new '
  'canonical type requires updating this CHECK and the TS union together — otherwise '
  'the classifier UPDATE silently fails with check_violation, the row stays at '
  'UNKNOWN, and the Process pipeline rejects it as "Could not determine import kind".';

NOTIFY pgrst, 'reload schema';

COMMIT;
