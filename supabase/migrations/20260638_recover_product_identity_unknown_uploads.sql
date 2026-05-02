-- 20260638_recover_product_identity_unknown_uploads.sql
--
-- Symptom this fixes
-- ──────────────────
-- Recent Product Identity CSV uploads (headers: UPC, Vendor, Seller SKU,
-- Mfg #, FNSKU, ASIN, Product Name) ended up with:
--   * raw_report_uploads.report_type = 'UNKNOWN'
--   * raw_report_uploads.status      = 'mapped' / 'needs_mapping'
--
-- Root cause
-- ──────────
-- `updateUploadSessionClassification(...)` resolved the organization from the
-- actor's profile, not from the upload row. Super-admin imports targeting a
-- tenant store (whose owner org differs from the actor's home org) caused
-- the UPDATE WHERE clause to silently miss the row. Result: report_type
-- stayed at 'UNKNOWN' even after the rule-based + AI classifier returned
-- 'PRODUCT_IDENTITY'.
--
-- This migration recovers any such row that:
--   * report_type = 'UNKNOWN'
--   * status      ∈ {mapped, needs_mapping, ready, uploaded, pending, failed}
--   * metadata.csv_headers contains the exact Product Identity header
--     signature:
--       UPC, Vendor, Seller SKU, Mfg #, FNSKU, ASIN, Product Name
--
-- Strictness is intentional. FBA Inventory and listing exports can contain
-- seller SKU / FNSKU / ASIN / product-name-like fields, so recovery must
-- require UPC + Vendor + Mfg # as well.
--
-- The migration is idempotent and SELECT-safe — re-running on a healthy DB
-- is a no-op. CHECK constraint already accepts 'PRODUCT_IDENTITY' (added
-- by 20260637_raw_report_uploads_check_product_identity.sql).

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────
-- Recovery: exact csv_headers fingerprint only.
-- ──────────────────────────────────────────────────────────────────────────
WITH header_signals AS (
  SELECT
    ru.id,
    -- tolerant lowercase/whitespace/hyphen normalization
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'upc') AS has_upc,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'vendor') AS has_vendor,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') IN ('seller sku')) AS has_seller_sku,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') IN ('mfg #', 'mfg#')) AS has_mfg,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'fnsku') AS has_fnsku,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'asin') AS has_asin,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'product name') AS has_product_name
  FROM public.raw_report_uploads ru
  CROSS JOIN LATERAL jsonb_array_elements_text(ru.metadata -> 'csv_headers') AS h(value)
  WHERE ru.report_type = 'UNKNOWN'
    AND ru.status IN ('mapped', 'ready', 'uploaded', 'pending', 'needs_mapping', 'failed')
    AND jsonb_typeof(ru.metadata -> 'csv_headers') = 'array'
  GROUP BY ru.id
)
UPDATE public.raw_report_uploads ru
SET
  report_type = 'PRODUCT_IDENTITY',
  status      = CASE
                  WHEN ru.status IN ('mapped', 'ready', 'uploaded') THEN ru.status
                  ELSE 'mapped'
                END,
  updated_at  = now(),
  metadata    = jsonb_set(
                   COALESCE(ru.metadata, '{}'::jsonb),
                   '{product_identity_recovery}',
                   to_jsonb(jsonb_build_object(
                     'recovered_at', now(),
                     'reason', 'csv_headers_exact_product_identity_signature',
                     'previous_report_type', 'UNKNOWN',
                     'previous_status', ru.status
                   )),
                   true
                )
FROM header_signals hs
WHERE ru.id = hs.id
  AND hs.has_upc
  AND hs.has_vendor
  AND hs.has_seller_sku
  AND hs.has_mfg
  AND hs.has_fnsku
  AND hs.has_asin
  AND hs.has_product_name;

NOTIFY pgrst, 'reload schema';

COMMIT;
