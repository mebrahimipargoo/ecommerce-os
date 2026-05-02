-- 20260639_revert_fba_inventory_product_identity_misclassification.sql
--
-- Corrective report_type migration for the overly broad Product Identity
-- recovery rule that briefly existed in 20260638.
--
-- What it does:
--   * Finds rows currently marked PRODUCT_IDENTITY whose file/header looks like
--     FBA Inventory and whose headers do NOT contain the exact Product Identity
--     signature:
--       UPC, Vendor, Seller SKU, Mfg #, FNSKU, ASIN, Product Name
--   * Reverts report_type to FBA_INVENTORY.
--   * Leaves destination data untouched. If a bad Product Identity process
--     already wrote products/catalog_products/product_identifier_map rows, use
--     the manual cleanup SQL in validate_product_identity_import.sql after
--     reviewing the affected upload ids.
--
-- This migration is intentionally conservative and idempotent.

BEGIN;

WITH header_flags AS (
  SELECT
    ru.id,
    ru.file_name,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'upc') AS has_upc,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'vendor') AS has_vendor,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'seller sku') AS has_seller_sku,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') IN ('mfg #', 'mfg#')) AS has_mfg,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'fnsku') AS has_fnsku,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'asin') AS has_asin,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = 'product name') AS has_product_name,
    BOOL_OR(regexp_replace(lower(btrim(h.value)), '[-_]+', ' ', 'g') = ANY (
      ARRAY[
        'available',
        'inbound quantity',
        'inbound working',
        'inbound received',
        'reserved',
        'total reserved quantity',
        'inventory supply at fba',
        'days of supply',
        'recommended replenishment',
        'sales',
        'snapshot date'
      ]
    )) AS has_fba_inventory_signal
  FROM public.raw_report_uploads ru
  LEFT JOIN LATERAL jsonb_array_elements_text(ru.metadata -> 'csv_headers') AS h(value)
    ON jsonb_typeof(ru.metadata -> 'csv_headers') = 'array'
  WHERE ru.report_type = 'PRODUCT_IDENTITY'
  GROUP BY ru.id, ru.file_name
),
suspicious AS (
  SELECT id
  FROM header_flags
  WHERE
    (
      lower(file_name) LIKE '%fba inventory%'
      OR lower(file_name) LIKE '%inventory%'
      OR has_fba_inventory_signal
    )
    AND NOT (
      has_upc
      AND has_vendor
      AND has_seller_sku
      AND has_mfg
      AND has_fnsku
      AND has_asin
      AND has_product_name
    )
)
UPDATE public.raw_report_uploads ru
SET
  report_type = 'FBA_INVENTORY',
  updated_at = now(),
  metadata = jsonb_set(
    COALESCE(ru.metadata, '{}'::jsonb),
    '{product_identity_correction}',
    to_jsonb(jsonb_build_object(
      'corrected_at', now(),
      'from_report_type', 'PRODUCT_IDENTITY',
      'to_report_type', 'FBA_INVENTORY',
      'reason', 'fba_inventory_signature_without_exact_product_identity_headers'
    )),
    true
  )
FROM suspicious s
WHERE ru.id = s.id;

NOTIFY pgrst, 'reload schema';

COMMIT;
