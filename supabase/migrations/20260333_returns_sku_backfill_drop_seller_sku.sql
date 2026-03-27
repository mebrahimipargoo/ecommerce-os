-- V16.4.18 — Standardize on `sku`; backfill from legacy `seller_sku` then drop seller_sku.
UPDATE public.returns
SET sku = seller_sku
WHERE (sku IS NULL OR trim(sku) = '')
  AND seller_sku IS NOT NULL
  AND trim(seller_sku) <> '';

ALTER TABLE public.returns DROP COLUMN IF EXISTS seller_sku;
DROP INDEX IF EXISTS idx_returns_seller_sku;
