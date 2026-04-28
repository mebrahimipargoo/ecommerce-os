-- 20260636_product_identity_idempotency.sql
--
-- Idempotency hardening for Product Identity CSV imports.
--
-- Problem
-- ───────
-- The same Product Identity CSV could be uploaded twice for the same
-- (organization_id, store_id) and yield two separate `raw_report_uploads`
-- rows, both reaching `synced` and both writing into `product_identifier_map`
-- / `catalog_products`. The application-side prefetch dedup was not enough
-- because the second upload had a different `source_upload_id`, so existing
-- bridge rows were updated by the first import but the second import added
-- duplicate bridge rows when the prefetch range did not surface them.
--
-- This migration adds two database-level invariants and a new lifecycle
-- value:
--   1. `raw_report_uploads.status = 'superseded'` is now a legal state.
--      Used by the new replace-flow when a user re-imports the same file.
--   2. UNIQUE partial index on `product_identifier_map` for product-identity
--      rows: `(organization_id, store_id, external_listing_id)` where
--      `external_listing_id LIKE 'product_identity:%'`. This is the deterministic
--      identity tuple produced by `lib/product-identity-import.ts` and is
--      disjoint from listing-derived `external_listing_id` values (which use
--      Amazon listing-id strings).
--
-- The migration includes a guarded pre-dedupe step that ONLY removes
-- duplicate product_identity rows (keeping the most recently updated row per
-- key). It never touches listing rows or any non product-identity bridge
-- rows. Without this step, `CREATE UNIQUE INDEX` would fail on databases
-- that already have duplicates from the bug.

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────
-- 1. Extend raw_report_uploads.status CHECK to allow 'superseded'.
--    Drop and re-create the constraint with the full known set so we don't
--    lose any value that prior migrations already added.
-- ──────────────────────────────────────────────────────────────────────────
ALTER TABLE public.raw_report_uploads
  DROP CONSTRAINT IF EXISTS raw_report_uploads_status_check;

ALTER TABLE public.raw_report_uploads
  ADD CONSTRAINT raw_report_uploads_status_check CHECK (
    status = ANY (ARRAY[
      'pending'::text,
      'uploading'::text,
      'processing'::text,
      'synced'::text,
      'complete'::text,
      'failed'::text,
      'cancelled'::text,
      'needs_mapping'::text,
      'ready'::text,
      'uploaded'::text,
      'mapped'::text,
      'staged'::text,
      'superseded'::text
    ])
  );

COMMENT ON COLUMN public.raw_report_uploads.status IS
  'Upload lifecycle. Adds superseded: a newer upload of the same file '
  '(same organization_id + store_id + report_type + content_sha256) replaced '
  'this one. Superseded rows keep their metadata for audit but their '
  'product_identifier_map / catalog_products rows tagged with the row id are '
  'removed by the supersede flow.';

-- ──────────────────────────────────────────────────────────────────────────
-- 2. Pre-dedupe product_identifier_map for product-identity rows ONLY.
--    Keep the row with the most recent updated_at / created_at per
--    (organization_id, store_id, external_listing_id). All other rows
--    (listing, ledger) are filtered out by the WHERE clause.
-- ──────────────────────────────────────────────────────────────────────────
WITH ranked AS (
  SELECT
    pim.id,
    ROW_NUMBER() OVER (
      PARTITION BY pim.organization_id, pim.store_id, pim.external_listing_id
      ORDER BY
        COALESCE(pim.updated_at, pim.created_at, pim.last_seen_at, 'epoch'::timestamptz) DESC,
        pim.id DESC
    ) AS rn
  FROM public.product_identifier_map pim
  WHERE pim.external_listing_id IS NOT NULL
    AND pim.external_listing_id LIKE 'product_identity:%'
)
DELETE FROM public.product_identifier_map pim
USING ranked
WHERE pim.id = ranked.id
  AND ranked.rn > 1;

-- ──────────────────────────────────────────────────────────────────────────
-- 3. Partial UNIQUE INDEX. Covers ONLY product-identity bridge rows.
--    Listing-sourced rows (external_listing_id = listing-id text) and
--    ledger-sourced rows (external_listing_id NULL) are unaffected.
-- ──────────────────────────────────────────────────────────────────────────
CREATE UNIQUE INDEX IF NOT EXISTS uq_product_identifier_map_product_identity
  ON public.product_identifier_map (organization_id, store_id, external_listing_id)
  WHERE external_listing_id IS NOT NULL
    AND external_listing_id LIKE 'product_identity:%';

COMMENT ON INDEX public.uq_product_identifier_map_product_identity IS
  'Idempotency guard for Product Identity CSV imports: at most one bridge '
  'row per (organization_id, store_id, external_listing_id) when the row '
  'came from a Product Identity import (external_listing_id starts with '
  '"product_identity:"). Listing and ledger rows are not affected by this '
  'index because the WHERE clause excludes them.';

-- ──────────────────────────────────────────────────────────────────────────
-- 4. Helpful index for the duplicate-detection lookup performed by the new
--    server action `findActiveProductIdentityImport`. Mirrors the WHERE
--    clause it uses (`organization_id, report_type, status, metadata
--    content_sha256, metadata import_store_id`).
-- ──────────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_raw_report_uploads_product_identity_dup_lookup
  ON public.raw_report_uploads (organization_id, report_type, status)
  WHERE report_type = 'PRODUCT_IDENTITY';

COMMENT ON INDEX public.idx_raw_report_uploads_product_identity_dup_lookup IS
  'Speeds up the per-store duplicate-content lookup for Product Identity '
  'imports issued by the server action that warns / supersedes on re-upload.';

NOTIFY pgrst, 'reload schema';

COMMIT;
