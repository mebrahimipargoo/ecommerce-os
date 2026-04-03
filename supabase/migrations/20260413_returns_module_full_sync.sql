-- =============================================================================
-- Returns Module — Full Schema Sync
-- Brings returns, packages, pallets, claim_submissions, and organization_settings
-- up to the schema expected by the application layer.
-- All statements are idempotent (ADD COLUMN IF NOT EXISTS).
-- Protected zones (claim_submissions.created_by, system_settings) are untouched.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- returns table
-- DB currently has: id, order_id, lpn, status, organization_id, store_id,
--   pallet_id, package_id, raw_return_data, created_at, product_id,
--   unit_sale_price, amazon_fees_lost, return_shipping_fee, currency,
--   condition_note, rma_number, marketplace
-- App layer also needs the columns below.
-- ---------------------------------------------------------------------------
ALTER TABLE public.returns
  ADD COLUMN IF NOT EXISTS item_name           text,
  ADD COLUMN IF NOT EXISTS conditions          text[]      DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS notes               text,
  ADD COLUMN IF NOT EXISTS photo_evidence      jsonb,
  ADD COLUMN IF NOT EXISTS expiration_date     text,
  ADD COLUMN IF NOT EXISTS batch_number        text,
  ADD COLUMN IF NOT EXISTS asin                text,
  ADD COLUMN IF NOT EXISTS fnsku               text,
  ADD COLUMN IF NOT EXISTS sku                 text,
  ADD COLUMN IF NOT EXISTS product_identifier  text,
  ADD COLUMN IF NOT EXISTS created_by          uuid,
  ADD COLUMN IF NOT EXISTS updated_by          uuid,
  ADD COLUMN IF NOT EXISTS updated_at          timestamptz DEFAULT now(),
  ADD COLUMN IF NOT EXISTS estimated_value     numeric,
  ADD COLUMN IF NOT EXISTS deleted_at          timestamptz;

COMMENT ON COLUMN public.returns.item_name IS 'Human-readable product name entered by the operator.';
COMMENT ON COLUMN public.returns.conditions IS 'Array of defect/condition labels (e.g. damaged, expired, wrong_item).';
COMMENT ON COLUMN public.returns.notes IS 'Free-text operator notes for this return item.';
COMMENT ON COLUMN public.returns.photo_evidence IS 'Structured gallery JSONB — urls, label_urls, outer_box_urls, inside_content_urls.';
COMMENT ON COLUMN public.returns.deleted_at IS 'Soft-delete timestamp — NULL means active.';

-- Unique partial index for lpn within an org (mirrors 20250319_returns_v4_constraints).
CREATE UNIQUE INDEX IF NOT EXISTS idx_returns_lpn
  ON public.returns (organization_id, lpn)
  WHERE lpn IS NOT NULL AND deleted_at IS NULL;

-- Lookup index for claim pipeline queries.
CREATE INDEX IF NOT EXISTS idx_returns_org_status
  ON public.returns (organization_id, status)
  WHERE deleted_at IS NULL;

-- ---------------------------------------------------------------------------
-- packages table
-- DB currently has: id, pallet_id, tracking_number, status, organization_id,
--   store_id, package_number, expected_item_count, actual_item_count,
--   created_at, carrier_name, rma_number
-- ---------------------------------------------------------------------------
ALTER TABLE public.packages
  ADD COLUMN IF NOT EXISTS manifest_url            text,
  ADD COLUMN IF NOT EXISTS discrepancy_note        text,
  ADD COLUMN IF NOT EXISTS order_id                text,
  ADD COLUMN IF NOT EXISTS created_by              uuid,
  ADD COLUMN IF NOT EXISTS updated_by              uuid,
  ADD COLUMN IF NOT EXISTS updated_at              timestamptz DEFAULT now(),
  ADD COLUMN IF NOT EXISTS photo_url               text,
  ADD COLUMN IF NOT EXISTS photo_return_label_url  text,
  ADD COLUMN IF NOT EXISTS photo_opened_url        text,
  ADD COLUMN IF NOT EXISTS photo_closed_url        text,
  ADD COLUMN IF NOT EXISTS manifest_photo_url      text,
  ADD COLUMN IF NOT EXISTS deleted_at              timestamptz,
  ADD COLUMN IF NOT EXISTS photo_evidence          jsonb,
  ADD COLUMN IF NOT EXISTS manifest_data           jsonb;

COMMENT ON COLUMN public.packages.photo_evidence IS 'Structured gallery JSONB — label_urls, outer_box_urls, inside_content_urls, sealed_box_urls.';
COMMENT ON COLUMN public.packages.manifest_data IS 'Parsed packing-slip lines [{sku, expected_qty, description}].';
COMMENT ON COLUMN public.packages.deleted_at IS 'Soft-delete timestamp — NULL means active.';

-- Unique index for tracking_number per org.
CREATE UNIQUE INDEX IF NOT EXISTS idx_packages_tracking
  ON public.packages (organization_id, tracking_number)
  WHERE tracking_number IS NOT NULL AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_packages_org_status
  ON public.packages (organization_id, status)
  WHERE deleted_at IS NULL;

-- ---------------------------------------------------------------------------
-- pallets table
-- DB currently has: id, pallet_number, status, organization_id, store_id,
--   item_count, created_at, tracking_number, notes
-- ---------------------------------------------------------------------------
ALTER TABLE public.pallets
  ADD COLUMN IF NOT EXISTS created_by         uuid,
  ADD COLUMN IF NOT EXISTS updated_by         uuid,
  ADD COLUMN IF NOT EXISTS updated_at         timestamptz DEFAULT now(),
  ADD COLUMN IF NOT EXISTS photo_url          text,
  ADD COLUMN IF NOT EXISTS bol_photo_url      text,
  ADD COLUMN IF NOT EXISTS manifest_photo_url text,
  ADD COLUMN IF NOT EXISTS deleted_at         timestamptz;

COMMENT ON COLUMN public.pallets.bol_photo_url IS 'Bill of lading photo URL (media bucket).';
COMMENT ON COLUMN public.pallets.deleted_at IS 'Soft-delete timestamp — NULL means active.';

CREATE INDEX IF NOT EXISTS idx_pallets_org_status
  ON public.pallets (organization_id, status)
  WHERE deleted_at IS NULL;

-- ---------------------------------------------------------------------------
-- claim_submissions table
-- Protected: created_by column and status values (including 'failed') — NOT TOUCHED.
-- DB currently has: id, organization_id, store_id, return_id, status,
--   submission_id, claim_amount, currency, reimbursement_amount,
--   source_payload, created_by, created_at
-- ---------------------------------------------------------------------------
ALTER TABLE public.claim_submissions
  ADD COLUMN IF NOT EXISTS updated_at  timestamptz DEFAULT now(),
  ADD COLUMN IF NOT EXISTS report_url  text;

COMMENT ON COLUMN public.claim_submissions.report_url IS 'URL to the filed claim report / confirmation PDF (set by Python agent).';

-- ---------------------------------------------------------------------------
-- organization_settings table
-- DB currently has: id, organization_id, is_ai_label_ocr_enabled,
--   default_claim_evidence, logo_url, credentials, updated_at
-- ---------------------------------------------------------------------------
ALTER TABLE public.organization_settings
  ADD COLUMN IF NOT EXISTS is_ai_packing_slip_ocr_enabled  boolean   DEFAULT false,
  ADD COLUMN IF NOT EXISTS company_display_name             text,
  ADD COLUMN IF NOT EXISTS default_store_id                 uuid,
  ADD COLUMN IF NOT EXISTS is_debug_mode_enabled            boolean   DEFAULT false;

COMMENT ON COLUMN public.organization_settings.is_ai_packing_slip_ocr_enabled IS 'Enables AI OCR on packing slip uploads.';
COMMENT ON COLUMN public.organization_settings.company_display_name IS 'Human-readable tenant name shown in the admin workspace picker.';
COMMENT ON COLUMN public.organization_settings.default_store_id IS 'FK to stores — pre-selected store for new returns/packages in this org.';
COMMENT ON COLUMN public.organization_settings.is_debug_mode_enabled IS 'Enables verbose debug logging/UI for this tenant.';
