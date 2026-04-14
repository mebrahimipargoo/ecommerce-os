-- =============================================================================
-- Amazon import engine: line-level dedupe keys + FPS columns + removal lock
-- =============================================================================
-- A) Preserve every distinct CSV line for landing-style tables by switching
--    INVENTORY_LEDGER, REPORTS_REPOSITORY, and REIMBURSEMENTS to
--    (organization_id, source_line_hash) where source_line_hash is a deterministic
--    fingerprint of the full mapped row (application-side, same as transactions).
-- B) Extend file_processing_status for upload bytes, sync %, and honest phases.
-- C) Serialize REMOVAL_SHIPMENT tree rebuild per (organization_id, store_id) so
--    concurrent removal imports do not DELETE/rebuild shipment_containers races.
-- =============================================================================

-- ── 1) amazon_inventory_ledger ─────────────────────────────────────────────
ALTER TABLE public.amazon_inventory_ledger
  ADD COLUMN IF NOT EXISTS source_line_hash text;

UPDATE public.amazon_inventory_ledger
SET source_line_hash = id::text
WHERE source_line_hash IS NULL;

ALTER TABLE public.amazon_inventory_ledger
  ALTER COLUMN source_line_hash SET NOT NULL;

DROP INDEX IF EXISTS public.uq_amazon_inventory_ledger_org_fnsku;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_inventory_ledger_org_line_hash
  ON public.amazon_inventory_ledger (organization_id, source_line_hash);

COMMENT ON COLUMN public.amazon_inventory_ledger.source_line_hash IS
  'Full-row content fingerprint; one DB row per distinct CSV line (replaces 5-col business key that collapsed lines).';

-- ── 2) amazon_reports_repository ─────────────────────────────────────────────
ALTER TABLE public.amazon_reports_repository
  ADD COLUMN IF NOT EXISTS source_line_hash text;

UPDATE public.amazon_reports_repository
SET source_line_hash = id::text
WHERE source_line_hash IS NULL;

ALTER TABLE public.amazon_reports_repository
  ALTER COLUMN source_line_hash SET NOT NULL;

DROP INDEX IF EXISTS public.uq_amazon_reports_repo_natural;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_reports_repo_org_line_hash
  ON public.amazon_reports_repository (organization_id, source_line_hash);

COMMENT ON COLUMN public.amazon_reports_repository.source_line_hash IS
  'Full-row fingerprint; replaces natural-key unique that merged distinct lines sharing date/type/order/sku/description.';

-- ── 3) amazon_reimbursements ─────────────────────────────────────────────────
ALTER TABLE public.amazon_reimbursements
  ADD COLUMN IF NOT EXISTS source_line_hash text;

UPDATE public.amazon_reimbursements
SET source_line_hash = id::text
WHERE source_line_hash IS NULL;

ALTER TABLE public.amazon_reimbursements
  ALTER COLUMN source_line_hash SET NOT NULL;

DROP INDEX IF EXISTS public.uq_amazon_reimbursements_org_reimb_sku;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_reimbursements_org_line_hash
  ON public.amazon_reimbursements (organization_id, source_line_hash);

COMMENT ON COLUMN public.amazon_reimbursements.source_line_hash IS
  'Full-row fingerprint; (reimbursement_id, sku) merged distinct reimbursement lines.';

-- ── 4) file_processing_status — bytes, sync %, phase ────────────────────────
ALTER TABLE public.file_processing_status
  ADD COLUMN IF NOT EXISTS file_size_bytes bigint,
  ADD COLUMN IF NOT EXISTS uploaded_bytes bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS sync_pct smallint NOT NULL DEFAULT 0 CHECK (sync_pct BETWEEN 0 AND 100),
  ADD COLUMN IF NOT EXISTS current_phase text NOT NULL DEFAULT 'pending';

-- Widen status to include syncing (ETL phase 3)
ALTER TABLE public.file_processing_status DROP CONSTRAINT IF EXISTS file_processing_status_status_check;
ALTER TABLE public.file_processing_status
  ADD CONSTRAINT file_processing_status_status_check CHECK (
    status IN ('pending','uploading','processing','syncing','complete','failed')
  );

COMMENT ON COLUMN public.file_processing_status.sync_pct IS
  'Phase 3: rows flushed / total staging rows (0–100).';
COMMENT ON COLUMN public.file_processing_status.current_phase IS
  'upload | process | sync | complete | failed — mirrors operator-facing ETL phase.';

-- ── 5) Per–target-store lock for REMOVAL_SHIPMENT pipeline rebuild ────────────
CREATE TABLE IF NOT EXISTS public.import_pipeline_locks (
  organization_id uuid NOT NULL,
  store_id        uuid NOT NULL,
  upload_id       uuid NOT NULL,
  locked_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id, store_id)
);

COMMENT ON TABLE public.import_pipeline_locks IS
  'Short-lived mutex: one active REMOVAL_SHIPMENT sync per (organization_id, store_id) to avoid shipment tree rebuild races.';

ALTER TABLE public.import_pipeline_locks ENABLE ROW LEVEL SECURITY;

NOTIFY pgrst, 'reload schema';
