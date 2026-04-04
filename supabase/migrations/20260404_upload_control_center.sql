-- Upload Control Center refactor
-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Add `source_upload_id` to amazon_ledger_staging so delete can cascade-clean.
-- 2. Add `ready` and `uploaded` lifecycle statuses to raw_report_uploads.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── amazon_ledger_staging: trackable cleanup ──────────────────────────────────
ALTER TABLE public.amazon_ledger_staging
  ADD COLUMN IF NOT EXISTS source_upload_id uuid
  REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_amazon_ledger_staging_upload_id
  ON public.amazon_ledger_staging (source_upload_id)
  WHERE source_upload_id IS NOT NULL;

COMMENT ON COLUMN public.amazon_ledger_staging.source_upload_id IS
  'FK to raw_report_uploads.id — populated when rows come from the ledger CSV pipeline.
   Used for targeted cleanup: DELETE FROM amazon_ledger_staging WHERE source_upload_id = $1.';

-- ── raw_report_uploads: extend status CHECK ───────────────────────────────────
-- Drop and recreate to include `ready` (upload done, ready to sync) and
-- `uploaded` (alias / legacy compat) alongside needs_mapping from prior migration.
ALTER TABLE public.raw_report_uploads DROP CONSTRAINT IF EXISTS raw_report_uploads_status_check;

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
      'uploaded'::text
    ])
  );

COMMENT ON COLUMN public.raw_report_uploads.status IS
  'Upload lifecycle:
   uploading     — record created, chunks being received
   ready         — upload + auto-mapping complete, Sync button enabled
   uploaded      — synonym / legacy alias for ready
   needs_mapping — upload complete, user must map columns before syncing
   pending       — legacy pre-refactor synonym for ready
   processing    — sync pipeline running
   synced        — rows written to domain table (expected_returns / expected_packages / products)
   complete      — synonym for synced (older pipeline)
   failed        — pipeline or upload error; row stays for manual cleanup
   cancelled     — user cancelled; row stays for manual cleanup';

NOTIFY pgrst, 'reload schema';
