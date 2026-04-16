-- Separated ETL phases: raw_synced lifecycle + granular file_processing_status columns.

-- ── raw_report_uploads: intermediate state after Phase 3 when Phase 4 is required ──
ALTER TABLE public.raw_report_uploads DROP CONSTRAINT IF EXISTS raw_report_uploads_status_check;

ALTER TABLE public.raw_report_uploads
  ADD CONSTRAINT raw_report_uploads_status_check CHECK (
    status = ANY (ARRAY[
      'pending'::text,
      'uploading'::text,
      'processing'::text,
      'synced'::text,
      'raw_synced'::text,
      'complete'::text,
      'failed'::text,
      'cancelled'::text,
      'needs_mapping'::text,
      'ready'::text,
      'uploaded'::text,
      'mapped'::text,
      'staged'::text
    ])
  );

COMMENT ON COLUMN public.raw_report_uploads.status IS
  'Upload lifecycle. raw_synced = Phase 3 (raw/domain landing) done; Phase 4 generic action pending where applicable.';

-- ── file_processing_status: per-phase %, counters, labels, timestamps ───────
ALTER TABLE public.file_processing_status
  ADD COLUMN IF NOT EXISTS phase1_upload_pct smallint NOT NULL DEFAULT 0    CHECK (phase1_upload_pct BETWEEN 0 AND 100),
  ADD COLUMN IF NOT EXISTS phase2_stage_pct smallint NOT NULL DEFAULT 0
    CHECK (phase2_stage_pct BETWEEN 0 AND 100),
  ADD COLUMN IF NOT EXISTS phase3_raw_sync_pct smallint NOT NULL DEFAULT 0
    CHECK (phase3_raw_sync_pct BETWEEN 0 AND 100),
  ADD COLUMN IF NOT EXISTS phase4_generic_pct smallint NOT NULL DEFAULT 0
    CHECK (phase4_generic_pct BETWEEN 0 AND 100),
  ADD COLUMN IF NOT EXISTS staged_rows_written integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS raw_rows_written integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS raw_rows_skipped_existing integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS generic_rows_written integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS file_rows_total integer,
  ADD COLUMN IF NOT EXISTS data_rows_total integer,
  ADD COLUMN IF NOT EXISTS current_target_table text,
  ADD COLUMN IF NOT EXISTS current_phase_label text,
  ADD COLUMN IF NOT EXISTS phase1_status text NOT NULL DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS phase1_started_at timestamptz,
  ADD COLUMN IF NOT EXISTS phase1_completed_at timestamptz,
  ADD COLUMN IF NOT EXISTS phase2_status text NOT NULL DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS phase2_started_at timestamptz,
  ADD COLUMN IF NOT EXISTS phase2_completed_at timestamptz,
  ADD COLUMN IF NOT EXISTS phase3_status text NOT NULL DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS phase3_started_at timestamptz,
  ADD COLUMN IF NOT EXISTS phase3_completed_at timestamptz,
  ADD COLUMN IF NOT EXISTS phase4_status text NOT NULL DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS phase4_started_at timestamptz,
  ADD COLUMN IF NOT EXISTS phase4_completed_at timestamptz;

COMMENT ON COLUMN public.file_processing_status.phase1_upload_pct IS 'Phase 1: bytes uploaded / file size.';
COMMENT ON COLUMN public.file_processing_status.phase2_stage_pct IS 'Phase 2: rows written to amazon_staging / data_rows_total.';
COMMENT ON COLUMN public.file_processing_status.phase3_raw_sync_pct IS 'Phase 3: raw landing progress vs staged rows.';
COMMENT ON COLUMN public.file_processing_status.phase4_generic_pct IS 'Phase 4: generic/catalog or shipment-tree progress.';
COMMENT ON COLUMN public.file_processing_status.current_phase_label IS 'Operator-facing phase line, e.g. Phase 3 Raw Sync → table.';

NOTIFY pgrst, 'reload schema';
