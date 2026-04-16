-- Unified Amazon import engine: extra file_processing_status counters & registry-oriented columns.

ALTER TABLE public.file_processing_status
  ADD COLUMN IF NOT EXISTS upload_bytes_written bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS upload_bytes_total bigint,
  ADD COLUMN IF NOT EXISTS duplicate_rows_skipped integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS rows_eligible_for_generic integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS canonical_rows_new integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS canonical_rows_updated integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS canonical_rows_unchanged integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS canonical_rows_invalid integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS phase_key text,
  ADD COLUMN IF NOT EXISTS phase_label text,
  ADD COLUMN IF NOT EXISTS next_action_key text,
  ADD COLUMN IF NOT EXISTS next_action_label text,
  ADD COLUMN IF NOT EXISTS stage_target_table text,
  ADD COLUMN IF NOT EXISTS sync_target_table text,
  ADD COLUMN IF NOT EXISTS generic_target_table text;

COMMENT ON COLUMN public.file_processing_status.upload_bytes_written IS 'Mirrors uploaded_bytes when populated; explicit name for unified engine contract.';
COMMENT ON COLUMN public.file_processing_status.upload_bytes_total IS 'Planned upload size in bytes (often file_size_bytes).';
COMMENT ON COLUMN public.file_processing_status.phase_key IS 'Engine phase: upload | process | sync | generic | complete | failed.';
COMMENT ON COLUMN public.file_processing_status.stage_target_table IS 'Phase 2 physical target (amazon_staging).';
COMMENT ON COLUMN public.file_processing_status.sync_target_table IS 'Phase 3 domain/raw table for this report.';
COMMENT ON COLUMN public.file_processing_status.generic_target_table IS 'Phase 4 enrichment target when supports_generic.';

NOTIFY pgrst, 'reload schema';
