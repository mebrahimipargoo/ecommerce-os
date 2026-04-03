-- =============================================================================
-- file_processing_status — granular real-time progress tracking for imports
-- =============================================================================
--
-- Purpose
-- -------
-- This table provides a dedicated, Realtime-enabled row per file upload that
-- tracks BOTH upload-to-storage progress AND server-side processing progress.
-- The UI subscribes via Supabase Realtime (`postgres_changes`, event=UPDATE)
-- to stream live dual progress bars without polling.
--
-- Relation to raw_report_uploads
-- --------------------------------
-- Each row here maps 1:1 to a `raw_report_uploads` row via `upload_id`.
-- The import pipeline upserts into this table at strategic checkpoints:
--   1. After each 5 MB chunk is stored (upload_pct advances).
--   2. Every 1 000 rows during CSV processing (process_pct advances).
--   3. On completion / failure (status + error_message finalised).
--
-- Why a separate table?
-- ----------------------
-- `raw_report_uploads.metadata` JSONB is patched per-chunk but is a large
-- write target shared with column-mapping, audit fields, etc.  A slim
-- dedicated table means Realtime payloads are tiny (< 200 bytes), and the
-- DB can enable replica identity FULL only on this table for fast diffing.
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.file_processing_status (
  id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),

  -- FK → raw_report_uploads; cascades so orphan rows never accumulate
  upload_id        uuid        NOT NULL
                   REFERENCES  public.raw_report_uploads(id) ON DELETE CASCADE,

  organization_id  uuid        NOT NULL,

  -- Lifecycle: pending → uploading → processing → complete | failed
  status           text        NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','uploading','processing','complete','failed')),

  -- Upload progress (0–100): bytes written to Supabase Storage
  upload_pct       smallint    NOT NULL DEFAULT 0 CHECK (upload_pct BETWEEN 0 AND 100),

  -- Processing progress (0–100): CSV rows imported to the returns table
  process_pct      smallint    NOT NULL DEFAULT 0 CHECK (process_pct BETWEEN 0 AND 100),

  -- Informational counters
  total_rows       integer,
  processed_rows   integer     NOT NULL DEFAULT 0,

  -- Error details (only populated when status = 'failed')
  error_message    text,

  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

-- ── Indexes ──────────────────────────────────────────────────────────────────

CREATE UNIQUE INDEX IF NOT EXISTS file_processing_status_upload_id_key
  ON public.file_processing_status (upload_id);

CREATE INDEX IF NOT EXISTS file_processing_status_org_id_idx
  ON public.file_processing_status (organization_id);

-- ── Auto-update `updated_at` on every write ──────────────────────────────────

CREATE OR REPLACE FUNCTION public.touch_file_processing_status()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_touch_file_processing_status
  ON public.file_processing_status;

CREATE TRIGGER trg_touch_file_processing_status
  BEFORE UPDATE ON public.file_processing_status
  FOR EACH ROW EXECUTE FUNCTION public.touch_file_processing_status();

-- ── RLS ───────────────────────────────────────────────────────────────────────

ALTER TABLE public.file_processing_status ENABLE ROW LEVEL SECURITY;

-- Authenticated users can see progress only for their own organisation
CREATE POLICY "file_processing_status_select"
  ON public.file_processing_status FOR SELECT
  USING (
    organization_id = (
      SELECT organization_id FROM public.profiles
      WHERE  id = auth.uid()
      LIMIT  1
    )
  );

-- Service-role writes (from API routes) always bypass RLS, so no INSERT policy
-- is needed here.  If you add a client-side write path, add:
--   CREATE POLICY "file_processing_status_insert"
--     ON public.file_processing_status FOR INSERT
--     WITH CHECK (organization_id = (SELECT organization_id FROM public.profiles WHERE id = auth.uid() LIMIT 1));

-- ── Enable Supabase Realtime ──────────────────────────────────────────────────
--
-- Full replica identity is required for Realtime to include the full NEW row
-- in UPDATE payloads (without it only PKs are sent, not progress percentages).

ALTER TABLE public.file_processing_status REPLICA IDENTITY FULL;

-- Add to the Realtime publication so clients can subscribe via postgres_changes
ALTER PUBLICATION supabase_realtime ADD TABLE public.file_processing_status;

-- Also enable Realtime on raw_report_uploads (already used by useImportProgress hook)
ALTER TABLE public.raw_report_uploads REPLICA IDENTITY FULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_publication_tables
    WHERE  pubname = 'supabase_realtime'
    AND    tablename = 'raw_report_uploads'
  ) THEN
    ALTER PUBLICATION supabase_realtime ADD TABLE public.raw_report_uploads;
  END IF;
END;
$$;

-- ── Notify PostgREST to reload schema ────────────────────────────────────────

NOTIFY pgrst, 'reload schema';
