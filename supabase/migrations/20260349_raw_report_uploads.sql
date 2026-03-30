-- Large Amazon CSV import pipeline: upload tracking, audit trail, private storage.

CREATE TABLE IF NOT EXISTS public.raw_report_uploads (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id     UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'::uuid,
  file_name           TEXT NOT NULL,
  report_type         TEXT NOT NULL DEFAULT 'returns',
  storage_prefix      TEXT,
  status              TEXT NOT NULL DEFAULT 'pending',
  upload_progress     SMALLINT NOT NULL DEFAULT 0,
  process_progress    SMALLINT NOT NULL DEFAULT 0,
  bytes_uploaded      BIGINT NOT NULL DEFAULT 0,
  total_bytes         BIGINT NOT NULL DEFAULT 0,
  part_count          INTEGER NOT NULL DEFAULT 0,
  row_count           BIGINT,
  column_mapping      JSONB,
  error_message       TEXT,
  uploaded_by         UUID REFERENCES public.user_profiles (id) ON DELETE SET NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT raw_report_uploads_report_type_check CHECK (
    report_type = ANY (ARRAY[
      'returns'::text,
      'reimbursements'::text,
      'inventory_adjustments'::text,
      'removals'::text,
      'other'::text
    ])
  ),
  CONSTRAINT raw_report_uploads_status_check CHECK (
    status = ANY (ARRAY[
      'pending'::text,
      'uploading'::text,
      'processing'::text,
      'complete'::text,
      'failed'::text,
      'cancelled'::text
    ])
  ),
  CONSTRAINT raw_report_uploads_progress_check CHECK (
    upload_progress >= 0 AND upload_progress <= 100
    AND process_progress >= 0 AND process_progress <= 100
  )
);

CREATE INDEX IF NOT EXISTS idx_raw_report_uploads_org_created
  ON public.raw_report_uploads (organization_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_report_uploads_org_status
  ON public.raw_report_uploads (organization_id, status);

COMMENT ON TABLE public.raw_report_uploads IS
  'Amazon raw CSV uploads; chunked parts live under storage_prefix in raw-reports bucket.';

CREATE TABLE IF NOT EXISTS public.raw_report_import_audit (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id     UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'::uuid,
  user_profile_id     UUID REFERENCES public.user_profiles (id) ON DELETE SET NULL,
  action              TEXT NOT NULL,
  entity_id           UUID REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  detail              JSONB,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_report_import_audit_org_created
  ON public.raw_report_import_audit (organization_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_report_import_audit_entity
  ON public.raw_report_import_audit (entity_id);

COMMENT ON TABLE public.raw_report_import_audit IS
  'Accountability log for import module actions; user_profile_id is the acting user.';

INSERT INTO storage.buckets (id, name, public)
VALUES ('raw-reports', 'raw-reports', false)
ON CONFLICT (id) DO NOTHING;
