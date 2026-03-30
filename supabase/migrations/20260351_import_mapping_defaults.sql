-- Per-org default CSV column mappings by report type (Imports modal upsert).

ALTER TABLE public.organization_settings
  ADD COLUMN IF NOT EXISTS import_mapping_defaults JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN public.organization_settings.import_mapping_defaults IS
  'Map of raw_report_uploads.report_type → { canonicalKey: fileHeader } for Imports mapping reuse.';
