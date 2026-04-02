-- Tenant column rename: raw_report_uploads.organization_id → company_id (app alignment).

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'raw_report_uploads' AND column_name = 'organization_id'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'raw_report_uploads' AND column_name = 'company_id'
  ) THEN
    DROP INDEX IF EXISTS public.idx_raw_report_uploads_org_created;
    DROP INDEX IF EXISTS public.idx_raw_report_uploads_org_status;
    ALTER TABLE public.raw_report_uploads RENAME COLUMN organization_id TO company_id;
    CREATE INDEX IF NOT EXISTS idx_raw_report_uploads_company_created
      ON public.raw_report_uploads (company_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_raw_report_uploads_company_status
      ON public.raw_report_uploads (company_id, status);
  END IF;
END $$;
