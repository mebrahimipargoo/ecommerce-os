-- Rename raw listing archive to Amazon-prefixed name; data, RLS, and FKs preserved.

BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'catalog_listing_rows_raw'
  ) THEN
    ALTER TABLE public.catalog_listing_rows_raw RENAME TO amazon_listing_report_rows_raw;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'idx_catalog_listing_rows_raw_org_upload') THEN
    ALTER INDEX public.idx_catalog_listing_rows_raw_org_upload RENAME TO idx_amazon_listing_report_rows_raw_org_upload;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'idx_catalog_listing_rows_raw_org_seller_sku') THEN
    ALTER INDEX public.idx_catalog_listing_rows_raw_org_seller_sku RENAME TO idx_amazon_listing_report_rows_raw_org_seller_sku;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'idx_catalog_listing_rows_raw_org_asin') THEN
    ALTER INDEX public.idx_catalog_listing_rows_raw_org_asin RENAME TO idx_amazon_listing_report_rows_raw_org_asin;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'uq_catalog_listing_rows_raw_upload_row') THEN
    ALTER INDEX public.uq_catalog_listing_rows_raw_upload_row RENAME TO uq_amazon_listing_report_rows_raw_upload_row;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'catalog_listing_rows_raw_parse_status_check'
      AND conrelid = 'public.amazon_listing_report_rows_raw'::regclass
  ) THEN
    ALTER TABLE public.amazon_listing_report_rows_raw
      RENAME CONSTRAINT catalog_listing_rows_raw_parse_status_check TO amazon_listing_report_rows_raw_parse_status_check;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'catalog_listing_rows_raw_source_upload_id_fkey'
      AND conrelid = 'public.amazon_listing_report_rows_raw'::regclass
  ) THEN
    ALTER TABLE public.amazon_listing_report_rows_raw
      RENAME CONSTRAINT catalog_listing_rows_raw_source_upload_id_fkey TO amazon_listing_report_rows_raw_source_upload_id_fkey;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'catalog_listing_rows_raw_store_id_fkey'
      AND conrelid = 'public.amazon_listing_report_rows_raw'::regclass
  ) THEN
    ALTER TABLE public.amazon_listing_report_rows_raw
      RENAME CONSTRAINT catalog_listing_rows_raw_store_id_fkey TO amazon_listing_report_rows_raw_store_id_fkey;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'amazon_listing_report_rows_raw'
      AND policyname = 'catalog_listing_rows_raw: org members can select'
  ) THEN
    ALTER POLICY "catalog_listing_rows_raw: org members can select" ON public.amazon_listing_report_rows_raw
      RENAME TO "amazon_listing_report_rows_raw: org members can select";
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'amazon_listing_report_rows_raw'
      AND policyname = 'catalog_listing_rows_raw: org members can insert'
  ) THEN
    ALTER POLICY "catalog_listing_rows_raw: org members can insert" ON public.amazon_listing_report_rows_raw
      RENAME TO "amazon_listing_report_rows_raw: org members can insert";
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'amazon_listing_report_rows_raw'
      AND policyname = 'catalog_listing_rows_raw: org members can update'
  ) THEN
    ALTER POLICY "catalog_listing_rows_raw: org members can update" ON public.amazon_listing_report_rows_raw
      RENAME TO "amazon_listing_report_rows_raw: org members can update";
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'amazon_listing_report_rows_raw'
      AND policyname = 'catalog_listing_rows_raw: org members can delete'
  ) THEN
    ALTER POLICY "catalog_listing_rows_raw: org members can delete" ON public.amazon_listing_report_rows_raw
      RENAME TO "amazon_listing_report_rows_raw: org members can delete";
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'amazon_listing_report_rows_raw'
      AND policyname = 'catalog_listing_rows_raw: service role bypass'
  ) THEN
    ALTER POLICY "catalog_listing_rows_raw: service role bypass" ON public.amazon_listing_report_rows_raw
      RENAME TO "amazon_listing_report_rows_raw: service role bypass";
  END IF;
END $$;

COMMENT ON TABLE public.amazon_listing_report_rows_raw IS
  'Physical lines from Amazon listing exports (All/Active/Category). One row per non-empty data line; canonical merge is catalog_products.';

COMMENT ON COLUMN public.amazon_listing_report_rows_raw.raw_payload IS
  'Normalized column mapping applied (same shape as listing mappers use).';

NOTIFY pgrst, 'reload schema';

COMMIT;
