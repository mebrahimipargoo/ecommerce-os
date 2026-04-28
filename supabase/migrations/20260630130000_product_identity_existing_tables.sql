-- Product identity import support on existing tables only.
-- No product_identifiers table and no new product identity tables.

BEGIN;

-- The All Items import is keyed by organization + store + SKU. The previous
-- live constraint was organization-wide, which prevented the same SKU in two
-- stores for one organization.
DO $$
BEGIN
  IF to_regclass('public.products') IS NULL THEN
    RAISE EXCEPTION 'Required table public.products does not exist';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'products'
      AND column_name IN ('organization_id', 'store_id', 'sku')
    GROUP BY table_schema, table_name
    HAVING COUNT(*) = 3
  ) THEN
    RAISE EXCEPTION 'public.products must have organization_id, store_id, and sku before adding store-scoped product identity uniqueness';
  END IF;

  ALTER TABLE public.products
    DROP CONSTRAINT IF EXISTS products_sku_organization_id_key;

  DROP INDEX IF EXISTS public.products_sku_organization_id_key;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.products'::regclass
      AND conname = 'products_organization_store_sku_key'
  ) THEN
    ALTER TABLE public.products
      ADD CONSTRAINT products_organization_store_sku_key
      UNIQUE NULLS NOT DISTINCT (organization_id, store_id, sku);
  END IF;
END $$;

COMMENT ON CONSTRAINT products_organization_store_sku_key ON public.products IS
  'Canonical product import key for seller catalog identity: organization_id + store_id + sku.';

CREATE INDEX IF NOT EXISTS idx_products_org_store_sku
  ON public.products (organization_id, store_id, sku);

-- UPC is a product identifier, but it is not unique and must not be used as an
-- identity constraint.
DO $$
BEGIN
  IF to_regclass('public.product_identifier_map') IS NULL THEN
    RAISE EXCEPTION 'Required table public.product_identifier_map does not exist';
  END IF;

  ALTER TABLE public.product_identifier_map
    ADD COLUMN IF NOT EXISTS upc_code text;

  COMMENT ON COLUMN public.product_identifier_map.upc_code IS
    'Validated UPC from product identity imports. Non-unique by design.';

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'product_identifier_map'
      AND column_name IN ('organization_id', 'store_id', 'upc_code')
    GROUP BY table_schema, table_name
    HAVING COUNT(*) = 3
  ) THEN
    EXECUTE
      'CREATE INDEX IF NOT EXISTS idx_product_identifier_map_org_store_upc
       ON public.product_identifier_map (organization_id, store_id, upc_code)
       WHERE upc_code IS NOT NULL';
  END IF;
END $$;

-- Extend the existing unresolved backlog table when present. This migration
-- intentionally does not create the table because the import is constrained to
-- existing tables only.
DO $$
DECLARE
  v_has_backlog boolean := to_regclass('public.catalog_identity_unresolved_backlog') IS NOT NULL;
  v_has_primary_key boolean;
  v_id_ready_for_primary_key boolean;
  v_source_upload_fk_ready boolean;
BEGIN
  IF NOT v_has_backlog THEN
    RAISE NOTICE 'Skipping catalog_identity_unresolved_backlog changes because the existing table is not present';
    RETURN;
  END IF;

  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS id uuid';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS organization_id uuid';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS store_id uuid';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS seller_sku text';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS asin text';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS current_catalog_fnsku text';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS item_name text';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS source_report_type text';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS source_upload_id uuid';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS identifier_type text';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS identifier_value text';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS reason text';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS candidate_product_ids uuid[]';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS raw_payload jsonb';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS created_at timestamptz';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD COLUMN IF NOT EXISTS updated_at timestamptz';

  EXECUTE 'UPDATE public.catalog_identity_unresolved_backlog SET id = gen_random_uuid() WHERE id IS NULL';
  EXECUTE 'UPDATE public.catalog_identity_unresolved_backlog SET candidate_product_ids = ''{}''::uuid[] WHERE candidate_product_ids IS NULL';
  EXECUTE 'UPDATE public.catalog_identity_unresolved_backlog SET raw_payload = ''{}''::jsonb WHERE raw_payload IS NULL';
  EXECUTE
    'UPDATE public.catalog_identity_unresolved_backlog
     SET
       identifier_type = COALESCE(identifier_type, ''LEGACY''),
       identifier_value = COALESCE(identifier_value, id::text),
       reason = COALESCE(reason, ''legacy_unresolved'')
     WHERE identifier_type IS NULL
        OR identifier_value IS NULL
        OR reason IS NULL';

  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ALTER COLUMN id SET DEFAULT gen_random_uuid()';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ALTER COLUMN candidate_product_ids SET DEFAULT ''{}''::uuid[]';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ALTER COLUMN raw_payload SET DEFAULT ''{}''::jsonb';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ALTER COLUMN created_at SET DEFAULT now()';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ALTER COLUMN updated_at SET DEFAULT now()';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ALTER COLUMN candidate_product_ids SET NOT NULL';
  EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ALTER COLUMN raw_payload SET NOT NULL';

  IF to_regclass('public.raw_report_uploads') IS NOT NULL
     AND NOT EXISTS (
       SELECT 1
       FROM pg_constraint
       WHERE conrelid = 'public.catalog_identity_unresolved_backlog'::regclass
         AND conname = 'catalog_identity_unresolved_backlog_source_upload_id_fkey'
     ) THEN
    EXECUTE
      'SELECT NOT EXISTS (
         SELECT 1
         FROM public.catalog_identity_unresolved_backlog b
         WHERE b.source_upload_id IS NOT NULL
           AND NOT EXISTS (
             SELECT 1
             FROM public.raw_report_uploads r
             WHERE r.id = b.source_upload_id
           )
       )'
      INTO v_source_upload_fk_ready;

    IF v_source_upload_fk_ready THEN
      EXECUTE
        'ALTER TABLE public.catalog_identity_unresolved_backlog
         ADD CONSTRAINT catalog_identity_unresolved_backlog_source_upload_id_fkey
         FOREIGN KEY (source_upload_id) REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL';
    ELSE
      RAISE NOTICE 'Skipping catalog_identity_unresolved_backlog.source_upload_id FK because existing values do not all match raw_report_uploads.id';
    END IF;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.catalog_identity_unresolved_backlog'::regclass
      AND contype = 'p'
  )
  INTO v_has_primary_key;

  IF NOT v_has_primary_key THEN
    EXECUTE
      'SELECT NOT EXISTS (
         SELECT id
         FROM public.catalog_identity_unresolved_backlog
         GROUP BY id
         HAVING id IS NULL OR COUNT(*) > 1
       )'
      INTO v_id_ready_for_primary_key;

    IF v_id_ready_for_primary_key THEN
      EXECUTE 'ALTER TABLE public.catalog_identity_unresolved_backlog ADD CONSTRAINT catalog_identity_unresolved_backlog_pkey PRIMARY KEY (id)';
    ELSE
      RAISE NOTICE 'Skipping primary key on catalog_identity_unresolved_backlog.id because existing id values are not unique';
    END IF;
  END IF;

  EXECUTE
    'CREATE INDEX IF NOT EXISTS idx_catalog_identity_unresolved_backlog_upload
     ON public.catalog_identity_unresolved_backlog (source_upload_id)
     WHERE source_upload_id IS NOT NULL';

  EXECUTE
    'CREATE INDEX IF NOT EXISTS idx_catalog_identity_unresolved_backlog_identifier
     ON public.catalog_identity_unresolved_backlog (organization_id, store_id, identifier_type, identifier_value)
     WHERE identifier_value IS NOT NULL';

  EXECUTE
    'CREATE UNIQUE INDEX IF NOT EXISTS uq_catalog_identity_unresolved_backlog_identity_reason
     ON public.catalog_identity_unresolved_backlog (
       organization_id,
       store_id,
       identifier_type,
       identifier_value,
       reason,
       seller_sku
     )
     NULLS NOT DISTINCT';

  EXECUTE
    'COMMENT ON COLUMN public.catalog_identity_unresolved_backlog.identifier_type IS
     ''Identifier family that could not be represented cleanly, e.g. SKU, ASIN, FNSKU, UPC.''';
  EXECUTE
    'COMMENT ON COLUMN public.catalog_identity_unresolved_backlog.identifier_value IS
     ''Validated identifier value associated with the conflict.''';
  EXECUTE
    'COMMENT ON COLUMN public.catalog_identity_unresolved_backlog.reason IS
     ''Machine-readable reason for identity import backlog entry.''';
  EXECUTE
    'COMMENT ON COLUMN public.catalog_identity_unresolved_backlog.candidate_product_ids IS
     ''Existing product ids associated with the conflicting identifier.''';
  EXECUTE
    'COMMENT ON COLUMN public.catalog_identity_unresolved_backlog.raw_payload IS
     ''Original CSV row or structured conflict context.''';
END $$;

NOTIFY pgrst, 'reload schema';

COMMIT;
