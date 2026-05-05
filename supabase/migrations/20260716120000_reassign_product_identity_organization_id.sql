-- Reassign organization_id on PIM / product-identity import tables.
--
-- Schema inventory aligned with Supabase snippet export (table_name + organization_id):
--   raw_report_uploads, product_identity_staging_rows, vendors, product_categories,
--   products, product_identifier_map, product_prices, catalog_identity_unresolved_backlog,
--   catalog_products.
--
-- Later repo migrations add columns on the same tables (e.g. products.sku, product_prices.amount,
-- product_prices.observed_at); those are not org keys — only organization_id is updated here.
--
-- Usage (SQL Editor, usually as postgres / service role):
--   SELECT public.reassign_product_identity_organization_id(
--     '<from-uuid>'::uuid,
--     '<to-uuid>'::uuid,
--     false,  -- dry run: counts only
--     false   -- include stores (see COMMENT)
--   );
--   Then rerun with third arg true to apply.
--
-- Risks: UNIQUE (organization_id, store_id, sku) on products and similar on catalog_products /
-- product_identifier_map may raise duplicate-key errors if p_to already has conflicting rows.
-- Ensure stores.organization_id matches the target org if you rely on org+store consistency.

BEGIN;

CREATE OR REPLACE FUNCTION public.reassign_product_identity_organization_id(
  p_from_organization_id uuid,
  p_to_organization_id uuid,
  p_apply boolean DEFAULT false,
  p_include_stores boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $fn$
DECLARE
  v_tables text[];
  v_t text;
  v_count bigint;
  v_updated bigint;
  v_tables_out jsonb := '{}'::jsonb;
BEGIN
  IF p_from_organization_id IS NULL OR p_to_organization_id IS NULL THEN
    RAISE EXCEPTION 'p_from_organization_id and p_to_organization_id are required';
  END IF;

  IF p_from_organization_id = p_to_organization_id THEN
    RAISE EXCEPTION 'from and to organization_id must differ';
  END IF;

  v_tables := ARRAY[
    'raw_report_uploads',
    'product_identity_staging_rows',
    'vendors',
    'product_categories'
  ];

  IF p_include_stores THEN
    v_tables := v_tables || ARRAY['stores'];
  END IF;

  v_tables := v_tables || ARRAY[
    'products',
    'product_identifier_map',
    'product_prices',
    'catalog_identity_unresolved_backlog',
    'catalog_products'
  ];

  FOREACH v_t IN ARRAY v_tables
  LOOP
    IF NOT EXISTS (
      SELECT 1
      FROM information_schema.tables t
      WHERE t.table_schema = 'public'
        AND t.table_name = v_t
        AND t.table_type = 'BASE TABLE'
    ) THEN
      v_tables_out := v_tables_out || jsonb_build_object(
        v_t,
        jsonb_build_object('skipped', true, 'reason', 'table missing')
      );
      CONTINUE;
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM information_schema.columns c
      WHERE c.table_schema = 'public'
        AND c.table_name = v_t
        AND c.column_name = 'organization_id'
    ) THEN
      v_tables_out := v_tables_out || jsonb_build_object(
        v_t,
        jsonb_build_object('skipped', true, 'reason', 'no organization_id column')
      );
      CONTINUE;
    END IF;

    EXECUTE format(
      'SELECT count(*)::bigint FROM public.%I WHERE organization_id = $1',
      v_t
    )
      INTO v_count
      USING p_from_organization_id;

    IF NOT p_apply THEN
      v_tables_out := v_tables_out || jsonb_build_object(
        v_t,
        jsonb_build_object('would_update', v_count)
      );
    ELSE
      EXECUTE format(
        'UPDATE public.%I SET organization_id = $1 WHERE organization_id = $2',
        v_t
      )
        USING p_to_organization_id, p_from_organization_id;
      GET DIAGNOSTICS v_updated = ROW_COUNT;
      v_tables_out := v_tables_out || jsonb_build_object(
        v_t,
        jsonb_build_object('updated', v_updated)
      );
    END IF;
  END LOOP;

  RETURN jsonb_build_object(
    'apply', p_apply,
    'include_stores', p_include_stores,
    'from_organization_id', p_from_organization_id,
    'to_organization_id', p_to_organization_id,
    'tables', v_tables_out
  );
END;
$fn$;

COMMENT ON FUNCTION public.reassign_product_identity_organization_id(uuid, uuid, boolean, boolean) IS
  'Dry-run or apply: set organization_id from one org to another on PIM/product-identity tables '
  'listed in migration 20260716120000 (matches Supabase column export). '
  'Optional p_include_stores updates public.stores — use only when every store row scoped to '
  'p_from should move to p_to. Does not touch amazon_* or other org-wide tables from the full export.';

GRANT EXECUTE ON FUNCTION public.reassign_product_identity_organization_id(uuid, uuid, boolean, boolean)
  TO service_role;

NOTIFY pgrst, 'reload schema';

COMMIT;
