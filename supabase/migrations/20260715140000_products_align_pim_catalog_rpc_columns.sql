-- Minimal fix for: column p.sku does not exist (pim_catalog_products_page reads public.products p).
-- Idempotent. Safe if amazon_raw/metadata already exist as non-jsonb (skips incompatible ALTER).
-- Run the whole file in Supabase SQL Editor on the same project as the app.

-- 1) Columns the RPC touches (sku first — nothing before it can roll this back in your session)
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS sku text;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS organization_id uuid;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS store_id uuid;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS product_name text;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS brand text;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS status text;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS vendor_id uuid;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS category_id uuid;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS vendor_name text;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS main_image_url text;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS mfg_part_number text;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS upc_code text;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS asin text;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS fnsku text;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS condition text;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS last_seen_at timestamptz;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS last_catalog_sync_at timestamptz;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS updated_at timestamptz;

-- jsonb columns: only ADD when missing (avoids failing whole script on weird legacy types)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'products' AND column_name = 'amazon_raw'
  ) THEN
    ALTER TABLE public.products
      ADD COLUMN amazon_raw jsonb NOT NULL DEFAULT '{}'::jsonb;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'products' AND column_name = 'metadata'
  ) THEN
    ALTER TABLE public.products
      ADD COLUMN metadata jsonb NOT NULL DEFAULT '{}'::jsonb;
  END IF;
END;
$$;

ALTER TABLE public.products ADD COLUMN IF NOT EXISTS barcode text;
ALTER TABLE public.products ADD COLUMN IF NOT EXISTS image_url text;

-- Backfill sku / names / image (optional columns guarded where needed)
UPDATE public.products
SET sku = NULLIF(btrim(barcode), '')
WHERE (sku IS NULL OR btrim(sku) = '')
  AND barcode IS NOT NULL
  AND btrim(barcode) <> '';

UPDATE public.products
SET sku = 'legacy-' || id::text
WHERE sku IS NULL OR btrim(sku) = '';

UPDATE public.products
SET product_name = COALESCE(NULLIF(btrim(product_name), ''), NULLIF(btrim(barcode), ''), 'Untitled product')
WHERE product_name IS NULL OR btrim(product_name) = '';

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns c
    WHERE c.table_schema = 'public' AND c.table_name = 'products' AND c.column_name = 'name'
  ) THEN
    EXECUTE $sql$
      UPDATE public.products
      SET product_name = COALESCE(
        NULLIF(btrim(product_name), ''),
        NULLIF(btrim(name::text), ''),
        NULLIF(btrim(barcode), ''),
        'Untitled product'
      )
      WHERE product_name IS NULL OR btrim(product_name) = ''
    $sql$;
  END IF;
END;
$$;

UPDATE public.products
SET main_image_url = COALESCE(NULLIF(btrim(main_image_url), ''), NULLIF(btrim(image_url), ''))
WHERE main_image_url IS NULL OR btrim(main_image_url) = '';

UPDATE public.products
SET updated_at = COALESCE(updated_at, last_catalog_sync_at, last_seen_at, now())
WHERE updated_at IS NULL;

-- Defaults only when column is jsonb (SET DEFAULT on wrong type would abort the script)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'products'
      AND column_name = 'amazon_raw' AND udt_name = 'jsonb'
  ) THEN
    ALTER TABLE public.products ALTER COLUMN amazon_raw SET DEFAULT '{}'::jsonb;
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'products'
      AND column_name = 'metadata' AND udt_name = 'jsonb'
  ) THEN
    ALTER TABLE public.products ALTER COLUMN metadata SET DEFAULT '{}'::jsonb;
  END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_products_org_store
  ON public.products (organization_id, store_id)
  WHERE organization_id IS NOT NULL AND store_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_products_org_store_sku
  ON public.products (organization_id, store_id, sku)
  WHERE sku IS NOT NULL;

COMMENT ON COLUMN public.products.sku IS
  'Seller SKU per organization + store; required by pim_catalog_products_page.';

NOTIFY pgrst, 'reload schema';
