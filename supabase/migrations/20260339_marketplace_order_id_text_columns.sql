-- External marketplace order IDs (Amazon, etc.) must be TEXT — never UUID.
-- Fixes: invalid input syntax for type uuid when operators enter values like "1243241".

DO $$
BEGIN
  -- returns.order_id
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'returns' AND column_name = 'order_id'
  ) THEN
    ALTER TABLE public.returns
      ALTER COLUMN order_id TYPE text USING (
        CASE WHEN order_id IS NULL THEN NULL ELSE order_id::text END
      );
  END IF;

  -- packages.order_id
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'packages' AND column_name = 'order_id'
  ) THEN
    ALTER TABLE public.packages
      ALTER COLUMN order_id TYPE text USING (
        CASE WHEN order_id IS NULL THEN NULL ELSE order_id::text END
      );
  END IF;
END $$;

COMMENT ON COLUMN public.returns.order_id IS 'External marketplace order id (TEXT). Not a UUID.';
COMMENT ON COLUMN public.packages.order_id IS 'External marketplace order id (TEXT). Not a UUID.';
