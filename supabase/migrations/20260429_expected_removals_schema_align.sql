-- Migration: Align expected_removals schema to the standardized column set.
--
-- The admin manually performed these changes on the live database.  This
-- migration makes the state idempotent so fresh environments (staging, CI)
-- end up with the same schema without errors.
--
-- Final column set for expected_removals:
--   order_id, sku, fnsku, disposition,
--   shipped_quantity, cancelled_quantity, disposed_quantity,
--   requested_quantity, status
--
-- Columns removed: tracking_number, order_status, order_date
-- Columns added  : fnsku, status  (order_id was already correct)

-- 1. Add new columns (IF NOT EXISTS is idempotent).
ALTER TABLE public.expected_removals
  ADD COLUMN IF NOT EXISTS fnsku text,
  ADD COLUMN IF NOT EXISTS status text;

-- 2. Rename order_status → status if the old column still exists.
--    (IF EXISTS guard prevents errors on fresh DBs that went straight to `status`.)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'expected_removals'
      AND column_name  = 'order_status'
  ) THEN
    ALTER TABLE public.expected_removals RENAME COLUMN order_status TO status;
  END IF;
END $$;

-- 3. Drop columns that no longer belong in this table.
ALTER TABLE public.expected_removals
  DROP COLUMN IF EXISTS tracking_number,
  DROP COLUMN IF EXISTS order_date;

-- 4. Re-create the unique index using the current column names.
DROP INDEX IF EXISTS expected_removals_org_order_sku_idx;
CREATE UNIQUE INDEX IF NOT EXISTS expected_removals_org_order_sku_idx
  ON public.expected_removals (organization_id, order_id, sku);

-- 5. Ensure RLS is enabled and the service_role bypass policy exists.
ALTER TABLE public.expected_removals ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename  = 'expected_removals'
      AND policyname = 'service_role bypass expected_removals'
  ) THEN
    EXECUTE $policy$
      CREATE POLICY "service_role bypass expected_removals"
        ON public.expected_removals
        AS PERMISSIVE FOR ALL
        TO service_role
        USING (true)
        WITH CHECK (true);
    $policy$;
  END IF;
END $$;

-- 6. Notify PostgREST to reload the schema cache so API calls pick up the
--    new / renamed columns immediately without a server restart.
NOTIFY pgrst, 'reload schema';
