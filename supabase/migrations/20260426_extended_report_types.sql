-- Extend raw_report_uploads.report_type CHECK constraint to include the four new
-- canonical smart-import types:  REIMBURSEMENTS | SETTLEMENT | SAFET_CLAIMS | TRANSACTIONS
--
-- The block is idempotent: drops the old constraint (if any) and recreates it with
-- all known values — canonical smart-import types plus legacy lowercase slugs.

DO $$
BEGIN
  -- Drop any existing CHECK constraint on report_type
  IF EXISTS (
    SELECT 1
    FROM   information_schema.table_constraints tc
    JOIN   information_schema.constraint_column_usage ccu
           ON tc.constraint_name = ccu.constraint_name
    WHERE  tc.table_schema  = 'public'
      AND  tc.table_name    = 'raw_report_uploads'
      AND  tc.constraint_type = 'CHECK'
      AND  ccu.column_name  = 'report_type'
  ) THEN
    -- Constraint name varies by environment — drop by scanning pg_constraint
    DECLARE
      v_constraint text;
    BEGIN
      SELECT conname INTO v_constraint
      FROM   pg_constraint c
      JOIN   pg_class      t ON t.oid = c.conrelid
      JOIN   pg_namespace  n ON n.oid = t.relnamespace
      WHERE  n.nspname = 'public'
        AND  t.relname = 'raw_report_uploads'
        AND  c.contype = 'c'
        AND  pg_get_constraintdef(c.oid) LIKE '%report_type%'
      LIMIT 1;

      IF v_constraint IS NOT NULL THEN
        EXECUTE format('ALTER TABLE public.raw_report_uploads DROP CONSTRAINT %I', v_constraint);
      END IF;
    END;
  END IF;
END $$;

-- Recreate with the full list of allowed report_type values
ALTER TABLE public.raw_report_uploads
  DROP CONSTRAINT IF EXISTS raw_report_uploads_report_type_check;

ALTER TABLE public.raw_report_uploads
  ADD CONSTRAINT raw_report_uploads_report_type_check CHECK (
    report_type = ANY (ARRAY[
      -- Smart-import canonical (rule-based / AI detected)
      'FBA_RETURNS'::text,
      'REMOVAL_ORDER'::text,
      'INVENTORY_LEDGER'::text,
      'REIMBURSEMENTS'::text,
      'SETTLEMENT'::text,
      'SAFET_CLAIMS'::text,
      'TRANSACTIONS'::text,
      'UNKNOWN'::text,
      -- Legacy slugs kept for backward compatibility
      'fba_customer_returns'::text,
      'reimbursements'::text,
      'inventory_ledger'::text,
      'safe_t_claims'::text,
      'transaction_view'::text,
      'settlement_repository'::text
    ])
  );

COMMENT ON COLUMN public.raw_report_uploads.report_type IS
  'Canonical report family written by the header classifier.
   Smart-import types: FBA_RETURNS | REMOVAL_ORDER | INVENTORY_LEDGER |
                       REIMBURSEMENTS | SETTLEMENT | SAFET_CLAIMS | TRANSACTIONS | UNKNOWN.
   Legacy values (fba_customer_returns, inventory_ledger, safe_t_claims, …) are retained
   for rows created before the 2026-04-26 migration.';

NOTIFY pgrst, 'reload schema';
