-- Amazon Reports Repository transaction CSV → amazon_reports_repository (Phase 3 sync).
-- Unique key matches application upsert: organization_id, date_time, transaction_type, order_id, sku, description

CREATE TABLE IF NOT EXISTS public.amazon_reports_repository (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id uuid NOT NULL,
  upload_id uuid,
  date_time timestamptz,
  settlement_id text,
  transaction_type text NOT NULL DEFAULT '',
  order_id text,
  sku text,
  description text,
  total_amount numeric NOT NULL DEFAULT 0,
  raw_data jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.amazon_reports_repository IS
  'Rows from Amazon Reports Repository transaction CSV (9-line preamble stripped at import).';

COMMENT ON COLUMN public.amazon_reports_repository.raw_data IS
  'JSONB for unmapped CSV columns (product sales, FBA fees, shipping credits, etc.).';

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_reports_repo_natural
  ON public.amazon_reports_repository (
    organization_id,
    date_time,
    transaction_type,
    order_id,
    sku,
    description
  )
  NULLS NOT DISTINCT;

-- Extend raw_report_uploads.report_type CHECK
DO $$
BEGIN
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

ALTER TABLE public.raw_report_uploads
  DROP CONSTRAINT IF EXISTS raw_report_uploads_report_type_check;

ALTER TABLE public.raw_report_uploads
  ADD CONSTRAINT raw_report_uploads_report_type_check CHECK (
    report_type = ANY (ARRAY[
      'FBA_RETURNS'::text,
      'REMOVAL_ORDER'::text,
      'INVENTORY_LEDGER'::text,
      'REIMBURSEMENTS'::text,
      'SETTLEMENT'::text,
      'SAFET_CLAIMS'::text,
      'TRANSACTIONS'::text,
      'REPORTS_REPOSITORY'::text,
      'UNKNOWN'::text,
      'fba_customer_returns'::text,
      'reimbursements'::text,
      'inventory_ledger'::text,
      'safe_t_claims'::text,
      'transaction_view'::text,
      'settlement_repository'::text
    ])
  );

NOTIFY pgrst, 'reload schema';
