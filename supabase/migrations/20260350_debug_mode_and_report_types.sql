-- Admin debug flag (technical UI overlays) + Amazon-aligned import report types.

ALTER TABLE public.organization_settings
  ADD COLUMN IF NOT EXISTS debug_mode BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN public.organization_settings.debug_mode IS
  'When true, show table names, org IDs, and other technical hints in the UI (admin setting).';

-- Migrate legacy report_type values before tightening CHECK constraint.
UPDATE public.raw_report_uploads SET report_type = 'fba_customer_returns' WHERE report_type = 'returns';
UPDATE public.raw_report_uploads SET report_type = 'inventory_ledger' WHERE report_type = 'inventory_adjustments';
UPDATE public.raw_report_uploads SET report_type = 'safe_t_claims' WHERE report_type = 'removals';
UPDATE public.raw_report_uploads SET report_type = 'transaction_view' WHERE report_type = 'other';

ALTER TABLE public.raw_report_uploads DROP CONSTRAINT IF EXISTS raw_report_uploads_report_type_check;

ALTER TABLE public.raw_report_uploads
  ALTER COLUMN report_type SET DEFAULT 'fba_customer_returns';

ALTER TABLE public.raw_report_uploads
  ADD CONSTRAINT raw_report_uploads_report_type_check CHECK (
    report_type = ANY (ARRAY[
      'fba_customer_returns'::text,
      'reimbursements'::text,
      'inventory_ledger'::text,
      'safe_t_claims'::text,
      'transaction_view'::text,
      'settlement_repository'::text
    ])
  );
