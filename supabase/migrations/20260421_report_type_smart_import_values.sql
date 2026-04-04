-- Canonical import kinds live on `raw_report_uploads.report_type` (no separate detected_type).
-- Drop mistaken column if a prior revision added it; extend report_type CHECK for smart-import values.

ALTER TABLE public.raw_report_uploads DROP CONSTRAINT IF EXISTS raw_report_uploads_detected_type_check;
ALTER TABLE public.raw_report_uploads DROP COLUMN IF EXISTS detected_type;

ALTER TABLE public.raw_report_uploads DROP CONSTRAINT IF EXISTS raw_report_uploads_report_type_check;

ALTER TABLE public.raw_report_uploads
  ADD CONSTRAINT raw_report_uploads_report_type_check CHECK (
    report_type = ANY (ARRAY[
      'FBA_RETURNS'::text,
      'REMOVAL_ORDER'::text,
      'INVENTORY_LEDGER'::text,
      'UNKNOWN'::text,
      'fba_customer_returns'::text,
      'reimbursements'::text,
      'inventory_ledger'::text,
      'safe_t_claims'::text,
      'transaction_view'::text,
      'settlement_repository'::text
    ])
  );

COMMENT ON COLUMN public.raw_report_uploads.report_type IS
  'Import kind: header-classified FBA_RETURNS | REMOVAL_ORDER | INVENTORY_LEDGER | UNKNOWN, or legacy Amazon slugs.';

NOTIFY pgrst, 'reload schema';
