-- =============================================================================
-- Physical-row identity for imports (staging + raw landing tables)
-- =============================================================================
-- OLD: Staging UNIQUE (upload_id, source_line_hash) collapsed duplicate lines.
--      Landing UNIQUE (organization_id, source_line_hash) did the same at sync.
-- NEW: Staging UNIQUE (organization_id, upload_id, row_number).
--      Landing UNIQUE (organization_id, source_file_sha256, source_physical_row_number)
--      with source_file_sha256 from raw_report_uploads.metadata.content_sha256.
-- =============================================================================

BEGIN;

-- ── 1) amazon_staging ────────────────────────────────────────────────────────
DROP INDEX IF EXISTS public.uq_amazon_staging_upload_line_hash;

CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_staging_org_upload_row_number
  ON public.amazon_staging (organization_id, upload_id, row_number);

-- ── 2) Add nullable columns first ────────────────────────────────────────────
ALTER TABLE public.amazon_inventory_ledger
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_reports_repository
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_reimbursements
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_transactions
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_all_orders
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_replacements
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_fba_grade_and_resell
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_manage_fba_inventory
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_fba_inventory
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_reserved_inventory
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_fee_preview
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_monthly_storage_fees
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_returns
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_settlements
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;
ALTER TABLE public.amazon_safet_claims
  ADD COLUMN IF NOT EXISTS source_file_sha256 text,
  ADD COLUMN IF NOT EXISTS source_physical_row_number integer;

-- ── 3) Backfill (derived list: file SHA + row index within upload) ────────────
UPDATE public.amazon_inventory_ledger AS t
SET
  source_file_sha256 = v.file_sha,
  source_physical_row_number = v.rn
FROM (
  SELECT
    t2.id,
    COALESCE(
      NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''),
      'legacy-' || t2.id::text
    ) AS file_sha,
    ROW_NUMBER() OVER (
      PARTITION BY t2.upload_id
      ORDER BY t2.created_at ASC, t2.id ASC
    ) AS rn
  FROM public.amazon_inventory_ledger t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_reports_repository AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_reports_repository t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_reimbursements AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_reimbursements t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_transactions AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_transactions t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_all_orders AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.source_upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_all_orders t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.source_upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_replacements AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.source_upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_replacements t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.source_upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_fba_grade_and_resell AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.source_upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_fba_grade_and_resell t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.source_upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_manage_fba_inventory AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.source_upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_manage_fba_inventory t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.source_upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_fba_inventory AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.source_upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_fba_inventory t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.source_upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_reserved_inventory AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.source_upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_reserved_inventory t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.source_upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_fee_preview AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.source_upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_fee_preview t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.source_upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_monthly_storage_fees AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.source_upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_monthly_storage_fees t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.source_upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_returns AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_returns t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_settlements AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_settlements t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.upload_id
) AS v
WHERE t.id = v.id;

UPDATE public.amazon_safet_claims AS t
SET source_file_sha256 = v.file_sha, source_physical_row_number = v.rn
FROM (
  SELECT t2.id,
    COALESCE(NULLIF(lower(trim(rr.metadata->>'content_sha256')), ''), 'legacy-' || t2.id::text) AS file_sha,
    ROW_NUMBER() OVER (PARTITION BY t2.upload_id ORDER BY t2.created_at, t2.id) AS rn
  FROM public.amazon_safet_claims t2
  LEFT JOIN public.raw_report_uploads rr ON rr.id = t2.upload_id
) AS v
WHERE t.id = v.id;

-- ── 4) Enforce NOT NULL ──────────────────────────────────────────────────────
ALTER TABLE public.amazon_inventory_ledger
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_reports_repository
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_reimbursements
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_transactions
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_all_orders
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_replacements
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_fba_grade_and_resell
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_manage_fba_inventory
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_fba_inventory
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_reserved_inventory
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_fee_preview
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_monthly_storage_fees
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_returns
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_settlements
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;
ALTER TABLE public.amazon_safet_claims
  ALTER COLUMN source_file_sha256 SET NOT NULL,
  ALTER COLUMN source_physical_row_number SET NOT NULL;

-- ── 5) Drop old hash uniques; add physical-line uniques ────────────────────────
DROP INDEX IF EXISTS public.uq_amazon_inventory_ledger_org_line_hash;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_inventory_ledger_org_file_row
  ON public.amazon_inventory_ledger (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_reports_repo_org_line_hash;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_reports_repo_org_file_row
  ON public.amazon_reports_repository (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_reimbursements_org_line_hash;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_reimbursements_org_file_row
  ON public.amazon_reimbursements (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_transactions_org_hash;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_transactions_org_file_row
  ON public.amazon_transactions (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_all_orders_org_hash;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_all_orders_org_file_row
  ON public.amazon_all_orders (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_replacements_org_hash;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_replacements_org_file_row
  ON public.amazon_replacements (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_fba_grade_and_resell_org_hash;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_fba_grade_resell_org_file_row
  ON public.amazon_fba_grade_and_resell (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_manage_fba_inventory_org_hash;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_manage_fba_inventory_org_file_row
  ON public.amazon_manage_fba_inventory (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_fba_inventory_org_hash;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_fba_inventory_org_file_row
  ON public.amazon_fba_inventory (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_reserved_inventory_org_hash;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_reserved_inventory_org_file_row
  ON public.amazon_reserved_inventory (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_fee_preview_org_hash;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_fee_preview_org_file_row
  ON public.amazon_fee_preview (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_monthly_storage_fees_org_hash;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_monthly_storage_fees_org_file_row
  ON public.amazon_monthly_storage_fees (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_returns_org_lpn;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_returns_org_file_row
  ON public.amazon_returns (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_settlements_org_upload_line;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_settlements_org_file_row
  ON public.amazon_settlements (organization_id, source_file_sha256, source_physical_row_number);

DROP INDEX IF EXISTS public.uq_amazon_safet_claims_org_claim_id;
CREATE UNIQUE INDEX IF NOT EXISTS uq_amazon_safet_claims_org_file_row
  ON public.amazon_safet_claims (organization_id, source_file_sha256, source_physical_row_number);

COMMIT;

NOTIFY pgrst, 'reload schema';
