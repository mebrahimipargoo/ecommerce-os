-- V16.4.14 — Item value for claim inheritance + recovered amount on submissions
ALTER TABLE public.returns
  ADD COLUMN IF NOT EXISTS estimated_value NUMERIC(14, 2);

COMMENT ON COLUMN public.returns.estimated_value IS
  'Expected item value for claims — inherited as claim_submissions.claim_amount when generated.';

ALTER TABLE public.claim_submissions
  ADD COLUMN IF NOT EXISTS reimbursement_amount NUMERIC(14, 2);

COMMENT ON COLUMN public.claim_submissions.reimbursement_amount IS
  'Actual amount reimbursed by marketplace when status is accepted (vs claim_amount requested).';
