-- V16.4.13 — Adapter-synced rows may omit a return link; optional JSON for marketplace fields.

ALTER TABLE public.claim_submissions
  ADD COLUMN IF NOT EXISTS source_payload JSONB NOT NULL DEFAULT '{}';

COMMENT ON COLUMN public.claim_submissions.source_payload IS
  'Adapter-only fields (claim_type, amazon_order_id, identifiers) when return_id is null.';

-- Allow marketplace-synced claims without a linked return row (FK permits NULL).
ALTER TABLE public.claim_submissions ALTER COLUMN return_id DROP NOT NULL;
