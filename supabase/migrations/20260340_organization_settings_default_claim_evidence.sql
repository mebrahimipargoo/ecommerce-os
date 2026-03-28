-- Default claim evidence checklist (Enterprise PDF generator) — JSONB on organization_settings.

CREATE TABLE IF NOT EXISTS public.organization_settings (
  organization_id uuid NOT NULL PRIMARY KEY,
  is_ai_label_ocr_enabled boolean NOT NULL DEFAULT false,
  is_ai_packing_slip_ocr_enabled boolean NOT NULL DEFAULT false,
  default_claim_evidence jsonb NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE public.organization_settings
  ADD COLUMN IF NOT EXISTS is_ai_label_ocr_enabled boolean NOT NULL DEFAULT false;

ALTER TABLE public.organization_settings
  ADD COLUMN IF NOT EXISTS is_ai_packing_slip_ocr_enabled boolean NOT NULL DEFAULT false;

ALTER TABLE public.organization_settings
  ADD COLUMN IF NOT EXISTS default_claim_evidence jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN public.organization_settings.default_claim_evidence IS
  'Admin checklist: which evidence photo categories are pre-selected for claim PDFs (boolean map by category key).';
