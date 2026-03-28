-- V16.4.14 — claim_submissions as sole live queue for the agent; deprecate legacy `claims`.
-- Backfill non-null FKs, default status ready_to_send, re-tighten return_id.

-- ─── 1. Deprecate legacy marketplace-sync `claims` table (no app reads; adapter may reference name) ─
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'claims'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'claims_deprecated'
  ) THEN
    ALTER TABLE public.claims RENAME TO claims_deprecated;
    COMMENT ON TABLE public.claims_deprecated IS
      'DEPRECATED (2026-03-33) — use public.claim_submissions as the live submission queue for the Python agent.';
  END IF;
END $$;

-- ─── 2. Fallback store for transition (FK target when historical rows lack store_id) ─
INSERT INTO public.stores (id, name, platform, organization_id, is_active)
VALUES (
  '00000000-0000-0000-0000-0000000000f1'::uuid,
  'Default channel (transition)',
  'amazon',
  '00000000-0000-0000-0000-000000000001'::uuid,
  true
)
ON CONFLICT (id) DO NOTHING;

-- ─── 3. Drop orphan adapter rows without a return link (master queue is return-scoped) ─
DELETE FROM public.claim_submissions WHERE return_id IS NULL;

-- ─── 4. Backfill store_id from return → package → first Amazon/active store in org ─
UPDATE public.claim_submissions cs
SET store_id = COALESCE(
  cs.store_id,
  (SELECT r.store_id FROM public.returns r WHERE r.id = cs.return_id),
  (
    SELECT p.store_id
    FROM public.returns r
    JOIN public.packages p ON p.id = r.package_id
    WHERE r.id = cs.return_id
  ),
  (
    SELECT s.id FROM public.stores s
    WHERE s.organization_id = cs.organization_id AND s.is_active = true
    ORDER BY CASE WHEN lower(s.platform) = 'amazon' THEN 0 ELSE 1 END, s.created_at ASC
    LIMIT 1
  ),
  '00000000-0000-0000-0000-0000000000f1'::uuid
)
WHERE cs.store_id IS NULL;

ALTER TABLE public.claim_submissions
  ALTER COLUMN store_id SET NOT NULL;

ALTER TABLE public.claim_submissions
  ALTER COLUMN return_id SET NOT NULL;

ALTER TABLE public.claim_submissions
  ALTER COLUMN status SET DEFAULT 'ready_to_send'::public.claim_submission_status;

COMMENT ON TABLE public.claim_submissions IS
  'Live submission queue for the Python claim agent: one row per return ready to file (status workflow + source_payload).';

COMMENT ON COLUMN public.claim_submissions.source_payload IS
  'JSONB: amazon_order_id, defect_reasons, defect_reason_labels, claim_type, package_box_evidence URLs, etc.';
