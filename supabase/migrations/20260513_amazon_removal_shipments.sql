-- Permanent audit/history: every raw Removal Shipment Detail row synced from staging.
-- Populated by FastAPI Phase 3 (_run_sync_removals) for rows that include tracking.

BEGIN;

CREATE TABLE IF NOT EXISTS public.amazon_removal_shipments (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id     uuid NOT NULL,
  upload_id           uuid REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  amazon_staging_id   uuid,
  raw_row             jsonb NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_amazon_removal_shipments_org_created
  ON public.amazon_removal_shipments (organization_id, created_at DESC);

ALTER TABLE public.amazon_removal_shipments ENABLE ROW LEVEL SECURITY;

-- PostgreSQL <15 does not support CREATE POLICY IF NOT EXISTS; use drop+create (idempotent).
DROP POLICY IF EXISTS "amazon_removal_shipments: service_role bypass"
  ON public.amazon_removal_shipments;
CREATE POLICY "amazon_removal_shipments: service_role bypass"
  ON public.amazon_removal_shipments
  AS PERMISSIVE FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

COMMENT ON TABLE public.amazon_removal_shipments IS
  'Append-only history of raw Removal Shipment Detail rows (per sync).';

NOTIFY pgrst, 'reload schema';

COMMIT;
