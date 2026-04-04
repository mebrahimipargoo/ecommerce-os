-- ============================================================
-- Migration: expected_removals + RLS hardening + junk cleanup
-- ============================================================

-- 1. Create expected_removals — canonical domain table for REMOVAL_ORDER imports.
--    Mirrors the structure of expected_packages but is the correct target table.
-- ============================================================
CREATE TABLE IF NOT EXISTS public.expected_removals (
  id                 uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  organization_id    uuid NOT NULL,
  upload_id          uuid REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  order_id           text NOT NULL,
  sku                text NOT NULL DEFAULT '',
  tracking_number    text,
  requested_quantity integer,
  shipped_quantity   integer,
  disposed_quantity  integer,
  cancelled_quantity integer,
  order_status       text,
  disposition        text,
  order_date         date,
  created_at         timestamp with time zone DEFAULT now() NOT NULL,
  updated_at         timestamp with time zone DEFAULT now() NOT NULL
);

COMMENT ON TABLE public.expected_removals IS
  'Removal Order Detail lines imported via the Universal Importer (REMOVAL_ORDER type).
   Each row is one (order_id, sku) pair from an Amazon removal shipment.
   upload_id enables targeted sync and cleanup per import session.';

-- Unique per (org, order, sku) — idempotent upserts
CREATE UNIQUE INDEX IF NOT EXISTS expected_removals_org_order_sku
  ON public.expected_removals (organization_id, order_id, sku);

CREATE INDEX IF NOT EXISTS idx_expected_removals_upload_id
  ON public.expected_removals (upload_id)
  WHERE upload_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_expected_removals_org
  ON public.expected_removals (organization_id);

-- ============================================================
-- 2. Enable RLS on expected_removals (new table)
-- ============================================================
ALTER TABLE public.expected_removals ENABLE ROW LEVEL SECURITY;

-- Members can only see their own organization's rows
CREATE POLICY "expected_removals: org members can select"
  ON public.expected_removals
  FOR SELECT
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY "expected_removals: org members can insert"
  ON public.expected_removals
  FOR INSERT
  WITH CHECK (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY "expected_removals: org members can update"
  ON public.expected_removals
  FOR UPDATE
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY "expected_removals: org members can delete"
  ON public.expected_removals
  FOR DELETE
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

-- Service role bypass (needed for server-side ETL pipelines)
CREATE POLICY "expected_removals: service role bypass"
  ON public.expected_removals
  AS PERMISSIVE
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- ============================================================
-- 3. Enable RLS on expected_packages (was showing red "RLS Disabled" tag)
-- ============================================================
ALTER TABLE public.expected_packages ENABLE ROW LEVEL SECURITY;

CREATE POLICY IF NOT EXISTS "expected_packages: org members can select"
  ON public.expected_packages
  FOR SELECT
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "expected_packages: org members can insert"
  ON public.expected_packages
  FOR INSERT
  WITH CHECK (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "expected_packages: org members can update"
  ON public.expected_packages
  FOR UPDATE
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "expected_packages: org members can delete"
  ON public.expected_packages
  FOR DELETE
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "expected_packages: service role bypass"
  ON public.expected_packages
  AS PERMISSIVE
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- ============================================================
-- 4. ONE-TIME CLEANUP: Remove junk Removal Order data that was
--    incorrectly inserted into expected_packages.
--
--    Deletes all expected_packages rows whose upload_id references
--    a raw_report_uploads row with report_type = 'REMOVAL_ORDER'.
-- ============================================================
DELETE FROM public.expected_packages ep
WHERE ep.upload_id IN (
  SELECT id
  FROM   public.raw_report_uploads
  WHERE  report_type = 'REMOVAL_ORDER'
);

-- ============================================================
-- 5. Enable RLS on expected_returns if not already enabled
-- ============================================================
ALTER TABLE public.expected_returns ENABLE ROW LEVEL SECURITY;

CREATE POLICY IF NOT EXISTS "expected_returns: service role bypass"
  ON public.expected_returns
  AS PERMISSIVE
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

CREATE POLICY IF NOT EXISTS "expected_returns: org members can select"
  ON public.expected_returns
  FOR SELECT
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "expected_returns: org members can insert"
  ON public.expected_returns
  FOR INSERT
  WITH CHECK (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "expected_returns: org members can update"
  ON public.expected_returns
  FOR UPDATE
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

CREATE POLICY IF NOT EXISTS "expected_returns: org members can delete"
  ON public.expected_returns
  FOR DELETE
  USING (
    organization_id IN (
      SELECT organization_id FROM public.profiles WHERE id = auth.uid()
    )
  );

NOTIFY pgrst, 'reload schema';
