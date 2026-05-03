-- Speed Phase 4 financial_reference_resolver scans: keyset reads by (org, upload, id).
CREATE INDEX IF NOT EXISTS idx_amazon_settlements_org_upload_id_id
  ON public.amazon_settlements (organization_id, upload_id, id);

NOTIFY pgrst, 'reload schema';
