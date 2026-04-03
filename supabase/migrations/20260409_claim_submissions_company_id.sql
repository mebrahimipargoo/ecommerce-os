-- Rename claim_submissions.organization_id → company_id to align with app column naming.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'claim_submissions'
      AND column_name  = 'organization_id'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'claim_submissions'
      AND column_name  = 'company_id'
  ) THEN
    -- Drop old indexes that reference organization_id
    DROP INDEX IF EXISTS public.idx_claim_submissions_org_status;
    DROP INDEX IF EXISTS public.idx_claim_submissions_org_created;

    -- Drop the hardcoded RLS policy that uses the old column name
    DROP POLICY IF EXISTS claim_submissions_org_isolation ON public.claim_submissions;

    -- Rename column
    ALTER TABLE public.claim_submissions
      RENAME COLUMN organization_id TO company_id;

    -- Recreate indexes with new column name
    CREATE INDEX IF NOT EXISTS idx_claim_submissions_company_status
      ON public.claim_submissions (company_id, status);

    CREATE INDEX IF NOT EXISTS idx_claim_submissions_company_created
      ON public.claim_submissions (company_id, created_at DESC);

    -- Recreate RLS policy using the renamed column
    CREATE POLICY "claim_submissions_company_isolation"
      ON public.claim_submissions FOR ALL
      USING (company_id = '00000000-0000-0000-0000-000000000001'::uuid);
  END IF;
END $$;
