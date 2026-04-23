-- Tenant vs internal (platform) classification for public.organizations.

ALTER TABLE public.organizations
  ADD COLUMN IF NOT EXISTS type text;

UPDATE public.organizations
SET type = 'tenant'
WHERE type IS NULL OR trim(type) = '';

ALTER TABLE public.organizations
  ALTER COLUMN type SET DEFAULT 'tenant',
  ALTER COLUMN type SET NOT NULL;

-- Add check constraint only if missing (idempotent; avoids dropping a valid constraint on re-run).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE c.conname = 'organizations_type_check'
      AND n.nspname = 'public'
      AND t.relname = 'organizations'
  ) THEN
    ALTER TABLE public.organizations
      ADD CONSTRAINT organizations_type_check
      CHECK (type IN ('tenant', 'internal'));
  END IF;
END $$;

COMMENT ON COLUMN public.organizations.type IS
  'tenant = customer company; internal = platform company.';
