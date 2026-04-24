-- Optional corporate / contact fields on public.organizations (tenant provisioning).

ALTER TABLE public.organizations
  ADD COLUMN IF NOT EXISTS registration_number text,
  ADD COLUMN IF NOT EXISTS ceo_name text,
  ADD COLUMN IF NOT EXISTS address text,
  ADD COLUMN IF NOT EXISTS phone text;

COMMENT ON COLUMN public.organizations.registration_number IS
  'Optional legal or company registration identifier.';
COMMENT ON COLUMN public.organizations.ceo_name IS
  'Optional CEO or primary executive name.';
COMMENT ON COLUMN public.organizations.address IS
  'Optional mailing or registered address.';
COMMENT ON COLUMN public.organizations.phone IS
  'Optional contact phone number.';
