-- ============================================================
-- Migration: Seed the default organization row
--
-- Context (Rule 5):
--   `organization_settings.organization_id` is a FK that references
--   `organizations(id) ON DELETE CASCADE`.
--   The `organizations` table was created outside tracked migrations and
--   is currently empty, which means EVERY write to `organization_settings`
--   (logo upload, branding save, claim-evidence defaults, etc.) crashes with
--   "violates foreign key constraint organization_settings_organization_id_fkey".
--
--   This migration seeds the canonical fallback/default organization row so
--   the application layer can always write against it. In production, each
--   new customer org should be inserted here by the sign-up/tenant-creation
--   flow (see lib/server-tenant.ts and the app's onboarding path).
-- ============================================================

-- 1. Ensure uuid-ossp is available for uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 2. Seed the fallback organization (idempotent)
INSERT INTO public.organizations (id, name, slug, plan, is_active)
VALUES (
  '00000000-0000-0000-0000-000000000001',
  'Default Organization',
  'default',
  'free',
  true
)
ON CONFLICT (id) DO NOTHING;

-- 3. Seed the corresponding organization_settings row (idempotent)
--    The upsert in the app uses ON CONFLICT (organization_id), so this row
--    must pre-exist to satisfy the FK before any app-layer writes.
INSERT INTO public.organization_settings (organization_id)
VALUES ('00000000-0000-0000-0000-000000000001')
ON CONFLICT (organization_id) DO NOTHING;
