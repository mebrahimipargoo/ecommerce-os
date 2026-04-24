-- ============================================================
-- Migration: Scoped roles catalog (system vs tenant)
-- Date: 2026-06-21
--
-- Adds / completes `public.roles` with:
--   key, name, description, scope ('system' | 'tenant'),
--   is_system, is_assignable, created_at
--
-- Seeds canonical roles, backfills `profiles.role_id`, widens
-- `profiles.role` CHECK for legacy text fallback, and wires the
-- auth signup trigger to default `role_id` → operator.
--
-- PROTECTED: Do not alter claim_submissions / system_settings flows.
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- 1. Roles table (create or extend)
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.roles (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  key text NOT NULL,
  name text NOT NULL DEFAULT '',
  description text,
  scope text NOT NULL DEFAULT 'tenant',
  is_system boolean NOT NULL DEFAULT true,
  is_assignable boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT roles_scope_check CHECK (scope IN ('system', 'tenant'))
);

ALTER TABLE public.roles
  ADD COLUMN IF NOT EXISTS description text;

ALTER TABLE public.roles
  ADD COLUMN IF NOT EXISTS scope text NOT NULL DEFAULT 'tenant';

ALTER TABLE public.roles
  DROP CONSTRAINT IF EXISTS roles_scope_check;

ALTER TABLE public.roles
  ADD CONSTRAINT roles_scope_check CHECK (scope IN ('system', 'tenant'));

ALTER TABLE public.roles
  ADD COLUMN IF NOT EXISTS is_system boolean NOT NULL DEFAULT true;

ALTER TABLE public.roles
  ADD COLUMN IF NOT EXISTS is_assignable boolean NOT NULL DEFAULT true;

ALTER TABLE public.roles
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();

COMMENT ON TABLE public.roles IS
  'Role catalog: system-side (internal staff) vs tenant-side (customer org). '
  'Runtime resolution prefers profiles.role_id → roles.key; profiles.role is legacy fallback.';

COMMENT ON COLUMN public.roles.scope IS
  'system = internal company roles; tenant = customer workspace roles.';

COMMENT ON COLUMN public.roles.is_system IS
  'True for built-in catalog rows (not deletable by tenants).';

COMMENT ON COLUMN public.roles.is_assignable IS
  'False for purely technical or reserved roles excluded from normal assignment UIs.';

CREATE UNIQUE INDEX IF NOT EXISTS roles_key_unique ON public.roles (key);

CREATE INDEX IF NOT EXISTS idx_roles_scope ON public.roles (scope);

-- ─────────────────────────────────────────────────────────────
-- 2. Seed / upsert canonical roles
-- ─────────────────────────────────────────────────────────────

INSERT INTO public.roles (key, name, description, scope, is_system, is_assignable)
VALUES
  (
    'super_admin',
    'Super Admin',
    'Platform owner — unrestricted internal access.',
    'system',
    true,
    true
  ),
  (
    'system_admin',
    'System Admin',
    'Internal operations administrator (below super admin).',
    'system',
    true,
    true
  ),
  (
    'system_employee',
    'System Employee',
    'Internal SaaS staff — multi-organization workspace access.',
    'system',
    true,
    true
  ),
  (
    'programmer',
    'Programmer',
    'Engineering / technical staff — internal tooling and diagnostics.',
    'system',
    true,
    true
  ),
  (
    'customer_service',
    'Customer Service',
    'Internal support — customer-facing workflows.',
    'system',
    true,
    true
  ),
  (
    'tenant_admin',
    'Tenant Admin',
    'Company administrator — full access within the customer organization.',
    'tenant',
    true,
    true
  ),
  (
    'employee',
    'Employee',
    'Standard tenant user — operational modules without org administration.',
    'tenant',
    true,
    true
  ),
  (
    'operator',
    'Operator',
    'Warehouse / floor operator — WMS-focused access (default signup role).',
    'tenant',
    true,
    true
  )
ON CONFLICT (key) DO UPDATE SET
  name          = EXCLUDED.name,
  description   = EXCLUDED.description,
  scope         = EXCLUDED.scope,
  is_system     = EXCLUDED.is_system,
  is_assignable = EXCLUDED.is_assignable;

-- Legacy alias: `admin` text on profiles historically meant tenant admin.
INSERT INTO public.roles (key, name, description, scope, is_system, is_assignable)
VALUES (
  'admin',
  'Admin (legacy)',
  'Deprecated alias for tenant_admin — kept for backward-compatible joins.',
  'tenant',
  true,
  false
)
ON CONFLICT (key) DO UPDATE SET
  name          = EXCLUDED.name,
  description   = EXCLUDED.description,
  scope         = EXCLUDED.scope,
  is_system     = EXCLUDED.is_system,
  is_assignable = EXCLUDED.is_assignable;

-- ─────────────────────────────────────────────────────────────
-- 3. profiles.role_id FK
-- ─────────────────────────────────────────────────────────────

ALTER TABLE public.profiles
  ADD COLUMN IF NOT EXISTS role_id uuid REFERENCES public.roles (id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_profiles_role_id ON public.profiles (role_id);

COMMENT ON COLUMN public.profiles.role_id IS
  'FK to public.roles.id — preferred source of truth for the user role key.';

-- ─────────────────────────────────────────────────────────────
-- 4. Backfill role_id from legacy profiles.role text
-- ─────────────────────────────────────────────────────────────

-- Legacy `profiles.role = 'admin'` means tenant admin — point at tenant_admin, not the `admin` alias row.
UPDATE public.profiles p
SET role_id = r.id
FROM public.roles r
WHERE r.key = 'tenant_admin'
  AND p.role_id IS NULL
  AND lower(trim(p.role)) = 'admin';

UPDATE public.profiles p
SET role_id = r.id
FROM public.roles r
WHERE p.role_id IS NULL
  AND lower(trim(p.role)) = r.key;

-- ─────────────────────────────────────────────────────────────
-- 5. Widen legacy profiles.role CHECK (text fallback)
-- ─────────────────────────────────────────────────────────────

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM   pg_constraint
    WHERE  conrelid = 'public.profiles'::regclass
    AND    contype  = 'c'
    AND    conname  = 'profiles_role_check'
  ) THEN
    ALTER TABLE public.profiles DROP CONSTRAINT profiles_role_check;
  END IF;
END $$;

ALTER TABLE public.profiles
  ADD CONSTRAINT profiles_role_check
  CHECK (
    role IN (
      'super_admin',
      'system_admin',
      'system_employee',
      'programmer',
      'customer_service',
      'tenant_admin',
      'admin',
      'employee',
      'operator'
    )
  );

COMMENT ON COLUMN public.profiles.role IS
  'Legacy role text (fallback). Prefer profiles.role_id → roles.key. '
  'Allowed values mirror assignable role keys plus legacy admin.';

-- ─────────────────────────────────────────────────────────────
-- 6. Auth trigger — default role_id to operator
--    (matches 20260417 team_groups + widened role set)
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.handle_new_auth_user()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  op_id uuid;
BEGIN
  SELECT id INTO op_id FROM public.roles WHERE key = 'operator' LIMIT 1;

  INSERT INTO public.profiles (
    id,
    full_name,
    role,
    organization_id,
    team_groups,
    role_id,
    created_at
  )
  VALUES (
    NEW.id,
    COALESCE(
      NEW.raw_user_meta_data->>'full_name',
      NEW.raw_user_meta_data->>'name',
      split_part(NEW.email, '@', 1)
    ),
    'operator',
    '00000000-0000-0000-0000-000000000001'::uuid,
    '[]'::jsonb,
    op_id,
    NOW()
  )
  ON CONFLICT (id) DO NOTHING;
  RETURN NEW;
END;
$$;

-- ─────────────────────────────────────────────────────────────
-- 7. RLS — authenticated users can read the roles catalog
--    (assignment UIs, joins). Writes remain service-role / migrations.
-- ─────────────────────────────────────────────────────────────

ALTER TABLE public.roles ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "roles_select_authenticated" ON public.roles;

CREATE POLICY "roles_select_authenticated"
  ON public.roles
  FOR SELECT
  TO authenticated
  USING (true);

GRANT SELECT ON public.roles TO authenticated;
GRANT SELECT ON public.roles TO service_role;

-- ─────────────────────────────────────────────────────────────
-- 8. PostgREST schema reload
-- ─────────────────────────────────────────────────────────────

NOTIFY pgrst, 'reload schema';
