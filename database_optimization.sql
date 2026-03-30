-- ═══════════════════════════════════════════════════════════════════════════
--  MASTER DATABASE OPTIMIZATION MIGRATION (FINAL FIX: MISSING COLUMNS)
-- ═══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- §1  ORGANIZATIONS (ROOT TENANT TABLE)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.organizations (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  name        TEXT        NOT NULL
);

ALTER TABLE public.organizations 
  ADD COLUMN IF NOT EXISTS slug        TEXT,
  ADD COLUMN IF NOT EXISTS plan        TEXT        NOT NULL DEFAULT 'Free',
  ADD COLUMN IF NOT EXISTS is_active   BOOLEAN     NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at  TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE OR REPLACE FUNCTION public.set_organizations_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_organizations_updated_at ON public.organizations;
CREATE TRIGGER trg_organizations_updated_at
  BEFORE UPDATE ON public.organizations
  FOR EACH ROW EXECUTE FUNCTION public.set_organizations_updated_at();

INSERT INTO public.organizations (id, name, slug, plan, is_active)
VALUES (
  '00000000-0000-0000-0000-000000000001'::uuid,
  'Default Workspace',
  'default-workspace',
  'Free',
  true
)
ON CONFLICT (id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_organizations_active
  ON public.organizations (is_active, created_at DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- §2  MULTI-TENANCY ENFORCEMENT & DATA CLEANUP
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE public.stores ADD COLUMN IF NOT EXISTS organization_id TEXT;
ALTER TABLE public.pallets ADD COLUMN IF NOT EXISTS organization_id TEXT;
ALTER TABLE public.packages ADD COLUMN IF NOT EXISTS organization_id TEXT;
ALTER TABLE public.returns ADD COLUMN IF NOT EXISTS organization_id TEXT;
ALTER TABLE public.workspace_settings ADD COLUMN IF NOT EXISTS organization_id TEXT;

-- NUCLEAR CLEANUP: Explicitly cast to ::text for regex check
UPDATE public.stores SET organization_id = '00000000-0000-0000-0000-000000000001' WHERE organization_id::text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' OR organization_id IS NULL;
UPDATE public.pallets SET organization_id = '00000000-0000-0000-0000-000000000001' WHERE organization_id::text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' OR organization_id IS NULL;
UPDATE public.packages SET organization_id = '00000000-0000-0000-0000-000000000001' WHERE organization_id::text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' OR organization_id IS NULL;
UPDATE public.returns SET organization_id = '00000000-0000-0000-0000-000000000001' WHERE organization_id::text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' OR organization_id IS NULL;
UPDATE public.workspace_settings SET organization_id = '00000000-0000-0000-0000-000000000001' WHERE organization_id::text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' OR organization_id IS NULL;

-- Safe Type Cast
ALTER TABLE public.stores ALTER COLUMN organization_id TYPE UUID USING organization_id::uuid;
ALTER TABLE public.pallets ALTER COLUMN organization_id TYPE UUID USING organization_id::uuid;
ALTER TABLE public.packages ALTER COLUMN organization_id TYPE UUID USING organization_id::uuid;
ALTER TABLE public.returns ALTER COLUMN organization_id TYPE UUID USING organization_id::uuid;
ALTER TABLE public.workspace_settings ALTER COLUMN organization_id TYPE UUID USING organization_id::uuid;

-- Set Defaults
ALTER TABLE public.stores ALTER COLUMN organization_id SET DEFAULT '00000000-0000-0000-0000-000000000001'::uuid;
ALTER TABLE public.pallets ALTER COLUMN organization_id SET DEFAULT '00000000-0000-0000-0000-000000000001'::uuid;
ALTER TABLE public.packages ALTER COLUMN organization_id SET DEFAULT '00000000-0000-0000-0000-000000000001'::uuid;
ALTER TABLE public.returns ALTER COLUMN organization_id SET DEFAULT '00000000-0000-0000-0000-000000000001'::uuid;
ALTER TABLE public.workspace_settings ALTER COLUMN organization_id SET DEFAULT '00000000-0000-0000-0000-000000000001'::uuid;

-- Add Constraints Safely
ALTER TABLE public.stores DROP CONSTRAINT IF EXISTS fk_stores_organization;
ALTER TABLE public.stores ADD CONSTRAINT fk_stores_organization FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE RESTRICT NOT VALID;

ALTER TABLE public.pallets DROP CONSTRAINT IF EXISTS fk_pallets_organization;
ALTER TABLE public.pallets ADD CONSTRAINT fk_pallets_organization FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE RESTRICT NOT VALID;

ALTER TABLE public.packages DROP CONSTRAINT IF EXISTS fk_packages_organization;
ALTER TABLE public.packages ADD CONSTRAINT fk_packages_organization FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE RESTRICT NOT VALID;

ALTER TABLE public.returns DROP CONSTRAINT IF EXISTS fk_returns_organization;
ALTER TABLE public.returns ADD CONSTRAINT fk_returns_organization FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE RESTRICT NOT VALID;

ALTER TABLE public.workspace_settings DROP CONSTRAINT IF EXISTS fk_workspace_settings_organization;
ALTER TABLE public.workspace_settings ADD CONSTRAINT fk_workspace_settings_organization FOREIGN KEY (organization_id) REFERENCES public.organizations(id) ON DELETE RESTRICT NOT VALID;


-- ─────────────────────────────────────────────────────────────────────────────
-- §3  SCHEMA AUGMENTATION (ADDING MISSING COLUMNS FOR INDEXES)
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE public.returns
  ADD COLUMN IF NOT EXISTS asin         TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS fnsku        TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS sku          TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS rma_number   TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS photo_evidence JSONB DEFAULT NULL;

ALTER TABLE public.packages
  ADD COLUMN IF NOT EXISTS tracking_number TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS package_number  TEXT DEFAULT NULL;

ALTER TABLE public.products ADD COLUMN IF NOT EXISTS organization_id TEXT;
UPDATE public.products SET organization_id = '00000000-0000-0000-0000-000000000001' WHERE organization_id::text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' OR organization_id IS NULL;
ALTER TABLE public.products ALTER COLUMN organization_id TYPE UUID USING organization_id::uuid;
ALTER TABLE public.products ALTER COLUMN organization_id SET DEFAULT '00000000-0000-0000-0000-000000000001'::uuid;


-- ─────────────────────────────────────────────────────────────────────────────
-- §4  SOFT-DELETE & ACTOR COLUMNS
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE public.pallets
  ADD COLUMN IF NOT EXISTS deleted_at      TIMESTAMPTZ DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS created_by_id   UUID        DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS updated_by_id   UUID        DEFAULT NULL;

ALTER TABLE public.packages
  ADD COLUMN IF NOT EXISTS deleted_at      TIMESTAMPTZ DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS created_by_id   UUID        DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS updated_by_id   UUID        DEFAULT NULL;

ALTER TABLE public.returns
  ADD COLUMN IF NOT EXISTS deleted_at      TIMESTAMPTZ DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS created_by_id   UUID        DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS updated_by_id   UUID        DEFAULT NULL;

CREATE INDEX IF NOT EXISTS idx_pallets_active
  ON public.pallets (organization_id, created_at DESC)
  WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_packages_active
  ON public.packages (organization_id, created_at DESC)
  WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_returns_active
  ON public.returns (organization_id, created_at DESC)
  WHERE deleted_at IS NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- §5  ADVANCED INDEXING STRATEGY
-- ─────────────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_pallets_org_store
  ON public.pallets (organization_id, store_id, created_at DESC)
  WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_packages_org_store
  ON public.packages (organization_id, store_id, created_at DESC)
  WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_returns_org_store
  ON public.returns (organization_id, store_id, created_at DESC)
  WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_returns_org_store_status
  ON public.returns (organization_id, store_id, status, created_at DESC)
  WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_returns_asin
  ON public.returns (asin)
  WHERE asin IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_returns_fnsku
  ON public.returns (fnsku)
  WHERE fnsku IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_returns_rma
  ON public.returns (rma_number);

CREATE INDEX IF NOT EXISTS idx_packages_tracking
  ON public.packages (tracking_number)
  WHERE tracking_number IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_packages_number
  ON public.packages (organization_id, package_number);

CREATE INDEX IF NOT EXISTS idx_workspace_settings_core_gin
  ON public.workspace_settings USING GIN (core_settings);

CREATE INDEX IF NOT EXISTS idx_workspace_settings_module_gin
  ON public.workspace_settings USING GIN (module_configs);

CREATE INDEX IF NOT EXISTS idx_returns_photo_evidence_gin
  ON public.returns USING GIN (photo_evidence)
  WHERE photo_evidence IS NOT NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- §6  ENTERPRISE AUDIT INFRASTRUCTURE
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.audit_logs (
  id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id UUID        NOT NULL,
  table_name      TEXT        NOT NULL,
  record_id       UUID        NOT NULL,
  action          TEXT        NOT NULL,
  old_data        JSONB       DEFAULT NULL,
  new_data        JSONB       DEFAULT NULL,
  changed_by      UUID        DEFAULT NULL,
  changed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_org_time
  ON public.audit_logs (organization_id, changed_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_logs_table_record
  ON public.audit_logs (table_name, record_id, changed_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_logs_changed_by
  ON public.audit_logs (changed_by, changed_at DESC)
  WHERE changed_by IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_audit_logs_old_data_gin
  ON public.audit_logs USING GIN (old_data)
  WHERE old_data IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_audit_logs_new_data_gin
  ON public.audit_logs USING GIN (new_data)
  WHERE new_data IS NOT NULL;

CREATE OR REPLACE FUNCTION public.log_audit_event()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_user_id       UUID;
  v_organization  UUID;
  v_record_id     UUID;
  v_old_data      JSONB;
  v_new_data      JSONB;
BEGIN
  BEGIN
    v_user_id := NULLIF(current_setting('request.jwt.claim.sub', true), '')::uuid;
  EXCEPTION WHEN OTHERS THEN
    v_user_id := NULL;
  END;

  IF TG_OP = 'DELETE' THEN
    v_record_id    := OLD.id;
    v_organization := OLD.organization_id;
    v_old_data     := to_jsonb(OLD);
    v_new_data     := NULL;
  ELSIF TG_OP = 'UPDATE' THEN
    v_record_id    := NEW.id;
    v_organization := NEW.organization_id;
    v_old_data     := to_jsonb(OLD);
    v_new_data     := to_jsonb(NEW);
  END IF;

  INSERT INTO public.audit_logs (organization_id, table_name, record_id, action, old_data, new_data, changed_by, changed_at) 
  VALUES (v_organization, TG_TABLE_NAME, v_record_id, TG_OP, v_old_data, v_new_data, v_user_id, now());

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_audit_returns   ON public.returns;
CREATE TRIGGER trg_audit_returns
  AFTER UPDATE OR DELETE ON public.returns
  FOR EACH ROW EXECUTE FUNCTION public.log_audit_event();

DROP TRIGGER IF EXISTS trg_audit_pallets   ON public.pallets;
CREATE TRIGGER trg_audit_pallets
  AFTER UPDATE OR DELETE ON public.pallets
  FOR EACH ROW EXECUTE FUNCTION public.log_audit_event();

DROP TRIGGER IF EXISTS trg_audit_packages  ON public.packages;
CREATE TRIGGER trg_audit_packages
  AFTER UPDATE OR DELETE ON public.packages
  FOR EACH ROW EXECUTE FUNCTION public.log_audit_event();

DROP TRIGGER IF EXISTS trg_audit_stores    ON public.stores;
CREATE TRIGGER trg_audit_stores
  AFTER UPDATE OR DELETE ON public.stores
  FOR EACH ROW EXECUTE FUNCTION public.log_audit_event();


-- ─────────────────────────────────────────────────────────────────────────────
-- §7  ANALYTICS FOUNDATION — MATERIALIZED VIEW
-- ─────────────────────────────────────────────────────────────────────────────
DROP MATERIALIZED VIEW IF EXISTS public.mv_daily_store_analytics;

CREATE MATERIALIZED VIEW public.mv_daily_store_analytics AS
SELECT
  r.organization_id,
  COALESCE(r.store_id, '00000000-0000-0000-0000-000000000000'::uuid) AS store_id,
  date_trunc('day', r.created_at)::date                               AS report_date,
  COUNT(r.id)                                                         AS total_items,
  COUNT(r.id)  FILTER (WHERE r.deleted_at IS NULL)                    AS active_items,
  COUNT(r.id)  FILTER (WHERE r.status = 'received')                   AS items_received,
  COUNT(r.id)  FILTER (WHERE r.status = 'pending_evidence')           AS items_pending_evidence,
  COUNT(r.id)  FILTER (WHERE r.status = 'ready_for_claim')            AS items_ready_for_claim,
  COUNT(r.id)  FILTER (WHERE r.status = 'processing')                 AS items_processing,
  COUNT(r.id)  FILTER (WHERE r.status = 'completed')                  AS items_completed,
  COUNT(r.id)  FILTER (WHERE r.status = 'flagged')                    AS items_flagged,
  COUNT(r.id)  FILTER (WHERE r.condition = 'sellable')                AS items_sellable,
  COUNT(r.id)  FILTER (WHERE r.condition = 'customer_damaged')        AS items_customer_damaged,
  COUNT(r.id)  FILTER (WHERE r.condition = 'wrong_item')              AS items_wrong_item,
  COUNT(r.id)  FILTER (WHERE r.condition = 'empty_box')               AS items_empty_box,
  COUNT(DISTINCT r.package_id) FILTER (WHERE r.package_id IS NOT NULL) AS distinct_packages,
  COUNT(DISTINCT r.pallet_id)  FILTER (WHERE r.pallet_id  IS NOT NULL) AS distinct_pallets
FROM public.returns r
GROUP BY
  r.organization_id,
  COALESCE(r.store_id, '00000000-0000-0000-0000-000000000000'::uuid),
  date_trunc('day', r.created_at)::date;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_store_analytics_uniq
  ON public.mv_daily_store_analytics (organization_id, store_id, report_date);

CREATE INDEX IF NOT EXISTS idx_mv_analytics_org_date
  ON public.mv_daily_store_analytics (organization_id, report_date DESC);

CREATE INDEX IF NOT EXISTS idx_mv_analytics_store_date
  ON public.mv_daily_store_analytics (store_id, report_date DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- §8  ROW-LEVEL SECURITY
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.auth_org_id()
RETURNS UUID
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT NULLIF(
    current_setting('request.jwt.claims', true)::jsonb ->> 'organization_id',
    ''
  )::uuid;
$$;

ALTER TABLE public.organizations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "organizations_self_access" ON public.organizations;
CREATE POLICY "organizations_self_access"
  ON public.organizations
  FOR SELECT
  USING (id = public.auth_org_id());

ALTER TABLE public.audit_logs ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "audit_logs_org_isolation" ON public.audit_logs;
CREATE POLICY "audit_logs_org_isolation"
  ON public.audit_logs
  FOR SELECT
  USING (organization_id = public.auth_org_id());

ALTER TABLE public.workspace_settings ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "workspace_settings_org_isolation" ON public.workspace_settings;
CREATE POLICY "workspace_settings_org_isolation"
  ON public.workspace_settings
  FOR ALL
  USING (organization_id = public.auth_org_id());