-- ─────────────────────────────────────────────────────────────────────────────
-- workspace_settings  (JSONB — scalable module config store)
-- core_settings  : general app-level settings (timezone, workspace name, etc.)
-- module_configs : per-module settings bag — add keys without schema migrations
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.workspace_settings (
  id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  core_settings   JSONB       NOT NULL DEFAULT '{}',
  module_configs  JSONB       NOT NULL DEFAULT '{}',
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Trigger to keep updated_at current
CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_workspace_settings_updated_at ON public.workspace_settings;
CREATE TRIGGER trg_workspace_settings_updated_at
  BEFORE UPDATE ON public.workspace_settings
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- Seed a default row with inventory FEFO thresholds
INSERT INTO public.workspace_settings (core_settings, module_configs)
VALUES (
  '{}',
  '{
    "inventory": {
      "fefo_critical_days": 30,
      "fefo_warning_days":  90
    }
  }'
)
ON CONFLICT DO NOTHING;
