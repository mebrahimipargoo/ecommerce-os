-- Default store for workspace (barcode fallback, etc.) — FK to stores table.

ALTER TABLE public.organization_settings
  ADD COLUMN IF NOT EXISTS default_store_id uuid REFERENCES public.stores (id) ON DELETE SET NULL;

COMMENT ON COLUMN public.organization_settings.default_store_id IS
  'Organization-wide default store (Settings → General). Used when no package/store match is found.';
