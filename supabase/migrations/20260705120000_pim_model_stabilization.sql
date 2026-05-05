-- PIM model stabilization: vendors, product_categories, product_prices;
-- additive columns on products / product_identifier_map;
-- optional store_id on Amazon domain tables that lacked it (no backfill — application + script).

BEGIN;

-- ── 1) vendors ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.vendors (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id   uuid NOT NULL REFERENCES public.organizations (id) ON DELETE CASCADE,
  name              text NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_vendors_org_lower_trim_name
  ON public.vendors (organization_id, lower(btrim(name)));

CREATE INDEX IF NOT EXISTS idx_vendors_organization_id
  ON public.vendors (organization_id);

COMMENT ON TABLE public.vendors IS
  'Vendor / supplier directory; products.vendor_id references this table.';

ALTER TABLE public.vendors ENABLE ROW LEVEL SECURITY;

CREATE POLICY IF NOT EXISTS "vendors: org members can select"
  ON public.vendors FOR SELECT
  USING (
    organization_id IN (SELECT organization_id FROM public.profiles WHERE id = auth.uid())
  );

CREATE POLICY IF NOT EXISTS "vendors: org members can insert"
  ON public.vendors FOR INSERT
  WITH CHECK (
    organization_id IN (SELECT organization_id FROM public.profiles WHERE id = auth.uid())
  );

CREATE POLICY IF NOT EXISTS "vendors: org members can update"
  ON public.vendors FOR UPDATE
  USING (
    organization_id IN (SELECT organization_id FROM public.profiles WHERE id = auth.uid())
  );

CREATE POLICY IF NOT EXISTS "vendors: org members can delete"
  ON public.vendors FOR DELETE
  USING (
    organization_id IN (SELECT organization_id FROM public.profiles WHERE id = auth.uid())
  );

CREATE POLICY IF NOT EXISTS "vendors: service role bypass"
  ON public.vendors AS PERMISSIVE FOR ALL TO service_role
  USING (true) WITH CHECK (true);

-- ── 2) product_categories ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.product_categories (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id   uuid NOT NULL REFERENCES public.organizations (id) ON DELETE CASCADE,
  name              text NOT NULL,
  slug              text,
  parent_id         uuid REFERENCES public.product_categories (id) ON DELETE SET NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_product_categories_org_lower_trim_name
  ON public.product_categories (organization_id, lower(btrim(name)));

CREATE INDEX IF NOT EXISTS idx_product_categories_organization_id
  ON public.product_categories (organization_id);

COMMENT ON TABLE public.product_categories IS
  'Per-organization product taxonomy; products.category_id references this table.';

ALTER TABLE public.product_categories ENABLE ROW LEVEL SECURITY;

CREATE POLICY IF NOT EXISTS "product_categories: org members can select"
  ON public.product_categories FOR SELECT
  USING (
    organization_id IN (SELECT organization_id FROM public.profiles WHERE id = auth.uid())
  );

CREATE POLICY IF NOT EXISTS "product_categories: org members can insert"
  ON public.product_categories FOR INSERT
  WITH CHECK (
    organization_id IN (SELECT organization_id FROM public.profiles WHERE id = auth.uid())
  );

CREATE POLICY IF NOT EXISTS "product_categories: org members can update"
  ON public.product_categories FOR UPDATE
  USING (
    organization_id IN (SELECT organization_id FROM public.profiles WHERE id = auth.uid())
  );

CREATE POLICY IF NOT EXISTS "product_categories: org members can delete"
  ON public.product_categories FOR DELETE
  USING (
    organization_id IN (SELECT organization_id FROM public.profiles WHERE id = auth.uid())
  );

CREATE POLICY IF NOT EXISTS "product_categories: service role bypass"
  ON public.product_categories AS PERMISSIVE FOR ALL TO service_role
  USING (true) WITH CHECK (true);

-- ── 3) product_prices (price history; not on products row) ───────────────────
CREATE TABLE IF NOT EXISTS public.product_prices (
  id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id    uuid NOT NULL REFERENCES public.organizations (id) ON DELETE CASCADE,
  store_id           uuid NOT NULL REFERENCES public.stores (id) ON DELETE CASCADE,
  product_id         uuid NOT NULL REFERENCES public.products (id) ON DELETE CASCADE,
  amount             numeric(18, 6) NOT NULL,
  currency           text NOT NULL DEFAULT 'USD',
  observed_at        timestamptz NOT NULL DEFAULT now(),
  source             text,
  source_upload_id   uuid REFERENCES public.raw_report_uploads (id) ON DELETE SET NULL,
  metadata           jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_product_prices_org_store_product_observed
  ON public.product_prices (organization_id, store_id, product_id, observed_at DESC);

COMMENT ON TABLE public.product_prices IS
  'Append-only price observations per org + store + product; latest row answers “current price”.';

ALTER TABLE public.product_prices ENABLE ROW LEVEL SECURITY;

CREATE POLICY IF NOT EXISTS "product_prices: org members can select"
  ON public.product_prices FOR SELECT
  USING (
    organization_id IN (SELECT organization_id FROM public.profiles WHERE id = auth.uid())
  );

CREATE POLICY IF NOT EXISTS "product_prices: org members can insert"
  ON public.product_prices FOR INSERT
  WITH CHECK (
    organization_id IN (SELECT organization_id FROM public.profiles WHERE id = auth.uid())
  );

CREATE POLICY IF NOT EXISTS "product_prices: org members can update"
  ON public.product_prices FOR UPDATE
  USING (
    organization_id IN (SELECT organization_id FROM public.profiles WHERE id = auth.uid())
  );

CREATE POLICY IF NOT EXISTS "product_prices: org members can delete"
  ON public.product_prices FOR DELETE
  USING (
    organization_id IN (SELECT organization_id FROM public.profiles WHERE id = auth.uid())
  );

CREATE POLICY IF NOT EXISTS "product_prices: service role bypass"
  ON public.product_prices AS PERMISSIVE FOR ALL TO service_role
  USING (true) WITH CHECK (true);

-- ── 4) products — PIM columns (additive) ─────────────────────────────────────
ALTER TABLE public.products
  ADD COLUMN IF NOT EXISTS vendor_id uuid REFERENCES public.vendors (id) ON DELETE SET NULL;

ALTER TABLE public.products
  ADD COLUMN IF NOT EXISTS category_id uuid REFERENCES public.product_categories (id) ON DELETE SET NULL;

ALTER TABLE public.products
  ADD COLUMN IF NOT EXISTS condition text;

ALTER TABLE public.products
  ADD COLUMN IF NOT EXISTS amazon_raw jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN public.products.amazon_raw IS
  'Raw Amazon-side payload snippets / SP-API blobs; not a substitute for normalized identifiers.';

CREATE INDEX IF NOT EXISTS idx_products_vendor_id
  ON public.products (vendor_id) WHERE vendor_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_products_category_id
  ON public.products (category_id) WHERE category_id IS NOT NULL;

-- ── 5) product_identifier_map — triage / ambiguity (no NOT NULL backfill here) ─
ALTER TABLE public.product_identifier_map
  ADD COLUMN IF NOT EXISTS resolution_notes text;

COMMENT ON COLUMN public.product_identifier_map.resolution_notes IS
  'Operator or importer notes when an identifier row is ambiguous or skipped re-pointing product_id.';

COMMENT ON COLUMN public.product_identifier_map.confidence_score IS
  'Canonical confidence for PIM / resolver (0–1 scale in practice).';

-- ── 6) Amazon tables: store_id column where missing (nullable until backfill) ─
DO $$
BEGIN
  IF to_regclass('public.amazon_transactions') IS NOT NULL THEN
    ALTER TABLE public.amazon_transactions
      ADD COLUMN IF NOT EXISTS store_id uuid REFERENCES public.stores (id) ON DELETE SET NULL;
    CREATE INDEX IF NOT EXISTS idx_amazon_transactions_org_store
      ON public.amazon_transactions (organization_id, store_id)
      WHERE store_id IS NOT NULL;
  END IF;

  IF to_regclass('public.amazon_reimbursements') IS NOT NULL THEN
    ALTER TABLE public.amazon_reimbursements
      ADD COLUMN IF NOT EXISTS store_id uuid REFERENCES public.stores (id) ON DELETE SET NULL;
    CREATE INDEX IF NOT EXISTS idx_amazon_reimbursements_org_store
      ON public.amazon_reimbursements (organization_id, store_id)
      WHERE store_id IS NOT NULL;
  END IF;

  IF to_regclass('public.amazon_safet_claims') IS NOT NULL THEN
    ALTER TABLE public.amazon_safet_claims
      ADD COLUMN IF NOT EXISTS store_id uuid REFERENCES public.stores (id) ON DELETE SET NULL;
    CREATE INDEX IF NOT EXISTS idx_amazon_safet_claims_org_store
      ON public.amazon_safet_claims (organization_id, store_id)
      WHERE store_id IS NOT NULL;
  END IF;

  IF to_regclass('public.amazon_returns') IS NOT NULL THEN
    ALTER TABLE public.amazon_returns
      ADD COLUMN IF NOT EXISTS store_id uuid REFERENCES public.stores (id) ON DELETE SET NULL;
    CREATE INDEX IF NOT EXISTS idx_amazon_returns_org_store
      ON public.amazon_returns (organization_id, store_id)
      WHERE store_id IS NOT NULL;
  END IF;

  IF to_regclass('public.amazon_inventory_ledger') IS NOT NULL THEN
    ALTER TABLE public.amazon_inventory_ledger
      ADD COLUMN IF NOT EXISTS store_id uuid REFERENCES public.stores (id) ON DELETE SET NULL;
    CREATE INDEX IF NOT EXISTS idx_amazon_inventory_ledger_org_store
      ON public.amazon_inventory_ledger (organization_id, store_id)
      WHERE store_id IS NOT NULL;
  END IF;
END $$;

NOTIFY pgrst, 'reload schema';

COMMIT;
