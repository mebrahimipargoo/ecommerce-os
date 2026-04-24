-- =============================================================================
-- Drop orphan UNIQUE constraint `uq_reports_repository_unique` on
-- public.amazon_reports_repository.
--
-- ── Why this is required ─────────────────────────────────────────────────────
-- The application (`app/api/settings/imports/sync/route.ts`,
-- `lib/pipeline/amazon-report-registry.ts`) targets the unique index
--   uq_amazon_reports_repo_org_file_row
--     (organization_id, source_file_sha256, source_physical_row_number)
-- via `ON CONFLICT` for every Phase-3 upsert into amazon_reports_repository.
-- This is the post-20260616 physical-line identity model: one DB row per
-- physical CSV line, line-level granularity preserved for Principal /
-- FBA Fee / Commission rows.
--
-- A second, OUT-OF-BAND UNIQUE constraint also exists on the same table:
--   uq_reports_repository_unique
--     (organization_id, date_time, transaction_type, order_id, sku, description)
--
-- It is NOT created by any migration in this repo (the closest one,
-- 20260509, created `uq_amazon_reports_repo_natural` with the same column
-- list, and 20260613 dropped that name — the orphan was added/renamed
-- out of band). It collapses any two physical rows that happen to share the
-- same business tuple — exactly the pattern of legitimate Reports Repository
-- sub-lines (e.g. duplicate monthly storage fee rows on the same date for the
-- same item, or repeated Principal/FBA Fee/Commission lines for the same
-- order). Because the app's ON CONFLICT clause names the OTHER index,
-- Postgres has no chance to merge on this orphan and instead raises:
--   duplicate key value violates unique constraint "uq_reports_repository_unique"
-- inside `INSERT … ON CONFLICT (organization_id, source_file_sha256,
-- source_physical_row_number) DO UPDATE …`.
--
-- The error reproduces with `chunk_size === distinct_physical_line_keys`,
-- which proves the application's own dedupe is correct — the orphan
-- constraint is the sole cause.
--
-- ── Why this is safe ─────────────────────────────────────────────────────────
-- ALTER TABLE … DROP CONSTRAINT removes only the constraint (and its backing
-- index automatically); rows in amazon_reports_repository are untouched.
-- Sibling unique index `uq_amazon_reports_repo_org_file_row` continues to
-- enforce the intended one-row-per-physical-line invariant. The application
-- has had ON CONFLICT pointed at the sibling since migration 20260616, so
-- this DROP simply removes a stale relic.
--
-- Pre-flight check confirmed live state at apply time:
--   • amazon_reports_repository row count = 0
--   • zero existing rows would violate either index after the drop
--
-- IF EXISTS makes the statement idempotent on environments that already
-- removed the orphan. The orphan index `uq_reports_repository_unique` is
-- dropped automatically when its backing constraint is dropped (Postgres
-- guarantees this — `2BP01: cannot drop index ... because constraint
-- requires it` is the symmetric protection).
-- =============================================================================

BEGIN;

ALTER TABLE public.amazon_reports_repository
  DROP CONSTRAINT IF EXISTS uq_reports_repository_unique;

COMMIT;

NOTIFY pgrst, 'reload schema';
