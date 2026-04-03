/**
 * PostgREST selectors for `returns`, `packages`, and `pallets`.
 * Uses `*` for base table columns so dropped legacy columns (e.g. standalone photo_*, *_id actor
 * columns, expiry_date) never appear in the SELECT list — only columns that still exist are returned.
 * Lives outside `actions.ts` because `"use server"` modules may only export async functions (Next.js 16+).
 *
 * PERFORMANCE NOTE: Embedded aggregate counts like `returns(count)` and `packages(count)` are
 * executed by PostgREST as correlated subqueries (one per row) and will cause statement timeouts
 * on any non-trivial dataset.  Instead we rely on the denormalised count columns that are already
 * maintained by the DB (`actual_item_count` on packages, `item_count` on pallets).
 */

/** `returns` rows with store embed (FK `store_id` → `stores`) — used for insert/update/detail. */
export const RETURN_SELECT = "*,stores(name,platform)";

/**
 * List/detail reads for the Items tab — explicit columns + single `stores` embed (no `*`).
 * Avoids PostgREST edge cases where `*` plus embeds can fan out duplicate parent rows.
 */
export const RETURN_LIST_SELECT =
  "id, organization_id, lpn, rma_number, marketplace, item_name, " +
  "asin, fnsku, sku, product_identifier, " +
  "conditions, status, notes, photo_evidence, " +
  "expiration_date, batch_number, store_id, pallet_id, package_id, " +
  "order_id, " +
  "created_by, updated_by, created_at, updated_at, estimated_value, deleted_at, " +
  "stores(name,platform)";

/**
 * Same shape as `RETURN_SELECT`, for `claim_submissions` → `returns` FK embeds:
 * `select('*, returns(' + RETURN_SELECT + ')')`
 */
export const RETURNS_EMBED_SELECTOR = RETURN_SELECT;

/**
 * `packages` list rows — explicit columns (no `*`) to avoid heavy JSONB and dropped-column issues.
 * `photo_evidence` is loaded on demand in the package drawer when needed.
 */
export const PACKAGE_LIST_SELECT =
  "id, organization_id, package_number, tracking_number, carrier_name, rma_number, " +
  "expected_item_count, actual_item_count, pallet_id, status, discrepancy_note, " +
  "store_id, order_id, created_at, updated_at, created_by, updated_by, " +
  "photo_url, photo_return_label_url, photo_opened_url, photo_closed_url, " +
  "manifest_photo_url, deleted_at, " +
  "stores(name,platform)";

/** Same as list + `photo_evidence` — used for insert/update responses so clients receive gallery JSONB. */
export const PACKAGE_MUTATION_SELECT = PACKAGE_LIST_SELECT.replace(
  "stores(name,platform)",
  "photo_evidence,stores(name,platform)",
);

/**
 * `pallets` list rows — explicit columns; omit `photo_evidence` JSONB from list queries for performance.
 * Includes carrier_name and amazon_order_id for Pallet → Package → Item auto-fill inheritance.
 */
export const PALLET_LIST_SELECT =
  "id, organization_id, pallet_number, tracking_number, notes, status, item_count, " +
  "carrier_name, amazon_order_id, " +
  "created_at, updated_at, created_by, updated_by, store_id, " +
  "photo_url, bol_photo_url, manifest_photo_url, deleted_at, " +
  "stores(name,platform)";

/** Insert/update/select-one pallets — same columns as list (`photo_url`, `bol_photo_url`, no JSONB). */
export const PALLET_MUTATION_SELECT = PALLET_LIST_SELECT;
