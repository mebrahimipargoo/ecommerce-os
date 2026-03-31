/**
 * PostgREST selectors for `returns`, `packages`, and `pallets`.
 * Uses `*` for base table columns so dropped legacy columns (e.g. standalone photo_*, *_id actor
 * columns, expiry_date) never appear in the SELECT list — only columns that still exist are returned.
 * Lives outside `actions.ts` because `"use server"` modules may only export async functions (Next.js 16+).
 */

/** `returns` rows with store embed (FK `store_id` → `stores`). */
export const RETURN_SELECT = "*,stores(name,platform)";

/**
 * Same shape as `RETURN_SELECT`, for `claim_submissions` → `returns` FK embeds:
 * `select('*, returns(' + RETURN_SELECT + ')')`
 */
export const RETURNS_EMBED_SELECTOR = RETURN_SELECT;

/** `packages` list/detail rows: store + live return count on the package. */
export const PACKAGE_LIST_SELECT = "*,stores(name,platform),returns(count)";

/** `pallets` list/detail rows: store + package count + return count on the pallet. */
export const PALLET_LIST_SELECT = "*,stores(name,platform),packages(count),returns(count)";
