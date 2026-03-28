/**
 * PostgREST column lists for `returns` queries.
 * Lives outside `actions.ts` because `"use server"` modules may only export async functions (Next.js 16+).
 */
export const RETURN_SELECT =
  "id,organization_id,lpn,marketplace,item_name,asin,fnsku,sku,product_identifier,conditions,status,notes,photo_evidence,expiration_date,batch_number,photo_item_url,photo_expiry_url,photo_return_label_url,store_id,stores(name,platform),pallet_id,package_id,order_id,estimated_value,created_by,created_by_id,updated_by,updated_by_id,created_at,updated_at";

/**
 * Columns embedded from `returns` on `claim_submissions` joins (Claim Engine / PDF).
 * Explicit list only — never `returns(*)` — so PostgREST does not request a non-existent
 * `product_identifier` column. Uses `asin`, `fnsku`, `sku`, and `conditions` (not `condition`).
 * Add `photo_urls` here only if that column exists in your `returns` table.
 */
export const RETURNS_CLAIM_EMBED =
  "id,asin,fnsku,sku,estimated_value,store_id,conditions,status,item_name,photo_item_url,photo_expiry_url,stores(name,platform)";
