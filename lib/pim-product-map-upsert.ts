import type { SupabaseClient } from "@supabase/supabase-js";

export type PimIdentifierMapPayload = {
  seller_sku: string | null;
  asin: string | null;
  fnsku: string | null;
  upc_code: string | null;
};

/**
 * Returns 409-style error message when another product already holds the same org+store+SKU bridge.
 */
export async function assertIdentifierMapSkuNotOwnedByOtherProduct(params: {
  supabase: SupabaseClient;
  organizationId: string;
  storeId: string;
  productId: string;
  sellerSku: string | null;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const sku = params.sellerSku?.trim();
  if (!sku) return { ok: true };
  const { data, error } = await params.supabase
    .from("product_identifier_map")
    .select("id, product_id")
    .eq("organization_id", params.organizationId)
    .eq("store_id", params.storeId)
    .eq("seller_sku", sku)
    .not("product_id", "is", null)
    .neq("product_id", params.productId)
    .limit(1)
    .maybeSingle();
  if (error) return { ok: false, error: error.message };
  if (data) {
    return {
      ok: false,
      error:
        "Another product already has this seller SKU in product_identifier_map for this store. Resolve the conflict before saving.",
    };
  }
  return { ok: true };
}

export async function upsertPrimaryIdentifierMapForPim(params: {
  supabase: SupabaseClient;
  organizationId: string;
  storeId: string;
  productId: string;
  identifiers: PimIdentifierMapPayload;
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const gate = await assertIdentifierMapSkuNotOwnedByOtherProduct({
    supabase: params.supabase,
    organizationId: params.organizationId,
    storeId: params.storeId,
    productId: params.productId,
    sellerSku: params.identifiers.seller_sku,
  });
  if (!gate.ok) return gate;

  const { data: existing, error: selErr } = await params.supabase
    .from("product_identifier_map")
    .select("id, product_id, seller_sku, asin, fnsku, upc_code, external_listing_id")
    .eq("organization_id", params.organizationId)
    .eq("store_id", params.storeId)
    .eq("product_id", params.productId)
    .order("is_primary", { ascending: false, nullsFirst: false })
    .order("last_seen_at", { ascending: false, nullsFirst: false })
    .limit(1)
    .maybeSingle();

  if (selErr) return { ok: false, error: selErr.message };

  const now = new Date().toISOString();
  const patch = {
    seller_sku: params.identifiers.seller_sku?.trim() || null,
    asin: params.identifiers.asin?.trim() || null,
    fnsku: params.identifiers.fnsku?.trim() || null,
    upc_code: params.identifiers.upc_code?.trim() || null,
    match_source: "pim_manual",
    source_report_type: "pim_ui",
    last_seen_at: now,
    updated_at: now,
  };

  if (existing?.id) {
    const prevPid = existing.product_id != null ? String(existing.product_id) : "";
    if (prevPid && prevPid !== params.productId) {
      return {
        ok: false,
        error: "Identifier map row is linked to a different product_id; refusing silent reassignment.",
      };
    }
    const { error: upErr } = await params.supabase.from("product_identifier_map").update(patch).eq("id", existing.id);
    if (upErr) return { ok: false, error: upErr.message };
    return { ok: true };
  }

  const externalListingId = `pim_manual:${params.productId}`;
  const insert = {
    organization_id: params.organizationId,
    store_id: params.storeId,
    product_id: params.productId,
    external_listing_id: externalListingId,
    is_primary: true,
    ...patch,
    first_seen_at: now,
    created_at: now,
  };

  const { error: insErr } = await params.supabase.from("product_identifier_map").insert(insert);
  if (insErr) {
    if (insErr.code === "23505") {
      return {
        ok: false,
        error: "Identifier map unique constraint conflict (duplicate external_listing_id or bridge key).",
      };
    }
    return { ok: false, error: insErr.message };
  }
  return { ok: true };
}
