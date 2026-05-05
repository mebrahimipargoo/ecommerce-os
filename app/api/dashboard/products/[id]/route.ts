import { NextResponse } from "next/server";
import { assertUserCanAccessOrganization } from "../../../../dashboard/products/pim-actions";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { upsertPrimaryIdentifierMapForPim } from "../../../../../lib/pim-product-map-upsert";
import { isUuidString } from "../../../../../lib/uuid";

function emptyToNull(v: unknown): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s : null;
}

function mergeMetadataPimNotes(prev: unknown, notes: string | null): Record<string, unknown> {
  const base =
    prev && typeof prev === "object" && !Array.isArray(prev) ? { ...(prev as Record<string, unknown>) } : {};
  const pimUi =
    base.pim_ui && typeof base.pim_ui === "object" && !Array.isArray(base.pim_ui)
      ? { ...(base.pim_ui as Record<string, unknown>) }
      : {};
  if (notes != null && notes.trim()) {
    pimUi.notes = notes.trim();
    pimUi.notes_updated_at = new Date().toISOString();
  } else {
    delete pimUi.notes;
    delete pimUi.notes_updated_at;
  }
  if (Object.keys(pimUi).length) base.pim_ui = pimUi;
  else delete base.pim_ui;
  return base;
}

export async function GET(req: Request, ctx: { params: Promise<{ id: string }> }) {
  const { id: productId } = await ctx.params;
  if (!isUuidString(productId)) {
    return NextResponse.json({ ok: false, error: "Invalid product id." }, { status: 400 });
  }
  const url = new URL(req.url);
  const organizationId = String(url.searchParams.get("organization_id") ?? "").trim();
  const storeId = String(url.searchParams.get("store_id") ?? "").trim();
  if (!isUuidString(organizationId) || !isUuidString(storeId)) {
    return NextResponse.json({ ok: false, error: "organization_id and store_id are required UUIDs." }, { status: 400 });
  }

  const gate = await assertUserCanAccessOrganization(organizationId);
  if (!gate.ok) {
    return NextResponse.json(
      { ok: false, error: gate.error },
      { status: gate.error === "Not signed in." ? 401 : 403 },
    );
  }

  const { data: product, error: pErr } = await supabaseServer
    .from("products")
    .select("*")
    .eq("id", productId)
    .eq("organization_id", organizationId)
    .eq("store_id", storeId)
    .maybeSingle();
  if (pErr || !product) {
    return NextResponse.json({ ok: false, error: "Product not found." }, { status: 404 });
  }

  const { data: maps, error: mErr } = await supabaseServer
    .from("product_identifier_map")
    .select("*")
    .eq("organization_id", organizationId)
    .eq("store_id", storeId)
    .eq("product_id", productId)
    .order("last_seen_at", { ascending: false, nullsFirst: false });
  if (mErr) {
    return NextResponse.json({ ok: false, error: mErr.message }, { status: 400 });
  }

  const { data: prices, error: prErr } = await supabaseServer
    .from("product_prices")
    .select("*")
    .eq("organization_id", organizationId)
    .eq("store_id", storeId)
    .eq("product_id", productId)
    .order("observed_at", { ascending: false });
  if (prErr) {
    return NextResponse.json({ ok: false, error: prErr.message }, { status: 400 });
  }

  const p = product as Record<string, unknown>;
  const sku = typeof p.sku === "string" ? p.sku.trim() : "";
  const asin = typeof p.asin === "string" ? p.asin.trim() : "";

  const catalogIds = new Set<string>();
  for (const m of maps ?? []) {
    const cid = (m as { catalog_product_id?: string | null }).catalog_product_id;
    if (cid && isUuidString(cid)) catalogIds.add(cid);
  }

  const catalogRows: Record<string, unknown>[] = [];
  const seenCatalog = new Set<string>();
  const pushCatalog = (rows: Record<string, unknown>[]) => {
    for (const row of rows) {
      const rid = String(row.id ?? "");
      if (!rid || seenCatalog.has(rid)) continue;
      seenCatalog.add(rid);
      catalogRows.push(row);
    }
  };
  if (sku && asin) {
    const { data: both } = await supabaseServer
      .from("catalog_products")
      .select("*")
      .eq("organization_id", organizationId)
      .eq("store_id", storeId)
      .match({ seller_sku: sku, asin })
      .order("last_seen_at", { ascending: false })
      .limit(50);
    pushCatalog((both ?? []) as Record<string, unknown>[]);
  }
  if (sku) {
    const { data: bySku } = await supabaseServer
      .from("catalog_products")
      .select("*")
      .eq("organization_id", organizationId)
      .eq("store_id", storeId)
      .eq("seller_sku", sku)
      .order("last_seen_at", { ascending: false })
      .limit(50);
    pushCatalog((bySku ?? []) as Record<string, unknown>[]);
  }
  if (asin) {
    const { data: byAsin } = await supabaseServer
      .from("catalog_products")
      .select("*")
      .eq("organization_id", organizationId)
      .eq("store_id", storeId)
      .eq("asin", asin)
      .order("last_seen_at", { ascending: false })
      .limit(50);
    pushCatalog((byAsin ?? []) as Record<string, unknown>[]);
  }
  if (catalogIds.size) {
    const { data: byId } = await supabaseServer.from("catalog_products").select("*").in("id", [...catalogIds]);
    pushCatalog((byId ?? []) as Record<string, unknown>[]);
  }

  return NextResponse.json({
    ok: true,
    product,
    product_identifier_map: maps ?? [],
    product_prices: prices ?? [],
    catalog_products: catalogRows,
  });
}

type PutBody = {
  organization_id?: string;
  store_id?: string;
  product_name?: string;
  brand?: string | null;
  vendor_name?: string | null;
  vendor_id?: string | null;
  category_id?: string | null;
  sku?: string;
  asin?: string | null;
  fnsku?: string | null;
  upc_code?: string | null;
  mfg_part_number?: string | null;
  status?: string | null;
  condition?: string | null;
  main_image_url?: string | null;
  notes?: string | null;
};

export async function PUT(req: Request, ctx: { params: Promise<{ id: string }> }) {
  const { id: productId } = await ctx.params;
  if (!isUuidString(productId)) {
    return NextResponse.json({ ok: false, error: "Invalid product id." }, { status: 400 });
  }
  let body: PutBody;
  try {
    body = (await req.json()) as PutBody;
  } catch {
    return NextResponse.json({ ok: false, error: "Invalid JSON body." }, { status: 400 });
  }

  const organizationId = String(body.organization_id ?? "").trim();
  const storeId = String(body.store_id ?? "").trim();
  if (!isUuidString(organizationId) || !isUuidString(storeId)) {
    return NextResponse.json({ ok: false, error: "organization_id and store_id must be UUIDs." }, { status: 400 });
  }

  const gate = await assertUserCanAccessOrganization(organizationId);
  if (!gate.ok) {
    return NextResponse.json(
      { ok: false, error: gate.error },
      { status: gate.error === "Not signed in." ? 401 : 403 },
    );
  }

  const { data: existing, error: exErr } = await supabaseServer
    .from("products")
    .select("id, metadata, sku")
    .eq("id", productId)
    .eq("organization_id", organizationId)
    .eq("store_id", storeId)
    .maybeSingle();
  if (exErr || !existing) {
    return NextResponse.json({ ok: false, error: "Product not found." }, { status: 404 });
  }

  const productName = String(body.product_name ?? "").trim();
  const skuRaw = String(body.sku ?? "").trim();
  if (!productName) {
    return NextResponse.json({ ok: false, error: "product_name is required." }, { status: 400 });
  }
  if (!skuRaw) {
    return NextResponse.json({ ok: false, error: "sku is required." }, { status: 400 });
  }

  let vendorId: string | null = null;
  const vendorIdRaw = body.vendor_id != null ? String(body.vendor_id).trim() : "";
  if (vendorIdRaw) {
    if (!isUuidString(vendorIdRaw)) {
      return NextResponse.json({ ok: false, error: "vendor_id must be a UUID when set." }, { status: 400 });
    }
    const { data: vrow } = await supabaseServer
      .from("vendors")
      .select("id, name")
      .eq("id", vendorIdRaw)
      .eq("organization_id", organizationId)
      .maybeSingle();
    if (!vrow) {
      return NextResponse.json({ ok: false, error: "vendor_id not found for this organization." }, { status: 400 });
    }
    vendorId = vendorIdRaw;
  }

  let categoryId: string | null = null;
  const categoryIdRaw = body.category_id != null ? String(body.category_id).trim() : "";
  if (categoryIdRaw) {
    if (!isUuidString(categoryIdRaw)) {
      return NextResponse.json({ ok: false, error: "category_id must be a UUID when set." }, { status: 400 });
    }
    const { data: crow } = await supabaseServer
      .from("product_categories")
      .select("id")
      .eq("id", categoryIdRaw)
      .eq("organization_id", organizationId)
      .maybeSingle();
    if (!crow) {
      return NextResponse.json({ ok: false, error: "category_id not found for this organization." }, { status: 400 });
    }
    categoryId = categoryIdRaw;
  }

  let vendorNameResolved = emptyToNull(body.vendor_name);
  if (vendorId) {
    const { data: vn } = await supabaseServer.from("vendors").select("name").eq("id", vendorId).maybeSingle();
    const n = String((vn as { name?: string } | null)?.name ?? "").trim();
    if (n) vendorNameResolved = n;
  }

  const metadata = mergeMetadataPimNotes((existing as { metadata?: unknown }).metadata, body.notes ?? null);

  const updatePayload: Record<string, unknown> = {
    product_name: productName,
    sku: skuRaw,
    brand: emptyToNull(body.brand),
    vendor_name: vendorNameResolved,
    vendor_id: vendorId,
    category_id: categoryId,
    asin: emptyToNull(body.asin),
    fnsku: emptyToNull(body.fnsku),
    upc_code: emptyToNull(body.upc_code),
    mfg_part_number: emptyToNull(body.mfg_part_number),
    status: emptyToNull(body.status),
    condition: emptyToNull(body.condition),
    main_image_url: emptyToNull(body.main_image_url),
    metadata,
    last_catalog_sync_at: new Date().toISOString(),
    last_seen_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };

  const { error: upErr } = await supabaseServer.from("products").update(updatePayload).eq("id", productId);
  if (upErr) {
    return NextResponse.json({ ok: false, error: upErr.message }, { status: 400 });
  }

  const mapRes = await upsertPrimaryIdentifierMapForPim({
    supabase: supabaseServer,
    organizationId,
    storeId,
    productId,
    identifiers: {
      seller_sku: skuRaw,
      asin: emptyToNull(body.asin),
      fnsku: emptyToNull(body.fnsku),
      upc_code: emptyToNull(body.upc_code),
    },
  });
  if (!mapRes.ok) {
    return NextResponse.json({ ok: false, error: mapRes.error }, { status: 409 });
  }

  return NextResponse.json({ ok: true, id: productId });
}
