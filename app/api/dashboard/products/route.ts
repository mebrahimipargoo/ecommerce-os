import { randomUUID } from "crypto";
import { NextResponse } from "next/server";
import { assertUserCanAccessOrganization } from "../../../dashboard/products/pim-actions";
import { supabaseServer } from "../../../../lib/supabase-server";
import { upsertPrimaryIdentifierMapForPim } from "../../../../lib/pim-product-map-upsert";
import { isUuidString } from "../../../../lib/uuid";

type CreateProductBody = {
  organization_id?: string;
  store_id?: string;
  sku?: string;
  product_name?: string;
  brand?: string | null;
  vendor_name?: string | null;
  vendor_id?: string | null;
  category_id?: string | null;
  main_image_url?: string | null;
  mfg_part_number?: string | null;
  upc_code?: string | null;
  asin?: string | null;
  fnsku?: string | null;
  status?: string | null;
  condition?: string | null;
  notes?: string | null;
};

function emptyToNull(v: unknown): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s : null;
}

export async function POST(req: Request) {
  let body: CreateProductBody;
  try {
    body = (await req.json()) as CreateProductBody;
  } catch {
    return NextResponse.json({ ok: false, error: "Invalid JSON body." }, { status: 400 });
  }

  const organizationId = String(body.organization_id ?? "").trim();
  const productName = String(body.product_name ?? "").trim();
  const skuRaw = String(body.sku ?? "").trim();
  const storeIdRaw = String(body.store_id ?? "").trim();

  if (!isUuidString(organizationId)) {
    return NextResponse.json({ ok: false, error: "Invalid organization_id." }, { status: 400 });
  }
  if (!productName) {
    return NextResponse.json({ ok: false, error: "product_name is required." }, { status: 400 });
  }
  if (!skuRaw) {
    return NextResponse.json({ ok: false, error: "sku is required (per-store catalog identity)." }, { status: 400 });
  }

  const gate = await assertUserCanAccessOrganization(organizationId);
  if (!gate.ok) {
    return NextResponse.json(
      { ok: false, error: gate.error },
      { status: gate.error === "Not signed in." ? 401 : 403 },
    );
  }

  let storeId = storeIdRaw && isUuidString(storeIdRaw) ? storeIdRaw : "";
  if (!storeId) {
    const { data: os } = await supabaseServer
      .from("organization_settings")
      .select("default_store_id")
      .eq("organization_id", organizationId)
      .maybeSingle();
    const d = (os as { default_store_id?: string | null } | null)?.default_store_id;
    if (typeof d === "string" && isUuidString(d)) storeId = d;
  }
  if (!storeId) {
    return NextResponse.json(
      { ok: false, error: "store_id is required, or set a default store in Settings → General." },
      { status: 400 },
    );
  }

  const { data: storeRow, error: storeErr } = await supabaseServer
    .from("stores")
    .select("id, organization_id")
    .eq("id", storeId)
    .maybeSingle();
  if (storeErr || !storeRow || String((storeRow as { organization_id?: string }).organization_id) !== organizationId) {
    return NextResponse.json({ ok: false, error: "Store not found for this organization." }, { status: 400 });
  }

  const brand = emptyToNull(body.brand);
  const vendorName = emptyToNull(body.vendor_name);
  const vendorIdRaw = body.vendor_id != null ? String(body.vendor_id).trim() : "";
  const categoryIdRaw = body.category_id != null ? String(body.category_id).trim() : "";
  let vendorId: string | null = null;
  if (vendorIdRaw) {
    if (!isUuidString(vendorIdRaw)) {
      return NextResponse.json({ ok: false, error: "vendor_id must be a UUID when set." }, { status: 400 });
    }
    const { data: vrow, error: vErr } = await supabaseServer
      .from("vendors")
      .select("id")
      .eq("id", vendorIdRaw)
      .eq("organization_id", organizationId)
      .maybeSingle();
    if (vErr || !vrow) {
      return NextResponse.json({ ok: false, error: "vendor_id not found for this organization." }, { status: 400 });
    }
    vendorId = vendorIdRaw;
  }
  let categoryId: string | null = null;
  if (categoryIdRaw) {
    if (!isUuidString(categoryIdRaw)) {
      return NextResponse.json({ ok: false, error: "category_id must be a UUID when set." }, { status: 400 });
    }
    const { data: crow, error: cErr } = await supabaseServer
      .from("product_categories")
      .select("id")
      .eq("id", categoryIdRaw)
      .eq("organization_id", organizationId)
      .maybeSingle();
    if (cErr || !crow) {
      return NextResponse.json({ ok: false, error: "category_id not found for this organization." }, { status: 400 });
    }
    categoryId = categoryIdRaw;
  }
  let vendorNameOut = vendorName;
  if (vendorId) {
    const { data: vn } = await supabaseServer.from("vendors").select("name").eq("id", vendorId).maybeSingle();
    const n = String((vn as { name?: string } | null)?.name ?? "").trim();
    if (n) vendorNameOut = n;
  }
  const mainImageUrl = emptyToNull(body.main_image_url);
  const mfgPartNumber = emptyToNull(body.mfg_part_number);
  const upcCode = emptyToNull(body.upc_code);
  const asin = emptyToNull(body.asin);
  const fnsku = emptyToNull(body.fnsku);
  const status = emptyToNull(body.status);
  const condition = emptyToNull(body.condition);
  const notes = typeof body.notes === "string" ? body.notes.trim() : "";
  const metadata: Record<string, unknown> = {
    pim_ui: {
      seed_source: "manual",
      created_at: new Date().toISOString(),
      ...(notes ? { notes, notes_updated_at: new Date().toISOString() } : {}),
    },
  };

  const barcode = `pim-manual-${randomUUID()}`;

  const insertPayload: Record<string, unknown> = {
    organization_id: organizationId,
    store_id: storeId,
    sku: skuRaw,
    product_name: productName,
    brand,
    vendor_name: vendorNameOut,
    vendor_id: vendorId,
    category_id: categoryId,
    main_image_url: mainImageUrl,
    mfg_part_number: mfgPartNumber,
    upc_code: upcCode,
    asin,
    fnsku,
    barcode,
    status,
    condition,
    metadata,
    last_catalog_sync_at: new Date().toISOString(),
    last_seen_at: new Date().toISOString(),
  };

  const { data: row, error: pErr } = await supabaseServer
    .from("products")
    .insert(insertPayload)
    .select("id")
    .single();

  if (pErr) {
    return NextResponse.json({ ok: false, error: pErr.message }, { status: 400 });
  }

  const newId = row?.id as string;
  const mapRes = await upsertPrimaryIdentifierMapForPim({
    supabase: supabaseServer,
    organizationId,
    storeId,
    productId: newId,
    identifiers: {
      seller_sku: skuRaw,
      asin,
      fnsku,
      upc_code: upcCode,
    },
  });
  if (!mapRes.ok) {
    await supabaseServer.from("products").delete().eq("id", newId);
    return NextResponse.json({ ok: false, error: mapRes.error }, { status: 409 });
  }

  return NextResponse.json({ ok: true, id: newId });
}

type CatalogProductRow = Record<string, unknown>;

function relationMissing(msg: string, rel: string): boolean {
  const m = msg.toLowerCase();
  const r = rel.toLowerCase();
  return m.includes(r) && (m.includes("schema cache") || m.includes("does not exist") || m.includes("could not find"));
}

async function enrichVendorNamesFromTable(rows: CatalogProductRow[]): Promise<void> {
  if (!rows.length) return;
  const ids = new Set<string>();
  for (const r of rows) {
    const vid = r.vendor_id;
    if (vid != null && String(vid).trim() && !(typeof r.vendor_name === "string" && r.vendor_name.trim())) {
      ids.add(String(vid).trim());
    }
  }
  if (!ids.size) return;
  const { data: vendors, error } = await supabaseServer.from("vendors").select("id, name").in("id", [...ids]);
  if (error || !vendors?.length) return;
  const idToName = new Map<string, string>();
  for (const v of vendors as { id?: unknown; name?: unknown }[]) {
    if (v.id == null) continue;
    const name = typeof v.name === "string" ? v.name.trim() : "";
    if (name) idToName.set(String(v.id), name);
  }
  for (const r of rows) {
    if (typeof r.vendor_name === "string" && r.vendor_name.trim()) continue;
    const vid = r.vendor_id != null ? String(r.vendor_id).trim() : "";
    if (!vid) continue;
    const n = idToName.get(vid);
    if (n) r.vendor_name = n;
  }
}

/** Catalog Hub: list org products from Supabase (primary source; FastAPI optional). */
export async function GET(req: Request) {
  const url = new URL(req.url);
  const organizationId = String(url.searchParams.get("organization_id") ?? "").trim();
  if (!isUuidString(organizationId)) {
    return NextResponse.json(
      { status: "error", error: "organization_id query parameter must be a valid UUID." },
      { status: 400 },
    );
  }

  const gate = await assertUserCanAccessOrganization(organizationId);
  if (!gate.ok) {
    return NextResponse.json(
      { status: "error", error: gate.error },
      { status: gate.error === "Not signed in." ? 401 : 403 },
    );
  }

  const selectAttempts = [
    "*, product_identifier_map(*), product_prices(*)",
    "*, product_identifier_map(*)",
    "*, product_prices(*)",
    "id, product_name, brand, main_image_url, image_url, sku, asin, fnsku, upc_code, vendor_id, vendor_name, last_catalog_sync_at, updated_at, status, product_identifier_map(seller_sku, asin, fnsku)",
    "id, product_name, brand, main_image_url, image_url, sku, asin, fnsku, upc_code, vendor_id, vendor_name, last_catalog_sync_at, updated_at, status",
  ];

  let data: CatalogProductRow[] = [];
  let lastError: string | null = null;
  let gotRows = false;

  for (const sel of selectAttempts) {
    const { data: rows, error } = await supabaseServer
      .from("products")
      .select(sel)
      .eq("organization_id", organizationId)
      .order("updated_at", { ascending: false })
      .limit(5000);
    if (!error) {
      data = (rows ?? []) as unknown as CatalogProductRow[];
      gotRows = true;
      break;
    }
    lastError = error.message ?? "Query failed.";
    const msg = lastError.toLowerCase();
    if (relationMissing(msg, "product_prices") || relationMissing(msg, "product_identifier_map")) {
      continue;
    }
    return NextResponse.json({ status: "error", error: lastError }, { status: 400 });
  }

  if (!gotRows && lastError != null) {
    return NextResponse.json({ status: "error", error: lastError }, { status: 400 });
  }

  await enrichVendorNamesFromTable(data);

  return NextResponse.json({ status: "success", products: data });
}
