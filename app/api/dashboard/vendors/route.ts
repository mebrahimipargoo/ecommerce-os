import { NextResponse } from "next/server";
import { assertUserCanAccessOrganization } from "../../../dashboard/products/pim-actions";
import { supabaseServer } from "../../../../lib/supabase-server";
import { isUuidString } from "../../../../lib/uuid";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const organizationId = String(url.searchParams.get("organization_id") ?? "").trim();
  const storeId = String(url.searchParams.get("store_id") ?? "").trim();
  const includeCounts = url.searchParams.get("include_counts") === "1" || url.searchParams.get("include_counts") === "true";
  if (!isUuidString(organizationId)) {
    return NextResponse.json({ ok: false, error: "organization_id must be a UUID." }, { status: 400 });
  }
  const gate = await assertUserCanAccessOrganization(organizationId);
  if (!gate.ok) {
    return NextResponse.json(
      { ok: false, error: gate.error },
      { status: gate.error === "Not signed in." ? 401 : 403 },
    );
  }
  const { data, error } = await supabaseServer
    .from("vendors")
    .select("id, name, created_at, updated_at")
    .eq("organization_id", organizationId)
    .order("name", { ascending: true });
  if (error) {
    return NextResponse.json({ ok: false, error: error.message }, { status: 400 });
  }
  const vendors = (data ?? []) as { id: string; name: string; created_at?: string; updated_at?: string }[];

  if (!includeCounts || !isUuidString(storeId)) {
    return NextResponse.json({ ok: true, vendors });
  }

  const { data: prows, error: pErr } = await supabaseServer
    .from("products")
    .select("vendor_id, asin, main_image_url, amazon_raw, category_id, status")
    .eq("organization_id", organizationId)
    .eq("store_id", storeId);
  if (pErr) {
    return NextResponse.json({ ok: false, error: pErr.message }, { status: 400 });
  }

  type Agg = { n: number; missing_asin: number; missing_image: number; missing_category: number; active_status: number };
  const aggBy = new Map<string, Agg>();
  const active = (s: string) => {
    const x = s.trim().toLowerCase();
    return x === "active" || x === "enabled" || x === "live";
  };
  const hasImage = (row: { main_image_url?: string | null; amazon_raw?: unknown }) => {
    const m = row.main_image_url?.trim();
    if (m) return true;
    const raw = row.amazon_raw;
    if (!raw || typeof raw !== "object") return false;
    const o = raw as Record<string, unknown>;
    const u =
      (typeof o.main_image_url === "string" && o.main_image_url.trim()) ||
      (typeof o.mainImageUrl === "string" && o.mainImageUrl.trim());
    return Boolean(u);
  };

  for (const r of prows ?? []) {
    const vid = String((r as { vendor_id?: string | null }).vendor_id ?? "");
    if (!vid) continue;
    let a = aggBy.get(vid);
    if (!a) a = { n: 0, missing_asin: 0, missing_image: 0, missing_category: 0, active_status: 0 };
    a.n += 1;
    const asin = String((r as { asin?: string | null }).asin ?? "").trim();
    if (!asin) a.missing_asin += 1;
    if (!hasImage(r as { main_image_url?: string; amazon_raw?: unknown })) a.missing_image += 1;
    if (!(r as { category_id?: string | null }).category_id) a.missing_category += 1;
    const st = String((r as { status?: string | null }).status ?? "").trim();
    if (st && active(st)) a.active_status += 1;
    aggBy.set(vid, a);
  }

  const enriched = vendors.map((v) => {
    const a = aggBy.get(v.id);
    return {
      ...v,
      product_count: a?.n ?? 0,
      missing_asin: a?.missing_asin ?? 0,
      missing_image: a?.missing_image ?? 0,
      missing_category: a?.missing_category ?? 0,
      active_status_count: a?.active_status ?? 0,
    };
  });

  return NextResponse.json({ ok: true, vendors: enriched });
}

type PostBody = { organization_id?: string; name?: string };

export async function POST(req: Request) {
  let body: PostBody;
  try {
    body = (await req.json()) as PostBody;
  } catch {
    return NextResponse.json({ ok: false, error: "Invalid JSON body." }, { status: 400 });
  }
  const organizationId = String(body.organization_id ?? "").trim();
  const name = String(body.name ?? "").trim();
  if (!isUuidString(organizationId)) {
    return NextResponse.json({ ok: false, error: "Invalid organization_id." }, { status: 400 });
  }
  if (!name) {
    return NextResponse.json({ ok: false, error: "name is required." }, { status: 400 });
  }
  const gate = await assertUserCanAccessOrganization(organizationId);
  if (!gate.ok) {
    return NextResponse.json(
      { ok: false, error: gate.error },
      { status: gate.error === "Not signed in." ? 401 : 403 },
    );
  }
  const { data, error } = await supabaseServer
    .from("vendors")
    .insert({ organization_id: organizationId, name })
    .select("id, name, created_at, updated_at")
    .single();
  if (error) {
    return NextResponse.json({ ok: false, error: error.message }, { status: 400 });
  }
  return NextResponse.json({ ok: true, vendor: data });
}
