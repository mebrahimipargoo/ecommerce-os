import { NextResponse } from "next/server";
import { assertUserCanAccessOrganization } from "../../../dashboard/products/pim-actions";
import { supabaseServer } from "../../../../lib/supabase-server";
import { isUuidString } from "../../../../lib/uuid";

function slugify(name: string): string {
  const s = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 120);
  return s || "category";
}

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
    .from("product_categories")
    .select("id, name, slug, parent_id, created_at, updated_at")
    .eq("organization_id", organizationId)
    .order("name", { ascending: true });
  if (error) {
    return NextResponse.json({ ok: false, error: error.message }, { status: 400 });
  }
  const categories = (data ?? []) as { id: string; name: string; slug?: string | null; parent_id?: string | null }[];

  if (!includeCounts || !isUuidString(storeId)) {
    return NextResponse.json({ ok: true, categories });
  }

  const { data: prows, error: pErr } = await supabaseServer
    .from("products")
    .select("category_id")
    .eq("organization_id", organizationId)
    .eq("store_id", storeId)
    .not("category_id", "is", null);
  if (pErr) {
    return NextResponse.json({ ok: false, error: pErr.message }, { status: 400 });
  }
  const byCat = new Map<string, number>();
  for (const r of prows ?? []) {
    const cid = String((r as { category_id?: string }).category_id ?? "");
    if (!cid) continue;
    byCat.set(cid, (byCat.get(cid) ?? 0) + 1);
  }

  const enriched = categories.map((c) => ({
    ...c,
    product_count: byCat.get(c.id) ?? 0,
  }));

  return NextResponse.json({ ok: true, categories: enriched });
}

type PostBody = { organization_id?: string; name?: string; slug?: string | null; parent_id?: string | null };

export async function POST(req: Request) {
  let body: PostBody;
  try {
    body = (await req.json()) as PostBody;
  } catch {
    return NextResponse.json({ ok: false, error: "Invalid JSON body." }, { status: 400 });
  }
  const organizationId = String(body.organization_id ?? "").trim();
  const name = String(body.name ?? "").trim();
  const slugRaw = body.slug != null ? String(body.slug).trim() : "";
  const parentRaw = body.parent_id != null ? String(body.parent_id).trim() : "";
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
  let parentId: string | null = null;
  if (parentRaw) {
    if (!isUuidString(parentRaw)) {
      return NextResponse.json({ ok: false, error: "parent_id must be a UUID when set." }, { status: 400 });
    }
    const { data: par } = await supabaseServer
      .from("product_categories")
      .select("id")
      .eq("id", parentRaw)
      .eq("organization_id", organizationId)
      .maybeSingle();
    if (!par) {
      return NextResponse.json({ ok: false, error: "parent_id not found in this organization." }, { status: 400 });
    }
    parentId = parentRaw;
  }
  const slug = slugRaw || slugify(name);
  const { data, error } = await supabaseServer
    .from("product_categories")
    .insert({ organization_id: organizationId, name, slug, parent_id: parentId })
    .select("id, name, slug, parent_id, created_at, updated_at")
    .single();
  if (error) {
    return NextResponse.json({ ok: false, error: error.message }, { status: 400 });
  }
  return NextResponse.json({ ok: true, category: data });
}
