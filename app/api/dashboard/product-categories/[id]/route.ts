import { NextResponse } from "next/server";
import { assertUserCanAccessOrganization } from "../../../../dashboard/products/pim-actions";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

function slugify(name: string): string {
  const s = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 120);
  return s || "category";
}

type PatchBody = {
  organization_id?: string;
  name?: string;
  slug?: string | null;
  parent_id?: string | null;
};

export async function PATCH(req: Request, ctx: { params: Promise<{ id: string }> }) {
  const { id: categoryId } = await ctx.params;
  if (!isUuidString(categoryId)) {
    return NextResponse.json({ ok: false, error: "Invalid category id." }, { status: 400 });
  }
  let body: PatchBody;
  try {
    body = (await req.json()) as PatchBody;
  } catch {
    return NextResponse.json({ ok: false, error: "Invalid JSON body." }, { status: 400 });
  }
  const organizationId = String(body.organization_id ?? "").trim();
  const name = body.name != null ? String(body.name).trim() : "";
  const slugRaw = body.slug != null ? String(body.slug).trim() : "";
  const parentRaw = body.parent_id !== undefined ? (body.parent_id == null ? "" : String(body.parent_id).trim()) : undefined;
  if (!isUuidString(organizationId)) {
    return NextResponse.json({ ok: false, error: "Invalid organization_id." }, { status: 400 });
  }
  const gate = await assertUserCanAccessOrganization(organizationId);
  if (!gate.ok) {
    return NextResponse.json(
      { ok: false, error: gate.error },
      { status: gate.error === "Not signed in." ? 401 : 403 },
    );
  }
  const { data: existing, error: exErr } = await supabaseServer
    .from("product_categories")
    .select("id, organization_id, name, slug, parent_id")
    .eq("id", categoryId)
    .maybeSingle();
  if (exErr || !existing || String((existing as { organization_id?: string }).organization_id) !== organizationId) {
    return NextResponse.json({ ok: false, error: "Category not found for this organization." }, { status: 404 });
  }

  const ex = existing as { name?: string; slug?: string | null; parent_id?: string | null };
  const nextName = name || String(ex.name ?? "");
  if (!nextName) {
    return NextResponse.json({ ok: false, error: "name cannot be empty." }, { status: 400 });
  }
  const nextSlug = slugRaw || (body.name != null ? slugify(nextName) : String(ex.slug ?? slugify(nextName)));

  let parentId: string | null | undefined = undefined;
  if (parentRaw !== undefined) {
    if (!parentRaw) {
      parentId = null;
    } else {
      if (!isUuidString(parentRaw)) {
        return NextResponse.json({ ok: false, error: "parent_id must be a UUID when set." }, { status: 400 });
      }
      if (parentRaw === categoryId) {
        return NextResponse.json({ ok: false, error: "Category cannot be its own parent." }, { status: 400 });
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
  }

  const patch: Record<string, unknown> = {
    name: nextName,
    slug: nextSlug,
    updated_at: new Date().toISOString(),
  };
  if (parentId !== undefined) patch.parent_id = parentId;

  const { data, error } = await supabaseServer
    .from("product_categories")
    .update(patch)
    .eq("id", categoryId)
    .select("id, name, slug, parent_id, created_at, updated_at")
    .single();
  if (error) {
    return NextResponse.json({ ok: false, error: error.message }, { status: 400 });
  }
  return NextResponse.json({ ok: true, category: data });
}

type DeleteBody = { organization_id?: string };

export async function DELETE(req: Request, ctx: { params: Promise<{ id: string }> }) {
  const { id: categoryId } = await ctx.params;
  if (!isUuidString(categoryId)) {
    return NextResponse.json({ ok: false, error: "Invalid category id." }, { status: 400 });
  }
  let body: DeleteBody;
  try {
    body = (await req.json()) as DeleteBody;
  } catch {
    return NextResponse.json({ ok: false, error: "Invalid JSON body." }, { status: 400 });
  }
  const organizationId = String(body.organization_id ?? "").trim();
  if (!isUuidString(organizationId)) {
    return NextResponse.json({ ok: false, error: "Invalid organization_id." }, { status: 400 });
  }
  const gate = await assertUserCanAccessOrganization(organizationId);
  if (!gate.ok) {
    return NextResponse.json(
      { ok: false, error: gate.error },
      { status: gate.error === "Not signed in." ? 401 : 403 },
    );
  }
  const { data: existing, error: exErr } = await supabaseServer
    .from("product_categories")
    .select("id, organization_id")
    .eq("id", categoryId)
    .maybeSingle();
  if (exErr || !existing || String((existing as { organization_id?: string }).organization_id) !== organizationId) {
    return NextResponse.json({ ok: false, error: "Category not found for this organization." }, { status: 404 });
  }
  const { count: childCount } = await supabaseServer
    .from("product_categories")
    .select("id", { count: "exact", head: true })
    .eq("parent_id", categoryId)
    .limit(1);
  if ((childCount ?? 0) > 0) {
    return NextResponse.json(
      { ok: false, error: "Delete or reassign child categories before removing this one." },
      { status: 409 },
    );
  }
  const { count: prodCount } = await supabaseServer
    .from("products")
    .select("id", { count: "exact", head: true })
    .eq("category_id", categoryId)
    .limit(1);
  if ((prodCount ?? 0) > 0) {
    return NextResponse.json(
      { ok: false, error: "Category is still linked to products; clear category_id on those products first." },
      { status: 409 },
    );
  }
  const { error } = await supabaseServer.from("product_categories").delete().eq("id", categoryId);
  if (error) {
    return NextResponse.json({ ok: false, error: error.message }, { status: 400 });
  }
  return NextResponse.json({ ok: true });
}
