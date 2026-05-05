import { NextResponse } from "next/server";
import { assertUserCanAccessOrganization } from "../../../../dashboard/products/pim-actions";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

type PatchBody = { organization_id?: string; name?: string };

export async function PATCH(req: Request, ctx: { params: Promise<{ id: string }> }) {
  const { id: vendorId } = await ctx.params;
  if (!isUuidString(vendorId)) {
    return NextResponse.json({ ok: false, error: "Invalid vendor id." }, { status: 400 });
  }
  let body: PatchBody;
  try {
    body = (await req.json()) as PatchBody;
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
  const { data: existing, error: exErr } = await supabaseServer
    .from("vendors")
    .select("id, organization_id")
    .eq("id", vendorId)
    .maybeSingle();
  if (exErr || !existing || String((existing as { organization_id?: string }).organization_id) !== organizationId) {
    return NextResponse.json({ ok: false, error: "Vendor not found for this organization." }, { status: 404 });
  }
  const { data, error } = await supabaseServer
    .from("vendors")
    .update({ name, updated_at: new Date().toISOString() })
    .eq("id", vendorId)
    .select("id, name, created_at, updated_at")
    .single();
  if (error) {
    return NextResponse.json({ ok: false, error: error.message }, { status: 400 });
  }
  return NextResponse.json({ ok: true, vendor: data });
}

type DeleteBody = { organization_id?: string };

export async function DELETE(req: Request, ctx: { params: Promise<{ id: string }> }) {
  const { id: vendorId } = await ctx.params;
  if (!isUuidString(vendorId)) {
    return NextResponse.json({ ok: false, error: "Invalid vendor id." }, { status: 400 });
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
    .from("vendors")
    .select("id, organization_id")
    .eq("id", vendorId)
    .maybeSingle();
  if (exErr || !existing || String((existing as { organization_id?: string }).organization_id) !== organizationId) {
    return NextResponse.json({ ok: false, error: "Vendor not found for this organization." }, { status: 404 });
  }
  const { count, error: cntErr } = await supabaseServer
    .from("products")
    .select("id", { count: "exact", head: true })
    .eq("vendor_id", vendorId)
    .limit(1);
  if (!cntErr && (count ?? 0) > 0) {
    return NextResponse.json(
      { ok: false, error: "Vendor is still linked to products; clear vendor_id on those products first." },
      { status: 409 },
    );
  }
  const { error } = await supabaseServer.from("vendors").delete().eq("id", vendorId);
  if (error) {
    return NextResponse.json({ ok: false, error: error.message }, { status: 400 });
  }
  return NextResponse.json({ ok: true });
}
