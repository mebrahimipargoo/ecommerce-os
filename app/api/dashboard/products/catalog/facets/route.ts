import { NextResponse } from "next/server";
import { assertUserCanAccessOrganization } from "../../../../../dashboard/products/pim-actions";
import { supabaseServer } from "../../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../../lib/uuid";

/** Distinct facet values for PIM filter dropdowns (org + store scoped). */
export async function GET(req: Request) {
  const url = new URL(req.url);
  const organizationId = String(url.searchParams.get("organization_id") ?? "").trim();
  const storeId = String(url.searchParams.get("store_id") ?? "").trim();
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

  const { data: brands, error: bErr } = await supabaseServer
    .from("products")
    .select("brand")
    .eq("organization_id", organizationId)
    .eq("store_id", storeId)
    .not("brand", "is", null);
  if (bErr) return NextResponse.json({ ok: false, error: bErr.message }, { status: 400 });
  const brandSet = new Set<string>();
  for (const r of brands ?? []) {
    const b = String((r as { brand?: unknown }).brand ?? "").trim();
    if (b) brandSet.add(b);
  }

  const { data: statuses, error: sErr } = await supabaseServer
    .from("products")
    .select("status")
    .eq("organization_id", organizationId)
    .eq("store_id", storeId)
    .not("status", "is", null);
  if (sErr) return NextResponse.json({ ok: false, error: sErr.message }, { status: 400 });
  const statusSet = new Set<string>();
  for (const r of statuses ?? []) {
    const s = String((r as { status?: unknown }).status ?? "").trim();
    if (s) statusSet.add(s);
  }

  const { data: ms, error: mErr } = await supabaseServer
    .from("product_identifier_map")
    .select("match_source")
    .eq("organization_id", organizationId)
    .eq("store_id", storeId)
    .not("match_source", "is", null);
  if (mErr) return NextResponse.json({ ok: false, error: mErr.message }, { status: 400 });
  const matchSources = new Set<string>();
  for (const r of ms ?? []) {
    const x = String((r as { match_source?: unknown }).match_source ?? "").trim();
    if (x) matchSources.add(x);
  }

  const { data: rt, error: rErr } = await supabaseServer
    .from("product_identifier_map")
    .select("source_report_type")
    .eq("organization_id", organizationId)
    .eq("store_id", storeId)
    .not("source_report_type", "is", null);
  if (rErr) return NextResponse.json({ ok: false, error: rErr.message }, { status: 400 });
  const reportTypes = new Set<string>();
  for (const r of rt ?? []) {
    const x = String((r as { source_report_type?: unknown }).source_report_type ?? "").trim();
    if (x) reportTypes.add(x);
  }

  return NextResponse.json({
    ok: true,
    facets: {
      brands: [...brandSet].sort((a, b) => a.localeCompare(b)),
      statuses: [...statusSet].sort((a, b) => a.localeCompare(b)),
      match_sources: [...matchSources].sort((a, b) => a.localeCompare(b)),
      source_report_types: [...reportTypes].sort((a, b) => a.localeCompare(b)),
    },
  });
}
