/**
 * Guarded backfill: set nullable `store_id` on `product_identifier_map` and `products`
 * only when the target store is unambiguous from `raw_report_uploads.metadata`
 * (import_store_id / ledger_store_id) or from sibling identifier-map rows for the same product.
 *
 * **Never run in production without operator sign-off.** Prefer staging first.
 *
 * Requires:
 *   NEXT_PUBLIC_SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 *
 * Usage (dry-run — default, no writes):
 *   npx tsx scripts/backfill-pim-store-id.ts
 *   npx tsx scripts/backfill-pim-store-id.ts --organization-id=<uuid>
 *
 * Writes (explicit opt-in only):
 *   npx tsx scripts/backfill-pim-store-id.ts --i-understand-this-writes [--organization-id=<uuid>]
 */

import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { resolveImportStoreIdFromMetadata } from "../lib/raw-report-upload-metadata";
import { isUuidString } from "../lib/uuid";

const PAGE = 300;

function parseArgs(argv: string[]): {
  write: boolean;
  organizationId: string | null;
} {
  let write = false;
  let organizationId: string | null = null;
  for (const a of argv) {
    if (a === "--i-understand-this-writes") write = true;
    const m = a.match(/^--organization-id=(.+)$/);
    if (m) organizationId = m[1].trim() || null;
  }
  if (organizationId && !isUuidString(organizationId)) {
    throw new Error(`Invalid --organization-id (expected UUID): ${organizationId}`);
  }
  return { write, organizationId };
}

function requireEnv(): { url: string; key: string } {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL?.trim();
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY?.trim();
  if (!url || !key) {
    throw new Error("Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
  }
  return { url, key };
}

async function storeBelongsToOrg(
  supabase: SupabaseClient,
  organizationId: string,
  storeId: string,
): Promise<boolean> {
  const { data, error } = await supabase
    .from("stores")
    .select("id")
    .eq("id", storeId)
    .eq("organization_id", organizationId)
    .maybeSingle();
  return !error && !!data;
}

async function loadUploadMetadataMap(
  supabase: SupabaseClient,
  uploadIds: string[],
): Promise<Map<string, unknown>> {
  const out = new Map<string, unknown>();
  const ids = [...new Set(uploadIds.filter(isUuidString))];
  for (let i = 0; i < ids.length; i += 200) {
    const chunk = ids.slice(i, i + 200);
    const { data, error } = await supabase.from("raw_report_uploads").select("id, metadata").in("id", chunk);
    if (error) {
      console.warn("[backfill] raw_report_uploads batch error:", error.message);
      continue;
    }
    for (const row of data ?? []) {
      const id = row && typeof row === "object" && "id" in row ? String((row as { id: unknown }).id) : "";
      if (id) out.set(id, (row as { metadata?: unknown }).metadata);
    }
  }
  return out;
}

async function backfillProductIdentifierMap(
  supabase: SupabaseClient,
  organizationId: string | null,
  write: boolean,
): Promise<{ examined: number; wouldUpdate: number; updated: number; skipped: number }> {
  let examined = 0;
  let wouldUpdate = 0;
  let updated = 0;
  let skipped = 0;
  let from = 0;

  for (;;) {
    let q = supabase
      .from("product_identifier_map")
      .select("id, organization_id, source_upload_id")
      .is("store_id", null)
      .not("source_upload_id", "is", null)
      .order("id", { ascending: true })
      .range(from, from + PAGE - 1);
    if (organizationId) q = q.eq("organization_id", organizationId);
    const { data, error } = await q;
    if (error) {
      console.error("[backfill] product_identifier_map select failed:", error.message);
      break;
    }
    const rows = data ?? [];
    if (!rows.length) break;

    const uploadIds = rows.map((r) => String((r as { source_upload_id?: unknown }).source_upload_id ?? ""));
    const metaByUpload = await loadUploadMetadataMap(supabase, uploadIds);

    for (const row of rows as { id: string; organization_id: string; source_upload_id: string | null }[]) {
      examined++;
      const oid = String(row.organization_id ?? "");
      const uid = row.source_upload_id ? String(row.source_upload_id) : "";
      if (!isUuidString(oid) || !isUuidString(uid)) {
        skipped++;
        continue;
      }
      const meta = metaByUpload.get(uid);
      const sid = resolveImportStoreIdFromMetadata(meta);
      if (!sid || !(await storeBelongsToOrg(supabase, oid, sid))) {
        skipped++;
        continue;
      }
      wouldUpdate++;
      if (write) {
        const { error: upErr } = await supabase.from("product_identifier_map").update({ store_id: sid }).eq("id", row.id);
        if (upErr) {
          console.warn("[backfill] skip update product_identifier_map", row.id, upErr.message);
          skipped++;
          wouldUpdate--;
        } else updated++;
      }
    }

    if (rows.length < PAGE) break;
    from += PAGE;
  }

  return { examined, wouldUpdate, updated, skipped };
}

async function backfillProductsFromIdentifierMaps(
  supabase: SupabaseClient,
  organizationId: string | null,
  write: boolean,
): Promise<{ examined: number; wouldUpdate: number; updated: number; skipped: number }> {
  let examined = 0;
  let wouldUpdate = 0;
  let updated = 0;
  let skipped = 0;
  let from = 0;

  for (;;) {
    let q = supabase
      .from("products")
      .select("id, organization_id")
      .is("store_id", null)
      .order("id", { ascending: true })
      .range(from, from + PAGE - 1);
    if (organizationId) q = q.eq("organization_id", organizationId);
    const { data, error } = await q;
    if (error) {
      console.error("[backfill] products select failed:", error.message);
      break;
    }
    const rows = data ?? [];
    if (!rows.length) break;

    const productIds = rows.map((r) => String((r as { id: unknown }).id)).filter(isUuidString);

    const { data: maps, error: mapErr } = await supabase
      .from("product_identifier_map")
      .select("product_id, store_id")
      .in("product_id", productIds)
      .not("store_id", "is", null);

    if (mapErr) {
      console.error("[backfill] maps for products failed:", mapErr.message);
      break;
    }

    const byProduct = new Map<string, Set<string>>();
    for (const m of maps ?? []) {
      const pid = String((m as { product_id?: unknown }).product_id ?? "");
      const sid = String((m as { store_id?: unknown }).store_id ?? "");
      if (!isUuidString(pid) || !isUuidString(sid)) continue;
      let set = byProduct.get(pid);
      if (!set) {
        set = new Set();
        byProduct.set(pid, set);
      }
      set.add(sid);
    }

    for (const row of rows as { id: string; organization_id: string }[]) {
      examined++;
      const pid = String(row.id);
      const oid = String(row.organization_id ?? "");
      if (!isUuidString(oid)) {
        skipped++;
        continue;
      }
      const set = byProduct.get(pid);
      if (!set || set.size !== 1) {
        skipped++;
        continue;
      }
      const sid = [...set][0];
      if (!(await storeBelongsToOrg(supabase, oid, sid))) {
        skipped++;
        continue;
      }
      wouldUpdate++;
      if (write) {
        const { error: upErr } = await supabase.from("products").update({ store_id: sid }).eq("id", pid);
        if (upErr) {
          console.warn("[backfill] skip update products", pid, upErr.message);
          skipped++;
          wouldUpdate--;
        } else updated++;
      }
    }

    if (rows.length < PAGE) break;
    from += PAGE;
  }

  return { examined, wouldUpdate, updated, skipped };
}

async function main(): Promise<void> {
  const { write, organizationId } = parseArgs(process.argv.slice(2));
  const { url, key } = requireEnv();
  const supabase = createClient(url, key, { auth: { persistSession: false } });

  console.log(
    write
      ? "[backfill] MODE: WRITE (rows will be updated)"
      : "[backfill] MODE: DRY-RUN (no database writes; pass --i-understand-this-writes to apply)",
  );
  if (organizationId) console.log("[backfill] organization filter:", organizationId);
  else console.log("[backfill] organization filter: (all orgs)");

  const m1 = await backfillProductIdentifierMap(supabase, organizationId, write);
  console.log("[backfill] product_identifier_map (from upload metadata):", m1);

  const m2 = await backfillProductsFromIdentifierMaps(supabase, organizationId, write);
  console.log("[backfill] products (unambiguous store from identifier maps):", m2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
