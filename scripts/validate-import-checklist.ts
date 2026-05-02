/**
 * Read-only checks after dev reset / Phase A setup (no deletes).
 * Loads `.env.local` for Supabase + Phase A env vars (does not print secrets).
 *
 *   npx tsx scripts/validate-import-checklist.ts
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { createClient } from "@supabase/supabase-js";

function loadEnvLocal(): void {
  const p = path.join(process.cwd(), ".env.local");
  if (!fs.existsSync(p)) return;
  const text = fs.readFileSync(p, "utf8");
  for (const line of text.split(/\r?\n/)) {
    const m = line.match(/^\s*([^#=]+)=(.*)$/);
    if (!m) continue;
    const key = m[1].trim();
    const val = m[2].trim();
    if (key) process.env[key] = val;
  }
}

async function main() {
  loadEnvLocal();
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    console.error("Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment / .env.local");
    process.exit(1);
  }

  console.log("Phase A env (Next.js Process route):");
  console.log(`  PHASE2_BATCH_SIZE=${process.env.PHASE2_BATCH_SIZE ?? "(unset — code default 500)"}`);
  console.log(`  PHASE2_INTER_BATCH_SLEEP_MS=${process.env.PHASE2_INTER_BATCH_SLEEP_MS ?? "(unset — code default 2000)"}`);
  console.log(`  PROGRESS_MIN_INTERVAL_MS=${process.env.PROGRESS_MIN_INTERVAL_MS ?? "(unset — code default 5000)"}`);

  const sb = createClient(url, key, { auth: { persistSession: false } });
  const { count: stagingCount, error: stErr } = await sb
    .from("amazon_staging")
    .select("*", { count: "exact", head: true });
  if (stErr) throw new Error(stErr.message);
  console.log(`\namazon_staging row count: ${typeof stagingCount === "number" ? stagingCount : "?"}`);

  const n = typeof stagingCount === "number" ? stagingCount : 0;
  if (n === 0) {
    console.log("Duplicate (org, upload, row_number) check: skipped (table empty).");
  } else if (n > 250_000) {
    console.log("Duplicate check: skipped (table very large — run SQL in Supabase dashboard).");
  } else {
    const { data, error } = await sb.from("amazon_staging").select("organization_id,upload_id,row_number");
    if (error) throw new Error(error.message);
    const seen = new Set<string>();
    let dup = 0;
    for (const row of data ?? []) {
      const o = String((row as { organization_id?: string }).organization_id ?? "");
      const u = String((row as { upload_id?: string }).upload_id ?? "");
      const r = String((row as { row_number?: number }).row_number ?? "");
      const k = `${o}|${u}|${r}`;
      if (seen.has(k)) dup += 1;
      seen.add(k);
    }
    console.log(`Duplicate row keys (same org|upload|row_number): ${dup}`);
    if (dup > 0) process.exit(1);
  }

  console.log("\nManual checks still required: Disk I/O chart, stop/resume import, Clear staging on two uploads.");
}

main().catch((e) => {
  console.error(e instanceof Error ? e.message : e);
  process.exit(1);
});
