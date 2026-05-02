/**
 * DEV-ONLY — Reset import-related tables (staging, progress, locks, audit).
 *
 * NEVER wire this into production builds or CI. Requires explicit env + stdin confirmation.
 *
 * Prerequisites: NEXT_PUBLIC_SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
 * Set: DEV_RESET_IMPORT_DATA=YES_I_CONFIRM_DEV_RESET_IMPORT_DATA_ON_DEV
 *
 *   npx tsx scripts/dev-reset-import-data.ts --dry-run
 *   npx tsx scripts/dev-reset-import-data.ts --execute
 *
 * Flags:
 *   --also-domain       For each raw_report_uploads row, delete domain rows with matching upload_id first.
 *   --also-upload-rows  Remove storage objects per upload, then delete raw_report_uploads (implies full session wipe).
 */

import * as readline from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

const CONFIRM_ENV = "YES_I_CONFIRM_DEV_RESET_IMPORT_DATA_ON_DEV";

type Args = { dryRun: boolean; execute: boolean; alsoDomain: boolean; alsoUploadRows: boolean };

function parseArgs(argv: string[]): Args {
  return {
    dryRun: argv.includes("--dry-run"),
    execute: argv.includes("--execute"),
    alsoDomain: argv.includes("--also-domain"),
    alsoUploadRows: argv.includes("--also-upload-rows"),
  };
}

async function tableCount(sb: SupabaseClient, table: string): Promise<number> {
  const { count, error } = await sb.from(table).select("*", { count: "exact", head: true });
  if (error) throw new Error(`${table}: ${error.message}`);
  return typeof count === "number" ? count : 0;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  if (process.env.NODE_ENV === "production") {
    console.error("Refusing to run: NODE_ENV is production.");
    process.exit(1);
  }
  if (process.env.VERCEL_ENV === "production") {
    console.error("Refusing to run: VERCEL_ENV is production.");
    process.exit(1);
  }
  if (process.env.DEV_RESET_IMPORT_DATA !== CONFIRM_ENV) {
    console.error(
      `Set DEV_RESET_IMPORT_DATA=${CONFIRM_ENV} (this script is dev-only and never runs automatically).`,
    );
    process.exit(1);
  }

  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    console.error("Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
    process.exit(1);
  }

  if (!args.dryRun && !args.execute) {
    console.error("Specify --dry-run or --execute.");
    process.exit(1);
  }
  if (args.dryRun && args.execute) {
    console.error("Use only one of --dry-run or --execute.");
    process.exit(1);
  }

  const sb = createClient(url, key, { auth: { persistSession: false } });

  const stagingTables = [
    "import_pipeline_locks",
    "file_processing_status",
    "product_identity_staging_rows",
    "amazon_listing_report_rows_raw",
    "amazon_staging",
    "raw_report_import_audit",
    "raw_report_uploads",
  ] as const;

  console.log("\n=== Dev reset import data — row counts ===\n");
  for (const t of stagingTables) {
    const n = await tableCount(sb, t);
    console.log(`  ${t}: ${n.toLocaleString()} rows`);
  }

  const domainTables = [
    "expected_returns",
    "expected_packages",
    "expected_removals",
    "amazon_returns",
    "amazon_removals",
    "amazon_removal_shipments",
    "amazon_inventory_ledger",
    "amazon_reimbursements",
    "amazon_settlements",
    "amazon_safet_claims",
    "amazon_transactions",
    "amazon_reports_repository",
  ] as const;

  if (args.alsoDomain) {
    console.log("\n--also-domain: DELETE from domain tables scoped to current upload_id / organization_id pairs\n");
    const { data: uploads, error: uErr } = await sb.from("raw_report_uploads").select("id, organization_id");
    if (uErr) throw new Error(uErr.message);
    const pairs = (uploads ?? []) as { id: string; organization_id: string }[];
    for (const tbl of domainTables) {
      let sub = 0;
      for (const { id, organization_id } of pairs) {
        const { count, error } = await sb
          .from(tbl)
          .select("*", { count: "exact", head: true })
          .eq("organization_id", organization_id)
          .eq("upload_id", id);
        if (error) {
          console.warn(`  [skip ${tbl}] ${error.message}`);
          break;
        }
        sub += typeof count === "number" ? count : 0;
      }
      console.log(`  ${tbl}: ~${sub.toLocaleString()} rows (for listed uploads)`);
    }
  }

  if (args.alsoUploadRows) {
    console.log("\n--also-upload-rows: storage cleanup + DELETE raw_report_uploads\n");
  }

  if (args.dryRun) {
    console.log("\nDry run — no changes. Re-run with --execute after typing RESET when prompted.\n");
    return;
  }

  const rl = readline.createInterface({ input, output });
  const typed = (await rl.question('\nType exactly "RESET" to apply deletes: ')).trim();
  rl.close();
  if (typed !== "RESET") {
    console.error("Aborted (confirmation did not match).");
    process.exit(1);
  }

  async function removeObjectsUnderPrefix(prefix: string) {
    const trimmed = prefix.replace(/\/+$/, "");
    if (!trimmed) return;
    const { data: files, error } = await sb.storage.from("raw-reports").list(trimmed);
    if (error || !files?.length) return;
    const paths = files.map((f) => `${trimmed}/${f.name}`);
    await sb.storage.from("raw-reports").remove(paths);
  }

  if (args.alsoDomain) {
    const { data: uploads, error: uErr } = await sb.from("raw_report_uploads").select("id, organization_id");
    if (uErr) throw new Error(uErr.message);
    const pairs = (uploads ?? []) as { id: string; organization_id: string }[];
    for (const tbl of domainTables) {
      for (const { id, organization_id } of pairs) {
        const { error } = await sb.from(tbl).delete().eq("organization_id", organization_id).eq("upload_id", id);
        if (error) console.warn(`  ${tbl}: ${error.message}`);
      }
      console.log(`Cleared domain table: ${tbl}`);
    }
  }

  // Filters that are true for all real rows (PostgREST requires a filter on bulk delete).
  async function clearStagingAndProgressTables() {
    await sb.from("import_pipeline_locks").delete().gte("locked_at", "1970-01-01T00:00:00Z");
    await sb.from("file_processing_status").delete().gte("upload_pct", 0);
    await sb.from("product_identity_staging_rows").delete().gte("source_physical_row_number", 0);
    await sb.from("amazon_listing_report_rows_raw").delete().gte("row_number", 1);
    await sb.from("amazon_staging").delete().gte("row_number", 0);
    await sb.from("raw_report_import_audit").delete().gte("created_at", "1970-01-01T00:00:00Z");
  }

  await clearStagingAndProgressTables();
  console.log(
    "Cleared import_pipeline_locks, file_processing_status, product_identity_staging_rows, " +
      "amazon_listing_report_rows_raw, amazon_staging, raw_report_import_audit.",
  );

  if (args.alsoUploadRows) {
    const { data: uploads, error: uErr } = await sb.from("raw_report_uploads").select("id, organization_id, metadata");
    if (uErr) throw new Error(uErr.message);
    for (const row of uploads ?? []) {
      const r = row as { id: string; organization_id: string; metadata?: Record<string, unknown> };
      const metaObj =
        r.metadata && typeof r.metadata === "object" && !Array.isArray(r.metadata) ? r.metadata : {};
      const prefix =
        typeof metaObj.storage_prefix === "string"
          ? metaObj.storage_prefix.trim()
          : typeof metaObj.storagePrefix === "string"
            ? String(metaObj.storagePrefix).trim()
            : "";
      if (prefix) await removeObjectsUnderPrefix(prefix);
      const ledgerPath =
        typeof metaObj.ledger_storage_path === "string" ? metaObj.ledger_storage_path.trim() : "";
      if (ledgerPath) await sb.storage.from("raw-reports").remove([ledgerPath]);
      await sb.from("raw_report_uploads").delete().eq("id", r.id).eq("organization_id", r.organization_id);
    }
    console.log("Deleted raw_report_uploads rows and attempted storage cleanup.");
  } else {
    console.log("raw_report_uploads unchanged.");
  }

  console.log("\nDone.\n");
}

main().catch((e) => {
  console.error(e instanceof Error ? e.message : e);
  process.exit(1);
});
