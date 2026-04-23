/**
 * CLI: sync modules, module_features, and permissions from the sidebar model
 * (`lib/sidebar-config.ts` + `lib/sidebar-catalog-extras.ts` merged in `lib/sidebar-sync.ts`).
 * Requires: NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
 *
 * Usage: npm run sync:sidebar
 */

import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { createClient } from "@supabase/supabase-js";
import { syncSidebarCatalogToDatabase } from "../lib/sidebar-sync";

function loadEnvLocal(): void {
  const p = join(process.cwd(), ".env.local");
  if (!existsSync(p)) return;
  for (const line of readFileSync(p, "utf8").split("\n")) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const i = t.indexOf("=");
    if (i === -1) continue;
    const k = t.slice(0, i).trim();
    let v = t.slice(i + 1).trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
      v = v.slice(1, -1);
    }
    if (process.env[k] == null || process.env[k] === "") {
      process.env[k] = v;
    }
  }
}

loadEnvLocal();

async function main(): Promise<void> {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    console.error("Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY (e.g. in .env.local).");
    process.exit(1);
  }
  const supabase = createClient(url, key, { auth: { persistSession: false } });
  const r = await syncSidebarCatalogToDatabase(supabase);
  if (!r.ok) {
    console.error("Sync failed:", r.error);
    process.exit(1);
  }
  console.log(
    `Sidebar catalog sync: ${r.modules} module row(s), ${r.moduleFeatures} feature row(s), ${r.permissions} permission row(s).`,
  );
}

void main();
