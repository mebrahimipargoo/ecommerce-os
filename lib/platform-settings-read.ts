import "server-only";

import { supabaseServer } from "./supabase-server";

/** Non-product fallback when the row is missing or the DB is unreachable at build time. */
const METADATA_TITLE_FALLBACK = "Workspace";

/**
 * Reads the singleton platform branding row (service role — not tenant-scoped).
 * Used for document title and other server-only surfaces.
 */
export async function getPlatformAppNameForMetadata(): Promise<string> {
  try {
    const { data, error } = await supabaseServer
      .from("platform_settings")
      .select("app_name")
      .eq("id", true)
      .maybeSingle();
    if (error || !data) return METADATA_TITLE_FALLBACK;
    const n = String((data as { app_name?: string | null }).app_name ?? "").trim();
    return n || METADATA_TITLE_FALLBACK;
  } catch {
    return METADATA_TITLE_FALLBACK;
  }
}
