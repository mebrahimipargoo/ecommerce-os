import { createClient, type SupabaseClient } from "@supabase/supabase-js";

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL?.trim() ?? "";
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY?.trim() ?? "";

/** True when the browser bundle was built with real Supabase credentials (set at build time). */
export function isSupabaseConfigured(): boolean {
  return supabaseUrl.length > 0 && supabaseAnonKey.length > 0;
}

if (typeof window !== "undefined" && !isSupabaseConfigured()) {
  console.error(
    "[Supabase] Missing NEXT_PUBLIC_SUPABASE_URL or NEXT_PUBLIC_SUPABASE_ANON_KEY — API calls will fail until configured.",
  );
}

/**
 * Browser client. Never throw at module load: a thrown Error here blanked the whole app on
 * rugged Android browsers (e.g. Zebra Enterprise Browser) when env was missing at runtime.
 * Use `isSupabaseConfigured()` to show a visible config banner instead of a white screen.
 */
export const supabase: SupabaseClient = createClient(
  isSupabaseConfigured() ? supabaseUrl : "https://placeholder.invalid.supabase.co",
  isSupabaseConfigured() ? supabaseAnonKey : "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.placeholder",
);

/** See `@/types/database.types` for `returns` / `packages` / `pallets` row shapes. */

