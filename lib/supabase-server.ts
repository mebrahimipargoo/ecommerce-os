import "server-only";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

/**
 * Server-only Supabase client using the service role key.
 * Use this for operations that require bypassing RLS or accessing sensitive data
 * (e.g. lwa_client_secret). Never import this file in client components.
 *
 * Typed rows live in `@/types/database.types` — wire `createClient<Database>` after
 * `supabase gen types` (full schema) so `.select()` column names stay in sync.
 */
function getServerSupabase(): SupabaseClient {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!supabaseUrl || !serviceRoleKey) {
    throw new Error(
      "Missing Supabase environment variables: NEXT_PUBLIC_SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY."
    );
  }

  return createClient(supabaseUrl, serviceRoleKey, {
    auth: { persistSession: false },
  });
}

export const supabaseServer = getServerSupabase();
