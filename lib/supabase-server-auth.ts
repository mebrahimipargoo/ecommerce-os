import "server-only";

import { createServerClient } from "@supabase/ssr";
import { cookies } from "next/headers";

import { isUuidString } from "./uuid";

/**
 * Browser session for Server Actions / Route Handlers: reads Supabase auth cookies
 * (service-role `supabaseServer` does not carry the user JWT).
 */
export async function getSessionUserIdFromCookies(): Promise<string | null> {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const anon = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
  if (!url || !anon) return null;

  try {
    const cookieStore = await cookies();
    const supabase = createServerClient(url, anon, {
      cookies: {
        getAll() {
          return cookieStore.getAll();
        },
        setAll(cookiesToSet) {
          try {
            cookiesToSet.forEach(({ name, value, options }) => {
              cookieStore.set(name, value, options);
            });
          } catch {
            /* Server Actions may be unable to mutate cookies in some contexts */
          }
        },
      },
    });

    const {
      data: { session },
    } = await supabase.auth.getSession();
    const uid = session?.user?.id;
    return uid && isUuidString(uid) ? uid : null;
  } catch {
    return null;
  }
}
