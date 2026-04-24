"use server";

import { supabaseServer } from "./supabase-server";
import { isUuidString } from "./uuid";

/**
 * Resolves an array of profile UUIDs to a { [uuid]: displayName } map.
 *
 * Uses the service-role key so it bypasses RLS — safe to call from any
 * server action or Next.js Server Component.  All data returned is
 * non-sensitive (display names only).
 */
export async function resolveProfileNames(
  ids: string[],
): Promise<Record<string, string>> {
  const validIds = ids.filter((id) => typeof id === "string" && isUuidString(id.trim())).map((id) => id.trim());
  if (!validIds.length) return {};

  const { data } = await supabaseServer
    .from("profiles")
    .select("id, full_name")
    .in("id", validIds);

  const result: Record<string, string> = {};
  for (const row of data ?? []) {
    const id = String(row.id ?? "").trim();
    const display = String(row.full_name ?? "").trim();
    if (id && display) result[id] = display;
  }
  return result;
}
