import "server-only";

import { getSessionUserIdFromCookies } from "./supabase-server-auth";
import { supabaseServer } from "./supabase-server";
import { resolveOrganizationId } from "./organization";
import { isUuidString } from "./uuid";

/**
 * Resolves the acting `profiles.id` for import/audit actions.
 * Priority: validated explicit id → cookie session user id → first admin in org → any org member.
 */
export async function resolveActorProfileId(
  explicitUserId?: string | null,
): Promise<string | null> {
  async function profileExists(id: string): Promise<boolean> {
    const { data, error } = await supabaseServer
      .from("profiles")
      .select("id")
      .eq("id", id)
      .maybeSingle();
    return !error && Boolean(data?.id);
  }

  const raw = explicitUserId?.trim();
  if (raw && isUuidString(raw) && (await profileExists(raw))) {
    return raw;
  }

  const fromCookies = await getSessionUserIdFromCookies();
  if (fromCookies && (await profileExists(fromCookies))) {
    return fromCookies;
  }

  const orgId = resolveOrganizationId();

  const { data: superAdmin } = await supabaseServer
    .from("profiles")
    .select("id")
    .eq("organization_id", orgId)
    .eq("role", "super_admin")
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (superAdmin?.id && typeof superAdmin.id === "string") {
    return superAdmin.id;
  }

  const { data: admin } = await supabaseServer
    .from("profiles")
    .select("id")
    .eq("organization_id", orgId)
    .eq("role", "admin")
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (admin?.id && typeof admin.id === "string") {
    return admin.id;
  }

  const { data: first } = await supabaseServer
    .from("profiles")
    .select("id")
    .eq("organization_id", orgId)
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();

  return typeof first?.id === "string" ? first.id : null;
}
