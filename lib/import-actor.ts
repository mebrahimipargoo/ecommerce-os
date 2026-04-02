import "server-only";

import { supabaseServer } from "./supabase-server";
import { resolveOrganizationId } from "./organization";
import { isUuidString } from "./uuid";

/**
 * Resolves the acting `profiles.id` for import/audit actions.
 * Priority: validated explicit id → first admin in org → any org member (silent fallback).
 */
export async function resolveActorProfileId(
  explicitUserId?: string | null,
): Promise<string | null> {
  const orgId = resolveOrganizationId();

  async function verifyInOrg(id: string): Promise<boolean> {
    const { data, error } = await supabaseServer
      .from("profiles")
      .select("id, company_id")
      .eq("id", id)
      .maybeSingle();
    if (error || !data?.id) return false;
    const row = data as { company_id?: string | null };
    const tenant = (row.company_id ?? "").trim();
    return tenant === orgId;
  }

  const raw = explicitUserId?.trim();
  if (raw && isUuidString(raw) && (await verifyInOrg(raw))) {
    return raw;
  }

  const { data: superAdmin } = await supabaseServer
    .from("profiles")
    .select("id")
    .eq("company_id", orgId)
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
    .eq("company_id", orgId)
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
    .eq("company_id", orgId)
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();

  return typeof first?.id === "string" ? first.id : null;
}
