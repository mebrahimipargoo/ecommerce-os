"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { loadTenantProfile, isSuperAdminRole } from "../../../lib/server-tenant";

export type PlatformMarketplaceRow = {
  id: string;
  name: string;
  slug: string;
  icon_url: string | null;
  created_at: string;
  updated_at: string;
};

async function requireSuperAdmin(actorProfileId: string | null | undefined): Promise<void> {
  const p = await loadTenantProfile(actorProfileId);
  if (!p || !isSuperAdminRole(p.role)) {
    throw new Error("Forbidden: Super Admin only.");
  }
}

export async function listPlatformMarketplaces(
  actorProfileId: string | null | undefined,
): Promise<{ ok: boolean; rows: PlatformMarketplaceRow[]; error?: string }> {
  try {
    await requireSuperAdmin(actorProfileId);
    const { data, error } = await supabaseServer
      .from("marketplaces")
      .select("id, name, slug, icon_url, created_at, updated_at")
      .order("name", { ascending: true });
    if (error) throw new Error(error.message);
    return { ok: true, rows: (data ?? []) as PlatformMarketplaceRow[] };
  } catch (e) {
    return {
      ok: false,
      rows: [],
      error: e instanceof Error ? e.message : "Failed to load platforms.",
    };
  }
}

/** Public read for Returns icons ΓÇö any authenticated server caller may use (small global catalog). */
export async function listPlatformMarketplaceIcons(): Promise<{
  ok: boolean;
  bySlug: Record<string, string>;
  error?: string;
}> {
  try {
    const { data, error } = await supabaseServer
      .from("marketplaces")
      .select("slug, icon_url");
    if (error) throw new Error(error.message);
    const bySlug: Record<string, string> = {};
    for (const row of data ?? []) {
      const r = row as { slug?: string; icon_url?: string | null };
      const slug = (r.slug ?? "").trim().toLowerCase();
      const url = (r.icon_url ?? "").trim();
      if (slug && url) bySlug[slug] = url;
    }
    return { ok: true, bySlug };
  } catch (e) {
    return {
      ok: false,
      bySlug: {},
      error: e instanceof Error ? e.message : "Failed to load marketplace icons.",
    };
  }
}

export async function upsertPlatformMarketplace(payload: {
  actorProfileId: string | null | undefined;
  id?: string | null;
  name: string;
  slug: string;
  icon_url?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  try {
    await requireSuperAdmin(payload.actorProfileId);
    const name = payload.name.trim();
    const slug = payload.slug.trim().toLowerCase().replace(/\s+/g, "-");
    if (!name || !slug) throw new Error("Name and slug are required.");
    const icon = payload.icon_url?.trim() || null;
    const id = payload.id?.trim();
    if (id) {
      const { error } = await supabaseServer
        .from("marketplaces")
        .update({ name, slug, icon_url: icon, updated_at: new Date().toISOString() })
        .eq("id", id);
      if (error) throw new Error(error.message);
    } else {
      const { error } = await supabaseServer.from("marketplaces").insert({
        name,
        slug,
        icon_url: icon,
      });
      if (error) throw new Error(error.message);
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Save failed." };
  }
}

export async function deletePlatformMarketplace(
  actorProfileId: string | null | undefined,
  id: string,
): Promise<{ ok: boolean; error?: string }> {
  try {
    await requireSuperAdmin(actorProfileId);
    const { error } = await supabaseServer.from("marketplaces").delete().eq("id", id);
    if (error) throw new Error(error.message);
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Delete failed." };
  }
}
