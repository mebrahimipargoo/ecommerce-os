"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { MarketplaceFactory } from "../../../lib/adapters/factory";
import type { AdapterProviderKey } from "../../../lib/adapters";

function maskSecret(value: string): string {
  const t = value?.trim() ?? "";
  if (!t) return "";
  if (t.length <= 6) return "•".repeat(t.length);
  return "•".repeat(8) + t.slice(-4);
}

const DEFAULT_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000001";
const DEFAULT_ROLE_REQUIRED = "admin";

/** RBAC context - when auth is added, derive from session */
export type RbacContext = {
  organization_id: string;
  user_role: string;
};

type MarketplaceRow = {
  id: string;
  provider: AdapterProviderKey;
  nickname: string;
  credentials: Record<string, string>;
  organization_id: string;
  role_required: string;
  created_at: string;
};

type MarketplaceInsertPayload = {
  provider: AdapterProviderKey;
  nickname: string;
  credentials: Record<string, string>;
  organization_id?: string;
  role_required?: string;
};

type MarketplaceUpdatePayload = {
  nickname?: string;
  credentials?: Record<string, string>;
};

type MarketplacePublicRow = {
  id: string;
  provider: AdapterProviderKey;
  nickname: string;
  /** Masked display ID from credentials (e.g. Seller ID) - never send raw credentials */
  display_id?: string;
  organization_id: string;
  role_required: string;
  created_at: string;
};

function getRbacContext(ctx?: RbacContext | null): RbacContext {
  return {
    organization_id: ctx?.organization_id ?? DEFAULT_ORGANIZATION_ID,
    user_role: ctx?.user_role ?? DEFAULT_ROLE_REQUIRED,
  };
}

function canAccess(roleRequired: string, userRole: string): boolean {
  const hierarchy = ["viewer", "editor", "admin"];
  const requiredIdx = hierarchy.indexOf(roleRequired);
  const userIdx = hierarchy.indexOf(userRole);
  return userIdx >= 0 && userIdx >= requiredIdx;
}

export async function testConnection(
  connectionId: string,
  ctx?: RbacContext | null
): Promise<{ ok: boolean; expiresIn?: number; error?: string }> {
  const rbac = getRbacContext(ctx);
  try {
    const { data, error } = await supabaseServer
      .from("marketplaces")
      .select("id, provider, credentials, organization_id, role_required")
      .eq("id", connectionId)
      .eq("organization_id", rbac.organization_id)
      .single();

    if (error) throw new Error(error.message);
    if (!data) throw new Error("Connection not found.");

    const row = data as MarketplaceRow;
    if (!canAccess(row.role_required, rbac.user_role)) {
      throw new Error("Insufficient role to access this connection.");
    }

    const adapter = MarketplaceFactory.getAdapter(row.provider);
    if (!adapter) throw new Error(`Unknown provider: ${row.provider}`);

    const result = await adapter.testConnection(row.credentials ?? {});
    return result;
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to verify connection.";
    return { ok: false, error: message };
  }
}

/** Legacy aliases for backward compatibility */
export async function testAmazonConnection(connectionId: string) {
  return testConnection(connectionId);
}
export async function testWalmartConnection(connectionId: string) {
  return testConnection(connectionId);
}

export async function syncClaims(
  connectionId: string,
  ctx?: RbacContext | null
): Promise<{ ok: boolean; claimsCount?: number; error?: string }> {
  const rbac = getRbacContext(ctx);
  try {
    const { data, error } = await supabaseServer
      .from("marketplaces")
      .select("id, provider, credentials, organization_id, role_required")
      .eq("id", connectionId)
      .eq("organization_id", rbac.organization_id)
      .single();

    if (error) throw new Error(error.message);
    if (!data) throw new Error("Connection not found.");

    const row = data as MarketplaceRow;
    if (!canAccess(row.role_required, rbac.user_role)) {
      throw new Error("Insufficient role to sync claims for this connection.");
    }

    const adapter = MarketplaceFactory.getAdapter(row.provider);
    if (!adapter) throw new Error(`Unknown provider: ${row.provider}`);

    const result = await adapter.syncClaims(connectionId, row.credentials ?? {});
    if (!result.ok) return { ok: false, error: result.error };

    const claims = result.claims ?? [];
    if (claims.length === 0) return { ok: true, claimsCount: 0 };

    const { error: insertError } = await supabaseServer
      .from("claims")
      .insert(
        claims.map((claim) => ({
          ...claim,
          organization_id: row.organization_id,
          marketplace_provider: row.provider,
        }))
      );

    if (insertError) throw new Error(insertError.message);

    return { ok: true, claimsCount: claims.length };
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to sync claims.";
    return { ok: false, error: message };
  }
}

export async function insertMarketplace(
  payload: MarketplaceInsertPayload,
  ctx?: RbacContext | null
): Promise<{ ok: boolean; data?: MarketplacePublicRow; error?: string }> {
  const rbac = getRbacContext(ctx);
  if (!canAccess(DEFAULT_ROLE_REQUIRED, rbac.user_role)) {
    return { ok: false, error: "Insufficient role to create connections." };
  }
  try {
    const row = {
      provider: payload.provider,
      nickname: payload.nickname,
      credentials: payload.credentials ?? {},
      organization_id: payload.organization_id ?? rbac.organization_id,
      role_required: payload.role_required ?? DEFAULT_ROLE_REQUIRED,
    };
    const { data, error } = await supabaseServer
      .from("marketplaces")
      .insert(row)
      .select("id, provider, nickname, credentials, organization_id, role_required, created_at")
      .single();

    if (error) throw new Error(error.message);
    const inserted = data as MarketplaceRow;
    const adapter = MarketplaceFactory.getAdapter(inserted.provider);
    const displayIdKey = adapter?.config.displayIdKey;
    const display_id = displayIdKey && inserted.credentials?.[displayIdKey]
      ? maskSecret(inserted.credentials[displayIdKey])
      : undefined;
    return {
      ok: true,
      data: {
        id: inserted.id,
        provider: inserted.provider,
        nickname: inserted.nickname,
        display_id,
        organization_id: inserted.organization_id,
        role_required: inserted.role_required,
        created_at: inserted.created_at,
      },
    };
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "Failed to save marketplace connection.";
    return { ok: false, error: message };
  }
}

export async function updateMarketplace(
  id: string,
  payload: MarketplaceUpdatePayload,
  ctx?: RbacContext | null
): Promise<{ ok: boolean; data?: MarketplacePublicRow; error?: string }> {
  const rbac = getRbacContext(ctx);
  try {
    const { data: existing, error: fetchError } = await supabaseServer
      .from("marketplaces")
      .select("organization_id, role_required, credentials")
      .eq("id", id)
      .single();

    if (fetchError || !existing) throw new Error("Connection not found.");
    const row = existing as { organization_id: string; role_required: string; credentials?: Record<string, string> };
    if (row.organization_id !== rbac.organization_id) {
      throw new Error("Connection not found.");
    }
    if (!canAccess(row.role_required, rbac.user_role)) {
      throw new Error("Insufficient role to update this connection.");
    }

    const update: Record<string, unknown> = {};
    if (payload.nickname !== undefined) update.nickname = payload.nickname;
    if (payload.credentials !== undefined) {
      const existingCreds = (row.credentials ?? {}) as Record<string, string>;
      const merged: Record<string, string> = { ...existingCreds };
      for (const [k, v] of Object.entries(payload.credentials)) {
        if (v != null && String(v).trim() !== "") merged[k] = String(v).trim();
      }
      update.credentials = merged;
    }

    if (Object.keys(update).length === 0) {
      throw new Error("No fields to update.");
    }

    const { data, error } = await supabaseServer
      .from("marketplaces")
      .update(update)
      .eq("id", id)
      .eq("organization_id", rbac.organization_id)
      .select("id, provider, nickname, credentials, organization_id, role_required, created_at")
      .single();

    if (error) throw new Error(error.message);
    const updated = data as MarketplaceRow;
    const adapter = MarketplaceFactory.getAdapter(updated.provider);
    const displayIdKey = adapter?.config.displayIdKey;
    const display_id = displayIdKey && updated.credentials?.[displayIdKey]
      ? maskSecret(updated.credentials[displayIdKey])
      : undefined;
    return {
      ok: true,
      data: {
        id: updated.id,
        provider: updated.provider,
        nickname: updated.nickname,
        display_id,
        organization_id: updated.organization_id,
        role_required: updated.role_required,
        created_at: updated.created_at,
      },
    };
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "Failed to update marketplace connection.";
    return { ok: false, error: message };
  }
}

export async function deleteMarketplace(
  id: string,
  ctx?: RbacContext | null
): Promise<{ ok: boolean; error?: string }> {
  const rbac = getRbacContext(ctx);
  try {
    const { data: existing, error: fetchError } = await supabaseServer
      .from("marketplaces")
      .select("organization_id, role_required")
      .eq("id", id)
      .single();

    if (fetchError || !existing) throw new Error("Connection not found.");
    const row = existing as { organization_id: string; role_required: string };
    if (row.organization_id !== rbac.organization_id) {
      throw new Error("Connection not found.");
    }
    if (!canAccess(row.role_required, rbac.user_role)) {
      throw new Error("Insufficient role to delete this connection.");
    }

    const { error } = await supabaseServer
      .from("marketplaces")
      .delete()
      .eq("id", id)
      .eq("organization_id", rbac.organization_id);

    if (error) throw new Error(error.message);
    return { ok: true };
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "Failed to delete connection.";
    return { ok: false, error: message };
  }
}

// ── Stores (stores table — higher-level than marketplace credentials) ─────────

export type StorePublicRow = {
  id: string;
  name: string;
  platform: string;
  is_active: boolean;
  marketplace_id: string | null;
  organization_id: string;
  created_at: string;
};

export async function listStores(
  ctx?: RbacContext | null
): Promise<{ ok: boolean; data?: StorePublicRow[]; error?: string }> {
  const rbac = getRbacContext(ctx);
  try {
    const { data, error } = await supabaseServer
      .from("stores")
      .select("id, name, platform, is_active, marketplace_id, organization_id, created_at")
      .eq("organization_id", rbac.organization_id)
      .order("created_at", { ascending: false });

    if (error) throw new Error(error.message);
    return { ok: true, data: (data ?? []) as StorePublicRow[] };
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load stores.";
    return { ok: false, error: message };
  }
}

export async function listMarketplaces(
  ctx?: RbacContext | null
): Promise<{
  ok: boolean;
  data?: MarketplacePublicRow[];
  error?: string;
}> {
  const rbac = getRbacContext(ctx);
  try {
    const { data, error } = await supabaseServer
      .from("marketplaces")
      .select("id, provider, nickname, credentials, organization_id, role_required, created_at")
      .eq("organization_id", rbac.organization_id)
      .order("created_at", { ascending: false });

    if (error) throw new Error(error.message);
    const rows = (data ?? []) as MarketplaceRow[];
    const publicRows: MarketplacePublicRow[] = rows.map((r) => {
      const adapter = MarketplaceFactory.getAdapter(r.provider);
      const displayIdKey = adapter?.config.displayIdKey;
      const display_id = displayIdKey && r.credentials?.[displayIdKey]
        ? maskSecret(r.credentials[displayIdKey])
        : undefined;
      return {
        id: r.id,
        provider: r.provider,
        nickname: r.nickname,
        display_id,
        organization_id: r.organization_id,
        role_required: r.role_required,
        created_at: r.created_at,
      };
    });
    return { ok: true, data: publicRows };
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "Failed to load active connections.";
    return { ok: false, error: message };
  }
}
