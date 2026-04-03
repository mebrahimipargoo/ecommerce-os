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

/** Load raw credentials JSON for the store editor (admin settings only). */
export async function getMarketplaceCredentialsForEdit(
  marketplaceId: string,
  ctx?: RbacContext | null
): Promise<{ ok: boolean; data?: Record<string, string>; error?: string }> {
  const rbac = getRbacContext(ctx);
  try {
    const { data, error } = await supabaseServer
      .from("marketplaces")
      .select("organization_id, role_required, credentials")
      .eq("id", marketplaceId)
      .eq("organization_id", rbac.organization_id)
      .single();

    if (error || !data) throw new Error("Connection not found.");
    const row = data as { role_required: string; credentials?: Record<string, string> };
    if (!canAccess(row.role_required, rbac.user_role)) {
      throw new Error("Insufficient role to view this connection.");
    }
    const raw = (row.credentials ?? {}) as Record<string, unknown>;
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(raw)) {
      if (v != null && String(v).trim() !== "") out[k] = String(v).trim();
    }
    return { ok: true, data: out };
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load credentials.";
    return { ok: false, error: message };
  }
}

/** Test adapter connectivity without persisting (uses credentials from the form). */
export async function testMarketplaceCredentials(
  provider: AdapterProviderKey,
  credentials: Record<string, string>,
  ctx?: RbacContext | null
): Promise<{ ok: boolean; expiresIn?: number; error?: string }> {
  const rbac = getRbacContext(ctx);
  if (!canAccess(DEFAULT_ROLE_REQUIRED, rbac.user_role)) {
    return { ok: false, error: "Insufficient role to test connections." };
  }
  try {
    const adapter = MarketplaceFactory.getAdapter(provider);
    if (!adapter) throw new Error(`Unknown provider: ${provider}`);
    return await adapter.testConnection(credentials ?? {});
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Connection test failed.";
    return { ok: false, error: message };
  }
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

    const { CLAIM_SUBMISSIONS_TABLE } = await import("../../claim-engine/claim-submissions-constants");

    const rows = claims.map((claim) => {
      const c = claim as Record<string, unknown>;
      const rawStatus = String(c.status ?? "pending");
      const status =
        rawStatus === "recovered"
          ? "accepted"
          : rawStatus === "suspicious"
            ? "evidence_requested"
            : "submitted";
      return {
        organization_id: row.organization_id,
        return_id: null,
        claim_amount: Number(c.amount) || 0,
        status,
        submission_id: typeof c.marketplace_claim_id === "string" ? c.marketplace_claim_id : null,
        report_url: null,
        store_id: null,
        source_payload: {
          claim_type: c.claim_type,
          amazon_order_id: c.amazon_order_id,
          item_name: c.item_name,
          asin: c.asin,
          fnsku: c.fnsku,
          sku: c.sku,
          marketplace_link_status: c.marketplace_link_status,
          marketplace_provider: row.provider,
        },
      };
    });

    const { error: insertError } = await supabaseServer.from(CLAIM_SUBMISSIONS_TABLE).insert(rows);

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

export type StoreInsertPayload = {
  name: string;
  platform: string;
  region?: string;
  marketplace_id?: string;
};

export async function listStores(
  _ctx?: RbacContext | null
): Promise<{ ok: boolean; data?: StorePublicRow[]; error?: string }> {
  try {
    const { data, error } = await supabaseServer
      .from("stores")
      .select("id, name, platform, is_active, marketplace_id, organization_id, created_at")
      .order("created_at", { ascending: false });

    if (error) throw new Error(error.message);
    return { ok: true, data: (data ?? []) as StorePublicRow[] };
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to load stores.";
    return { ok: false, error: message };
  }
}

export async function deleteStore(
  id: string,
  ctx?: RbacContext | null
): Promise<{ ok: boolean; error?: string }> {
  const rbac = getRbacContext(ctx);
  try {
    const { error } = await supabaseServer
      .from("stores")
      .delete()
      .eq("id", id)
      .eq("organization_id", rbac.organization_id);

    if (error) {
      // Fallback without org filter for local dev
      const { error: fbErr } = await supabaseServer.from("stores").delete().eq("id", id);
      if (fbErr) throw new Error(fbErr.message);
    }
    return { ok: true };
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to delete store.";
    return { ok: false, error: message };
  }
}

export async function updateStore(
  id: string,
  payload: { name?: string; is_active?: boolean; marketplace_id?: string | null },
  ctx?: RbacContext | null
): Promise<{ ok: boolean; data?: StorePublicRow; error?: string }> {
  const rbac = getRbacContext(ctx);
  try {
    const update: Record<string, unknown> = {};
    if (payload.name !== undefined) update.name = payload.name.trim();
    if (payload.is_active !== undefined) update.is_active = payload.is_active;
    if (payload.marketplace_id !== undefined) {
      update.marketplace_id = payload.marketplace_id;
    }
    if (Object.keys(update).length === 0) throw new Error("No fields to update.");

    const { data, error } = await supabaseServer
      .from("stores")
      .update(update)
      .eq("id", id)
      .eq("organization_id", rbac.organization_id)
      .select("id, name, platform, is_active, marketplace_id, organization_id, created_at")
      .single();

    if (error) {
      // Fallback without org filter
      const { data: fb, error: fbErr } = await supabaseServer
        .from("stores")
        .update(update)
        .eq("id", id)
        .select("id, name, platform, is_active, marketplace_id, created_at")
        .single();
      if (fbErr) throw new Error(fbErr.message);
      return { ok: true, data: fb as StorePublicRow };
    }
    return { ok: true, data: data as StorePublicRow };
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to update store.";
    return { ok: false, error: message };
  }
}

export async function insertStore(
  payload: StoreInsertPayload,
  ctx?: RbacContext | null
): Promise<{ ok: boolean; data?: StorePublicRow; error?: string }> {
  const rbac = getRbacContext(ctx);
  if (!canAccess(DEFAULT_ROLE_REQUIRED, rbac.user_role)) {
    return { ok: false, error: "Insufficient role to create stores." };
  }
  try {
    const row: Record<string, unknown> = {
      name: payload.name.trim(),
      platform: payload.platform,
      is_active: true,
      organization_id: rbac.organization_id,
    };
    if (payload.region)         row.region         = payload.region;
    if (payload.marketplace_id) row.marketplace_id  = payload.marketplace_id;

    const { data, error } = await supabaseServer
      .from("stores")
      .insert(row)
      .select("id, name, platform, is_active, marketplace_id, organization_id, created_at")
      .single();

    if (error) throw new Error(error.message);
    return { ok: true, data: data as StorePublicRow };
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to create store.";
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
    let { data, error } = await supabaseServer
      .from("marketplaces")
      .select("id, provider, nickname, credentials, organization_id, role_required, created_at")
      .eq("organization_id", rbac.organization_id)
      .order("created_at", { ascending: false });

    if (error) {
      // Fallback without org filter
      const fb = await supabaseServer
        .from("marketplaces")
        .select("id, provider, nickname, credentials, organization_id, role_required, created_at")
        .order("created_at", { ascending: false });
      if (fb.error) throw new Error(fb.error.message);
      data = fb.data;
    }

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
