"use server";

import { randomBytes } from "crypto";
import { assertUserCanAccessOrganization } from "../dashboard/products/pim-actions";
import { hashOrganizationApiKeySecret } from "../../lib/organization-workspace-api-key";
import { supabaseServer } from "../../lib/supabase-server";
import { resolveOrganizationId } from "../../lib/organization";
import { isUuidString } from "../../lib/uuid";

export type OrganizationApiKeyRow = {
  id: string;
  organization_id: string;
  name: string;
  role: string;
  /** SHA-256 hex digest at rest — UI masks this; never the live secret. */
  api_key: string;
  created_at: string;
};

/**
 * Creates a new API key. Returns the full secret exactly once — only the digest is stored in `api_key`.
 */
export async function createOrganizationApiKey(input: {
  label: string;
  roleTag: string;
}): Promise<
  { ok: true; id: string; plaintextKey: string } | { ok: false; error: string }
> {
  const orgId = resolveOrganizationId();
  const name = input.label.trim().slice(0, 200) || "API key";
  const role = input.roleTag.trim().slice(0, 128) || "integration";
  const plaintextKey = `eos_${randomBytes(32).toString("hex")}`;
  const keyDigest = hashOrganizationApiKeySecret(plaintextKey);

  try {
    const { data, error } = await supabaseServer
      .from("organization_api_keys")
      .insert({
        organization_id: orgId,
        name,
        role,
        api_key: keyDigest,
      })
      .select("id")
      .single();

    if (error) return { ok: false, error: error.message };
    if (!data?.id) return { ok: false, error: "Insert failed." };
    return { ok: true, id: data.id as string, plaintextKey };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Create failed." };
  }
}

export async function listOrganizationApiKeys(): Promise<
  { ok: true; rows: OrganizationApiKeyRow[] } | { ok: false; error: string }
> {
  const orgId = resolveOrganizationId();
  try {
    const { data, error } = await supabaseServer
      .from("organization_api_keys")
      .select("id, organization_id, name, role, api_key, created_at")
      .eq("organization_id", orgId)
      .order("created_at", { ascending: false });

    if (error) return { ok: false, error: error.message };
    return { ok: true, rows: (data ?? []) as OrganizationApiKeyRow[] };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "List failed." };
  }
}

export async function revokeOrganizationApiKey(
  id: string,
): Promise<{ ok: boolean; error?: string }> {
  if (!isUuidString(id)) return { ok: false, error: "Invalid id." };
  const orgId = resolveOrganizationId();
  try {
    const { error } = await supabaseServer
      .from("organization_api_keys")
      .delete()
      .eq("id", id)
      .eq("organization_id", orgId);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Revoke failed." };
  }
}

// ── LLM Provider keys (plaintext, cross-device) ────────────────────────────
// These rows store the actual plaintext API key (not a digest) so the server
// can read and forward it to third-party AI providers (OpenAI, Gemini, etc.).
// They are distinguished from workspace agent keys by role = 'llm_provider'.

/**
 * Upserts an LLM provider key (e.g. OpenAI) into organization_api_keys.
 * Stored as plaintext so server-side AI calls can use it directly.
 * Rows are identified by (organization_id, name, role='llm_provider').
 */
export async function upsertProviderApiKey(
  name: string,
  plaintextKey: string,
): Promise<{ ok: boolean; error?: string }> {
  const orgId = resolveOrganizationId();
  const safeName = name.trim().slice(0, 200) || "LLM Provider";
  try {
    // Delete any existing row with the same name to simulate upsert
    // (organization_api_keys may not have a unique constraint on name+org)
    await supabaseServer
      .from("organization_api_keys")
      .delete()
      .eq("organization_id", orgId)
      .eq("name", safeName)
      .eq("role", "llm_provider");

    const { error } = await supabaseServer
      .from("organization_api_keys")
      .insert({
        organization_id: orgId,
        name: safeName,
        role: "llm_provider",
        api_key: plaintextKey.trim(),
      });

    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Upsert failed." };
  }
}

/**
 * Fetches the plaintext LLM provider key by name.
 * Returns null if not found.
 */
export async function getProviderApiKey(
  name: string,
): Promise<string | null> {
  const orgId = resolveOrganizationId();
  try {
    const { data } = await supabaseServer
      .from("organization_api_keys")
      .select("api_key")
      .eq("organization_id", orgId)
      .eq("name", name.trim())
      .eq("role", "llm_provider")
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();
    const key = data?.api_key?.trim();
    return key || null;
  } catch {
    return null;
  }
}

/**
 * Stores Google service account JSON as plaintext in `organization_api_keys` (name `google_sheets_api`),
 * matching FastAPI `_get_google_creds` / `etl/sync-google-sheets`.
 */
export async function upsertGoogleSheetsServiceAccountForOrganization(
  organizationId: string,
  rawJson: string,
): Promise<{ ok: boolean; error?: string }> {
  if (!isUuidString(organizationId)) {
    return { ok: false, error: "Invalid organization." };
  }
  const gate = await assertUserCanAccessOrganization(organizationId);
  if (!gate.ok) return { ok: false, error: gate.error };

  const trimmed = rawJson.trim();
  if (!trimmed) {
    return { ok: false, error: "Paste your service account JSON." };
  }
  try {
    JSON.parse(trimmed);
  } catch {
    return { ok: false, error: "Invalid JSON." };
  }

  try {
    await supabaseServer
      .from("organization_api_keys")
      .delete()
      .eq("organization_id", organizationId)
      .eq("name", "google_sheets_api");

    const { error } = await supabaseServer.from("organization_api_keys").insert({
      organization_id: organizationId,
      name: "google_sheets_api",
      role: "integration",
      api_key: trimmed,
    });
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Save failed." };
  }
}

