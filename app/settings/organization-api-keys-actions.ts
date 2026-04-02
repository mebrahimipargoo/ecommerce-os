"use server";

import { randomBytes } from "crypto";
import { hashOrganizationApiKeySecret } from "../../lib/organization-workspace-api-key";
import { supabaseServer } from "../../lib/supabase-server";
import { resolveOrganizationId } from "../../lib/organization";
import { isUuidString } from "../../lib/uuid";

export type OrganizationApiKeyRow = {
  id: string;
  company_id: string;
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
        company_id: orgId,
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
      .select("id, company_id, name, role, api_key, created_at")
      .eq("company_id", orgId)
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
      .eq("company_id", orgId);
    if (error) return { ok: false, error: error.message };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Revoke failed." };
  }
}

