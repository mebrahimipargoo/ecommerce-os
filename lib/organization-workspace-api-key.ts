import "server-only";

import { createHash } from "crypto";

import { supabaseServer } from "./supabase-server";

export function hashOrganizationApiKeySecret(plaintext: string): string {
  return createHash("sha256").update(plaintext, "utf8").digest("hex");
}

/**
 * Validates a presented workspace API key (e.g. `X-Workspace-API-Key` on agent routes).
 */
export async function verifyOrganizationApiKey(
  presentedSecret: string,
): Promise<{ ok: true; organizationId: string; keyId: string } | { ok: false }> {
  const trimmed = presentedSecret.trim();
  if (!trimmed.startsWith("eos_") || trimmed.length < 20) return { ok: false };
  const digest = hashOrganizationApiKeySecret(trimmed);
  try {
    const { data, error } = await supabaseServer
      .from("organization_api_keys")
      .select("id, company_id")
      .eq("api_key", digest)
      .maybeSingle();
    if (error || !data) return { ok: false };
    return {
      ok: true,
      organizationId: data.company_id as string,
      keyId: data.id as string,
    };
  } catch {
    return { ok: false };
  }
}
