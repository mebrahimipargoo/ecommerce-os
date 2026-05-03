import "server-only";

import { supabaseServer } from "./supabase-server";

/**
 * Reads the OpenAI API key for an organization (server-only).
 *
 * Priority order:
 *  1. organization_api_keys WHERE name='OpenAI' AND role='llm_provider'  (saved from Settings UI)
 *  2. organization_settings.credentials.openai_api_key                   (legacy path)
 *  3. OPENAI_API_KEY env variable                                         (server default)
 */
export async function getOrganizationOpenAIApiKey(organizationId: string): Promise<string | null> {
  // 1. Check organization_api_keys (plaintext provider key saved from Settings page)
  try {
    const { data: rows } = await supabaseServer
      .from("organization_api_keys")
      .select("api_key, name")
      .eq("organization_id", organizationId)
      .eq("role", "llm_provider")
      .order("created_at", { ascending: false })
      .limit(12);
    const list = rows ?? [];
    const exact = list.find((r) => String(r.name ?? "").trim() === "OpenAI");
    const dbKeyExact = exact?.api_key?.trim();
    if (dbKeyExact) return dbKeyExact;
    for (const r of list) {
      const n = String(r.name ?? "").toLowerCase();
      const k = typeof r.api_key === "string" ? r.api_key.trim() : "";
      if (!k) continue;
      if (n.includes("openai") || n.includes("gpt") || n === "chatgpt" || n === "llm") return k;
    }
  } catch {
    /* ignore — fall through to legacy path */
  }

  // 2. Legacy: organization_settings.credentials.openai_api_key
  try {
    const { data } = await supabaseServer
      .from("organization_settings")
      .select("credentials")
      .eq("organization_id", organizationId)
      .maybeSingle();
    const creds = (data?.credentials as Record<string, unknown> | null) ?? {};
    const key = typeof creds.openai_api_key === "string" ? creds.openai_api_key.trim() : "";
    if (key) return key;
  } catch {
    /* ignore */
  }

  // 3. Server env fallback
  const env = process.env.OPENAI_API_KEY?.trim();
  return env || null;
}
