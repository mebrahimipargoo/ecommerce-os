import "server-only";

import { supabaseServer } from "./supabase-server";

/**
 * Reads the OpenAI API key for an organization (server-only).
 * Order: `organization_settings.credentials.openai_api_key`, then `OPENAI_API_KEY` env.
 */
export async function getOrganizationOpenAIApiKey(organizationId: string): Promise<string | null> {
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
  const env = process.env.OPENAI_API_KEY?.trim();
  return env || null;
}
