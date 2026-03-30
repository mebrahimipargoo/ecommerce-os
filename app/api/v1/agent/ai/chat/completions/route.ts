import { NextResponse } from "next/server";

import { getOrganizationOpenAIApiKey } from "@/lib/organization-openai-key";
import { verifyOrganizationApiKey } from "@/lib/organization-workspace-api-key";

const OPENAI_CHAT_COMPLETIONS = "https://api.openai.com/v1/chat/completions";

/** Long-running LLM calls (incl. streaming) on Vercel / similar hosts. */
export const maxDuration = 300;

const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, X-Workspace-API-Key",
  "Access-Control-Max-Age": "86400",
};

function jsonError(message: string, status: number) {
  return NextResponse.json({ error: { message } }, { status, headers: CORS_HEADERS });
}

/**
 * Secure proxy: external agents send `X-Workspace-API-Key` (org key from settings);
 * the server attaches the org OpenAI key and forwards to OpenAI. The OpenAI key is never returned.
 */
export async function OPTIONS() {
  return new Response(null, { status: 204, headers: CORS_HEADERS });
}

export async function POST(req: Request) {
  const workspaceKey = req.headers.get("x-workspace-api-key") ?? "";
  const verified = await verifyOrganizationApiKey(workspaceKey);
  if (!verified.ok) {
    return jsonError("Invalid or missing X-Workspace-API-Key.", 401);
  }

  const openaiKey = await getOrganizationOpenAIApiKey(verified.organizationId);
  if (!openaiKey) {
    return jsonError(
      "OpenAI API key is not configured for this organization. Set credentials.openai_api_key on organization_settings or OPENAI_API_KEY on the server.",
      503,
    );
  }

  const contentType = req.headers.get("content-type") || "application/json";
  let body: ArrayBuffer;
  try {
    body = await req.arrayBuffer();
  } catch {
    return jsonError("Invalid request body.", 400);
  }

  const upstream = await fetch(OPENAI_CHAT_COMPLETIONS, {
    method: "POST",
    headers: {
      "Content-Type": contentType,
      Authorization: `Bearer ${openaiKey}`,
    },
    body,
  });

  const outHeaders = new Headers(CORS_HEADERS);
  upstream.headers.forEach((value, key) => {
    const lower = key.toLowerCase();
    if (
      lower === "transfer-encoding" ||
      lower === "connection" ||
      lower === "keep-alive" ||
      lower === "proxy-authenticate" ||
      lower === "proxy-authorization" ||
      lower === "te" ||
      lower === "trailers" ||
      lower === "upgrade"
    ) {
      return;
    }
    outHeaders.set(key, value);
  });

  if (!upstream.body) {
    const text = await upstream.text();
    return new NextResponse(text, { status: upstream.status, headers: outHeaders });
  }

  return new Response(upstream.body, {
    status: upstream.status,
    statusText: upstream.statusText,
    headers: outHeaders,
  });
}
