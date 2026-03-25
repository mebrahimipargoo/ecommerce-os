import { NextResponse } from "next/server";

const VISION_MODEL = "gpt-4o";
const PROMPT =
  "Read this packing slip. Return ONLY a valid JSON array of objects with keys barcode (string) and expected_qty (number). Do not include markdown formatting.";

/**
 * Proxies packing-slip vision to OpenAI (avoids browser CORS on api.openai.com).
 * Pass the operator key: `Authorization: Bearer sk-...` (same key stored in localStorage on the client).
 */
export async function POST(req: Request) {
  const auth = req.headers.get("authorization");
  const apiKey = auth?.startsWith("Bearer ") ? auth.slice(7).trim() : "";
  if (!apiKey) {
    return NextResponse.json({ error: "Missing Authorization: Bearer <OpenAI key>" }, { status: 401 });
  }

  let body: { imageBase64?: string; mimeType?: string };
  try {
    body = (await req.json()) as { imageBase64?: string; mimeType?: string };
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const b64 = body.imageBase64?.trim();
  if (!b64) {
    return NextResponse.json({ error: "Missing imageBase64" }, { status: 400 });
  }
  const mime = body.mimeType?.trim() || "image/jpeg";
  const dataUrl = `data:${mime};base64,${b64}`;

  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: VISION_MODEL,
      temperature: 0.1,
      max_tokens: 4096,
      messages: [
        {
          role: "user",
          content: [
            { type: "text", text: PROMPT },
            { type: "image_url", image_url: { url: dataUrl, detail: "high" } },
          ],
        },
      ],
    }),
  });

  const json = (await res.json()) as {
    error?: { message?: string };
    choices?: { message?: { content?: string | null } }[];
  };

  if (!res.ok) {
    return NextResponse.json(
      { error: json.error?.message ?? `OpenAI error (${res.status})` },
      { status: res.status >= 400 && res.status < 600 ? res.status : 502 },
    );
  }

  const content = json.choices?.[0]?.message?.content;
  if (!content || typeof content !== "string") {
    return NextResponse.json({ error: "Empty OpenAI response" }, { status: 502 });
  }

  return NextResponse.json({ content });
}
