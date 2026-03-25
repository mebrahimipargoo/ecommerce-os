/**
 * Packing-slip line extraction via Next.js API route (proxies OpenAI; avoids browser CORS).
 * Key is read from localStorage on the client and sent as Bearer to `/api/openai/packing-slip`.
 */

export type PackingSlipLine = { barcode: string; expected_qty: number };

function stripJsonFence(text: string): string {
  const t = text.trim();
  const fence = /^```(?:json)?\s*([\s\S]*?)```$/m.exec(t);
  if (fence) return fence[1].trim();
  return t;
}

function parseLinesJson(content: string): PackingSlipLine[] {
  const raw = stripJsonFence(content);
  const parsed = JSON.parse(raw) as unknown;
  if (!Array.isArray(parsed)) throw new Error("Response is not a JSON array.");
  const out: PackingSlipLine[] = [];
  for (const row of parsed) {
    if (!row || typeof row !== "object") continue;
    const o = row as Record<string, unknown>;
    const barcode = String(o.barcode ?? "").trim();
    const q = Number(o.expected_qty);
    if (!barcode || !Number.isFinite(q) || q < 0) continue;
    out.push({ barcode, expected_qty: Math.floor(q) });
  }
  return out;
}

export async function fetchPackingSlipLinesWithOpenAI(
  imageFile: File,
  apiKey: string,
): Promise<{ ok: true; lines: PackingSlipLine[] } | { ok: false; error: string }> {
  if (!apiKey) return { ok: false, error: "Missing API key." };

  const buf = await imageFile.arrayBuffer();
  const bytes = new Uint8Array(buf);
  let binary = "";
  for (let i = 0; i < bytes.byteLength; i++) binary += String.fromCharCode(bytes[i]!);
  const base64 = btoa(binary);
  const mimeType = imageFile.type || "image/jpeg";

  const res = await fetch("/api/openai/packing-slip", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({ imageBase64: base64, mimeType }),
  });

  const json = (await res.json()) as { error?: string; content?: string };

  if (!res.ok) {
    return { ok: false, error: json.error ?? `Request failed (${res.status}).` };
  }

  const content = json.content;
  if (!content || typeof content !== "string") {
    return { ok: false, error: "Empty response from vision proxy." };
  }

  try {
    const lines = parseLinesJson(content);
    return { ok: true, lines };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : "Could not parse JSON from OpenAI.",
    };
  }
}
