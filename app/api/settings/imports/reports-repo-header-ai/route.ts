import { NextResponse } from "next/server";

import { getOrganizationOpenAIApiKey } from "../../../../../lib/organization-openai-key";
import { resolveWriteOrganizationId } from "../../../../../lib/server-tenant";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";

type Body = { lines?: unknown; actor_user_id?: unknown };

/**
 * POST /api/settings/imports/reports-repo-header-ai
 *
 * When keyword + regex header detection fails, returns the 0-based line index of the CSV
 * header row by scanning up to 20 text lines (lightweight gpt-4o-mini JSON response).
 */
export async function POST(req: Request): Promise<Response> {
  try {
    const body = (await req.json()) as Body;
    const linesRaw = body.lines;
    const actor =
      typeof body.actor_user_id === "string" && isUuidString(body.actor_user_id.trim())
        ? body.actor_user_id.trim()
        : null;

    const lines = Array.isArray(linesRaw)
      ? linesRaw.slice(0, 20).map((l) => String(l ?? "").trimEnd())
      : [];

    if (lines.length === 0) {
      return NextResponse.json({ ok: false, error: "Missing lines array." }, { status: 400 });
    }

    const orgId = await resolveWriteOrganizationId(actor, null);
    if (!isUuidString(orgId)) {
      return NextResponse.json({ ok: false, error: "Invalid organization scope." }, { status: 400 });
    }

    const key = await getOrganizationOpenAIApiKey(orgId);
    if (!key) {
      return NextResponse.json({
        ok: true,
        line_index: null,
        skipped: "no_openai_key",
      });
    }

    const listing = lines.map((line, i) => `${i}: ${line.slice(0, 500)}`).join("\n");

    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${key}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        temperature: 0,
        max_tokens: 64,
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content: `You identify which line (0-19) is the CSV column header row for an Amazon seller report.
The header row typically contains BOTH "date" and "time" (or "date/time") AND "settlement" near "id".
Respond ONLY with JSON: {"line_index": number} where line_index is 0-19, or -1 if unsure.`,
          },
          { role: "user", content: `Lines:\n${listing}` },
        ],
      }),
    });

    if (!res.ok) {
      return NextResponse.json({ ok: true, line_index: null, skipped: "openai_http" });
    }

    const json = (await res.json()) as { choices?: { message?: { content?: string } }[] };
    const raw = json.choices?.[0]?.message?.content?.trim() ?? "";
    let line_index: number | null = null;
    try {
      const parsed = JSON.parse(raw) as { line_index?: number };
      if (typeof parsed.line_index === "number" && Number.isFinite(parsed.line_index)) {
        const n = Math.floor(parsed.line_index);
        line_index = n >= 0 && n < 20 ? n : null;
      }
    } catch {
      line_index = null;
    }

    return NextResponse.json({ ok: true, line_index });
  } catch (e) {
    return NextResponse.json(
      { ok: false, error: e instanceof Error ? e.message : "AI header detection failed." },
      { status: 500 },
    );
  }
}
