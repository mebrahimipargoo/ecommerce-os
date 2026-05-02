import type { NextRequest } from "next/server";
import { NextResponse } from "next/server";

/**
 * Proxies /api/etl/* to Python FastAPI.
 *
 * Multipart: forward the raw bytes + original Content-Type (with boundary).
 * `request.formData()` often fails on large bodies ("Failed to parse body as FormData");
 * re-sending FormData from Node can also strip fields. Raw forward matches the client wire format.
 *
 * `ETL_API_ORIGIN` in `.env.local` overrides http://127.0.0.1:8000
 */
export const runtime = "nodejs";
export const maxDuration = 300;

const DEFAULT_ETL_ORIGIN = "http://127.0.0.1:8000";

function etlOrigin(): string {
  return (process.env.ETL_API_ORIGIN?.trim() || DEFAULT_ETL_ORIGIN).replace(/\/$/, "");
}

function upstreamUrl(pathSegments: string[] | undefined): string {
  const origin = etlOrigin();
  const suffix = pathSegments?.length ? pathSegments.join("/") : "";
  return suffix ? `${origin}/etl/${suffix}` : `${origin}/etl`;
}

const REQ_HEADER_ALLOWLIST = ["content-type", "accept", "authorization", "accept-language"] as const;

function forwardRequestHeaders(incoming: Headers): Headers {
  const out = new Headers();
  for (const name of REQ_HEADER_ALLOWLIST) {
    const v = incoming.get(name);
    if (v) out.set(name, v);
  }
  return out;
}

function forwardAuthHeaders(incoming: Headers): Headers {
  const out = new Headers();
  for (const name of ["authorization", "accept-language", "accept"] as const) {
    const v = incoming.get(name);
    if (v) out.set(name, v);
  }
  return out;
}

function forwardResponseHeaders(up: Headers): Headers {
  const out = new Headers();
  const ct = up.get("content-type");
  if (ct) out.set("content-type", ct);
  if (!out.has("access-control-allow-origin")) {
    out.set("access-control-allow-origin", "*");
  }
  return out;
}

const CORS_PREFLIGHT_HEADERS = new Headers({
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, POST, DELETE, PUT, PATCH, OPTIONS",
  "access-control-allow-headers": "content-type, authorization, accept, accept-language",
  "access-control-max-age": "86400",
});

async function proxy(request: NextRequest, path: string[] | undefined) {
  const method = request.method.toUpperCase();
  let url = upstreamUrl(path);
  const search = request.nextUrl.search;
  if (search && (method === "GET" || method === "HEAD" || method === "DELETE")) {
    url += search;
  }

  if (method === "POST" || method === "PUT" || method === "PATCH") {
    const ct = request.headers.get("content-type") || "";
    if (ct.includes("multipart/form-data")) {
      let buf: ArrayBuffer;
      try {
        buf = await request.arrayBuffer();
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return NextResponse.json(
          {
            detail:
              `Could not read multipart upload body (${msg}). ` +
              "Try a smaller file or check your network; the request may have been truncated.",
          },
          { status: 400, headers: { "access-control-allow-origin": "*" } },
        );
      }
      if (!buf.byteLength) {
        return NextResponse.json(
          { detail: "Empty upload body." },
          { status: 400, headers: { "access-control-allow-origin": "*" } },
        );
      }
      const headers = forwardRequestHeaders(request.headers);
      try {
        const upstream = await fetch(url, {
          method,
          headers,
          body: buf,
        });
        return new NextResponse(upstream.body, {
          status: upstream.status,
          statusText: upstream.statusText,
          headers: forwardResponseHeaders(upstream.headers),
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        return NextResponse.json(
          {
            detail:
              `Could not reach Python ETL at ${etlOrigin()}. ` +
              `Run uvicorn on port 8000 or set ETL_API_ORIGIN in .env.local. (${msg})`,
          },
          { status: 503, headers: { "access-control-allow-origin": "*" } },
        );
      }
    }

    try {
      const body = await request.arrayBuffer();
      const headers = forwardRequestHeaders(request.headers);
      const upstream = await fetch(url, {
        method,
        headers,
        body: body.byteLength ? body : undefined,
      });
      return new NextResponse(upstream.body, {
        status: upstream.status,
        statusText: upstream.statusText,
        headers: forwardResponseHeaders(upstream.headers),
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return NextResponse.json(
        {
          detail:
            `Could not reach Python ETL at ${etlOrigin()}. ` +
            `Run uvicorn on port 8000 or set ETL_API_ORIGIN in .env.local. (${msg})`,
        },
        { status: 503, headers: { "access-control-allow-origin": "*" } },
      );
    }
  }

  try {
    const headers = forwardRequestHeaders(request.headers);
    const upstream = await fetch(url, { method, headers });
    return new NextResponse(upstream.body, {
      status: upstream.status,
      statusText: upstream.statusText,
      headers: forwardResponseHeaders(upstream.headers),
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json(
      {
        detail:
          `Could not reach Python ETL at ${etlOrigin()}. ` +
          `Run uvicorn on port 8000 or set ETL_API_ORIGIN in .env.local. (${msg})`,
      },
      { status: 503, headers: { "access-control-allow-origin": "*" } },
    );
  }
}

type RouteCtx = { params: Promise<{ path?: string[] }> };

export async function OPTIONS(_request: NextRequest, _ctx: RouteCtx) {
  return new NextResponse(null, { status: 204, headers: CORS_PREFLIGHT_HEADERS });
}

export async function GET(request: NextRequest, ctx: RouteCtx) {
  const { path } = await ctx.params;
  return proxy(request, path);
}

export async function POST(request: NextRequest, ctx: RouteCtx) {
  const { path } = await ctx.params;
  return proxy(request, path);
}

export async function DELETE(request: NextRequest, ctx: RouteCtx) {
  const { path } = await ctx.params;
  return proxy(request, path);
}

export async function PUT(request: NextRequest, ctx: RouteCtx) {
  const { path } = await ctx.params;
  return proxy(request, path);
}

export async function PATCH(request: NextRequest, ctx: RouteCtx) {
  const { path } = await ctx.params;
  return proxy(request, path);
}
