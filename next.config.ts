import type { NextConfig } from "next";

/** ETL traffic is proxied by `app/api/etl/[[...path]]/route.ts` (streaming) — no rewrites needed. */
const nextConfig: NextConfig = {};

export default nextConfig;
