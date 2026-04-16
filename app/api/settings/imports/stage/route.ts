/**
 * POST /api/settings/imports/stage
 *
 * Backward-compatible alias for Phase 2 (raw file → `amazon_staging`).
 * Prefer POST /api/settings/imports/process for the unified pipeline entry.
 *
 * Registry-driven: same Phase 2 engine as Process (including listing report types).
 */

import { executeAmazonPhase2Staging, type StageRequestBody } from "../../../../../lib/pipeline/amazon-phase2-staging";

export const runtime = "nodejs";
export const maxDuration = 300;

export async function POST(req: Request): Promise<Response> {
  const body = (await req.json()) as StageRequestBody;
  return executeAmazonPhase2Staging(body);
}
