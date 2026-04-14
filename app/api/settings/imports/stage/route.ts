/**
 * POST /api/settings/imports/stage
 *
 * Backward-compatible alias for Phase 2 (raw file → `amazon_staging`).
 * Prefer POST /api/settings/imports/process for the unified pipeline entry.
 */

import { executeAmazonPhase2Staging } from "../../../../../lib/pipeline/amazon-phase2-staging";

export const runtime = "nodejs";
export const maxDuration = 300;

export async function POST(req: Request): Promise<Response> {
  return executeAmazonPhase2Staging(req);
}
