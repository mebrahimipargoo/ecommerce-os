import "server-only";

import { isUuidString } from "./uuid";

/**
 * Comma-separated `organizations.id` values treated as internal/platform orgs
 * (e.g. RECOVRA staff). Used to populate the “Platform Users” tab on `/platform/users`.
 *
 * Server-only env: `INTERNAL_PLATFORM_ORGANIZATION_IDS`
 */
export function parseInternalPlatformOrganizationIdsFromEnv(): string[] {
  const raw = process.env.INTERNAL_PLATFORM_ORGANIZATION_IDS?.trim() ?? "";
  if (!raw) return [];
  return [...new Set(raw.split(",").map((s) => s.trim()).filter((id) => isUuidString(id)))];
}
