/**
 * Tenant / organization context for multi-tenant data fetches.
 * Wire `NEXT_PUBLIC_ORGANIZATION_ID` (or server-only `ORGANIZATION_ID`) to the signed-in org when auth is added.
 */
export const FALLBACK_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000001";

export function resolveOrganizationId(): string {
  const fromEnv =
    (typeof process !== "undefined" && process.env?.NEXT_PUBLIC_ORGANIZATION_ID) ||
    (typeof process !== "undefined" && process.env?.ORGANIZATION_ID);
  if (typeof fromEnv === "string" && fromEnv.trim().length > 0) return fromEnv.trim();
  return FALLBACK_ORGANIZATION_ID;
}
