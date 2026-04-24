import { normalizeRoleKeyForBranding } from "./tenant-branding-permissions";

/** Who may use `/platform/access` (permissions, user access inspector, role/group allow-lists). Canonical `roles.key` / legacy normalized. */
const PLATFORM_ACCESS_CATALOG_ROLE_KEYS = new Set([
  "super_admin",
  "programmer",
  "system_admin",
]);

export function canManagePlatformAccessCatalog(roleKeyRaw: string | null | undefined): boolean {
  const k = normalizeRoleKeyForBranding(roleKeyRaw ?? null);
  return PLATFORM_ACCESS_CATALOG_ROLE_KEYS.has(k);
}
