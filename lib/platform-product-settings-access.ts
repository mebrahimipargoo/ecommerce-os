import { normalizeRoleKeyForBranding } from "./tenant-branding-permissions";

const PLATFORM_PRODUCT_SETTINGS_ROLE_KEYS = new Set(["super_admin"]);

/** Who may read/write `public.platform_settings` (app name / logo) — canonical role keys. */
export function canEditPlatformProductSettings(roleKeyRaw: string | null | undefined): boolean {
  const k = normalizeRoleKeyForBranding(roleKeyRaw ?? null);
  return PLATFORM_PRODUCT_SETTINGS_ROLE_KEYS.has(k);
}
