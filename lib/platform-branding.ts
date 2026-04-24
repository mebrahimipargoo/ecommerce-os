/**
 * Shell chrome helpers that are not tenant-specific.
 *
 * Product name and logo URL are loaded from `public.platform_settings` via
 * {@link ../components/PlatformBrandingContext} (client) and
 * {@link ./platform-settings-read} (server metadata).
 *
 * Optional env override for tagline only (product subtitle under the name).
 */

const trim = (v: string | undefined) => (v ?? "").trim();

/** Short subtitle under the platform name in the shell chrome (optional env). */
export const PLATFORM_TAGLINE = trim(process.env.NEXT_PUBLIC_PLATFORM_TAGLINE) || "Returns ERP";

/** 1–2 character mark when no `platform_settings.logo_url` is set. */
export function monogramFromAppName(raw: string): string {
  const t = raw.replace(/\s+/g, "").trim();
  if (t.length >= 2) return t.slice(0, 2).toUpperCase();
  if (t.length === 1) return `${t}${t}`.toUpperCase();
  return "";
}
