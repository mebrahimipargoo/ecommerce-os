"use client";

/**
 * Tenant / workspace white-label cache (organization + workspace_settings).
 *
 * Used when tenant visuals must refresh (e.g. after saving Settings) and for
 * `BrandLogoImage`. Data is read from `organization_settings` only (not `platform_settings`).
 * App shell chrome (sidebar product row) must use {@link ./PlatformBrandingContext} +
 * {@link LogoMark} only — never this context for platform identity.
 */

import React, {
  createContext, useCallback, useContext, useEffect, useMemo, useState,
} from "react";
import { getTenantBrandingForActor } from "../app/settings/tenant-organization-branding-actions";
import { normalizeTenantLogoUrl } from "../lib/tenant-logo-url";
import { useUserRole } from "./UserRoleContext";

export type BrandingContextValue = {
  /** Tenant display name from `organization_settings` / `organizations.name` (not platform name). */
  companyName: string;
  /** Resolved public URL for tenant logo (`organization_settings.logo_url`). */
  logoUrl: string;
  loading: boolean;
  refresh: () => Promise<void>;
};

const BrandingContext = createContext<BrandingContextValue | null>(null);

export function BrandingProvider({ children }: { children: React.ReactNode }) {
  const { organizationId, actorUserId, profileLoading } = useUserRole();
  const [companyName, setCompanyName] = useState("");
  const [logoUrl, setLogoUrl] = useState("");
  const [loading, setLoading] = useState(true);

  // Track the last org+actor pair so we don't re-fetch branding on every
  // profile-loading cycle that doesn't change the effective identity.
  const lastFetchedKey = React.useRef<string | null>(null);

  const refresh = useCallback(async () => {
    if (!actorUserId?.trim()) {
      setCompanyName("");
      setLogoUrl("");
      setLoading(false);
      return;
    }
    setLoading(true);
    try {
      const b = await getTenantBrandingForActor(actorUserId, organizationId);
      setCompanyName(b.company_display_name.trim());
      setLogoUrl(normalizeTenantLogoUrl(b.logo_url));
    } catch {
      setCompanyName("");
      setLogoUrl("");
    } finally {
      setLoading(false);
    }
  }, [organizationId, actorUserId]);

  useEffect(() => {
    if (profileLoading) return;
    // Only re-fetch when the effective identity actually changed — avoids
    // cascading re-renders when TOKEN_REFRESHED fires on tab focus.
    const key = `${actorUserId ?? ""}:${organizationId ?? ""}`;
    if (key === lastFetchedKey.current) return;
    lastFetchedKey.current = key;
    void refresh();
  }, [refresh, profileLoading, actorUserId, organizationId]);

  const value = useMemo(
    () => ({ companyName, logoUrl, loading, refresh }),
    [companyName, logoUrl, loading, refresh],
  );

  return (
    <BrandingContext.Provider value={value}>{children}</BrandingContext.Provider>
  );
}

export function useBranding(): BrandingContextValue {
  const ctx = useContext(BrandingContext);
  if (!ctx) {
    return {
      companyName: "",
      logoUrl: "",
      loading: false,
      refresh: async () => {},
    };
  }
  return ctx;
}
