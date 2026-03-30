"use client";

import React, {
  createContext, useCallback, useContext, useEffect, useMemo, useState,
} from "react";
import { getCoreSettings } from "../app/settings/workspace-settings-actions";

export type BrandingContextValue = {
  companyName: string;
  /** Resolved public URL for tenant logo (organization_settings + core_settings). */
  logoUrl: string;
  loading: boolean;
  refresh: () => Promise<void>;
};

const BrandingContext = createContext<BrandingContextValue | null>(null);

export function BrandingProvider({ children }: { children: React.ReactNode }) {
  const [companyName, setCompanyName] = useState("");
  const [logoUrl, setLogoUrl] = useState("");
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const cfg = await getCoreSettings();
      const name =
        (typeof cfg.company_name === "string" && cfg.company_name.trim()) ||
        (typeof cfg.workspace_name === "string" && String(cfg.workspace_name).trim()) ||
        "";
      const logo =
        (typeof cfg.company_logo_url === "string" && cfg.company_logo_url.trim()) ||
        (typeof cfg.logo_url === "string" && cfg.logo_url.trim()) ||
        "";
      setCompanyName(name);
      setLogoUrl(logo);
    } catch {
      setCompanyName("");
      setLogoUrl("");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

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
