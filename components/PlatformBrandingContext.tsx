"use client";

import React, {
  createContext, useCallback, useContext, useEffect, useMemo, useState,
} from "react";
import { supabase } from "@/src/lib/supabase";
import { normalizeTenantLogoUrl } from "../lib/tenant-logo-url";

export type PlatformBrandingContextValue = {
  /** From `public.platform_settings.app_name` */
  platformAppName: string;
  /** Resolved public URL for `platform_settings.logo_url` */
  platformLogoUrl: string;
  loading: boolean;
  refresh: () => Promise<void>;
};

const PlatformBrandingContext = createContext<PlatformBrandingContextValue | null>(null);

export function PlatformBrandingProvider({ children }: { children: React.ReactNode }) {
  const [platformAppName, setPlatformAppName] = useState("");
  const [platformLogoUrl, setPlatformLogoUrl] = useState("");
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const { data, error } = await supabase
        .from("platform_settings")
        .select("app_name, logo_url")
        .eq("id", true)
        .maybeSingle();
      if (error || !data) {
        setPlatformAppName("");
        setPlatformLogoUrl("");
        return;
      }
      const row = data as { app_name?: string | null; logo_url?: string | null };
      const name = String(row.app_name ?? "").trim();
      setPlatformAppName(name);
      setPlatformLogoUrl(normalizeTenantLogoUrl(String(row.logo_url ?? "").trim()));
    } catch {
      setPlatformAppName("");
      setPlatformLogoUrl("");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const value = useMemo(
    () => ({ platformAppName, platformLogoUrl, loading, refresh }),
    [platformAppName, platformLogoUrl, loading, refresh],
  );

  return (
    <PlatformBrandingContext.Provider value={value}>{children}</PlatformBrandingContext.Provider>
  );
}

export function usePlatformBranding(): PlatformBrandingContextValue {
  const ctx = useContext(PlatformBrandingContext);
  if (!ctx) {
    throw new Error("usePlatformBranding must be used within PlatformBrandingProvider");
  }
  return ctx;
}
