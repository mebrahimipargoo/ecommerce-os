"use client";

import React, { useState } from "react";
import { LogoMark } from "./LogoMark";
import { useBranding } from "./BrandingContext";
import { BRAND_LOGO_IMG_CLASSNAME } from "../lib/brand-logo-classes";
import { normalizeTenantLogoUrl } from "../lib/tenant-logo-url";

/**
 * Tenant organization logo from {@link BrandingContext} (DB-backed), or
 * {@link LogoMark} (platform mark from `platform_settings`) if missing or broken.
 *
 * Do not use for AppShell sidebar chrome — use {@link LogoMark} + `PlatformBrandingContext` there.
 */
export function BrandLogoImage({ className }: { className?: string }) {
  const { logoUrl } = useBranding();
  const [broken, setBroken] = useState(false);
  const resolved = normalizeTenantLogoUrl(logoUrl);

  if (!resolved || broken) {
    return <LogoMark />;
  }

  return (
    <div className="flex h-9 min-h-9 min-w-9 shrink-0 items-center justify-center">
      {/* eslint-disable-next-line @next/next/no-img-element */}
      <img
        src={resolved}
        alt=""
        aria-hidden
        className={[BRAND_LOGO_IMG_CLASSNAME, className ?? ""].filter(Boolean).join(" ")}
        onError={() => setBroken(true)}
      />
    </div>
  );
}
