"use client";

import React, { useState } from "react";
import { LogoMark } from "./LogoMark";
import { useBranding } from "./BrandingContext";
import { BRAND_LOGO_IMG_CLASSNAME } from "../lib/brand-logo-classes";

/**
 * Tenant logo from organization / workspace settings, or {@link LogoMark} if missing or broken.
 */
export function BrandLogoImage({ className }: { className?: string }) {
  const { logoUrl } = useBranding();
  const [broken, setBroken] = useState(false);

  if (!logoUrl?.trim() || broken) {
    return <LogoMark />;
  }

  return (
    /* eslint-disable-next-line @next/next/no-img-element */
    <img
      src={logoUrl}
      alt=""
      aria-hidden
      className={[BRAND_LOGO_IMG_CLASSNAME, className ?? ""].filter(Boolean).join(" ")}
      onError={() => setBroken(true)}
    />
  );
}
