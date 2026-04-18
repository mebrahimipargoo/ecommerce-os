import React, { useState } from "react";
import { monogramFromAppName } from "../lib/platform-branding";
import { usePlatformBranding } from "./PlatformBrandingContext";
/**
 * Platform logo or monogram for AppShell (sidebar + mobile drawer).
 * Data source: `public.platform_settings` via {@link usePlatformBranding}.
 */
export function LogoMark({ className }: { className?: string }) {
  const { platformAppName, platformLogoUrl, loading } = usePlatformBranding();
  const [imgBroken, setImgBroken] = useState(false);
  const monogram = monogramFromAppName(platformAppName);
  const showImg = Boolean(platformLogoUrl) && !imgBroken;

  return (
    <div
      className={[
        "flex h-9 w-9 shrink-0 items-center justify-center overflow-hidden rounded-xl bg-primary/10 ring-1 ring-primary/40",
        className ?? "",
      ].filter(Boolean).join(" ")}
    >
      {showImg ? (
        // eslint-disable-next-line @next/next/no-img-element
        <img
          src={platformLogoUrl}
          alt=""
          aria-hidden
          className="h-full w-full object-contain p-0.5"
          onError={() => setImgBroken(true)}
        />
      ) : (
        <span className="text-[11px] font-bold leading-none tracking-tight text-primary">
          {loading ? "…" : monogram || "·"}
        </span>
      )}
    </div>
  );
}
