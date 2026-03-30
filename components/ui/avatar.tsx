"use client";

import * as React from "react";

/**
 * shadcn-style Avatar primitives — circular crop with object-cover for photos.
 */
export function Avatar({
  className = "",
  children,
}: {
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div
      className={`relative flex h-12 w-12 shrink-0 overflow-hidden rounded-full border border-border bg-muted ${className}`}
    >
      {children}
    </div>
  );
}

export function AvatarImage({
  src,
  alt,
  className = "",
  onError,
}: {
  src: string;
  alt: string;
  className?: string;
  onError?: () => void;
}) {
  return (
    /* eslint-disable-next-line @next/next/no-img-element */
    <img
      src={src}
      alt={alt}
      className={`aspect-square h-full w-full object-cover ${className}`}
      onError={onError}
    />
  );
}

export function AvatarFallback({
  className = "",
  children,
}: {
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div
      className={`flex h-full w-full items-center justify-center rounded-full bg-muted text-sm font-semibold text-muted-foreground ${className}`}
    >
      {children}
    </div>
  );
}
