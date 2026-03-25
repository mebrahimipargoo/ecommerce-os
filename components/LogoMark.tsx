import React from "react";

/** Small app mark — shared by AppShell sidebar, TopHeader, and mobile drawer. */
export function LogoMark() {
  return (
    <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-xl bg-primary/10 ring-1 ring-primary/40">
      <span className="text-sm font-bold text-primary">OS</span>
    </div>
  );
}
