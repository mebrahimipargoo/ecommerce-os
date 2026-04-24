"use client";

/**
 * When internal staff selects a **tenant** org in the workspace switcher, `/platform/*`
 * is not part of that UI mode — redirect away (backend / RLS unchanged).
 */

import React, { useEffect } from "react";
import { usePathname, useRouter } from "next/navigation";
import { Loader2 } from "lucide-react";
import { useUserRole } from "./UserRoleContext";

export function PlatformWorkspaceGate({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const { workspaceViewMode, workspaceViewModeReady, profileLoading } = useUserRole();

  useEffect(() => {
    if (profileLoading || !workspaceViewModeReady) return;
    if (workspaceViewMode !== "tenant") return;
    const p = pathname ?? "";
    if (!p.startsWith("/platform")) return;
    router.replace("/");
  }, [profileLoading, workspaceViewMode, workspaceViewModeReady, pathname, router]);

  if (profileLoading || !workspaceViewModeReady) {
    return (
      <div className="flex min-h-[40vh] items-center justify-center gap-2 text-muted-foreground">
        <Loader2 className="h-6 w-6 shrink-0 animate-spin" aria-hidden />
        <span className="text-sm">Loading workspace…</span>
      </div>
    );
  }

  if (workspaceViewMode === "tenant") {
    return (
      <div className="flex min-h-[40vh] items-center justify-center gap-2 text-muted-foreground">
        <Loader2 className="h-6 w-6 shrink-0 animate-spin" aria-hidden />
        <span className="text-sm">Switching context…</span>
      </div>
    );
  }

  return <>{children}</>;
}
