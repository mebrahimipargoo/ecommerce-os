"use client";

/**
 * Workspace org + “view as” in the mobile nav drawer so the same controls exist
 * as in TopHeader on every screen size.
 */

import React from "react";
import Link from "next/link";
import { Building2 } from "lucide-react";
import { useUserRole } from "./UserRoleContext";
import { useBranding } from "./BrandingContext";
import { useRbacPermissions } from "../hooks/useRbacPermissions";
import { ViewAsUserPicker } from "./ViewAsUserPicker";
import { WorkspaceOrganizationPicker } from "./WorkspaceOrganizationPicker";

function TenantMarkCompact({ logoUrl }: { logoUrl: string }) {
  const [broken, setBroken] = React.useState(false);
  const url = logoUrl.trim();
  if (!url || broken) {
    return <Building2 className="h-4 w-4 shrink-0 text-muted-foreground" aria-hidden />;
  }
  return (
    // eslint-disable-next-line @next/next/no-img-element
    <img
      src={url}
      alt=""
      className="h-5 max-h-5 w-auto max-w-[3.5rem] shrink-0 object-contain"
      onError={() => setBroken(true)}
    />
  );
}

export function DrawerWorkspaceBar({ onClose }: { onClose: () => void }) {
  const perms = useRbacPermissions();
  const { logoUrl: tenantLogoUrl } = useBranding();
  const {
    organizationId,
    organizationName,
    workspaceOrganizations,
    setWorkspaceOrganizationId,
    viewAsProfileId,
    setViewAsProfileId,
    viewAsProfileOptions,
    viewAsProfileOptionsLoading,
    actorName,
    actorUserId,
  } = useUserRole();

  const viewAsOptionsOthers = React.useMemo(
    () => viewAsProfileOptions.filter((p) => p.profile_id !== actorUserId),
    [viewAsProfileOptions, actorUserId],
  );

  const showViewAs =
    perms.canSwitchOrganization &&
    Boolean(organizationId) &&
    (viewAsProfileOptionsLoading || viewAsOptionsOthers.length > 0);

  if (!perms.canSwitchOrganization && !showViewAs) {
    return (
      <div className="space-y-2 border-b border-sidebar-border px-3 py-2">
        <Link
          href="/"
          onClick={onClose}
          className="flex items-center gap-2 rounded-lg border border-border bg-muted/30 px-2 py-2 text-xs font-medium text-sidebar-foreground transition hover:bg-muted/50"
        >
          <TenantMarkCompact logoUrl={tenantLogoUrl} />
          <span className="min-w-0 truncate">{organizationName}</span>
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-2 border-b border-sidebar-border px-3 py-2">
      {perms.canSwitchOrganization && workspaceOrganizations.length > 0 ? (
        <div className="flex items-center gap-2 rounded-lg border border-border bg-muted/30 px-2 py-1.5">
          <Link
            href="/"
            onClick={onClose}
            className="shrink-0 rounded-md p-0.5 outline-none ring-sidebar-ring hover:bg-muted/60 focus-visible:ring-2"
            title="Home / Dashboard"
            aria-label="Go to home / dashboard"
          >
            <TenantMarkCompact logoUrl={tenantLogoUrl} />
          </Link>
          <WorkspaceOrganizationPicker
            options={workspaceOrganizations}
            value={organizationId}
            onChange={setWorkspaceOrganizationId}
            dense
            highZ
            leadingIcon={false}
            triggerClassName="flex w-full min-w-0 flex-1 items-center justify-between gap-0.5 border-0 bg-transparent py-0.5 text-left text-[11px] font-medium text-sidebar-foreground outline-none ring-sidebar-ring focus-visible:ring-2 disabled:opacity-50"
          />
        </div>
      ) : (
        <Link
          href="/"
          onClick={onClose}
          className="flex items-center gap-2 rounded-lg border border-border bg-muted/30 px-2 py-2 text-xs font-medium text-sidebar-foreground transition hover:bg-muted/50"
        >
          <TenantMarkCompact logoUrl={tenantLogoUrl} />
          <span className="min-w-0 truncate">{organizationName}</span>
        </Link>
      )}
      {showViewAs ? (
        <ViewAsUserPicker
          actorName={actorName}
          actorUserId={actorUserId}
          options={viewAsProfileOptions}
          value={viewAsProfileId}
          onChange={setViewAsProfileId}
          disabled={viewAsProfileOptionsLoading}
          dense
          highZ
        />
      ) : null}
    </div>
  );
}
