import "server-only";

import { canPickWorkspaceOrganizationForTenantBranding } from "./tenant-branding-permissions";
import { isUuidString } from "./uuid";

/** Subset of company-settings actor fields used for org resolution (sync helper — not a Server Action). */
export type CompanyBrandingOrgActor = {
  organizationId: string | null;
  roleKey: string | null;
};

export function resolveEffectiveCompanyOrganizationId(
  actor: CompanyBrandingOrgActor,
  organizationIdHint: string | null | undefined,
): string | null {
  const hint = organizationIdHint?.trim();
  const hintOk = hint && isUuidString(hint) ? hint : null;
  const home =
    actor.organizationId && isUuidString(actor.organizationId) ? actor.organizationId : null;
  if (canPickWorkspaceOrganizationForTenantBranding(actor.roleKey)) {
    return hintOk ?? home;
  }
  return home;
}
