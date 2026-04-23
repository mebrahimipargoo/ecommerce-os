export const ORG_ENTITLEMENTS_UPDATED_EVENT = "ecommerce-org-entitlements-updated" as const;

export type OrgEntitlementsUpdatedDetail = {
  organizationId: string;
};

/** Fired from org Modules & entitlements after a successful save so Access Management and other UIs can refresh. */
export function dispatchOrgEntitlementsUpdated(organizationId: string) {
  if (typeof window === "undefined") return;
  const id = organizationId.trim();
  if (!id) return;
  window.dispatchEvent(
    new CustomEvent<OrgEntitlementsUpdatedDetail>(ORG_ENTITLEMENTS_UPDATED_EVENT, { detail: { organizationId: id } }),
  );
}
