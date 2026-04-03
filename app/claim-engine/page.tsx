import { resolveOrganizationId } from "../../lib/organization";
import { getOrganizationClaimEvidenceDefaults } from "../settings/organization-claim-evidence-actions";
import { getCoreSettings } from "../settings/workspace-settings-actions";
import { listStores } from "../settings/adapters/actions";
import { ClaimEngineClient } from "./ClaimEngineClient";
import { listClaimRowsForClaimEngine } from "./claim-actions";
import { getClaimEngineKpis } from "./claim-crm-actions";
import { listClaimSubmissions } from "./claim-submission-actions";

export const dynamic = "force-dynamic";

export default async function ClaimEnginePage() {
  const DEFAULT_ORGANIZATION_ID = resolveOrganizationId();
  const claimsRes = await listClaimRowsForClaimEngine(DEFAULT_ORGANIZATION_ID);
  const claims = claimsRes.ok ? claimsRes.data : [];
  const claimsError = claimsRes.ok ? null : claimsRes.error ?? null;

  const [coreSettings, storesRes, subRes, kpiRes, claimEvDefaults] = await Promise.all([
    getCoreSettings(),
    listStores(),
    listClaimSubmissions(DEFAULT_ORGANIZATION_ID),
    getClaimEngineKpis(DEFAULT_ORGANIZATION_ID),
    getOrganizationClaimEvidenceDefaults({ organizationId: DEFAULT_ORGANIZATION_ID }),
  ]);

  const stores =
    storesRes.ok && storesRes.data
      ? storesRes.data
          .filter((s) => s.is_active !== false)
          .map((s) => ({ id: s.id, name: s.name, platform: s.platform }))
      : [];

  return (
    <ClaimEngineClient
      claims={claims}
      claimsError={claimsError}
      coreSettings={coreSettings}
      stores={stores}
      organizationId={DEFAULT_ORGANIZATION_ID}
      claimSubmissions={subRes.ok ? subRes.data : []}
      submissionsError={subRes.ok ? null : subRes.error ?? null}
      kpis={kpiRes.ok && kpiRes.data ? kpiRes.data : null}
      kpisError={kpiRes.ok ? null : kpiRes.error ?? null}
      defaultClaimEvidence={claimEvDefaults}
    />
  );
}
