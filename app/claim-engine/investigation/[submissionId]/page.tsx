import { notFound } from "next/navigation";
import { getClaimInvestigationPayload } from "../../claim-crm-actions";
import { ClaimInvestigationClient } from "../../ClaimInvestigationClient";

export const dynamic = "force-dynamic";

const DEFAULT_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000001";

export default async function ClaimInvestigationPage({
  params,
}: {
  params: Promise<{ submissionId: string }>;
}) {
  const { submissionId } = await params;
  const res = await getClaimInvestigationPayload(submissionId, DEFAULT_ORGANIZATION_ID);
  if (!res.ok || !res.data) notFound();

  return (
    <ClaimInvestigationClient
      submission={res.data.submission}
      logs={res.data.logs}
      returnRow={res.data.returnRow}
      previewUrl={res.data.preview_url}
    />
  );
}
