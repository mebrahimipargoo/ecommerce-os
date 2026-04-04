import { notFound } from "next/navigation";
import { getClaimInvestigationPayload } from "../../claim-crm-actions";
import { ClaimInvestigationClient } from "../../ClaimInvestigationClient";

export const dynamic = "force-dynamic";

const DEFAULT_ORGANIZATION_ID = "00000000-0000-0000-0000-000000000001";

export default async function ClaimInvestigationPage({
  params,
  searchParams,
}: {
  params: Promise<{ submissionId: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const { submissionId } = await params;
  const sp = await searchParams;
  const ro = sp.readonly;
  const readOnly =
    ro === "1" ||
    ro === "true" ||
    (Array.isArray(ro) ? ro[0] === "1" || ro[0] === "true" : false);

  const res = await getClaimInvestigationPayload(submissionId, DEFAULT_ORGANIZATION_ID);
  if (!res.ok || !res.data) notFound();

  return (
    <ClaimInvestigationClient
      submission={res.data.submission}
      logs={res.data.logs}
      returnRow={res.data.returnRow}
      previewUrl={res.data.preview_url}
      readOnly={readOnly}
    />
  );
}
