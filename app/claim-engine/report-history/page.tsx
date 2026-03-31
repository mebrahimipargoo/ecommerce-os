import { Suspense } from "react";
import { resolveOrganizationId } from "../../../lib/organization";
import { listClaimReportHistory } from "../claim-report-history-actions";
import { ClaimReportHistoryClient } from "../ClaimReportHistoryClient";

/** Always load latest submissions from Supabase (not static at build time). */
export const dynamic = "force-dynamic";

function defaultRangeIso(): { dateFrom: string; dateTo: string } {
  const now = new Date();
  const to = now.toISOString();
  const from = new Date(now);
  from.setUTCDate(from.getUTCDate() - 90);
  return { dateFrom: from.toISOString(), dateTo: to };
}

async function ReportHistoryLoader() {
  const organizationId = resolveOrganizationId();
  const { dateFrom, dateTo } = defaultRangeIso();
  const res = await listClaimReportHistory({ organizationId, dateFrom, dateTo });
  return (
    <ClaimReportHistoryClient
      organizationId={organizationId}
      initialRows={res.ok ? res.data : []}
      initialError={res.ok ? null : res.error ?? "Failed to load report history."}
    />
  );
}

export default function ClaimReportHistoryPage() {
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <Suspense
        fallback={
          <div className="flex flex-1 items-center justify-center p-8 text-sm text-slate-500">
            Loading report history…
          </div>
        }
      >
        <ReportHistoryLoader />
      </Suspense>
    </div>
  );
}
