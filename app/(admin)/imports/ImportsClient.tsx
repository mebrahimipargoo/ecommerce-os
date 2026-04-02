"use client";

import React, { useCallback, useState } from "react";
import { Database } from "lucide-react";
import { RawReportUploader } from "./RawReportUploader";
import { RawReportImportsPanel } from "./RawReportImportsPanel";
import { useUserRole } from "../../../components/UserRoleContext";

export function ImportsClient() {
  const { organizationId } = useUserRole();
  const [historyKey, setHistoryKey] = useState(0);

  const refreshHistory = useCallback(() => {
    setHistoryKey((k) => k + 1);
  }, []);

  return (
    <div className="mx-auto w-full max-w-6xl px-4 py-6 md:px-6">
      <div className="mb-8 flex flex-col gap-2">
        <div className="flex items-center gap-2 text-muted-foreground">
          <Database className="h-5 w-5 shrink-0" aria-hidden />
          <span className="text-xs font-semibold uppercase tracking-widest">Data Management</span>
        </div>
        <h1 className="text-2xl font-semibold tracking-tight text-foreground">Imports</h1>
        <p className="max-w-2xl text-sm text-muted-foreground">
          Amazon Inventory Ledger (date filters + manual CSV) for staging, plus chunked raw report
          uploads (40MB parts). CSV, TXT, and Excel supported.
        </p>
      </div>

      <RawReportUploader onUploadComplete={refreshHistory} />

      <RawReportImportsPanel key={historyKey} companyId={organizationId} />
    </div>
  );
}
