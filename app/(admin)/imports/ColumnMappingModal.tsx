"use client";

import React, { useEffect, useState } from "react";
import { Check, Loader2, X } from "lucide-react";
import { approveColumnMapping } from "./import-actions";
import { CANONICAL_FIELDS_PER_TYPE } from "../../../lib/csv-import-detected-type";
import { REPORT_TYPE_SPECS } from "../../../lib/csv-import-mapping";
import type { RawReportType } from "../../../lib/raw-report-types";

/**
 * Clean, deduplicated list shown in the mapping modal.
 * Canonical smart-import types are listed first (with "(detected)" labels),
 * legacy/manual-override types follow, UNKNOWN last.
 * Old lowercase duplicates (fba_customer_returns, inventory_ledger …) are intentionally
 * omitted — they are superseded by the canonical uppercase variants above.
 */
const MODAL_REPORT_TYPES: RawReportType[] = [
  // ── Canonical auto-detected types ─────────────────────────────────────────
  "FBA_RETURNS",
  "REMOVAL_ORDER",
  "REMOVAL_SHIPMENT",
  "INVENTORY_LEDGER",
  "REIMBURSEMENTS",
  "SETTLEMENT",
  "SAFET_CLAIMS",
  "TRANSACTIONS",
  "REPORTS_REPOSITORY",
  "PRODUCT_IDENTITY",
  "ALL_ORDERS",
  "REPLACEMENTS",
  "FBA_GRADE_AND_RESELL",
  "MANAGE_FBA_INVENTORY",
  "FBA_INVENTORY",
  "INBOUND_PERFORMANCE",
  "AMAZON_FULFILLED_INVENTORY",
  "RESERVED_INVENTORY",
  "FEE_PREVIEW",
  "MONTHLY_STORAGE_FEES",
  "CATEGORY_LISTINGS",
  "ALL_LISTINGS",
  "ACTIVE_LISTINGS",
  // ── Fallback ──────────────────────────────────────────────────────────────
  "UNKNOWN",
];
import type { RawReportUploadRow } from "../../../lib/raw-report-upload-row";
import { useUserRole } from "../../../components/UserRoleContext";

type Props = {
  row: RawReportUploadRow;
  onClose: () => void;
  /** Called after the mapping is saved so the parent can refresh the list. */
  onSaved: () => void;
};

/** Derives the available CSV header options from the stored metadata or existing column_mapping values. */
function resolveAvailableHeaders(row: RawReportUploadRow): string[] {
  const meta = row.metadata;
  if (
    meta &&
    typeof meta === "object" &&
    Array.isArray((meta as Record<string, unknown>).csv_headers)
  ) {
    return (meta as Record<string, unknown>).csv_headers as string[];
  }
  // Fallback: use the values from any existing mapping as options.
  if (row.column_mapping) {
    return Object.values(row.column_mapping).filter(Boolean);
  }
  return [];
}

export function ColumnMappingModal({ row, onClose, onSaved }: Props) {
  const { actorUserId } = useUserRole();

  const [reportType, setReportType] = useState<RawReportType>(
    (MODAL_REPORT_TYPES.includes(row.report_type as RawReportType)
      ? row.report_type
      : "UNKNOWN") as RawReportType,
  );

  const [mapping, setMapping] = useState<Record<string, string>>(row.column_mapping ?? {});
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);

  const availableHeaders = resolveAvailableHeaders(row);
  const fields = CANONICAL_FIELDS_PER_TYPE[reportType] ?? [];

  // Reset mapping selections when report type changes.
  useEffect(() => {
    setMapping(row.column_mapping ?? {});
  }, [reportType, row.column_mapping]);

  const missingRequired = fields.filter((f) => f.required && !mapping[f.key]);

  async function handleSave() {
    if (missingRequired.length > 0) return;
    setSaving(true);
    setErr(null);
    try {
      const res = await approveColumnMapping({
        uploadId: row.id,
        mapping,
        reportType,
        actorUserId,
      });
      if (!res.ok) {
        setErr(res.error ?? "Failed to save mapping.");
        return;
      }
      setSaved(true);
      setTimeout(() => {
        onSaved();
        onClose();
      }, 800);
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Save failed.");
    } finally {
      setSaving(false);
    }
  }

  return (
    /* Backdrop */
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4 backdrop-blur-sm"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="relative w-full max-w-lg rounded-2xl border border-border bg-card shadow-xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-border px-6 py-4">
          <div>
            <h2 className="text-base font-semibold text-foreground">Map CSV Columns</h2>
            <p className="mt-0.5 text-xs text-muted-foreground">
              Match each field to the actual column name in your CSV file.
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="rounded-lg p-1.5 text-muted-foreground hover:bg-accent"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="space-y-5 px-6 py-5">
          {/* File name */}
          <p className="truncate font-mono text-xs text-muted-foreground">{row.file_name}</p>

          {/* Report type selector */}
          <div>
            <label className="mb-1 block text-xs font-medium text-foreground">
              Report type
            </label>
            <select
              value={reportType}
              onChange={(e) => setReportType(e.target.value as RawReportType)}
              className="h-9 w-full rounded-lg border border-border bg-background px-2 text-sm text-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              {MODAL_REPORT_TYPES.map((v) => {
                const s = REPORT_TYPE_SPECS[v];
                return (
                  <option key={v} value={v}>
                    {s?.shortLabel ?? v}
                  </option>
                );
              })}
            </select>
          </div>

          {/* Field mapping rows */}
          <div className="space-y-3">
            <p className="text-xs font-medium text-foreground">Column assignments</p>

            {availableHeaders.length === 0 && (
              <p className="rounded-lg border border-amber-300/50 bg-amber-50/50 px-3 py-2 text-xs text-amber-700 dark:border-amber-700/40 dark:bg-amber-950/20 dark:text-amber-400">
                No CSV headers were captured during upload. Type the column name manually or
                re-upload the file.
              </p>
            )}

            {fields.map((field) => (
              <div key={field.key} className="flex items-center gap-3">
                <div className="w-44 shrink-0">
                  <span className="text-xs font-medium text-foreground">{field.label}</span>
                  {field.required && (
                    <span className="ml-1 text-[10px] font-semibold uppercase tracking-wider text-destructive">
                      required
                    </span>
                  )}
                </div>
                {availableHeaders.length > 0 ? (
                  <select
                    value={mapping[field.key] ?? ""}
                    onChange={(e) =>
                      setMapping((prev) => {
                        const next = { ...prev };
                        if (e.target.value) {
                          next[field.key] = e.target.value;
                        } else {
                          delete next[field.key];
                        }
                        return next;
                      })
                    }
                    className={[
                      "h-8 flex-1 rounded-lg border bg-background px-2 text-xs text-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring",
                      field.required && !mapping[field.key]
                        ? "border-destructive/60"
                        : "border-border",
                    ].join(" ")}
                  >
                    <option value="">— skip —</option>
                    {availableHeaders.map((h) => (
                      <option key={h} value={h}>
                        {h}
                      </option>
                    ))}
                  </select>
                ) : (
                  <input
                    type="text"
                    placeholder="exact column header"
                    value={mapping[field.key] ?? ""}
                    onChange={(e) =>
                      setMapping((prev) => {
                        const next = { ...prev };
                        if (e.target.value.trim()) {
                          next[field.key] = e.target.value.trim();
                        } else {
                          delete next[field.key];
                        }
                        return next;
                      })
                    }
                    className={[
                      "h-8 flex-1 rounded-lg border bg-background px-2 text-xs text-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring",
                      field.required && !mapping[field.key]
                        ? "border-destructive/60"
                        : "border-border",
                    ].join(" ")}
                  />
                )}
              </div>
            ))}
          </div>

          {/* Validation hint */}
          {missingRequired.length > 0 && (
            <p className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
              Map required fields before saving:{" "}
              {missingRequired.map((f) => f.label).join(", ")}.
            </p>
          )}

          {err && (
            <p className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
              {err}
            </p>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 border-t border-border px-6 py-4">
          <button
            type="button"
            onClick={onClose}
            className="inline-flex items-center gap-2 rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium text-foreground transition hover:bg-accent"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={saving || missingRequired.length > 0 || saved}
            onClick={() => void handleSave()}
            className="inline-flex items-center gap-2 rounded-lg bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground shadow transition hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {saving ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                Saving…
              </>
            ) : saved ? (
              <>
                <Check className="h-4 w-4" />
                Saved
              </>
            ) : (
              "Save Mapping & Enable Sync"
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
