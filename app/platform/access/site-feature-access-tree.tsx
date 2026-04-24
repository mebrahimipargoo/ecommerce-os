"use client";

import React, { useMemo, useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import {
  computeFeatureAccessRows,
  type AccessSource,
  type FeatureAccessRow,
} from "../../../lib/site-feature-access-rows";

function sourceLabel(s: AccessSource): string {
  switch (s) {
    case "role":
      return "Role";
    case "database":
      return "Database";
    case "both":
      return "Role + DB";
    default:
      return "—";
  }
}

function AccessCell({
  granted,
  source,
}: {
  granted: boolean;
  source: AccessSource;
}) {
  if (!granted) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  return (
    <div className="flex flex-col gap-0.5">
      <span className="inline-flex w-fit rounded-md bg-emerald-500/15 px-2 py-0.5 text-xs font-medium text-emerald-800 dark:text-emerald-200">
        Yes
      </span>
      <span className="text-[10px] text-muted-foreground">{sourceLabel(source)}</span>
    </div>
  );
}

/** Write cell uses a different hue */
function WriteCell({
  granted,
  source,
}: {
  granted: boolean;
  source: AccessSource;
}) {
  if (!granted) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  return (
    <div className="flex flex-col gap-0.5">
      <span className="inline-flex w-fit rounded-md bg-sky-500/15 px-2 py-0.5 text-xs font-medium text-sky-900 dark:text-sky-100">
        Yes
      </span>
      <span className="text-[10px] text-muted-foreground">{sourceLabel(source)}</span>
    </div>
  );
}

export type SiteFeatureAccessTreeProps = {
  roleKey: string;
  permissionKeys: string[];
};

export function SiteFeatureAccessTree({ roleKey, permissionKeys }: SiteFeatureAccessTreeProps) {
  const rows = useMemo(
    () => computeFeatureAccessRows(roleKey, permissionKeys),
    [roleKey, permissionKeys],
  );

  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({});

  const toggle = (id: string) => {
    setCollapsed((p) => ({ ...p, [id]: !p[id] }));
  };

  const visibleRows: FeatureAccessRow[] = [];
  let i = 0;
  while (i < rows.length) {
    const row = rows[i]!;
    visibleRows.push(row);
    if (row.isGroup && collapsed[row.id]) {
      const d = row.depth;
      i += 1;
      while (i < rows.length && rows[i]!.depth > d) {
        i += 1;
      }
      continue;
    }
    i += 1;
  }

  return (
    <div className="overflow-x-auto rounded-xl border border-border bg-card shadow-sm">
      <table className="w-full min-w-[720px] border-collapse text-sm">
        <thead>
          <tr className="border-b border-border bg-muted/40 text-left text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
            <th className="px-3 py-2.5">Feature</th>
            <th className="px-3 py-2.5">Route</th>
            <th className="w-[100px] px-3 py-2.5">Read</th>
            <th className="w-[100px] px-3 py-2.5">Write</th>
          </tr>
        </thead>
        <tbody>
          {visibleRows.map((r) => {
            if (r.isGroup) {
              const open = !collapsed[r.id];
              return (
                <tr key={r.id} className="border-b border-border bg-muted/25">
                  <td colSpan={4} className="px-2 py-2">
                    <button
                      type="button"
                      onClick={() => toggle(r.id)}
                      className="flex w-full items-center gap-2 rounded-md px-1 py-1 text-left text-sm font-semibold transition hover:bg-muted/60"
                    >
                      {open ? (
                        <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" />
                      ) : (
                        <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />
                      )}
                      {r.label}
                    </button>
                  </td>
                </tr>
              );
            }
            return (
              <tr key={r.id} className="border-b border-border last:border-0">
                <td
                  className="px-3 py-2 align-top font-medium"
                  style={{ paddingLeft: `${12 + r.depth * 16}px` }}
                >
                  {r.label}
                </td>
                <td className="px-3 py-2 align-top font-mono text-xs text-muted-foreground">
                  {r.path ?? "—"}
                </td>
                <td className="px-3 py-2 align-top">
                  <AccessCell granted={r.read} source={r.readSource} />
                </td>
                <td className="px-3 py-2 align-top">
                  <WriteCell granted={r.write} source={r.writeSource} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      <p className="border-t border-border px-3 py-2 text-[11px] text-muted-foreground">
        <strong className="font-medium text-foreground">Read</strong> = view / navigate;{" "}
        <strong className="font-medium text-foreground">Write</strong> = create or change data.{" "}
        <strong className="font-medium text-foreground">Role</strong> = default app rules for this catalog role;{" "}
        <strong className="font-medium text-foreground">Database</strong> = explicit permission keys from role + groups.
      </p>
    </div>
  );
}
