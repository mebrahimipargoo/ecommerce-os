"use client";

import { useMemo, useState } from "react";

export type SortDir = "asc" | "desc";

/**
 * Lightweight column sort + global text filter (no TanStack dependency).
 * `pickText` should return a lowercase string to match against `filter`.
 */
export function useTableSortFilter<T>(
  rows: T[],
  options: {
    filter: string;
    sortKey: string | null;
    sortDir: SortDir;
    columns: { key: string; pickText: (row: T) => string }[];
  },
) {
  const { filter, sortKey, sortDir, columns } = options;

  return useMemo(() => {
    const q = filter.trim().toLowerCase();
    let list = rows;
    if (q) {
      list = rows.filter((row) =>
        columns.some((c) => c.pickText(row).toLowerCase().includes(q)),
      );
    }
    if (sortKey) {
      const col = columns.find((c) => c.key === sortKey);
      if (col) {
        list = [...list].sort((a, b) => {
          const va = col.pickText(a).toLowerCase();
          const vb = col.pickText(b).toLowerCase();
          const cmp = va.localeCompare(vb, undefined, { numeric: true });
          return sortDir === "asc" ? cmp : -cmp;
        });
      }
    }
    return list;
  }, [rows, filter, sortKey, sortDir, columns]);
}

export function useSortFilterState() {
  const [filter, setFilter] = useState("");
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  function toggleSort(key: string) {
    if (sortKey !== key) {
      setSortKey(key);
      setSortDir("asc");
    } else {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    }
  }

  return { filter, setFilter, sortKey, setSortKey, sortDir, setSortDir, toggleSort };
}
