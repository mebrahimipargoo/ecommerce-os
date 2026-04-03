"use client";

/**
 * useProfileNames — resolves an array of profile UUIDs to display names.
 *
 * Uses `resolveProfileNames` server action (service-role key) so it works
 * even when RLS restricts the anon/client Supabase from reading profiles.
 *
 * Usage:
 *   const names = useProfileNames([pallet.created_by, pallet.updated_by]);
 *   const label = names[pallet.created_by ?? ""] ?? "—";
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { isUuidString } from "../lib/uuid";
import { resolveProfileNames } from "../lib/profile-names-actions";

export function useProfileNames(
  ids: (string | null | undefined)[],
): Record<string, string> {
  const [names, setNames] = useState<Record<string, string>>({});
  // Stable ref so the effect closure always sees the latest cache.
  const namesRef = useRef(names);
  namesRef.current = names;

  // Stable cache key — sorted so argument order doesn't cause re-fetches.
  const cacheKey = useMemo(() => {
    return ids
      .filter((id): id is string => typeof id === "string" && isUuidString(id.trim()))
      .map((id) => id.trim())
      .sort()
      .join(",");
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ids.map((id) => id ?? "").join(",")]);

  useEffect(() => {
    if (!cacheKey) return;

    const validIds = cacheKey.split(",").filter(Boolean);
    const missing = validIds.filter((id) => !(id in namesRef.current));
    if (!missing.length) return;

    let cancelled = false;
    void resolveProfileNames(missing).then((resolved) => {
      if (cancelled) return;
      setNames((prev) => ({ ...prev, ...resolved }));
    });

    return () => { cancelled = true; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cacheKey]);

  return names;
}
