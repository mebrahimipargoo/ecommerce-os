import { isUuidString } from "./uuid";

/**
 * Placeholder UUID written by `resolveActorUserId()` when the actor string is
 * not a real UUID (legacy behaviour before actor_profile_id was wired up).
 * Records created before the fix carry this value — we display "Admin" for them.
 */
const MVP_ACTOR_UUID = "00000000-0000-0000-0000-0000000000fe";

/**
 * Human-readable operator label for UI table cells.
 *
 * Resolution order:
 *   1. `nameMap[created_by]` — pre-resolved via `useProfileNames` hook (preferred).
 *   2. If `created_by` is the legacy MVP placeholder UUID, show "Admin".
 *   3. If `created_by` is not a UUID (legacy plain-text actor name), return it as-is.
 *   4. Truncated UUID prefix as last-resort fallback.
 *   5. "—" when nothing is available.
 *
 * Pass `nameMap` from `useProfileNames([...created_by ids])` in the table component
 * to avoid showing raw UUIDs in the Operator column.
 */
export function operatorDisplayLabel(
  row: { created_by?: string | null },
  nameMap?: Record<string, string>,
): string {
  const t = row.created_by?.trim();
  if (!t) return "—";

  // Resolved via server-action backed name cache.
  if (nameMap && nameMap[t]) return nameMap[t];

  // Legacy placeholder — real profile never existed for this value.
  if (t === MVP_ACTOR_UUID) return "Admin";

  // Legacy plain-text actor slug (e.g. "operator", "admin").
  if (!isUuidString(t)) return t;

  // Real UUID not yet resolved — show short prefix to avoid visual noise.
  return `${t.slice(0, 8)}…`;
}
