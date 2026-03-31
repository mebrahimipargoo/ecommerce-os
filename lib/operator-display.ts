/** Human-readable operator for UI — `created_by` is the actor user id (UUID). */
export function operatorDisplayLabel(row: {
  created_by?: string | null;
}): string {
  const t = row.created_by?.trim();
  if (t) return t;
  return "—";
}
