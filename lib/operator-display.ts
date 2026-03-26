/** Human-readable operator for UI when legacy `created_by` text is absent. */
export function operatorDisplayLabel(row: {
  created_by?: string | null;
  created_by_id?: string | null;
}): string {
  const t = row.created_by?.trim();
  if (t) return t;
  const id = row.created_by_id?.trim();
  if (id) return `${id.slice(0, 8)}…`;
  return "—";
}
