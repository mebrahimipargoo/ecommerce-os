/** Human-readable labels for `returns.conditions` / claim source_payload (no React). */
export const CLAIM_DEFECT_LABELS: Record<string, string> = {
  damaged_box: "Damaged Box",
  damaged_customer: "Damaged Product",
  damaged_carrier: "Damaged (carrier)",
  damaged_warehouse: "Damaged (warehouse)",
  scratched: "Scratched",
  wrong_item_junk: "Wrong Item (unsellable)",
  wrong_item_different: "Wrong Item",
  missing_parts: "Missing Parts",
  empty_box: "Empty Box",
  missing_item: "Missing Item",
  expired: "Expired",
  sellable: "Sellable / OK",
};

export function defectReasonsPayload(conditions: string[]): {
  defect_reasons: string[];
  defect_reason_labels: string[];
  defect_reason_primary: string | null;
} {
  const keys = conditions.filter((k) => k !== "sellable");
  const labels = keys.map((k) => CLAIM_DEFECT_LABELS[k] ?? k);
  return {
    defect_reasons: keys,
    defect_reason_labels: labels,
    defect_reason_primary: keys.length ? (labels[0] ?? null) : null,
  };
}
