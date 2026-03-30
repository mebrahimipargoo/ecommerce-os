/**
 * Amazon claim queue rules shared by returns actions, logistics sync, and batch jobs.
 * Claim-eligible defect reasons match `deriveStatus` / wizard condition chips.
 */
export const CLAIM_CONDITIONS = new Set([
  "empty_box",
  "missing_item",
  "damaged_customer",
  "damaged_carrier",
  "damaged_warehouse",
  "damaged_box",
  "scratched",
  "wrong_item_junk",
  "wrong_item_different",
  "missing_parts",
  "expired",
]);

export function isAmazonStore(
  marketplace: string | null | undefined,
  storePlatform?: string | null,
): boolean {
  const m = (marketplace ?? "").toLowerCase();
  if (m === "amazon") return true;
  const p = (storePlatform ?? "").toLowerCase();
  return p === "amazon" || p.includes("amazon");
}

/** True when this return should enqueue `claim_submissions` for the Python agent (Amazon + claimable defect). */
export function shouldAutoEnqueueAmazonClaimSubmission(
  marketplace: string | null | undefined,
  conditions: string[] | null | undefined,
  storePlatform?: string | null,
): boolean {
  if (!isAmazonStore(marketplace, storePlatform)) return false;
  const list = conditions ?? [];
  return list.some((c) => CLAIM_CONDITIONS.has(c));
}

export function storePlatformFromEmbed(stores: unknown): string | null {
  const s = Array.isArray(stores) ? stores[0] : stores;
  return (s as { platform?: string } | null)?.platform ?? null;
}
