type ExpectedItemLike = { sku: string; description?: string | null };
type PkgLike = {
  package_number: string;
  /** AI-extracted expected line items from packing slip. Null = no manifest scanned yet. */
  expected_items?: ExpectedItemLike[] | null;
};

/**
 * Returns true if the scanned item name appears to match the package's expected items list.
 *
 * Key rules:
 * - If no manifest has been scanned (expected_items is null/empty) → return true (no warning).
 * - Only fires the "unexpected item" warning when a real packing slip has been scanned AND
 *   the item genuinely doesn't appear on any line.
 */
export function itemMatchesPackageExpectation(itemName: string, pkg: PkgLike): boolean {
  const t = itemName.trim().toLowerCase();
  if (!t) return true;

  // No manifest scanned yet — never fire a spurious warning
  if (!pkg.expected_items?.length) return true;

  return pkg.expected_items.some((e) => {
    const skuMatch   = (e.sku ?? "").trim().toLowerCase();
    const descMatch  = (e.description ?? "").toLowerCase().split(/\s+/).slice(0, 3).join(" ");
    return (
      (skuMatch.length  >= 4 && t.includes(skuMatch))  ||
      (descMatch.length >= 5 && t.includes(descMatch))
    );
  });
}
