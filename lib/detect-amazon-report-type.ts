import type { RawReportType } from "./raw-report-types";

/**
 * Heuristic auto-detection for standard Amazon CSV exports from file name and first row text.
 * More specific rules run first (Ledger vs Return vs SAFE-T) before broad filename matches like "Order".
 */
export function detectReportType(
  fileName: string,
  firstRowString: string,
): RawReportType | null {
  const name = fileName.toLowerCase();
  const row = firstRowString.toLowerCase();

  if (name.includes("ledger") || row.includes("starting warehouse balance")) {
    return "inventory_ledger";
  }
  if (name.includes("return") || row.includes("license plate number")) {
    return "fba_customer_returns";
  }
  if (name.includes("safe-t")) {
    return "safe_t_claims";
  }
  if (name.includes("removal") || name.includes("order")) {
    /** Amazon "Removal Order Detail" — closest canonical bucket for order-shaped exports. */
    return "transaction_view";
  }
  return null;
}
