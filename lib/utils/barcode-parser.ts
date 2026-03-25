import type { DefaultProductSource } from "../openai-settings";

/**
 * Infers the product source (marketplace) from a scanned barcode.
 *
 * Detection rules:
 * - Barcodes starting with "X00" or "B00" are Amazon FNSKUs → returns "amazon"
 * - All other barcodes fall back to `defaultSource`
 *
 * @param barcode       The raw scanned barcode string.
 * @param defaultSource Fallback source when no prefix match is found
 *                      (e.g. the user's saved Default Product Source setting,
 *                      or the currently selected marketplace in the form).
 * @returns             The detected or fallback source string.
 */
export function parseBarcodeSource(
  barcode: string,
  defaultSource: string,
): DefaultProductSource | string {
  const trimmed = barcode.trim().toUpperCase();

  if (trimmed.startsWith("X00") || trimmed.startsWith("B00")) {
    return "amazon";
  }

  return defaultSource;
}
