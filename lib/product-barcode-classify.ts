/**
 * Classifies Amazon / retail product scans for the returns wizard.
 * - FNSKU: Amazon FC labels often start with X00…
 * - ASIN: 10 alphanumeric starting with B (Amazon standard catalog id)
 * - UPC/EAN: 8–13 digits (numeric)
 */
export type ProductBarcodeKind = "fnsku" | "asin" | "upc_ean" | "unknown";

export function classifyProductBarcode(raw: string): {
  kind: ProductBarcodeKind;
  /** Normalized code for the target field (uppercase ASIN/FNSKU, digits-only for UPC/EAN). */
  normalized: string;
} {
  const t = raw.trim();
  if (!t) return { kind: "unknown", normalized: "" };

  const digitsOnly = t.replace(/\D/g, "");
  if (digitsOnly.length >= 8 && digitsOnly.length <= 13 && /^\d+$/.test(digitsOnly)) {
    return { kind: "upc_ean", normalized: digitsOnly };
  }

  const upper = t.toUpperCase().replace(/\s+/g, "");
  if (upper.startsWith("X00")) {
    return { kind: "fnsku", normalized: upper };
  }

  if (upper.length === 10 && upper.startsWith("B") && /^B[0-9A-Z]{9}$/.test(upper)) {
    return { kind: "asin", normalized: upper };
  }

  return { kind: "unknown", normalized: t };
}
