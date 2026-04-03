/**
 * Classifies Amazon / retail product scans for the returns wizard.
 *
 * Pattern reference (aligned with useBarcodeRouter):
 *   ASIN   — exactly 10 chars starting with B0  (^B0[A-Z0-9]{8}$)
 *   FNSKU  — exactly 10 chars starting with X0  (^X0[A-Z0-9]{8}$)
 *   LPN    — starts with "LPN" followed by alphanumerics
 *   UPC/EAN — 12–13 digit numeric string
 *
 * NOTE: For new code prefer the full `useBarcodeRouter` hook in hooks/useBarcodeRouter.ts
 * which also handles catalog upserts and the `requires_investigation` flag.
 * This function is kept for backward-compatibility with existing call sites.
 */

export type ProductBarcodeKind = "fnsku" | "asin" | "lpn" | "upc_ean" | "unknown";

const RE_ASIN  = /^B0[A-Z0-9]{8}$/;
const RE_FNSKU = /^X0[A-Z0-9]{8}$/;
const RE_LPN   = /^LPN[A-Z0-9]+$/;
const RE_UPC   = /^\d{12,13}$/;

export function classifyProductBarcode(raw: string): {
  kind: ProductBarcodeKind;
  /** Normalised code for the target field (upper-cased ASIN/FNSKU/LPN, digits-only for UPC/EAN). */
  normalized: string;
} {
  const trimmed = raw.trim();
  if (!trimmed) return { kind: "unknown", normalized: "" };

  const upper = trimmed.toUpperCase().replace(/\s+/g, "");

  if (RE_ASIN.test(upper))  return { kind: "asin",    normalized: upper };
  if (RE_FNSKU.test(upper)) return { kind: "fnsku",   normalized: upper };
  if (RE_LPN.test(upper))   return { kind: "lpn",     normalized: upper };

  const digitsOnly = trimmed.replace(/\D/g, "");
  if (RE_UPC.test(digitsOnly)) return { kind: "upc_ean", normalized: digitsOnly };

  return { kind: "unknown", normalized: trimmed };
}
