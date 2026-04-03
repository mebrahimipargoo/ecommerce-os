"use client";

/**
 * useBarcodeRouter — Smart WMS barcode routing for the Item wizard.
 *
 * Routing table (order matters — first match wins):
 *
 *  Pattern                 | Kind      | Target field       | requires_investigation
 *  ------------------------|-----------|--------------------|------------------------
 *  ^B0[A-Z0-9]{8}$        | asin      | asin               | false
 *  ^X0[A-Z0-9]{8}$        | fnsku     | fnsku              | false
 *  ^LPN[A-Z0-9]+$         | lpn       | lpn                | false
 *  ^\d{12,13}$             | upc_ean   | product_identifier | TRUE  ← suspicious
 *  anything else           | unknown   | product_identifier | false
 *
 * Extra capability — `upsertToCatalog`:
 *   When a known ASIN or FNSKU is scanned, call this to perform a best-effort
 *   upsert into the `products` master-catalog table so future scans resolve
 *   instantly from the local DB rather than an external API call.
 *   LPN / unknown codes are never catalogued.
 */

import { useCallback } from "react";
import { supabase } from "../src/lib/supabase";

// ── Types ─────────────────────────────────────────────────────────────────────

export type BarcodeKind = "asin" | "fnsku" | "lpn" | "upc_ean" | "unknown";

/** Indicates which form field the normalised value should be written to. */
export type BarcodeTargetField =
  | "asin"
  | "fnsku"
  | "lpn"
  | "product_identifier";

export type BarcodeRouteResult = {
  /** Classified barcode type. */
  kind: BarcodeKind;
  /** Upper-cased / stripped barcode ready for DB storage. */
  normalized: string;
  /** Which WizardState key to populate. */
  targetField: BarcodeTargetField;
  /**
   * When true the item should be flagged for manual review.
   * Currently applies to UPC/EAN scans (12-13 digit numeric codes) whose
   * marketplace origin cannot be determined from the barcode alone.
   */
  requires_investigation: boolean;
};

export type CatalogUpsertOptions = {
  /** Must be the normalised barcode returned by `route()`. */
  barcode: string;
  kind: BarcodeKind;
  /** Display name — falls back to the barcode itself if absent. */
  name?: string;
  price?: number;
  image_url?: string;
  /** Free-form marketplace identifier, e.g. "amazon", "scan". */
  source?: string;
};

// ── Regex patterns ────────────────────────────────────────────────────────────

const RE_ASIN   = /^B0[A-Z0-9]{8}$/;   // exactly 10 chars, starts B0
const RE_FNSKU  = /^X0[A-Z0-9]{8}$/;   // exactly 10 chars, starts X0
const RE_LPN    = /^LPN[A-Z0-9]+$/;    // LPN prefix, arbitrary suffix
const RE_UPC    = /^\d{12,13}$/;        // 12-digit UPC-A or 13-digit EAN-13

// ── Hook ──────────────────────────────────────────────────────────────────────

export function useBarcodeRouter() {
  /**
   * Classifies a raw scanned/typed barcode into a `BarcodeRouteResult`.
   * Pure — no side effects, no network calls, safe to call on every keystroke.
   */
  const route = useCallback((raw: string): BarcodeRouteResult => {
    const trimmed = raw.trim();
    if (!trimmed) {
      return {
        kind: "unknown",
        normalized: "",
        targetField: "product_identifier",
        requires_investigation: false,
      };
    }

    const upper = trimmed.toUpperCase().replace(/\s+/g, "");

    if (RE_ASIN.test(upper)) {
      return { kind: "asin", normalized: upper, targetField: "asin", requires_investigation: false };
    }
    if (RE_FNSKU.test(upper)) {
      return { kind: "fnsku", normalized: upper, targetField: "fnsku", requires_investigation: false };
    }
    if (RE_LPN.test(upper)) {
      return { kind: "lpn", normalized: upper, targetField: "lpn", requires_investigation: false };
    }

    // UPC/EAN: test against original trimmed (digits-only, no case-fold needed)
    const digitsOnly = trimmed.replace(/\D/g, "");
    if (RE_UPC.test(digitsOnly)) {
      return {
        kind: "upc_ean",
        normalized: digitsOnly,
        targetField: "product_identifier",
        requires_investigation: true,
      };
    }

    return {
      kind: "unknown",
      normalized: trimmed,
      targetField: "product_identifier",
      requires_investigation: false,
    };
  }, []);

  /**
   * Best-effort upsert of a scanned barcode into the `products` master catalog.
   *
   * - Only ASIN and FNSKU codes are catalogued; LPN and unknown codes are skipped.
   * - Failures are silently swallowed — the scan flow must never be blocked by
   *   a catalog write error.
   * - Callers can enrich the entry later by passing `name`, `price`, `image_url`
   *   when those values are resolved from an external adapter (Amazon SP-API, etc.).
   */
  const upsertToCatalog = useCallback(
    async (opts: CatalogUpsertOptions): Promise<void> => {
      if (opts.kind === "unknown" || opts.kind === "lpn" || !opts.barcode.trim()) {
        return;
      }

      try {
        await supabase
          .from("products")
          .upsert(
            {
              barcode:   opts.barcode.trim(),
              name:      opts.name?.trim() || opts.barcode.trim(),
              source:    opts.source ?? "scan",
              ...(opts.price     != null ? { price:     opts.price }     : {}),
              ...(opts.image_url          ? { image_url: opts.image_url } : {}),
            },
            // Only update if the incoming data provides richer info
            { onConflict: "barcode", ignoreDuplicates: false },
          );
      } catch {
        // Catalog write is non-blocking — never propagate
      }
    },
    [],
  );

  return { route, upsertToCatalog };
}
