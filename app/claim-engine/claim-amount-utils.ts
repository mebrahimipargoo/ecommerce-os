import type { SupabaseClient } from "@supabase/supabase-js";
import type { ReturnRecord } from "../returns/returns-action-types";

function num(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

/** Prefer return.estimated_value; optional catalog price from `products` (barcode = ASIN/FNSKU/SKU). */
export async function resolveInitialClaimAmountUsd(
  supabase: SupabaseClient,
  returnRow: ReturnRecord | null,
  fallbackUsd = 0,
): Promise<number> {
  const fromReturn = num((returnRow as { estimated_value?: unknown })?.estimated_value);
  if (fromReturn > 0) return roundMoney(fromReturn);

  const catalog = await fetchCatalogPriceUsd(supabase, returnRow);
  if (catalog > 0) return catalog;

  return roundMoney(fallbackUsd);
}

export function resolveClaimAmountFromReturnSync(returnRow: ReturnRecord | null): number {
  const fromReturn = num((returnRow as { estimated_value?: unknown })?.estimated_value);
  if (fromReturn > 0) return roundMoney(fromReturn);
  return 0;
}

async function fetchCatalogPriceUsd(
  supabase: SupabaseClient,
  returnRow: ReturnRecord | null,
): Promise<number> {
  if (!returnRow) return 0;
  const keys = [returnRow.asin, returnRow.fnsku, returnRow.sku]
    .map((x) => (typeof x === "string" ? x.trim() : ""))
    .filter(Boolean);

  for (const barcode of keys) {
    const { data } = await supabase
      .from("products")
      .select("price")
      .eq("barcode", barcode)
      .limit(1)
      .maybeSingle();
    const p = num((data as { price?: unknown } | null)?.price);
    if (p > 0) return roundMoney(p);
  }
  return 0;
}

function roundMoney(n: number): number {
  return Math.round(n * 100) / 100;
}
