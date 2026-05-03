/**
 * Strict positional Inventory Ledger (15 Amazon columns) — staging keys only.
 * Mapping semantics must NOT depend on HEADERLESS_INVENTORY_LEDGER_SYNTHETIC_HEADERS.
 */

export const LEDGER_POSITIONAL_COLUMN_COUNT = 15;

/** `ledger_pos_01` … `ledger_pos_N` for csv-parser / `amazon_staging.raw_row`. */
export function buildInventoryLedgerPositionalStagingHeaders(minWidth: number): string[] {
  const n = Math.max(LEDGER_POSITIONAL_COLUMN_COUNT, Math.floor(minWidth), 1);
  const out: string[] = [];
  for (let i = 1; i <= n; i++) {
    out.push(`ledger_pos_${String(i).padStart(2, "0")}`);
  }
  return out;
}

export function ledgerPositionalHeaderKey(index1Based: number): string {
  return `ledger_pos_${String(index1Based).padStart(2, "0")}`;
}

/** True when staging row was written with positional `ledger_pos_*` keys (headerless real format). */
export function rawRowUsesInventoryLedgerPositionalKeys(raw: Record<string, unknown> | null | undefined): boolean {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return false;
  const o = raw as Record<string, unknown>;
  return typeof o.ledger_pos_01 === "string" || typeof o.ledger_pos_1 === "string";
}
