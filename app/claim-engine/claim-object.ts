import type { PackageRecord, PalletRecord, ReturnRecord } from "../returns/returns-action-types";
import { resolveClaimAmountFromReturnSync } from "./claim-amount-utils";
import type { ClaimRecord } from "./claim-types";

/**
 * Domain object: when a return item is ready for claim, the claim inherits
 * pallet/package context and product identifiers from the parent return row.
 */
export class ClaimObject {
  constructor(
    public readonly returnRow: ReturnRecord,
    public readonly pallet: PalletRecord | null,
    public readonly packageRow: PackageRecord | null,
    /** Optional `claim_submissions` row for financial ROI when present. */
    public readonly submissionSnapshot?: Record<string, unknown>,
  ) {}

  /** Build from a return that has reached `ready_for_claim` (or pipeline equivalent). */
  static fromReadyReturn(
    returnRow: ReturnRecord,
    pallet: PalletRecord | null,
    packageRow: PackageRecord | null,
  ): ClaimObject {
    return new ClaimObject(returnRow, pallet, packageRow);
  }

  /** Minimal `ClaimRecord` used before a `claim_submissions` row exists (PDF pipeline). */
  toSyntheticClaimRecord(organizationId: string): ClaimRecord {
    const r = this.returnRow;
    const requested = this.requestedAmountUsd;
    return {
      id: `synthetic-return:${r.id}`,
      company_id: organizationId,
      amount: requested,
      reimbursement_amount: this.recoveredAmountUsd,
      status: "pending",
      claim_type: null,
      marketplace_provider: null,
      created_at: r.created_at,
      amazon_order_id: r.order_id ?? null,
      return_id: r.id,
      item_name: r.item_name,
      asin: r.asin ?? null,
      fnsku: r.fnsku ?? null,
      sku: r.sku ?? null,
      marketplace_claim_id: null,
      marketplace_link_status: null,
      store_id: r.store_id ?? null,
    };
  }

  /** Requested claim amount (submission row or `returns.estimated_value` / catalog sync). */
  get requestedAmountUsd(): number {
    const snap = this.submissionSnapshot;
    if (snap && snap.claim_amount != null) {
      const n = Number(snap.claim_amount);
      if (Number.isFinite(n) && n > 0) return Math.round(n * 100) / 100;
    }
    return resolveClaimAmountFromReturnSync(this.returnRow);
  }

  /** Actual reimbursement when marketplace pays out (accepted claims). */
  get recoveredAmountUsd(): number | null {
    const snap = this.submissionSnapshot;
    if (!snap || snap.reimbursement_amount == null) return null;
    const n = Number(snap.reimbursement_amount);
    if (!Number.isFinite(n) || n <= 0) return null;
    return Math.round(n * 100) / 100;
  }
}
