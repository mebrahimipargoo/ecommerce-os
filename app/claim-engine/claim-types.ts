/**
 * Shared claim shapes for Claim Engine (backed by `claim_submissions`).
 */
export type ClaimRecord = {
  id: string;
  organization_id: string;
  amount: number;
  /** Actual reimbursement when accepted — migration 20260331_returns_estimated_value_reimbursement.sql */
  reimbursement_amount?: number | null;
  /** Submission / workflow status (see claim_submission_status enum). */
  status: string;
  claim_type: string | null;
  marketplace_provider: string | null;
  created_at: string;
  amazon_order_id: string | null;
  return_id?: string | null;
  item_name?: string | null;
  asin?: string | null;
  fnsku?: string | null;
  sku?: string | null;
  marketplace_claim_id?: string | null;
  marketplace_link_status?: string | null;
  store_id?: string | null;
};
