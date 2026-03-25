import { getAmazonAccessToken } from "../amazon/sp-api";
import type { BaseMarketplaceAdapter, ConnectResult, TestResult, SyncClaimsResult } from "./base";
import { ADAPTER_CONFIGS } from "./configs";

const config = ADAPTER_CONFIGS.amazon_sp_api;

/** Matches the exact snake_case columns in the claims table */
export type MockClaim = {
  claim_type: string;
  amount: number;
  status: "pending" | "recovered" | "suspicious";
  amazon_order_id: string;
};

const MOCK_CLAIM_TYPES: { claim_type: string; status: MockClaim["status"] }[] = [
  { claim_type: "Lost in FC", status: "pending" },
  { claim_type: "Damaged Return", status: "suspicious" },
  { claim_type: "Customer Refund Not Returned", status: "pending" },
  { claim_type: "Carrier Damage - Recovered", status: "recovered" },
  { claim_type: "Inbound Shipment Discrepancy", status: "pending" },
  { claim_type: "Inventory Adjustment Claim", status: "recovered" },
  { claim_type: "Return Abuse Detected", status: "suspicious" },
];

function randomBetween(min: number, max: number): number {
  return Math.round((Math.random() * (max - min) + min) * 100) / 100;
}

function fakeOrderId(): string {
  const seg = () => String(Math.floor(Math.random() * 9_000_000) + 1_000_000);
  return `114-${seg()}-${seg()}`;
}

/** Generates 3–5 realistic mock claims mapped to exact DB column names */
export function generateMockAmazonClaims(): MockClaim[] {
  const count = Math.floor(Math.random() * 3) + 3; // 3, 4, or 5
  const shuffled = [...MOCK_CLAIM_TYPES].sort(() => Math.random() - 0.5);
  return shuffled.slice(0, count).map((type) => ({
    claim_type: type.claim_type,
    amount: randomBetween(15, 300),
    status: type.status,
    amazon_order_id: fakeOrderId(),
  }));
}

function toCredentials(form: Record<string, string>): Record<string, string> {
  return {
    seller_id: form.sellerId ?? form.seller_id ?? "",
    lwa_client_id: form.lwaClientId ?? form.lwa_client_id ?? "",
    lwa_client_secret: form.lwaClientSecret ?? form.lwa_client_secret ?? "",
  };
}

export const amazonAdapter: BaseMarketplaceAdapter = {
  config,

  async connect(credentials: Record<string, string>): Promise<ConnectResult> {
    const result = await this.testConnection(credentials);
    return result;
  },

  async testConnection(credentials: Record<string, string>): Promise<TestResult> {
    try {
      const c = credentials.lwa_client_id && credentials.lwa_client_secret
        ? credentials
        : toCredentials(credentials as unknown as Record<string, string>);
      const token = await getAmazonAccessToken({
        lwaClientId: c.lwa_client_id,
        lwaClientSecret: c.lwa_client_secret,
      });
      return { ok: true, expiresIn: token.expiresIn };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to verify Amazon connection.";
      return { ok: false, error: message };
    }
  },

  /**
   * Simulates SP-API Finances/Reports sync.
   * Pass organizationId via credentials map under key "__org_id" from the server action.
   */
  async syncClaims(_connectionId: string, _credentials: Record<string, string>): Promise<SyncClaimsResult> {
    try {
      const mockClaims = generateMockAmazonClaims();
      return { ok: true, claimsCount: mockClaims.length, claims: mockClaims };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to sync Amazon claims.";
      return { ok: false, error: message };
    }
  },
};
