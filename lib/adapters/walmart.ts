import { getWalmartAccessToken } from "../walmart/api";
import type { BaseMarketplaceAdapter, ConnectResult, TestResult, SyncClaimsResult } from "./base";
import { ADAPTER_CONFIGS } from "./configs";

const config = ADAPTER_CONFIGS.walmart_api;

type WalmartMockClaim = {
  claim_type: string;
  amount: number;
  status: "pending" | "recovered" | "suspicious";
  amazon_order_id: string; // reused as generic order_id column
};

const WALMART_CLAIM_TYPES: { claim_type: string; status: WalmartMockClaim["status"] }[] = [
  { claim_type: "Return Discrepancy", status: "pending" },
  { claim_type: "Missing Inventory", status: "pending" },
  { claim_type: "Overcharge on Returns", status: "suspicious" },
  { claim_type: "Carrier Loss - Recovered", status: "recovered" },
  { claim_type: "Fulfillment Center Shortage", status: "pending" },
  { claim_type: "Damaged Inbound Shipment", status: "suspicious" },
  { claim_type: "Customer Return Not Received", status: "recovered" },
];

function randomBetween(min: number, max: number): number {
  return Math.round((Math.random() * (max - min) + min) * 100) / 100;
}

function fakeWalmartOrderId(): string {
  return `WMT-${String(Math.floor(Math.random() * 9_000_000_000) + 1_000_000_000)}`;
}

function generateMockWalmartClaims(): WalmartMockClaim[] {
  const count = Math.floor(Math.random() * 3) + 3; // 3, 4, or 5
  const shuffled = [...WALMART_CLAIM_TYPES].sort(() => Math.random() - 0.5);
  return shuffled.slice(0, count).map((type) => ({
    claim_type: type.claim_type,
    amount: randomBetween(15, 300),
    status: type.status,
    amazon_order_id: fakeWalmartOrderId(),
  }));
}

function toCredentials(form: Record<string, string>): Record<string, string> {
  return {
    client_id: form.clientId ?? form.client_id ?? "",
    client_secret: form.clientSecret ?? form.client_secret ?? "",
  };
}

export const walmartAdapter: BaseMarketplaceAdapter = {
  config,

  async connect(credentials: Record<string, string>): Promise<ConnectResult> {
    const result = await this.testConnection(credentials);
    return result;
  },

  async testConnection(credentials: Record<string, string>): Promise<TestResult> {
    try {
      const c = credentials.client_id && credentials.client_secret
        ? credentials
        : toCredentials(credentials as unknown as Record<string, string>);
      const token = await getWalmartAccessToken({
        clientId: c.client_id,
        clientSecret: c.client_secret,
      });
      return { ok: true, expiresIn: token.expiresIn };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to verify Walmart connection.";
      return { ok: false, error: message };
    }
  },

  /** Simulates Walmart Returns/Inventory API sync without hitting real endpoints */
  async syncClaims(_connectionId: string, _credentials: Record<string, string>): Promise<SyncClaimsResult> {
    try {
      const mockClaims = generateMockWalmartClaims();
      return { ok: true, claimsCount: mockClaims.length, claims: mockClaims };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to sync Walmart claims.";
      return { ok: false, error: message };
    }
  },
};
