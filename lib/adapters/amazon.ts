import { getAmazonAccessToken } from "../amazon/sp-api";
import type { BaseMarketplaceAdapter, ConnectResult, TestResult, SyncClaimsResult } from "./base";
import { ADAPTER_CONFIGS } from "./configs";

const config = ADAPTER_CONFIGS.amazon_sp_api;

/** Matches snake_case columns written to the claims table (adapter sync). */
export type MockClaim = {
  claim_type: string;
  amount: number;
  status: "pending" | "recovered" | "suspicious";
  amazon_order_id: string;
  item_name?: string;
  asin?: string;
  fnsku?: string;
  sku?: string;
  marketplace_claim_id?: string;
  marketplace_link_status?: string;
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
  return shuffled.slice(0, count).map((type, i) => ({
    claim_type: type.claim_type,
    amount: randomBetween(15, 300),
    status: type.status,
    amazon_order_id: fakeOrderId(),
    item_name: `Synced SKU — Return line ${i + 1}`,
    asin: `B0${String(Math.floor(Math.random() * 1e7)).padStart(7, "0")}`,
    fnsku: `X00${String(Math.floor(Math.random() * 1e5)).padStart(5, "0")}`,
    sku: `MSKU-${String(Math.floor(Math.random() * 1e6))}`,
    marketplace_claim_id: `AMZ-CLM-${String(Math.floor(Math.random() * 1e9))}`,
    marketplace_link_status: "pending",
  }));
}

function normalizeLwa(
  credentials: Record<string, string>,
): { lwa_client_id: string; lwa_client_secret: string; refresh_token?: string } {
  const lwa_client_id =
    credentials.lwa_client_id ?? (credentials as { lwaClientId?: string }).lwaClientId ?? "";
  const lwa_client_secret =
    credentials.lwa_client_secret ??
    (credentials as { lwaClientSecret?: string }).lwaClientSecret ??
    "";
  const refresh_token =
    credentials.refresh_token ?? (credentials as { refreshToken?: string }).refreshToken ?? "";
  return {
    lwa_client_id,
    lwa_client_secret,
    refresh_token: refresh_token.trim() || undefined,
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
      const c = normalizeLwa(credentials);
      if (!c.lwa_client_id.trim() || !c.lwa_client_secret.trim()) {
        return { ok: false, error: "LWA Client ID and Client Secret are required." };
      }
      const token = await getAmazonAccessToken({
        lwaClientId: c.lwa_client_id.trim(),
        lwaClientSecret: c.lwa_client_secret.trim(),
        refreshToken: c.refresh_token,
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
