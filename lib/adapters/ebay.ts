/**
 * eBay API adapter (placeholder).
 * Implement testConnection(), connect(), syncClaims() to enable eBay connections.
 * Add to ADAPTER_LIST in configs.ts when ready.
 */

import type { BaseMarketplaceAdapter, ConnectResult, TestResult, SyncClaimsResult } from "./base";
import { ADAPTER_CONFIGS } from "./configs";

const config = ADAPTER_CONFIGS.ebay_api;

export const ebayAdapter: BaseMarketplaceAdapter = {
  config,

  async connect(): Promise<ConnectResult> {
    return { ok: false, error: "eBay adapter not yet implemented." };
  },

  async testConnection(): Promise<TestResult> {
    return { ok: false, error: "eBay adapter not yet implemented." };
  },

  async syncClaims(): Promise<SyncClaimsResult> {
    return { ok: false, error: "eBay adapter not yet implemented." };
  },
};
