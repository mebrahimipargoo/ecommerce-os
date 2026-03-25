/**
 * Adapter registry. Add new providers by:
 * 1. Create lib/adapters/<provider>.ts implementing BaseMarketplaceAdapter
 * 2. Add to ADAPTER_REGISTRY and ADAPTER_BY_PROVIDER
 */

import type { BaseMarketplaceAdapter, AdapterProviderKey } from "./base";
import { MarketplaceFactory } from "./factory";

export type { BaseMarketplaceAdapter, AdapterConfig, AdapterFormField, AdapterProviderKey } from "./base";
export { MarketplaceFactory } from "./factory";

/** @deprecated Use MarketplaceFactory.getAdapter(provider) */
export function getAdapter(provider: AdapterProviderKey): BaseMarketplaceAdapter | undefined {
  return MarketplaceFactory.getAdapter(provider);
}

export function getAllAdapters(): BaseMarketplaceAdapter[] {
  return MarketplaceFactory.supportedProviders
    .map((p) => MarketplaceFactory.getAdapter(p))
    .filter((a): a is BaseMarketplaceAdapter => a != null);
}
