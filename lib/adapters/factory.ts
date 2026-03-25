/**
 * MarketplaceFactory: Instantiates the correct adapter dynamically based on provider name.
 * Add new providers by creating lib/adapters/<provider>.ts and registering in the factory.
 */

import type { BaseMarketplaceAdapter, AdapterProviderKey } from "./base";
import { amazonAdapter } from "./amazon";
import { walmartAdapter } from "./walmart";
import { ebayAdapter } from "./ebay";

const ADAPTER_MAP = new Map<AdapterProviderKey, BaseMarketplaceAdapter>([
  ["amazon_sp_api", amazonAdapter],
  ["walmart_api", walmartAdapter],
  ["ebay_api", ebayAdapter],
]);

export class MarketplaceFactory {
  /**
   * Returns the adapter for the given provider, or undefined if not supported.
   */
  static getAdapter(provider: string): BaseMarketplaceAdapter | undefined {
    return ADAPTER_MAP.get(provider as AdapterProviderKey);
  }

  /**
   * Returns the adapter for the given provider. Throws if provider is unknown.
   */
  static requireAdapter(provider: string): BaseMarketplaceAdapter {
    const adapter = MarketplaceFactory.getAdapter(provider);
    if (!adapter) {
      throw new Error(`Unknown marketplace provider: ${provider}`);
    }
    return adapter;
  }

  /** List of supported provider keys */
  static get supportedProviders(): AdapterProviderKey[] {
    return Array.from(ADAPTER_MAP.keys());
  }

  /** Check if a provider is supported */
  static isSupported(provider: string): boolean {
    return ADAPTER_MAP.has(provider as AdapterProviderKey);
  }
}
