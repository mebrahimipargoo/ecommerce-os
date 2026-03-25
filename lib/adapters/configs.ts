/**
 * Adapter configs for UI (client-safe). No API/credentials logic.
 * Add new providers by adding a config here and creating lib/adapters/<provider>.ts
 */

import type { AdapterConfig, AdapterProviderKey } from "./base";

const AMAZON_FORM_FIELDS = [
  { key: "sellerId", label: "Seller ID", type: "text" as const, placeholder: "e.g. A1BCDEFGHIJKL", required: true, credentialsKey: "seller_id" },
  { key: "lwaClientId", label: "LWA Client ID", type: "text" as const, placeholder: "amzn1.application-oa2-client...", required: true, credentialsKey: "lwa_client_id" },
  { key: "lwaClientSecret", label: "LWA Client Secret", type: "password" as const, placeholder: "Paste the client secret", required: true, credentialsKey: "lwa_client_secret" },
];

const WALMART_FORM_FIELDS = [
  { key: "clientId", label: "Client ID", type: "text" as const, placeholder: "Walmart API Client ID", required: true, credentialsKey: "client_id" },
  { key: "clientSecret", label: "Client Secret", type: "password" as const, placeholder: "Paste the client secret", required: true, credentialsKey: "client_secret" },
];

export type AdapterUIConfig = AdapterConfig & { accentRingClass: string; iconKey: "shopping_bag" | "globe" | "tag" };

export const ADAPTER_CONFIGS: Record<AdapterProviderKey, AdapterUIConfig> = {
  amazon_sp_api: {
    provider: "amazon_sp_api",
    name: "Amazon SP-API",
    iconKey: "shopping_bag",
    description: "Connect Amazon SP-API to ingest returns signals and automate claims evidence workflows.",
    badge: "SP-API credentials",
    formFields: AMAZON_FORM_FIELDS,
    displayIdKey: "seller_id",
    accentRingClass: "ring-sky-500/25 dark:ring-sky-400/25 border-sky-500/20 dark:border-sky-400/20",
  },
  walmart_api: {
    provider: "walmart_api",
    name: "Walmart API",
    iconKey: "globe",
    description: "Connect Walmart Marketplace API to unify returns data and streamline recovery operations.",
    badge: "Marketplace API credentials",
    formFields: WALMART_FORM_FIELDS,
    displayIdKey: "client_id",
    accentRingClass: "ring-indigo-500/25 dark:ring-indigo-400/25 border-indigo-500/20 dark:border-indigo-400/20",
  },
  ebay_api: {
    provider: "ebay_api",
    name: "eBay API",
    iconKey: "tag",
    description: "Connect eBay API for returns and seller management. (Placeholder - add implementation in lib/adapters/ebay.ts)",
    badge: "API credentials",
    formFields: [
      { key: "appId", label: "App ID", type: "text" as const, placeholder: "eBay App ID", required: true, credentialsKey: "app_id" },
      { key: "certId", label: "Cert ID", type: "text" as const, placeholder: "eBay Cert ID", required: true, credentialsKey: "cert_id" },
      { key: "devId", label: "Dev ID", type: "text" as const, placeholder: "eBay Dev ID", required: true, credentialsKey: "dev_id" },
    ],
    displayIdKey: "app_id",
    accentRingClass: "ring-amber-500/25 dark:ring-amber-400/25 border-amber-500/20 dark:border-amber-400/20",
  },
};

/** All configs for dynamic card rendering. Filter out unimplemented providers if needed. */
export const ADAPTER_LIST = Object.values(ADAPTER_CONFIGS) as AdapterUIConfig[];

export function getAdapterConfig(provider: AdapterProviderKey) {
  return ADAPTER_CONFIGS[provider];
}

export type { AdapterProviderKey };
