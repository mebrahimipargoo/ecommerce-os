/**
 * Base interface for marketplace adapters.
 * Add new providers by implementing this interface in lib/adapters/<provider>.ts
 */

export type AdapterProviderKey = "amazon_sp_api" | "walmart_api" | "ebay_api";

export type ConnectResult = {
  ok: boolean;
  connectionId?: string;
  error?: string;
};

export type TestResult = {
  ok: boolean;
  expiresIn?: number;
  error?: string;
};

export type SyncClaimsResult = {
  ok: boolean;
  claimsCount?: number;
  error?: string;
  /** Adapter-specific claim objects to be persisted by the server action */
  claims?: Record<string, unknown>[];
};

/** Form field definition for dynamic connection modal */
export type AdapterFormField = {
  key: string;
  label: string;
  type: "text" | "password";
  placeholder?: string;
  required?: boolean;
  /** Key in credentials JSON to use when saving (defaults to key in camelCase) */
  credentialsKey?: string;
};

/** Adapter configuration for UI and registry */
export type AdapterConfig = {
  provider: AdapterProviderKey;
  name: string;
  description: string;
  badge: string;
  /** Fields to render in the connection modal */
  formFields: AdapterFormField[];
  /** Key in credentials to display as "Seller/Store ID" in list view */
  displayIdKey?: string;
  /** Optional UI accent (e.g. ring color class) */
  accentRingClass?: string;
};

/** Base interface - implement connect, testConnection, syncClaims */
export interface BaseMarketplaceAdapter {
  readonly config: AdapterConfig;

  /** Validate credentials and establish connection (handled by server actions) */
  connect(credentials: Record<string, string>): Promise<ConnectResult>;

  /** Test credentials without persisting */
  testConnection(credentials: Record<string, string>): Promise<TestResult>;

  /** Sync claims from the marketplace API */
  syncClaims(connectionId: string, credentials: Record<string, string>): Promise<SyncClaimsResult>;
}
