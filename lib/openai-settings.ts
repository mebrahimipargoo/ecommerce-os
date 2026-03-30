/** Browser-side AI & peripheral configuration (localStorage). */

// ── Storage keys ──────────────────────────────────────────────────────────────

export const OPENAI_API_KEY_STORAGE_KEY  = "ecommerce_os_openai_api_key";
export const GEMINI_API_KEY_STORAGE_KEY  = "ecommerce_os_gemini_api_key";
export const AI_PROVIDER_STORAGE_KEY     = "ecommerce_os_ai_provider";
export const AI_BASE_URL_STORAGE_KEY     = "ecommerce_os_ai_base_url";
export const AI_UNIFIED_KEY_STORAGE_KEY  = "ecommerce_os_ai_key";
export const BARCODE_MODE_STORAGE_KEY          = "ecommerce_os_barcode_mode";
export const LABEL_PRINTER_STORAGE_KEY         = "ecommerce_os_label_printer";
export const DEFAULT_PRODUCT_SOURCE_STORAGE_KEY = "ecommerce_os_default_product_source";

// ── Types ─────────────────────────────────────────────────────────────────────

export type AIProvider           = "openai" | "gemini" | "custom";
export type BarcodeMode          = "physical" | "camera";
export type LabelPrinter         = "system" | "zebra_zd410" | "zebra_zd620" | "brother_ql";
export type DefaultProductSource = "amazon" | "walmart" | "target" | "unknown";

export const DEFAULT_BASE_URLS: Record<AIProvider, string> = {
  openai: "https://api.openai.com/v1",
  gemini: "https://generativelanguage.googleapis.com/v1beta",
  custom: "",
};

// ── OpenAI key (legacy — kept for backward-compat with packing-slip vision) ───

export function getOpenAIApiKeyFromStorage(): string | null {
  if (typeof window === "undefined") return null;
  const v = localStorage.getItem(OPENAI_API_KEY_STORAGE_KEY)?.trim();
  return v || null;
}

export function setOpenAIApiKeyInStorage(key: string): void {
  localStorage.setItem(OPENAI_API_KEY_STORAGE_KEY, key.trim());
}

export function clearOpenAIApiKeyFromStorage(): void {
  localStorage.removeItem(OPENAI_API_KEY_STORAGE_KEY);
}

// ── Gemini key (legacy) ───────────────────────────────────────────────────────

export function getGeminiApiKeyFromStorage(): string | null {
  if (typeof window === "undefined") return null;
  const v = localStorage.getItem(GEMINI_API_KEY_STORAGE_KEY)?.trim();
  return v || null;
}

export function setGeminiApiKeyInStorage(key: string): void {
  localStorage.setItem(GEMINI_API_KEY_STORAGE_KEY, key.trim());
}

export function clearGeminiApiKeyFromStorage(): void {
  localStorage.removeItem(GEMINI_API_KEY_STORAGE_KEY);
}

// ── AI provider ───────────────────────────────────────────────────────────────

export function getAIProviderFromStorage(): AIProvider {
  if (typeof window === "undefined") return "openai";
  return (localStorage.getItem(AI_PROVIDER_STORAGE_KEY) as AIProvider) || "openai";
}

export function setAIProviderInStorage(provider: AIProvider): void {
  localStorage.setItem(AI_PROVIDER_STORAGE_KEY, provider);
}

// ── Base URL ──────────────────────────────────────────────────────────────────

export function getAIBaseURLFromStorage(provider: AIProvider = "openai"): string {
  if (typeof window === "undefined") return DEFAULT_BASE_URLS[provider];
  return localStorage.getItem(AI_BASE_URL_STORAGE_KEY) || DEFAULT_BASE_URLS[provider];
}

export function setAIBaseURLInStorage(url: string): void {
  localStorage.setItem(AI_BASE_URL_STORAGE_KEY, url.trim());
}

// ── Unified API key (new, provider-agnostic) ──────────────────────────────────

export function getAIUnifiedKeyFromStorage(): string | null {
  if (typeof window === "undefined") return null;
  const v = localStorage.getItem(AI_UNIFIED_KEY_STORAGE_KEY)?.trim();
  return v || null;
}

export function setAIUnifiedKeyInStorage(key: string): void {
  localStorage.setItem(AI_UNIFIED_KEY_STORAGE_KEY, key.trim());
}

export function clearAIUnifiedKeyFromStorage(): void {
  localStorage.removeItem(AI_UNIFIED_KEY_STORAGE_KEY);
}

// ── Hardware: Barcode mode ────────────────────────────────────────────────────

export function getBarcodeModeFromStorage(): BarcodeMode {
  if (typeof window === "undefined") return "physical";
  return (localStorage.getItem(BARCODE_MODE_STORAGE_KEY) as BarcodeMode) || "physical";
}

export function setBarcodeModeInStorage(mode: BarcodeMode): void {
  localStorage.setItem(BARCODE_MODE_STORAGE_KEY, mode);
}

// ── Hardware: Label printer ───────────────────────────────────────────────────

export function getLabelPrinterFromStorage(): LabelPrinter {
  if (typeof window === "undefined") return "system";
  return (localStorage.getItem(LABEL_PRINTER_STORAGE_KEY) as LabelPrinter) || "system";
}

export function setLabelPrinterInStorage(printer: LabelPrinter): void {
  localStorage.setItem(LABEL_PRINTER_STORAGE_KEY, printer);
}

// ── General: Default product source ──────────────────────────────────────────

export function getDefaultProductSourceFromStorage(): DefaultProductSource {
  if (typeof window === "undefined") return "unknown";
  return (localStorage.getItem(DEFAULT_PRODUCT_SOURCE_STORAGE_KEY) as DefaultProductSource) || "unknown";
}

export function setDefaultProductSourceInStorage(source: DefaultProductSource): void {
  localStorage.setItem(DEFAULT_PRODUCT_SOURCE_STORAGE_KEY, source);
}

// ── Multi-API configurations ──────────────────────────────────────────────────

export const AI_CONFIGS_STORAGE_KEY        = "ecommerce_os_ai_configs";
export const AI_GLOBAL_PROVIDER_STORAGE_KEY = "ecommerce_os_ai_global_provider_id";
export const AI_ROLE_ASSIGNMENTS_STORAGE_KEY = "ecommerce_os_ai_role_assignments";

/**
 * Free-form role / tag for each saved API connection (e.g. default, ocr_vision, Agent, Bot).
 * Stored as plain string in localStorage — not a database ENUM.
 */
export type AIRole = string;

/** Flat list of suggested tags (creatable comboboxes may use grouped metadata below). */
export const SUGGESTED_AI_ROLE_TAGS: readonly string[] = [
  "default",
  "ocr_vision",
  "agent",
  "bot",
  "integration",
  "system",
  "api",
] as const;

export type AIRoleTagSuggestion = {
  value: string;
  label: string;
  description: string;
};

/** Grouped presets for enterprise role/tag pickers (custom values still allowed). */
export const AI_ROLE_TAG_GROUPS: readonly {
  group: string;
  description: string;
  tags: readonly AIRoleTagSuggestion[];
}[] = [
  {
    group: "General chat & tasks",
    description: "Default routing for summaries, decisions, and in-app chat.",
    tags: [
      { value: "default", label: "default", description: "General chat & default routing" },
      { value: "system", label: "system", description: "Internal automation & system tasks" },
    ],
  },
  {
    group: "Hardware & imaging",
    description: "Scanners, packing slips, and camera / OCR flows.",
    tags: [
      { value: "ocr_vision", label: "ocr_vision", description: "Vision, packing slips, label OCR" },
    ],
  },
  {
    group: "External & automation",
    description: "Bots, agents, and integrations calling your workspace.",
    tags: [
      { value: "agent", label: "agent", description: "External agents & copilots" },
      { value: "bot", label: "bot", description: "Chatbots & messaging integrations" },
      { value: "integration", label: "integration", description: "Sync jobs & third-party APIs" },
      { value: "api", label: "api", description: "Programmatic / headless access" },
    ],
  },
] as const;

/** Normalize tag: trim, max length, empty → "default". */
export function normalizeAIRoleTag(raw: string): string {
  const t = raw.trim().slice(0, 80);
  return t || "default";
}

/** Connection health / test status. */
export type AIConfigStatus = "untested" | "active" | "testing" | "error";

/** A single saved API connection entry. */
export interface AIConfig {
  id: string;
  /** User-defined friendly label (e.g. "Gemini Flash — Chat", "GPT-4o Vision"). */
  providerName: string;
  provider: AIProvider;
  baseURL: string;
  apiKey: string;
  /** Legacy per-entry role tag — superseded by AIRoleAssignments but kept for compat. */
  role: AIRole;
  status?: AIConfigStatus;
  /** When true this entry acts as the sole API for all tasks — overrides role assignments. */
  isGlobalOverride?: boolean;
}

/** Explicit role-based routing: which saved config handles each task type. */
export interface AIRoleAssignments {
  defaultGeneral: string | null; // config id
  defaultVision:  string | null; // config id
}

export function getAIConfigsFromStorage(): AIConfig[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = localStorage.getItem(AI_CONFIGS_STORAGE_KEY);
    return raw ? (JSON.parse(raw) as AIConfig[]) : [];
  } catch {
    return [];
  }
}

export function setAIConfigsInStorage(configs: AIConfig[]): void {
  localStorage.setItem(AI_CONFIGS_STORAGE_KEY, JSON.stringify(configs));
}

export function clearAIConfigsFromStorage(): void {
  localStorage.removeItem(AI_CONFIGS_STORAGE_KEY);
}

export function getAIRoleAssignmentsFromStorage(): AIRoleAssignments {
  if (typeof window === "undefined") return { defaultGeneral: null, defaultVision: null };
  try {
    const raw = localStorage.getItem(AI_ROLE_ASSIGNMENTS_STORAGE_KEY);
    return raw ? (JSON.parse(raw) as AIRoleAssignments) : { defaultGeneral: null, defaultVision: null };
  } catch {
    return { defaultGeneral: null, defaultVision: null };
  }
}

export function setAIRoleAssignmentsInStorage(assignments: AIRoleAssignments): void {
  localStorage.setItem(AI_ROLE_ASSIGNMENTS_STORAGE_KEY, JSON.stringify(assignments));
}

// ── Default Store ID ──────────────────────────────────────────────────────────
// Saves the UUID of the user's chosen fallback store from the stores table.
// Used when no parent package is selected and barcode prefix doesn't match.

export const DEFAULT_STORE_ID_STORAGE_KEY = "ecommerce_os_default_store_id";

export function getDefaultStoreIdFromStorage(): string {
  if (typeof window === "undefined") return "";
  return localStorage.getItem(DEFAULT_STORE_ID_STORAGE_KEY) ?? "";
}

export function setDefaultStoreIdInStorage(id: string): void {
  if (!id) {
    localStorage.removeItem(DEFAULT_STORE_ID_STORAGE_KEY);
  } else {
    localStorage.setItem(DEFAULT_STORE_ID_STORAGE_KEY, id);
  }
}

/** Returns the config that should be used for a given role, respecting the global override. */
export function resolveAIConfig(
  configs: AIConfig[],
  assignments: AIRoleAssignments,
  role: "general" | "vision",
): AIConfig | null {
  const globalOverride = configs.find((c) => c.isGlobalOverride);
  if (globalOverride) return globalOverride;
  const id = role === "vision" ? assignments.defaultVision : assignments.defaultGeneral;
  return configs.find((c) => c.id === id) ?? configs[0] ?? null;
}
