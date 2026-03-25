/** Browser-side AI & peripheral configuration (localStorage). */

// ── Storage keys ──────────────────────────────────────────────────────────────

export const OPENAI_API_KEY_STORAGE_KEY  = "ecommerce_os_openai_api_key";
export const GEMINI_API_KEY_STORAGE_KEY  = "ecommerce_os_gemini_api_key";
export const AI_PROVIDER_STORAGE_KEY     = "ecommerce_os_ai_provider";
export const AI_BASE_URL_STORAGE_KEY     = "ecommerce_os_ai_base_url";
export const AI_UNIFIED_KEY_STORAGE_KEY  = "ecommerce_os_ai_key";
export const BARCODE_MODE_STORAGE_KEY    = "ecommerce_os_barcode_mode";
export const LABEL_PRINTER_STORAGE_KEY   = "ecommerce_os_label_printer";

// ── Types ─────────────────────────────────────────────────────────────────────

export type AIProvider   = "openai" | "gemini" | "custom";
export type BarcodeMode  = "physical" | "camera";
export type LabelPrinter = "system" | "zebra_zd410" | "zebra_zd620" | "brother_ql";

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
