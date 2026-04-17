/** Pick first non-empty value from a JSONB/raw row using case-insensitive header keys. */

export function pickRawPayloadFields(
  raw: Record<string, string> | Record<string, unknown> | null | undefined,
  keys: string[],
): string {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return "";
  const lower = new Map<string, string>();
  for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
    lower.set(String(k).trim().toLowerCase(), String(v ?? "").trim());
  }
  for (const key of keys) {
    const v = lower.get(key.toLowerCase());
    if (v) return v;
  }
  return "";
}
