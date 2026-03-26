/** RFC 4122 UUID v4 pattern (loose — accepts any variant nibble in third group). */
const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function isUuidString(s: string | undefined | null): boolean {
  if (s == null || typeof s !== "string") return false;
  return UUID_RE.test(s.trim());
}
