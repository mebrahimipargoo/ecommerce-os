/** RFC 4122 UUID v4 pattern (loose — accepts any variant nibble in third group). */
const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function isUuidString(s: string | undefined | null): boolean {
  if (s == null || typeof s !== "string") return false;
  return UUID_RE.test(s.trim());
}

/**
 * Use for FK columns when silent coalesce to null is OK (e.g. cleaning optional query params).
 * For writes where a non-empty non-UUID must surface a field-specific error, use `uuidFkOrNull` instead.
 */
export function uuidOrNull(v: string | null | undefined): string | null {
  if (v == null) return null;
  const t = String(v).trim();
  if (t === "") return null;
  return isUuidString(t) ? t : null;
}

/**
 * Same as `uuidOrNull` for empty input, but if the value is non-empty and not a UUID,
 * throws with `fieldLabel` so API callers fail before Postgres (avoids opaque DB errors).
 */
export function uuidFkOrNull(v: string | null | undefined, fieldLabel: string): string | null {
  if (v == null) return null;
  const t = String(v).trim();
  if (t === "") return null;
  if (!isUuidString(t)) {
    throw new Error(
      `Invalid ${fieldLabel}: "${t}" is not a valid UUID. Use the record id from the system, not a tracking or RMA number.`,
    );
  }
  return t;
}

/** Client-side: returns a user-facing message if `v` is non-empty but not a UUID; otherwise null. */
export function uuidFkInvalidMessage(
  v: string | null | undefined,
  fieldLabel: string,
): string | null {
  if (v == null) return null;
  const t = String(v).trim();
  if (t === "") return null;
  if (!isUuidString(t)) {
    return `${fieldLabel} must be a valid record id, not "${t}". Pick an option from the dropdown or scan until a row matches — tracking numbers alone are not ids.`;
  }
  return null;
}
