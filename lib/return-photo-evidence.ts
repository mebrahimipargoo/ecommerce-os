/**
 * `returns.photo_evidence` JSONB: SmartCamera category counts (numbers) plus optional URL slots.
 * URL keys are stored in the same object — not separate DB columns.
 */
export const RETURN_PHOTO_EVIDENCE_URL_KEYS = ["item_url", "expiry_url", "return_label_url"] as const;
export type ReturnPhotoEvidenceUrlKey = (typeof RETURN_PHOTO_EVIDENCE_URL_KEYS)[number];

export type ReturnPhotoEvidenceRow = Record<string, string | number | null | undefined> | null;

export function getReturnPhotoEvidenceUrls(pe: ReturnPhotoEvidenceRow | undefined): {
  item_url: string;
  expiry_url: string;
  return_label_url: string;
} {
  const o = pe ?? {};
  const s = (k: string) => (typeof o[k] === "string" ? o[k].trim() : "");
  return {
    item_url: s("item_url"),
    expiry_url: s("expiry_url"),
    return_label_url: s("return_label_url"),
  };
}

/** Category slug → photo count (excludes URL keys). */
export function photoEvidenceCategoryCounts(pe: ReturnPhotoEvidenceRow): Record<string, number> {
  const out: Record<string, number> = {};
  if (!pe) return out;
  for (const [k, v] of Object.entries(pe)) {
    if ((RETURN_PHOTO_EVIDENCE_URL_KEYS as readonly string[]).includes(k)) continue;
    if (typeof v === "number" && v > 0) out[k] = v;
  }
  return out;
}

export function photoEvidenceNumericTotal(pe: ReturnPhotoEvidenceRow): number {
  return Object.values(photoEvidenceCategoryCounts(pe)).reduce((a, b) => a + b, 0);
}

export function hasReturnPhotoEvidenceCounts(pe: ReturnPhotoEvidenceRow): boolean {
  return Object.values(photoEvidenceCategoryCounts(pe)).some((n) => n > 0);
}

export function hasReturnPhotoEvidenceUrlSlots(pe: ReturnPhotoEvidenceRow): boolean {
  const u = getReturnPhotoEvidenceUrls(pe);
  return !!(u.item_url || u.expiry_url || u.return_label_url);
}

/**
 * Merges SmartCamera counts with optional URL slots for insert/update payloads.
 */
export function mergeReturnPhotoEvidence(
  counts: Record<string, number> | null | undefined,
  urls: Partial<Record<ReturnPhotoEvidenceUrlKey, string>>,
): Record<string, string | number> | null {
  const out: Record<string, string | number> = {};
  if (counts) {
    for (const [k, v] of Object.entries(counts)) {
      if (typeof v === "number" && v > 0) out[k] = v;
    }
  }
  for (const k of RETURN_PHOTO_EVIDENCE_URL_KEYS) {
    const t = urls[k]?.trim();
    if (t) out[k] = t;
  }
  return Object.keys(out).length ? out : null;
}
