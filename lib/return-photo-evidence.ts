/**
 * `returns.photo_evidence` JSONB: SmartCamera category counts (numbers) plus optional URL slots.
 * URL keys are stored in the same object — not separate DB columns.
 */
export const RETURN_PHOTO_EVIDENCE_URL_KEYS = ["item_url", "expiry_url", "return_label_url"] as const;
export type ReturnPhotoEvidenceUrlKey = (typeof RETURN_PHOTO_EVIDENCE_URL_KEYS)[number];

export type ReturnPhotoEvidenceRow = Record<string, string | number | string[] | null | undefined> | null;

const GALLERY_URLS_KEY = "urls";

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

/** Category slug → photo count (excludes URL keys and gallery `urls` array). */
export function photoEvidenceCategoryCounts(pe: ReturnPhotoEvidenceRow): Record<string, number> {
  const out: Record<string, number> = {};
  if (!pe) return out;
  for (const [k, v] of Object.entries(pe)) {
    if ((RETURN_PHOTO_EVIDENCE_URL_KEYS as readonly string[]).includes(k)) continue;
    if (k === GALLERY_URLS_KEY) continue;
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

export function getReturnPhotoEvidenceGalleryUrls(pe: ReturnPhotoEvidenceRow | undefined): string[] {
  const o = pe ?? {};
  const raw = o[GALLERY_URLS_KEY];
  if (!Array.isArray(raw)) return [];
  return raw.filter((x): x is string => typeof x === "string" && x.trim().length > 0);
}

export function hasReturnPhotoEvidenceUrlSlots(pe: ReturnPhotoEvidenceRow): boolean {
  const u = getReturnPhotoEvidenceUrls(pe);
  if (u.item_url || u.expiry_url || u.return_label_url) return true;
  return getReturnPhotoEvidenceGalleryUrls(pe).length > 0;
}

/**
 * Merges SmartCamera counts with optional URL slots for insert/update payloads.
 */
export function mergeReturnPhotoEvidence(
  counts: Record<string, number> | null | undefined,
  urls: Partial<Record<ReturnPhotoEvidenceUrlKey, string>>,
  options?: { galleryUrls?: string[] },
): Record<string, string | number | string[]> | null {
  const out: Record<string, string | number | string[]> = {};
  if (counts) {
    for (const [k, v] of Object.entries(counts)) {
      if (typeof v === "number" && v > 0) out[k] = v;
    }
  }
  for (const k of RETURN_PHOTO_EVIDENCE_URL_KEYS) {
    const t = urls[k]?.trim();
    if (t) out[k] = t;
  }
  const g = options?.galleryUrls?.map((s) => s.trim()).filter(Boolean) ?? [];
  if (g.length > 0) out[GALLERY_URLS_KEY] = g;
  return Object.keys(out).length ? out : null;
}
