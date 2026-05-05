/**
 * Resolve catalog image URL without calling SP-API.
 * Order: products.main_image_url → common keys inside amazon_raw → null (caller shows placeholder).
 */

export function resolvePimDisplayImageUrl(
  mainImageUrl: unknown,
  amazonRaw: unknown,
): string | null {
  const fromMain =
    typeof mainImageUrl === "string" && mainImageUrl.trim() ? mainImageUrl.trim() : "";
  if (fromMain) return fromMain;

  if (!amazonRaw || typeof amazonRaw !== "object" || Array.isArray(amazonRaw)) return null;
  const raw = amazonRaw as Record<string, unknown>;

  const candidates = [
    raw.main_image_url,
    raw.mainImageUrl,
    raw.primary_image_url,
    raw.image_url,
    raw.imageUrl,
  ];
  for (const c of candidates) {
    if (typeof c === "string" && c.trim()) return c.trim();
  }

  const images = raw.images;
  if (Array.isArray(images) && images.length > 0) {
    const first = images[0];
    if (typeof first === "string" && first.trim()) return first.trim();
    if (first && typeof first === "object" && !Array.isArray(first)) {
      const o = first as Record<string, unknown>;
      const u = o.url ?? o.link ?? o.src;
      if (typeof u === "string" && u.trim()) return u.trim();
    }
  }

  return null;
}
