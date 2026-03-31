/**
 * `pallets.photo_evidence` / `packages.photo_evidence` JSONB — canonical shape `{ urls: string[] }`.
 */

export type EntityPhotoEvidenceJson = { urls: string[] };

export function normalizeEntityPhotoEvidenceUrls(raw: unknown): string[] {
  if (!raw || typeof raw !== "object") return [];
  const u = (raw as { urls?: unknown }).urls;
  if (!Array.isArray(u)) return [];
  return u.filter((x): x is string => typeof x === "string" && x.trim().length > 0);
}

export function buildEntityPhotoEvidence(urls: string[]): EntityPhotoEvidenceJson | null {
  const clean = urls.map((s) => s.trim()).filter(Boolean);
  return clean.length ? { urls: clean } : null;
}

/** Merge new URLs onto existing `photo_evidence` (dedupes identical strings, preserves order). */
export function mergeEntityPhotoEvidence(existing: unknown, extra: string[]): EntityPhotoEvidenceJson | null {
  const base = normalizeEntityPhotoEvidenceUrls(existing);
  const add = extra.map((s) => s.trim()).filter(Boolean);
  const seen = new Set(base);
  const out = [...base];
  for (const u of add) {
    if (seen.has(u)) continue;
    seen.add(u);
    out.push(u);
  }
  return buildEntityPhotoEvidence(out);
}

/** Updates claim slots `[0]` opened, `[1]` label, `[2]` closed; preserves any URLs from index 3+ (e.g. outer box extras). */
export function setPackageClaimEvidenceSlot(
  existing: unknown,
  slot: 0 | 1 | 2,
  url: string,
): EntityPhotoEvidenceJson | null {
  const cur = normalizeEntityPhotoEvidenceUrls(existing);
  const o = slot === 0 ? url.trim() : (cur[0] ?? "").trim();
  const l = slot === 1 ? url.trim() : (cur[1] ?? "").trim();
  const c = slot === 2 ? url.trim() : (cur[2] ?? "").trim();
  const tail = cur.slice(3);
  const head: string[] = [];
  if (o) head.push(o);
  if (l) head.push(l);
  if (c) head.push(c);
  return buildEntityPhotoEvidence([...head, ...tail]);
}

/** Boxed package flow: `photo_evidence.urls[0]` = opened box, `[1]` = return label (canonical JSONB only). */
export function resolvePackageClaimPhotoUrls(pkg: { photo_evidence?: unknown }): {
  opened: string | null;
  label: string | null;
} {
  const urls = normalizeEntityPhotoEvidenceUrls(pkg.photo_evidence);
  return {
    opened: urls[0] ?? null,
    label: urls[1] ?? null,
  };
}
