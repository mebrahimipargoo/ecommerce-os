/**
 * `packages.photo_evidence` JSONB — canonical shape `{ urls: string[] }` plus structured keys.
 * Pallets use TEXT columns `manifest_photo_url`, `bol_photo_url`, `photo_url` only (no JSONB on `pallets`).
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

function firstString(arr: unknown): string | null {
  if (!Array.isArray(arr)) return null;
  const s = arr.find((x) => typeof x === "string" && x.trim().length > 0);
  return s ? String(s).trim() : null;
}

function structUrls(raw: unknown, key: string): string[] {
  if (!raw || typeof raw !== "object") return [];
  const v = (raw as Record<string, unknown>)[key];
  if (!Array.isArray(v)) return [];
  return v.filter((x): x is string => typeof x === "string" && x.trim().length > 0);
}

/** Persists multi-photo package fields in JSONB alongside legacy TEXT columns (first URL per slot). */
export function buildStructuredPackagePhotoEvidence(parts: {
  label_urls?: string[];
  outer_box_urls?: string[];
  inside_content_urls?: string[];
  sealed_box_urls?: string[];
}): Record<string, unknown> | null {
  const label_urls = (parts.label_urls ?? []).map((s) => s.trim()).filter(Boolean);
  const outer_box_urls = (parts.outer_box_urls ?? []).map((s) => s.trim()).filter(Boolean);
  const inside_content_urls = (parts.inside_content_urls ?? []).map((s) => s.trim()).filter(Boolean);
  const sealed_box_urls = (parts.sealed_box_urls ?? []).map((s) => s.trim()).filter(Boolean);
  const o: Record<string, unknown> = {};
  if (label_urls.length) o.label_urls = label_urls;
  if (outer_box_urls.length) o.outer_box_urls = outer_box_urls;
  if (inside_content_urls.length) o.inside_content_urls = inside_content_urls;
  if (sealed_box_urls.length) o.sealed_box_urls = sealed_box_urls;
  const flat = [...label_urls, ...outer_box_urls, ...inside_content_urls, ...sealed_box_urls];
  if (flat.length) o.urls = flat;
  return Object.keys(o).length ? o : null;
}

/**
 * Pallet gallery order for UI / claims: manifest scan, Bill of Lading, pallet overview — matches `pallets` TEXT columns.
 */
export function palletPhotoEvidenceUrlsFromRow(p: {
  manifest_photo_url?: string | null;
  bol_photo_url?: string | null;
  photo_url?: string | null;
} | null | undefined): string[] {
  if (!p) return [];
  const m = String(p.manifest_photo_url ?? "").trim();
  const bol = String(p.bol_photo_url ?? "").trim();
  const ov = String(p.photo_url ?? "").trim();
  return [m, bol, ov].filter(Boolean);
}

/** Boxed package flow: prefer dedicated columns, then structured JSONB keys, then flat `urls`. */
export function resolvePackageClaimPhotoUrls(pkg: {
  photo_evidence?: unknown;
  photo_opened_url?: string | null;
  photo_return_label_url?: string | null;
}): {
  opened: string | null;
  label: string | null;
} {
  const pe = pkg.photo_evidence;
  const openedCol = String(pkg.photo_opened_url ?? "").trim();
  const labelCol = String(pkg.photo_return_label_url ?? "").trim();
  const inside = structUrls(pe, "inside_content_urls");
  const labels = structUrls(pe, "label_urls");
  const flat = normalizeEntityPhotoEvidenceUrls(pe);
  return {
    opened:
      openedCol ||
      firstString(inside) ||
      flat[0] ||
      null,
    label:
      labelCol ||
      firstString(labels) ||
      flat[1] ||
      null,
  };
}
