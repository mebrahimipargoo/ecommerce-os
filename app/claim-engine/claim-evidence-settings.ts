/**
 * Global default claim evidence (organization_settings.default_claim_evidence JSONB)
 * and per-slot mapping for the Claim PDF generator.
 */

import {
  getReturnPhotoEvidenceUrls,
  type ReturnPhotoEvidenceRow,
} from "../../lib/return-photo-evidence";
import {
  normalizeEntityPhotoEvidenceUrls,
  palletPhotoEvidenceUrlsFromRow,
} from "../../lib/entity-photo-evidence";

export type ClaimEvidenceKey =
  | "outer_box"
  | "opened_box"
  | "box_label"
  | "packing_slip_manifest"
  | "defective_item"
  | "expiry"
  | "pallet_bol"
  | "pallet_overview";

/** Persisted shape — omitted keys fall back to presets. */
export type DefaultClaimEvidence = Partial<Record<ClaimEvidenceKey, boolean>>;

export const CLAIM_EVIDENCE_KEY_LABELS: Record<ClaimEvidenceKey, string> = {
  outer_box: "Outer Box",
  opened_box: "Opened Box",
  box_label: "Box Label",
  packing_slip_manifest: "Packing Slip / Manifest",
  defective_item: "Defective Item Condition Photos",
  expiry: "Expiry Photo (if applicable)",
  pallet_bol: "Pallet BOL",
  pallet_overview: "Pallet Overview Photo",
};

/** Default checked in System Settings (admin checklist). */
export const DEFAULT_CLAIM_EVIDENCE_PRESETS: Record<ClaimEvidenceKey, boolean> = {
  outer_box: true,
  opened_box: true,
  box_label: true,
  packing_slip_manifest: true,
  defective_item: true,
  expiry: true,
  pallet_bol: false,
  pallet_overview: false,
};

export function mergeDefaultClaimEvidence(
  db: DefaultClaimEvidence | null | undefined,
): Record<ClaimEvidenceKey, boolean> {
  return { ...DEFAULT_CLAIM_EVIDENCE_PRESETS, ...(db ?? {}) };
}

export type ClaimEvidenceSlot = {
  /** Stable id for checkbox state */
  id: string;
  /** Human-readable label on PDF */
  label: string;
  url: string;
  scope: "pallet" | "package" | "item";
  settingsKey: ClaimEvidenceKey;
};

function isHttpUrl(u: string): boolean {
  return /^https?:\/\//i.test(u.trim());
}

/**
 * Lists inherited photo URLs from pallet, package, and return rows for the claim PDF picker.
 * Order: pallet → package → item. De-duplicates identical URLs.
 */
export function buildClaimEvidenceSlots(detail: {
  returnRow: {
    photo_evidence?: ReturnPhotoEvidenceRow;
  } | null;
  packageRow: {
    photo_evidence?: unknown;
  } | null;
  /** Pallet TEXT columns only (no JSONB on `pallets`). */
  pallet: {
    manifest_photo_url?: string | null;
    bol_photo_url?: string | null;
    photo_url?: string | null;
  } | null;
}): ClaimEvidenceSlot[] {
  const slots: ClaimEvidenceSlot[] = [];
  const seen = new Set<string>();

  function add(
    url: string | null | undefined,
    label: string,
    scope: ClaimEvidenceSlot["scope"],
    settingsKey: ClaimEvidenceKey,
    idSuffix: string,
  ) {
    const u = typeof url === "string" ? url.trim() : "";
    if (!u || !isHttpUrl(u) || seen.has(u)) return;
    seen.add(u);
    slots.push({
      id: `${scope}-${settingsKey}-${idSuffix}`,
      label,
      url: u,
      scope,
      settingsKey,
    });
  }

  const plt = detail.pallet;
  const pkg = detail.packageRow;
  const ret = detail.returnRow;

  const pltUrls = plt ? palletPhotoEvidenceUrlsFromRow(plt) : [];
  const pkgUrls = pkg ? normalizeEntityPhotoEvidenceUrls(pkg.photo_evidence) : [];

  if (pltUrls.length > 0) {
    add(pltUrls[0], "Packing Slip / Manifest (pallet)", "pallet", "packing_slip_manifest", "manifest");
    add(pltUrls[1], "Bill of Lading (pallet)", "pallet", "pallet_bol", "bol");
    add(pltUrls[2], "Pallet Overview", "pallet", "pallet_overview", "overview");
    pltUrls.forEach((u, i) => {
      if (i <= 2) return;
      add(u, "Pallet evidence", "pallet", "pallet_overview", `pe-${i}`);
    });
  }

  if (pkgUrls.length > 0) {
    add(pkgUrls[0], "Opened Box", "package", "opened_box", "opened");
    add(pkgUrls[1], "Box Label", "package", "box_label", "label");
    add(pkgUrls[2], "Damaged Outer Box (closed)", "package", "outer_box", "closed");
    pkgUrls.forEach((u, i) => {
      if (i <= 2) return;
      add(u, "Package evidence", "package", "outer_box", `pe-${i}`);
    });
  }

  if (ret) {
    const ev = getReturnPhotoEvidenceUrls(ret.photo_evidence);
    add(ev.item_url, "Defective Item Condition", "item", "defective_item", "item");
    add(ev.expiry_url, "Expiry Label", "item", "expiry", "expiry");
    const rl = ev.return_label_url.trim();
    const pkgLabel = (pkgUrls[1] ?? "").trim();
    if (rl && rl !== pkgLabel) {
      add(ev.return_label_url, "Return Label (item)", "item", "box_label", "ret-label");
    }
  }

  return slots;
}

export function initialSlotSelection(
  slots: ClaimEvidenceSlot[],
  mergedDefaults: Record<ClaimEvidenceKey, boolean>,
): Record<string, boolean> {
  const out: Record<string, boolean> = {};
  for (const s of slots) {
    out[s.id] = mergedDefaults[s.settingsKey] !== false;
  }
  return out;
}
