"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { resolveOrganizationId } from "../../lib/organization";
import { isUuidString, uuidFkOrNull, uuidOrNull } from "../../lib/uuid";
import {
  CLAIM_SUBMISSION_RETURN_ID_COLUMN,
  CLAIM_SUBMISSIONS_TABLE,
} from "../claim-engine/claim-submissions-constants";
import { defectReasonsPayload } from "./claim-condition-labels";
import {
  CLAIM_CONDITIONS,
  shouldAutoEnqueueAmazonClaimSubmission,
  storePlatformFromEmbed,
} from "./claim-queue-helpers";
import { RETURN_SELECT } from "./returns-constants";

const DEFAULT_ORG = resolveOrganizationId();

/** Rejects invalid client values (e.g. mistyped default store id in localStorage). */
function resolveOrganizationIdForWrite(clientOrg?: string | null): string {
  const raw = clientOrg?.trim();
  if (raw && isUuidString(raw)) return raw;
  return resolveOrganizationId();
}

async function fetchPackageOrderId(packageId: string | null | undefined): Promise<string | null> {
  const id = uuidOrNull(packageId ?? null);
  if (!id) return null;
  const { data } = await supabaseServer
    .from("packages")
    .select("order_id")
    .eq("id", id)
    .maybeSingle();
  const t = (data as { order_id?: string | null } | null)?.order_id?.trim();
  return t || null;
}
/** Seeded in migration 20260333 — last-resort FK for claim_submissions.store_id when resolving from return/package fails. */
const DEFAULT_TRANSITION_STORE_ID = "00000000-0000-0000-0000-0000000000f1";
/** Legacy default label for audit logs (TEXT only — never write to UUID columns). */
const DEFAULT_ACTOR = "operator";

/** Coerce package.expected_item_count — never pass arrays/objects to INTEGER columns. */
function coerceNonNegativeInt(v: unknown, fallback: number): number {
  if (Array.isArray(v) || (typeof v === "object" && v !== null)) return fallback;
  if (typeof v === "number" && Number.isFinite(v) && v >= 0) return Math.floor(v);
  if (typeof v === "string" && v.trim()) {
    const n = parseInt(v.trim(), 10);
    if (Number.isFinite(n) && n >= 0) return n;
  }
  return fallback;
}

function omitUndefined<T extends Record<string, unknown>>(obj: T): Record<string, unknown> {
  return Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== undefined));
}

/**
 * Seeded MVP operator when the UI passes a display name (e.g. "Maysam") instead of
 * `auth.users.id`. Keeps `created_by_id` / `updated_by_id` valid UUIDs.
 */
const MVP_ACTOR_USER_ID = "00000000-0000-0000-0000-0000000000fe";

/** Maps UI `actor` (name or UUID) to a UUID for `created_by_id` / `updated_by_id`. */
function resolveActorUserId(actor?: string | null): string {
  const raw = (actor ?? DEFAULT_ACTOR).trim();
  if (isUuidString(raw)) return raw;
  return MVP_ACTOR_USER_ID;
}

// ─── Organisation Settings ────────────────────────────────────────────────────

export interface OrgSettings {
  is_ai_label_ocr_enabled: boolean;
  is_ai_packing_slip_ocr_enabled: boolean;
}

const FALLBACK_ORG_SETTINGS: OrgSettings = {
  is_ai_label_ocr_enabled: false,
  is_ai_packing_slip_ocr_enabled: false,
};

/**
 * Reads AI feature flags from the `organization_settings` table.
 * Gracefully returns all-disabled if the table is not yet provisioned.
 */
export async function getOrgSettings(orgId = resolveOrganizationId()): Promise<OrgSettings> {
  try {
    const { data, error } = await supabaseServer
      .from("organization_settings")
      .select("is_ai_label_ocr_enabled, is_ai_packing_slip_ocr_enabled")
      .eq("organization_id", orgId)
      .single();
    if (error) return FALLBACK_ORG_SETTINGS;
    return data ?? FALLBACK_ORG_SETTINGS;
  } catch {
    return FALLBACK_ORG_SETTINGS;
  }
}

// ─── Duplicate / unique-constraint error helper ───────────────────────────────

function parseDuplicateError(message: string): string {
  const m = message.toLowerCase();
  if (m.includes("idx_returns_lpn") || (m.includes("duplicate") && m.includes("lpn")))
    return "🚨 This LPN has already been scanned. Each return label must be unique — check for a double-scan.";
  if (m.includes("idx_packages_tracking") || (m.includes("duplicate") && m.includes("tracking")))
    return "🚨 This tracking number is already registered. Each package must be unique — check for a duplicate.";
  if (m.includes("duplicate key") || m.includes("unique constraint") || m.includes("unique violation"))
    return "🚨 A duplicate record already exists. Please verify the identifier and try again.";
  return message;
}

// ─── Claim-eligible conditions: see ./claim-queue-helpers (CLAIM_CONDITIONS) ───

/** True when the package has outer-box + return-label photos operators can inherit for claims. */
async function fetchPackageInheritedClaimPhotosReady(packageId: string | null | undefined): Promise<boolean> {
  const id = uuidOrNull(packageId ?? null);
  if (!id) return false;
  const { data } = await supabaseServer
    .from("packages")
    .select("photo_opened_url, photo_closed_url, photo_return_label_url, photo_url")
    .eq("id", id)
    .maybeSingle();
  if (!data) return false;
  /** Opened-box evidence must be on `photo_opened_url` — `photo_url` is optional outer-only. */
  const opened = !!String(data.photo_opened_url ?? "").trim();
  const label = !!String(data.photo_return_label_url ?? "").trim();
  return opened && label;
}

function deriveStatus(
  conditions: string[],
  photoEvidence: Record<string, number> | null | undefined,
  opts?: {
    /** Package has outer/box + return-label photos — counts toward ready_for_claim without per-item box uploads. */
    packageHasInheritedClaimPhotos?: boolean;
    /** Item-level evidence URLs (item photo / expiry photo). */
    hasItemEvidenceUrls?: boolean;
  },
): string {
  const needsClaim = conditions.some((c) => CLAIM_CONDITIONS.has(c));
  if (!needsClaim) return "received";
  const hasCategoryPhotos = photoEvidence != null && Object.values(photoEvidence).some((n) => n > 0);
  const inherited = !!opts?.packageHasInheritedClaimPhotos;
  const itemUrls = !!opts?.hasItemEvidenceUrls;
  if (hasCategoryPhotos || inherited || itemUrls) return "ready_for_claim";
  return "pending_evidence";
}

/** Builds `source_payload` for `claim_submissions` including package + pallet evidence and optional extra URLs (e.g. condition-category uploads). Exported for batch sync. */
export async function buildClaimSubmissionSourcePayloadForReturn(
  conditions: string[],
  amazonOrderId: string | null | undefined,
  packageId: string | null | undefined,
  claimMeta?: {
    expirationDate?: string | null;
    batchNumber?: string | null;
    operatorNotes?: string | null;
  },
  extras?: { selectedEvidenceUrls?: string[] | null },
): Promise<Record<string, unknown>> {
  const base = buildClaimSourcePayload(conditions, amazonOrderId, claimMeta);
  const withPkg = await mergePackageBoxEvidenceIntoPayload(packageId, base);
  const withPlt = await mergePalletEvidenceIntoPayload(packageId, withPkg);
  const urls = extras?.selectedEvidenceUrls?.filter((u) => typeof u === "string" && u.trim().length > 0) ?? [];
  if (urls.length > 0) return { ...withPlt, selected_claim_evidence_urls: urls };
  return withPlt;
}

function buildClaimSourcePayload(
  conditions: string[],
  /** External marketplace order id (TEXT) — stored as `amazon_order_id` in JSONB only. */
  amazonOrderId: string | null | undefined,
  claimMeta?: {
    expirationDate?: string | null;
    batchNumber?: string | null;
    operatorNotes?: string | null;
  },
): Record<string, unknown> {
  const oid = amazonOrderId?.trim();
  const payload: Record<string, unknown> = {
    ...defectReasonsPayload(conditions),
    claim_type: conditions.find((c) => CLAIM_CONDITIONS.has(c)) ?? null,
  };
  if (oid) payload.amazon_order_id = oid;
  const opNotes = claimMeta?.operatorNotes?.trim();
  if (opNotes) payload.operator_notes = opNotes;
  if (conditions.includes("expired")) {
    const exp = claimMeta?.expirationDate?.trim();
    if (exp) payload.expiration_date = exp;
    const lot = claimMeta?.batchNumber?.trim();
    if (lot) payload.batch_number = lot;
  }
  return payload;
}

/** Merges package-level box / label photos into the JSONB claim payload (box → item inheritance). */
async function mergePackageBoxEvidenceIntoPayload(
  packageId: string | null | undefined,
  payload: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const id = uuidOrNull(packageId ?? null);
  if (!id) return payload;
  const { data } = await supabaseServer
    .from("packages")
    .select("photo_opened_url, photo_closed_url, photo_return_label_url, photo_url")
    .eq("id", id)
    .maybeSingle();
  if (!data) return payload;
  const closed = data.photo_closed_url ?? null;
  const opened = data.photo_opened_url ?? null;
  const label = data.photo_return_label_url ?? null;
  const outer = opened || closed || data.photo_url || null;
  return {
    ...payload,
    package_box_evidence: {
      outer_box_url: outer,
      return_label_url: label,
      photo_closed_url: closed,
      photo_opened_url: opened,
      photo_return_label_url: label,
    },
  };
}

/** Pallet-level manifest / BOL / box photos linked via package → pallet (item inherits for claims). */
async function mergePalletEvidenceIntoPayload(
  packageId: string | null | undefined,
  payload: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const pkgId = uuidOrNull(packageId ?? null);
  if (!pkgId) return payload;
  const { data: pkg } = await supabaseServer
    .from("packages")
    .select("pallet_id")
    .eq("id", pkgId)
    .maybeSingle();
  const palletId = pkg?.pallet_id ? String(pkg.pallet_id).trim() : "";
  if (!palletId || !isUuidString(palletId)) return payload;
  const { data: plt } = await supabaseServer
    .from("pallets")
    .select("manifest_photo_url, bol_photo_url, photo_url")
    .eq("id", palletId)
    .maybeSingle();
  if (!plt) return payload;
  return {
    ...payload,
    pallet_evidence: {
      manifest_photo_url: (plt as { manifest_photo_url?: string | null }).manifest_photo_url ?? null,
      bol_photo_url: (plt as { bol_photo_url?: string | null }).bol_photo_url ?? null,
      photo_url: (plt as { photo_url?: string | null }).photo_url ?? null,
    },
  };
}

/** Resolves non-null `store_id` for `claim_submissions` (return → package → default Amazon store). Exported for batch sync. */
export async function resolveClaimSubmissionStoreId(
  organizationId: string,
  preferred: string | null | undefined,
  hint?: { store_id?: string | null; package_id?: string | null },
): Promise<string> {
  const pref = preferred?.trim();
  if (pref && isUuidString(pref)) return pref;
  const hintStore = hint?.store_id?.trim();
  if (hintStore && isUuidString(hintStore)) return hintStore;
  const hintPkgId = uuidOrNull(hint?.package_id ?? null);
  if (hintPkgId) {
    const { data: pkg } = await supabaseServer
      .from("packages")
      .select("store_id")
      .eq("id", hintPkgId)
      .maybeSingle();
    const sid = pkg?.store_id ? String(pkg.store_id).trim() : "";
    if (sid && isUuidString(sid)) return sid;
  }
  const { data: amazon } = await supabaseServer
    .from("stores")
    .select("id")
    .eq("organization_id", organizationId)
    .eq("is_active", true)
    .ilike("platform", "amazon")
    .limit(1)
    .maybeSingle();
  if (amazon?.id) return amazon.id as string;
  const { data: anyStore } = await supabaseServer
    .from("stores")
    .select("id")
    .eq("organization_id", organizationId)
    .eq("is_active", true)
    .limit(1)
    .maybeSingle();
  if (anyStore?.id) return anyStore.id as string;
  return DEFAULT_TRANSITION_STORE_ID;
}

/** Queues a `claim_submissions` row for the Python agent (`ready_to_send`). */
async function upsertClaimSubmissionForReadyReturn(opts: {
  organizationId: string;
  returnId: string;
  storeId: string | null | undefined;
  claimAmount: number;
  sourcePayload: Record<string, unknown>;
  /** Used to resolve store_id when null (package → default store). */
  returnHint?: { store_id?: string | null; package_id?: string | null };
}): Promise<{ error: { message: string } | null }> {
  const orgRow = opts.organizationId.trim();
  const organizationId = isUuidString(orgRow) ? orgRow : resolveOrganizationId();
  const rid = opts.returnId.trim();
  if (!isUuidString(rid)) {
    return { error: { message: "Invalid return id for claim_submissions (expected UUID)." } };
  }
  const storeId = await resolveClaimSubmissionStoreId(
    organizationId,
    opts.storeId,
    opts.returnHint,
  );
  const row = {
    organization_id: organizationId,
    [CLAIM_SUBMISSION_RETURN_ID_COLUMN]: rid,
    store_id: storeId,
    status: "ready_to_send" as const,
    claim_amount: opts.claimAmount,
    report_url: null as string | null,
    source_payload: opts.sourcePayload,
    updated_at: new Date().toISOString(),
  };
  const { data: existing, error: selErr } = await supabaseServer
    .from(CLAIM_SUBMISSIONS_TABLE)
    .select("id")
    .eq(CLAIM_SUBMISSION_RETURN_ID_COLUMN, rid)
    .maybeSingle();
  if (selErr) return { error: { message: selErr.message } };
  if (existing && typeof (existing as { id?: string }).id === "string") {
    const { error } = await supabaseServer
      .from(CLAIM_SUBMISSIONS_TABLE)
      .update({
        organization_id: row.organization_id,
        store_id: row.store_id,
        status: row.status,
        claim_amount: row.claim_amount,
        report_url: row.report_url,
        source_payload: row.source_payload,
        updated_at: row.updated_at,
      })
      .eq("id", (existing as { id: string }).id);
    return { error: error ? { message: error.message } : null };
  }
  const { error } = await supabaseServer.from(CLAIM_SUBMISSIONS_TABLE).insert(row);
  return { error: error ? { message: error.message } : null };
}

// ─── Internal audit helpers ───────────────────────────────────────────────────

async function logReturnAudit(opts: {
  organizationId: string; returnId?: string; palletId?: string; packageId?: string;
  action: "created" | "updated" | "status_changed" | "deleted";
  field?: string; oldValue?: string; newValue?: string; actor: string;
}) {
  await supabaseServer.from("return_audit_log").insert({
    organization_id: opts.organizationId, return_id: opts.returnId ?? null,
    pallet_id: opts.palletId ?? null, action: opts.action,
    field: opts.field ?? null, old_value: opts.oldValue ?? null,
    new_value: opts.newValue ?? null, actor: opts.actor,
  });
}

async function logPalletAudit(opts: {
  organizationId: string; palletId?: string;
  action: "created" | "updated" | "status_changed" | "deleted";
  field?: string; oldValue?: string; newValue?: string; actor: string;
}) {
  await supabaseServer.from("pallet_audit_log").insert({
    organization_id: opts.organizationId, pallet_id: opts.palletId ?? null,
    action: opts.action, field: opts.field ?? null,
    old_value: opts.oldValue ?? null, new_value: opts.newValue ?? null, actor: opts.actor,
  });
}

async function logPackageAudit(opts: {
  organizationId: string; packageId?: string;
  action: "created" | "updated" | "status_changed" | "deleted";
  field?: string; oldValue?: string; newValue?: string; actor: string;
}) {
  await supabaseServer.from("package_audit_log").insert({
    organization_id: opts.organizationId, package_id: opts.packageId ?? null,
    action: opts.action, field: opts.field ?? null,
    old_value: opts.oldValue ?? null, new_value: opts.newValue ?? null, actor: opts.actor,
  }).then(() => null);
}

// ─── Pallet Types ─────────────────────────────────────────────────────────────

export type PalletStatus = "open" | "closed" | "submitted";

export type PalletRecord = {
  id: string; organization_id: string;
  pallet_number: string;
  /** Inbound shipment / pro tracking — optional text (not a UUID). */
  tracking_number?: string | null;
  manifest_photo_url: string | null;
  bol_photo_url?: string | null; // post-migration field
  photo_url?: string | null; // box/pallet general photo — added in 20260324_media_photo_urls.sql
  status: PalletStatus; notes: string | null; item_count: number;
  /** Legacy text label (older rows); prefer `created_by_id` for new schema. */
  created_by?: string | null;
  created_by_id?: string | null;
  /** Last actor who updated this row — HR / performance analytics */
  updated_by?: string | null;
  updated_by_id?: string | null;
  created_at: string; updated_at: string;
  /** UUID of the connected store — added in 20260325_pallets_store_id.sql */
  store_id?: string | null;
  /** Joined store row — present when fetched with stores(name,platform) select */
  stores?: { name: string; platform: string } | null;
  /** Live counts from `listPallets` relation embeds (optional). */
  child_packages_count?: number;
  child_returns_count?: number;
};

export type PalletInsertPayload = {
  pallet_number: string; manifest_photo_url?: string;
  bol_photo_url?: string;
  photo_url?: string;
  store_id?: string;
  notes?: string; organization_id?: string; created_by?: string;
};

// ─── Package Types ────────────────────────────────────────────────────────────

export type PackageStatus = "open" | "closed" | "suspicious" | "submitted";

/** A single line-item extracted from a packing slip by AI OCR. */
export type ExpectedItem = { sku: string; expected_qty: number; description?: string };

export type PackageRecord = {
  id: string; organization_id: string;
  package_number: string; tracking_number: string | null;
  carrier_name: string | null;
  /** RMA number at the package level — added in 20260325_packages_rma_number.sql */
  rma_number?: string | null;
  expected_item_count: number; actual_item_count: number;
  pallet_id: string | null; status: PackageStatus;
  discrepancy_note: string | null;
  // Post-migration field — only present after 20250325_packages_expected_items.sql is applied
  expected_items?: ExpectedItem[] | null;
  /** URL of the uploaded packing-slip photo in the manifests storage bucket */
  manifest_url?: string | null;
  /** Packing slip / manifest image in the media bucket — migration 20260327_packages_manifest_photo_url.sql */
  manifest_photo_url?: string | null;
  /** Parsed manifest lines (JSONB) — migration 20260337_packages_manifest_data.sql */
  manifest_data?: ExpectedItem[] | null;
  /** General box/package photo — added in 20260324_media_photo_urls.sql */
  photo_url?: string | null;
  /** Claim evidence: closed-box photo — added in 20260325_claims_evidence_expiry.sql */
  photo_closed_url?: string | null;
  /** Claim evidence: opened-box photo — added in 20260325_claims_evidence_expiry.sql */
  photo_opened_url?: string | null;
  /** Claim evidence: return-label photo — added in 20260325_packages_return_label_photo.sql */
  photo_return_label_url?: string | null;
  /** UUID of the connected store — added in 20260325_saas_stores_usage.sql */
  store_id?: string | null;
  /** Joined store row — present when fetched with stores(name,platform) select */
  stores?: { name: string; platform: string } | null;
  created_by?: string | null;
  created_by_id?: string | null;
  /** Last actor who updated this row — HR / performance analytics */
  updated_by?: string | null;
  updated_by_id?: string | null;
  created_at: string; updated_at: string;
  /** External order id — migration 20260327_packages_amazon_order_id.sql */
  order_id?: string | null;
};

function sumManifestLineQty(lines: ExpectedItem[] | null | undefined): number {
  if (!lines?.length) return 0;
  return lines.reduce((a, it) => a + (Number(it.expected_qty) > 0 ? Number(it.expected_qty) : 1), 0);
}

export type PackageInsertPayload = {
  package_number: string; tracking_number?: string;
  carrier_name?: string; rma_number?: string; expected_item_count?: number;
  pallet_id?: string; store_id?: string; organization_id?: string; created_by?: string;
  manifest_url?: string;
  manifest_photo_url?: string;
  /** JSONB manifest lines — do not send arrays to `expected_item_count` (integer). */
  manifest_data?: ExpectedItem[] | null;
  photo_url?: string;
  photo_closed_url?: string;
  photo_opened_url?: string;
  photo_return_label_url?: string;
  order_id?: string | null;
};

export type PackageUpdatePayload = Partial<Pick<
  PackageRecord,
  | "carrier_name" | "tracking_number" | "rma_number" | "expected_item_count" | "status" | "discrepancy_note" | "pallet_id" | "manifest_url" | "manifest_photo_url" | "manifest_data" | "photo_url" | "photo_closed_url" | "photo_opened_url" | "photo_return_label_url"
  | "order_id"
  | "expected_items"
>>;

// ─── Return Types ─────────────────────────────────────────────────────────────

export type ReturnInsertPayload = {
  /** Optional orphan label / LPN when item is not tied to a package (tracking lives on package). */
  lpn?: string;
  marketplace: string; item_name: string;
  /** Amazon Standard Identification Number */
  asin?: string;
  /** Fulfillment Network SKU (FBA label / network ID) */
  fnsku?: string;
  /** Stock Keeping Unit (Seller / warehouse MSKU) — column `sku` in DB V16.4+ */
  sku?: string;
  conditions: string[];
  notes?: string; photo_evidence?: Record<string, number>;
  expiration_date?: string; batch_number?: string;
  /** Claim evidence photo URLs — added in 20260325_claims_evidence_expiry.sql */
  photo_item_url?: string;
  photo_expiry_url?: string;
  /** Loose-item return label — migration 20260338_returns_photo_return_label_url.sql */
  photo_return_label_url?: string;
  pallet_id?: string; package_id?: string;
  /** UUID of the connected store — added in 20260325_saas_stores_usage.sql */
  store_id?: string;
  /**
   * Amazon / marketplace order id — stored ONLY in `returns.order_id` (TEXT) and in
   * `claim_submissions.source_payload.amazon_order_id` (JSONB). Never use for UUID FK columns.
   */
  amazon_order_id?: string | null;
  /** @deprecated Prefer `amazon_order_id` — same TEXT column `returns.order_id`. */
  order_id?: string | null;
  customer_id?: string | null;
  /**
   * Condition-category evidence URLs uploaded during receiving (local files → storage).
   * Not used for operator-side claim filtering; `buildClaimSubmissionSourcePayloadForReturn` still merges
   * package/pallet evidence from the DB. Claims Management may refine attachments later.
   */
  claim_evidence_selected_urls?: string[] | null;
  organization_id?: string; created_by?: string;
};

export type ReturnRecord = {
  id: string; organization_id: string;
  lpn: string | null;
  inherited_tracking_number?: string | null;
  inherited_carrier?: string | null;
  marketplace: string; item_name: string;
  /** Amazon Standard Identification Number — added in 20260326_returns_asin_fnsku_name.sql */
  asin?: string | null;
  /** Fulfillment Network SKU — added in 20260326_returns_asin_fnsku_name.sql */
  fnsku?: string | null;
  /** Stock Keeping Unit — unified with DB column `sku` (replaces legacy seller_sku). */
  sku?: string | null;
  /** Raw scanned product / barcode from intake — migration 20250325_returns_product_inherited_tracking.sql `product_identifier`. */
  product_identifier?: string | null;
  conditions: string[]; status: string;
  notes: string | null; photo_evidence: Record<string, number> | null;
  expiration_date: string | null; batch_number: string | null;
  /** Claim evidence URLs — added in 20260325_claims_evidence_expiry.sql */
  photo_item_url?: string | null;
  photo_expiry_url?: string | null;
  photo_return_label_url?: string | null;
  /** UUID of the connected store — added in 20260325_saas_stores_usage.sql */
  store_id?: string | null;
  /** Joined store row — present when fetched with stores(name,platform) select */
  stores?: { name: string; platform: string } | null;
  pallet_id: string | null; package_id: string | null;
  // Post-migration fields — null/undefined until 20250324_returns_order_customer_ids.sql is applied
  order_id?: string | null;
  customer_id?: string | null;
  created_by?: string | null;
  created_by_id?: string | null;
  /** Last actor who touched this row — HR / performance analytics */
  updated_by?: string | null;
  updated_by_id?: string | null;
  created_at: string; updated_at: string;
  /** Expected item value for claims — migration 20260331_returns_estimated_value_reimbursement.sql */
  estimated_value?: number | null;
};

export type ReturnUpdatePayload = Partial<Pick<
  ReturnRecord,
  | "lpn" | "item_name" | "notes" | "status"
  | "conditions" | "expiration_date" | "batch_number"
  | "package_id" | "pallet_id"
  | "photo_evidence" | "photo_item_url" | "photo_expiry_url" | "photo_return_label_url"
  | "asin" | "fnsku" | "sku" | "product_identifier" | "store_id" | "marketplace"
  | "order_id"
>>;

// ─── Audit Log Types ──────────────────────────────────────────────────────────

export type AuditLogRecord = {
  id: string; organization_id: string;
  return_id: string | null; pallet_id: string | null;
  action: string; field: string | null;
  old_value: string | null; new_value: string | null;
  actor: string; created_at: string;
};

// ─── SELECT strings ───────────────────────────────────────────────────────────

// NOTE: The following columns are only available AFTER their respective migrations are applied.
// They are omitted from SELECT to prevent PostgREST 400 errors that silently blank all data.
//   • inherited_tracking_number / inherited_carrier / bol_photo_url
//       → migration: 20250325_returns_product_inherited_tracking.sql
//   • order_id / customer_id
//       → migration: 20250324_returns_order_customer_ids.sql
//   • asin / fnsku (included in RETURN_SELECT in returns-constants.ts)
//       → migration: 20260326_returns_asin_fnsku_name.sql  (run this before deploying)
// NOTE: store_id in PALLET_SELECT requires migration 20260325_pallets_store_id.sql to be applied first.
const PALLET_SELECT = "id,organization_id,pallet_number,tracking_number,manifest_photo_url,bol_photo_url,photo_url,store_id,status,notes,item_count,created_by,created_by_id,updated_by,updated_by_id,created_at,updated_at";
const PKG_SELECT    = "id,organization_id,package_number,tracking_number,carrier_name,rma_number,expected_item_count,actual_item_count,pallet_id,store_id,status,discrepancy_note,manifest_url,manifest_photo_url,manifest_data,photo_url,photo_closed_url,photo_opened_url,photo_return_label_url,order_id,expected_items,created_by,created_by_id,updated_by,updated_by_id,created_at,updated_at";

/** Supabase relation embeds: live child counts + joined store name/platform. */
const PKG_LIST_SELECT = `${PKG_SELECT},stores(name,platform),returns(count)`;
const PALLET_LIST_SELECT = `${PALLET_SELECT},stores(name,platform),packages(count),returns(count)`;

function parseManifestData(raw: unknown): ExpectedItem[] | null | undefined {
  if (raw == null) return raw as null | undefined;
  if (Array.isArray(raw)) return raw as ExpectedItem[];
  if (typeof raw === "string") {
    try {
      const p = JSON.parse(raw) as unknown;
      return Array.isArray(p) ? (p as ExpectedItem[]) : null;
    } catch {
      return null;
    }
  }
  return null;
}

function normalizePackageRow(row: Record<string, unknown>): PackageRecord {
  const ret = row.returns as { count: number }[] | undefined;
  const { returns: _r, ...rest } = row;
  const base = rest as PackageRecord;
  const md = parseManifestData(rest.manifest_data);
  const withManifest = md !== undefined ? { ...base, manifest_data: md } : base;
  const c = ret?.[0]?.count;
  if (typeof c === "number") return { ...withManifest, actual_item_count: c };
  return withManifest;
}

function normalizePalletRow(row: Record<string, unknown>): PalletRecord {
  const pkgs = row.packages as { count: number }[] | undefined;
  const rets = row.returns as { count: number }[] | undefined;
  const { packages: _p, returns: _r, ...rest } = row;
  const base = rest as PalletRecord;
  const pc = pkgs?.[0]?.count;
  const rc = rets?.[0]?.count;
  return {
    ...base,
    ...(typeof pc === "number" ? { child_packages_count: pc } : {}),
    ...(typeof rc === "number"
      ? { child_returns_count: rc, item_count: rc }
      : {}),
  };
}

// ─── Pallet Actions ───────────────────────────────────────────────────────────

export async function createPallet(
  payload: PalletInsertPayload,
): Promise<{ ok: boolean; data?: PalletRecord; error?: string }> {
  try {
    const orgId = resolveOrganizationIdForWrite(payload.organization_id);
    const actor = payload.created_by      ?? DEFAULT_ACTOR;
    const insertRow: Record<string, unknown> = {
      organization_id: orgId,
      pallet_number: payload.pallet_number.trim(),
      manifest_photo_url: payload.manifest_photo_url ?? null,
      notes: payload.notes?.trim() || null,
      status: "open",
      created_by_id: resolveActorUserId(payload.created_by),
    };
    // bol_photo_url / photo_url / store_id only inserted if migration has been applied
    if (payload.bol_photo_url) insertRow.bol_photo_url = payload.bol_photo_url;
    if (payload.photo_url)     insertRow.photo_url     = payload.photo_url;
    const sid = uuidFkOrNull(payload.store_id ?? null, "store_id");
    if (sid) insertRow.store_id = sid;
    const { data, error } = await supabaseServer.from("pallets")
      .insert(insertRow)
      .select(PALLET_LIST_SELECT).single();
    if (error) throw new Error(error.message);
    const row = normalizePalletRow(data as Record<string, unknown>);
    void logPalletAudit({ organizationId: orgId, palletId: row.id, action: "created", actor });
    return { ok: true, data: row };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to create pallet." };
  }
}

export async function listPallets(organizationId?: string): Promise<{ ok: boolean; data: PalletRecord[]; error?: string }> {
  try {
    const { data, error } = await supabaseServer.from("pallets").select(PALLET_LIST_SELECT)
      .eq("organization_id", organizationId ?? DEFAULT_ORG)
      .is("deleted_at", null)
      .order("created_at", { ascending: false }).limit(200);
    if (error) {
      console.error("[listPallets] Supabase error:", error.message, "| code:", error.code);
      throw new Error(error.message);
    }
    const rows = (data ?? []) as Record<string, unknown>[];
    return { ok: true, data: rows.map(normalizePalletRow) };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Failed to load pallets.";
    console.error("[listPallets] Caught error:", msg);
    return { ok: false, data: [], error: msg };
  }
}

export async function listOpenPallets(organizationId?: string): Promise<{ ok: boolean; data: PalletRecord[]; error?: string }> {
  try {
    const { data, error } = await supabaseServer.from("pallets").select(PALLET_LIST_SELECT)
      .eq("organization_id", organizationId ?? DEFAULT_ORG).eq("status", "open")
      .is("deleted_at", null)
      .order("created_at", { ascending: false }).limit(50);
    if (error) throw new Error(error.message);
    const rows = (data ?? []) as Record<string, unknown>[];
    return { ok: true, data: rows.map(normalizePalletRow) };
  } catch (err) {
    return { ok: false, data: [], error: err instanceof Error ? err.message : "Failed to load open pallets." };
  }
}

export async function updatePalletStatus(
  palletId: string, status: PalletStatus, actor?: string,
): Promise<{ ok: boolean; error?: string }> {
  try {
    const id = uuidOrNull(palletId);
    if (!id) throw new Error("Invalid pallet id.");
    const { error } = await supabaseServer.from("pallets")
      .update({ status, updated_by_id: resolveActorUserId(actor) }).eq("id", id);
    if (error) throw new Error(error.message);
    void logPalletAudit({ organizationId: DEFAULT_ORG, palletId: id, action: "status_changed", field: "status", newValue: status, actor: actor ?? DEFAULT_ACTOR });
    return { ok: true };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to update pallet." };
  }
}

export type PalletUpdatePayload = Partial<Pick<
  PalletRecord,
  | "status" | "notes" | "tracking_number" | "bol_photo_url" | "manifest_photo_url" | "photo_url"
>>;

export async function updatePallet(
  palletId: string,
  updates: PalletUpdatePayload,
  actor?: string,
  organizationId?: string | null,
): Promise<{ ok: boolean; data?: PalletRecord; error?: string }> {
  try {
    const id = uuidOrNull(palletId);
    if (!id) throw new Error("Invalid pallet id.");
    const org = resolveOrganizationIdForWrite(organizationId);
    const row: Record<string, unknown> = {
      ...omitUndefined(updates as Record<string, unknown>),
      updated_by_id: resolveActorUserId(actor),
    };
    delete row.updated_by;
    delete row.created_by;
    if ("notes" in row && row.notes !== undefined && row.notes !== null) {
      row.notes = String(row.notes).trim() || null;
    }
    if ("tracking_number" in row && row.tracking_number !== undefined && row.tracking_number !== null) {
      row.tracking_number = String(row.tracking_number).trim() || null;
    }
    const { data, error } = await supabaseServer.from("pallets")
      .update(row)
      .eq("id", id)
      .eq("organization_id", org)
      .select(PALLET_LIST_SELECT)
      .single();
    if (error) throw new Error(error.message);
    const rec = normalizePalletRow(data as Record<string, unknown>);
    void logPalletAudit({
      organizationId: org,
      palletId: id,
      action: "updated",
      actor: actor ?? DEFAULT_ACTOR,
    });
    return { ok: true, data: rec };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to update pallet." };
  }
}

export async function deletePallet(palletId: string, actor?: string): Promise<{ ok: boolean; error?: string }> {
  try {
    const id = uuidOrNull(palletId);
    if (!id) throw new Error("Invalid pallet id.");
    void logPalletAudit({ organizationId: DEFAULT_ORG, palletId: id, action: "deleted", actor: actor ?? DEFAULT_ACTOR });
    const { error } = await supabaseServer.from("pallets").delete().eq("id", id);
    if (error) throw new Error(error.message);
    return { ok: true };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to delete pallet." };
  }
}

// ─── Package Actions ──────────────────────────────────────────────────────────

export async function createPackage(
  payload: PackageInsertPayload,
): Promise<{ ok: boolean; data?: PackageRecord; error?: string }> {
  try {
    const orgId = resolveOrganizationIdForWrite(payload.organization_id);
    const actor = payload.created_by      ?? DEFAULT_ACTOR;
    const md = parseManifestData(payload.manifest_data);
    const lines = Array.isArray(md) && md.length > 0 ? md : null;
    const qtyFromManifest = sumManifestLineQty(lines);
    const fallbackCount = coerceNonNegativeInt(payload.expected_item_count, 0);
    const expected_item_count = qtyFromManifest > 0 ? qtyFromManifest : fallbackCount;
    const palletIdFk = uuidFkOrNull(payload.pallet_id ?? null, "pallet_id");
    const storeIdFk = uuidFkOrNull(payload.store_id ?? null, "store_id");
    const insertRow: Record<string, unknown> = {
      organization_id:     orgId,
      package_number:      payload.package_number.trim(),
      tracking_number:     payload.tracking_number?.trim() || null,
      carrier_name:        payload.carrier_name?.trim() || null,
      rma_number:          payload.rma_number?.trim() || null,
      expected_item_count,
      pallet_id:           palletIdFk,
      manifest_url:        payload.manifest_url ?? null,
      status:              "open",
      created_by_id:       resolveActorUserId(payload.created_by),
    };
    if (storeIdFk) insertRow.store_id = storeIdFk;
    if (payload.manifest_photo_url) insertRow.manifest_photo_url = payload.manifest_photo_url;
    if (lines) {
      insertRow.manifest_data = lines;
      insertRow.expected_items = lines;
    }
    if (payload.photo_url) insertRow.photo_url = payload.photo_url;
    if (payload.photo_closed_url) insertRow.photo_closed_url = payload.photo_closed_url;
    if (payload.photo_opened_url) insertRow.photo_opened_url = payload.photo_opened_url;
    if (payload.photo_return_label_url) insertRow.photo_return_label_url = payload.photo_return_label_url;
    if (payload.order_id?.trim()) insertRow.order_id = payload.order_id.trim();

    const { data, error } = await supabaseServer.from("packages")
      .insert(insertRow)
      .select(PKG_LIST_SELECT).single();
    if (error) throw new Error(parseDuplicateError(error.message));
    void logPackageAudit({ organizationId: orgId, packageId: (data as { id: string }).id, action: "created", actor });
    return { ok: true, data: normalizePackageRow(data as Record<string, unknown>) };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to create package." };
  }
}

export async function updatePackage(
  packageId: string,
  updates: PackageUpdatePayload,
  actor?: string,
): Promise<{ ok: boolean; data?: PackageRecord; error?: string }> {
  try {
    const pkgId = uuidOrNull(packageId);
    if (!pkgId) throw new Error("Invalid package id.");
    const payload = omitUndefined({
      ...updates,
      updated_by_id: resolveActorUserId(actor),
    } as Record<string, unknown>);
    delete payload.updated_by;
    delete payload.created_by;
    if ("pallet_id" in payload) payload.pallet_id = uuidOrNull(payload.pallet_id as string);
    if ("store_id" in payload) {
      const s = uuidOrNull(payload.store_id as string);
      payload.store_id = s;
    }
    if ("manifest_data" in payload && payload.manifest_data != null) {
      const parsed = parseManifestData(payload.manifest_data);
      if (Array.isArray(parsed) && parsed.length > 0) {
        payload.manifest_data = parsed;
        payload.expected_items = parsed;
      }
    }
    if ("expected_item_count" in payload) {
      payload.expected_item_count = coerceNonNegativeInt(payload.expected_item_count, 0);
    }
    const { data, error } = await supabaseServer.from("packages")
      .update(payload)
      .eq("id", pkgId).select(PKG_LIST_SELECT).single();
    if (error) throw new Error(parseDuplicateError(error.message));
    const row = normalizePackageRow(data as Record<string, unknown>);
    // Keep denormalized returns.pallet_id in sync when package moves between pallets
    if ("pallet_id" in payload) {
      const { error: syncErr } = await supabaseServer.from("returns")
        .update({ pallet_id: row.pallet_id })
        .eq("package_id", pkgId);
      if (syncErr) console.error("[updatePackage] sync returns.pallet_id:", syncErr.message);
    }
    void logPackageAudit({
      organizationId: row.organization_id ?? DEFAULT_ORG,
      packageId: pkgId,
      action: "updated",
      actor: actor ?? DEFAULT_ACTOR,
    });
    return { ok: true, data: row };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to update package." };
  }
}

export async function listPackages(organizationId?: string): Promise<{ ok: boolean; data: PackageRecord[]; error?: string }> {
  try {
    const { data, error } = await supabaseServer.from("packages").select(PKG_LIST_SELECT)
      .eq("organization_id", organizationId ?? DEFAULT_ORG)
      .is("deleted_at", null)
      .order("created_at", { ascending: false }).limit(200);
    if (error) {
      console.error("[listPackages] Supabase error:", error.message, "| code:", error.code);
      throw new Error(error.message);
    }
    const rows = (data ?? []) as Record<string, unknown>[];
    return { ok: true, data: rows.map(normalizePackageRow) };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Failed to load packages.";
    console.error("[listPackages] Caught error:", msg);
    return { ok: false, data: [], error: msg };
  }
}

export async function listOpenPackages(organizationId?: string): Promise<{ ok: boolean; data: PackageRecord[]; error?: string }> {
  try {
    const { data, error } = await supabaseServer.from("packages").select(PKG_LIST_SELECT)
      .eq("organization_id", organizationId ?? DEFAULT_ORG).eq("status", "open")
      .is("deleted_at", null)
      .order("created_at", { ascending: false }).limit(50);
    if (error) throw new Error(error.message);
    const rows = (data ?? []) as Record<string, unknown>[];
    return { ok: true, data: rows.map(normalizePackageRow) };
  } catch (err) {
    return { ok: false, data: [], error: err instanceof Error ? err.message : "Failed to load open packages." };
  }
}

export async function listReturnsByPackage(packageId: string): Promise<{ ok: boolean; data: ReturnRecord[]; error?: string }> {
  try {
    const id = uuidOrNull(packageId);
    if (!id) return { ok: true, data: [] };
    const { data, error } = await supabaseServer.from("returns").select(RETURN_SELECT)
      .eq("package_id", id).is("deleted_at", null).order("created_at", { ascending: false });
    if (error) throw new Error(error.message);
    return { ok: true, data: (data ?? []) as unknown as ReturnRecord[] };
  } catch (err) {
    return { ok: false, data: [], error: err instanceof Error ? err.message : "Failed to load package items." };
  }
}

export async function closePackage(
  packageId: string,
  opts?: { discrepancyNote?: string; actor?: string },
): Promise<{ ok: boolean; status?: PackageStatus; error?: string }> {
  try {
    const id = uuidOrNull(packageId);
    if (!id) throw new Error("Invalid package id.");
    const actor = opts?.actor ?? DEFAULT_ACTOR;
    const { data: pkg, error: fetchErr } = await supabaseServer.from("packages")
      .select("expected_item_count,actual_item_count,organization_id")
      .eq("id", id).single();
    if (fetchErr) throw new Error(fetchErr.message);
    const hasDiscrepancy = (pkg.expected_item_count > 0 && pkg.expected_item_count !== pkg.actual_item_count) || !!opts?.discrepancyNote;
    const newStatus: PackageStatus = hasDiscrepancy ? "suspicious" : "closed";
    const { error } = await supabaseServer.from("packages")
      .update({ status: newStatus, discrepancy_note: opts?.discrepancyNote ?? null, updated_by_id: resolveActorUserId(actor) })
      .eq("id", id);
    if (error) throw new Error(error.message);
    void logPackageAudit({ organizationId: pkg.organization_id ?? DEFAULT_ORG, packageId: id, action: "status_changed", field: "status", newValue: newStatus, actor });
    return { ok: true, status: newStatus };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to close package." };
  }
}

export async function deletePackage(packageId: string, actor?: string): Promise<{ ok: boolean; error?: string }> {
  try {
    const id = uuidOrNull(packageId);
    if (!id) throw new Error("Invalid package id.");
    void logPackageAudit({ organizationId: DEFAULT_ORG, packageId: id, action: "deleted", actor: actor ?? DEFAULT_ACTOR });
    const { error } = await supabaseServer.from("packages").delete().eq("id", id);
    if (error) throw new Error(error.message);
    return { ok: true };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to delete package." };
  }
}

// ─── Return Actions ───────────────────────────────────────────────────────────

export async function insertReturn(
  payload: ReturnInsertPayload,
): Promise<{ ok: boolean; data?: ReturnRecord; error?: string }> {
  try {
    const orgId = resolveOrganizationIdForWrite(payload.organization_id);
    const packageIdFk = uuidFkOrNull(payload.package_id ?? null, "package_id");
    const packageInherited = await fetchPackageInheritedClaimPhotosReady(packageIdFk);
    const hasItemUrls = !!(
      payload.photo_item_url?.trim() ||
      payload.photo_expiry_url?.trim() ||
      payload.photo_return_label_url?.trim()
    );
    const status = deriveStatus(payload.conditions, payload.photo_evidence ?? null, {
      packageHasInheritedClaimPhotos: packageInherited,
      hasItemEvidenceUrls: hasItemUrls,
    });

    /** Pallet FK only when linked to a real package — inherit from package row (never raw user text). */
    let effectivePalletId: string | null = null;
    if (packageIdFk) {
      const { data: pkg } = await supabaseServer.from("packages")
        .select("pallet_id").eq("id", packageIdFk).maybeSingle();
      const pl = pkg?.pallet_id ? String(pkg.pallet_id).trim() : "";
      effectivePalletId = pl && isUuidString(pl) ? pl : null;
    }

    const rawStore = payload.store_id?.trim();
    if (rawStore && !isUuidString(rawStore)) {
      throw new Error(
        `Invalid store_id: "${rawStore}" is not a valid UUID. Pick a Sales Channel from the dropdown.`,
      );
    }
    const resolvedStoreId = await resolveClaimSubmissionStoreId(orgId, rawStore, {
      package_id: packageIdFk,
    });

    const orderFromPackage = await fetchPackageOrderId(packageIdFk);
    const amazonOrderFromPayload = (
      payload.amazon_order_id?.trim() ||
      payload.order_id?.trim() ||
      ""
    ).trim();
    /** TEXT marketplace order id only — never assign to store_id / package_id / pallet_id. */
    const effectiveAmazonOrderId =
      (amazonOrderFromPayload || orderFromPackage || "").trim() || null;

    const insertRow: Record<string, unknown> = {
      organization_id: orgId,
      lpn:             payload.lpn?.trim() || null,
      marketplace:     payload.marketplace,
      item_name:       payload.item_name.trim(),
      conditions:      payload.conditions,
      notes:           payload.notes?.trim() || null,
      photo_evidence:  payload.photo_evidence ?? null,
      expiration_date: payload.expiration_date || null,
      batch_number:    payload.batch_number?.trim() || null,
      pallet_id:       effectivePalletId,
      package_id:      packageIdFk,
      status,
      created_by_id:   resolveActorUserId(payload.created_by),
      store_id:        resolvedStoreId,
    };
    // Post-migration columns — only written once their migrations are applied
    if (payload.asin)             insertRow.asin             = payload.asin.trim();
    if (payload.fnsku)            insertRow.fnsku            = payload.fnsku.trim();
    if (payload.sku)              insertRow.sku              = payload.sku.trim();
    if (effectiveAmazonOrderId) insertRow.order_id = String(effectiveAmazonOrderId);
    if (payload.customer_id)    insertRow.customer_id    = payload.customer_id.trim();
    if (payload.photo_item_url)   insertRow.photo_item_url   = payload.photo_item_url;
    if (payload.photo_expiry_url) insertRow.photo_expiry_url = payload.photo_expiry_url;
    if (payload.photo_return_label_url) insertRow.photo_return_label_url = payload.photo_return_label_url;

    const { data, error } = await supabaseServer.from("returns")
      .insert(insertRow).select(RETURN_SELECT).single();

    if (error) throw new Error(parseDuplicateError(error.message));
    const rec = data as unknown as ReturnRecord;

    if (status === "ready_for_claim") {
      let storePlat: string | null | undefined;
      if (resolvedStoreId) {
        const { data: st } = await supabaseServer
          .from("stores")
          .select("platform")
          .eq("id", resolvedStoreId)
          .maybeSingle();
        storePlat = (st as { platform?: string } | null)?.platform ?? null;
      }
      if (shouldAutoEnqueueAmazonClaimSubmission(payload.marketplace, payload.conditions, storePlat)) {
        const ev = rec.estimated_value;
        const n = Number(ev);
        const claimAmount = Number.isFinite(n) && n > 0 ? n : 100;
        const sourcePayload = await buildClaimSubmissionSourcePayloadForReturn(
          payload.conditions,
          effectiveAmazonOrderId,
          packageIdFk,
          {
            expirationDate: payload.expiration_date ?? null,
            batchNumber: payload.batch_number ?? null,
            operatorNotes: payload.notes ?? null,
          },
          { selectedEvidenceUrls: payload.claim_evidence_selected_urls ?? null },
        );

        const { error: subErr } = await upsertClaimSubmissionForReadyReturn({
          organizationId: resolveOrganizationIdForWrite(rec.organization_id),
          returnId: rec.id,
          storeId: rec.store_id ?? resolvedStoreId ?? null,
          claimAmount,
          sourcePayload,
          returnHint: { store_id: rec.store_id ?? resolvedStoreId ?? null, package_id: rec.package_id ?? packageIdFk },
        });
        if (subErr) {
          await supabaseServer.from("returns").delete().eq("id", rec.id);
          throw new Error(
            `Return could not be queued for claim_submissions: ${subErr.message}. Check claim_submissions columns and RLS.`,
          );
        }
      }
    }

    void logReturnAudit({
      organizationId: orgId,
      returnId: rec.id,
      palletId: rec.pallet_id ?? undefined,
      packageId: rec.package_id ?? undefined,
      action: "created",
      newValue: JSON.stringify({ conditions: payload.conditions, status }),
      actor: payload.created_by ?? DEFAULT_ACTOR,
    });
    return { ok: true, data: rec };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to save return." };
  }
}

export async function updateReturn(
  returnId: string,
  updates: ReturnUpdatePayload,
  actor?: string,
): Promise<{ ok: boolean; data?: ReturnRecord; error?: string }> {
  try {
    const rid = uuidOrNull(returnId);
    if (!rid) throw new Error("Invalid return id.");
    const { data: existing, error: loadErr } = await supabaseServer
      .from("returns")
      .select(RETURN_SELECT)
      .eq("id", rid)
      .single();
    if (loadErr || !existing) throw new Error(loadErr?.message ?? "Return not found.");

    const patch: Record<string, unknown> = { ...updates, updated_by_id: resolveActorUserId(actor) };
    delete patch.updated_by;
    delete patch.created_by;
    // Remove post-migration columns from patch — they may not be in the DB yet
    delete patch.inherited_tracking_number;
    delete patch.inherited_carrier;
    delete patch.customer_id;

    const ex = existing as unknown as ReturnRecord;
    const nextConditions = (updates.conditions !== undefined ? updates.conditions : ex.conditions) ?? [];
    const nextPhotoEvidence = updates.photo_evidence !== undefined ? updates.photo_evidence : ex.photo_evidence;

    if ("package_id" in updates) {
      const raw = updates.package_id;
      if (raw === null || raw === "") {
        patch.package_id = null;
        patch.pallet_id = null;
      } else {
        const nid = uuidOrNull(String(raw));
        if (nid) {
          patch.package_id = nid;
        } else {
          patch.package_id = null;
          patch.pallet_id = null;
        }
      }
    }
    if ("pallet_id" in updates) {
      patch.pallet_id = uuidOrNull(updates.pallet_id as string);
    }

    const nextPackageId =
      updates.package_id !== undefined
        ? uuidOrNull(
            updates.package_id === null || updates.package_id === ""
              ? null
              : String(updates.package_id),
          )
        : uuidOrNull(ex.package_id);
    const nextPhotoItem =
      updates.photo_item_url !== undefined ? updates.photo_item_url : ex.photo_item_url;
    const nextPhotoExpiry =
      updates.photo_expiry_url !== undefined ? updates.photo_expiry_url : ex.photo_expiry_url;
    const nextPhotoReturnLabel =
      updates.photo_return_label_url !== undefined
        ? updates.photo_return_label_url
        : ex.photo_return_label_url;
    const packageInherited = await fetchPackageInheritedClaimPhotosReady(nextPackageId);
    const hasItemUrls = !!(
      String(nextPhotoItem ?? "").trim() ||
      String(nextPhotoExpiry ?? "").trim() ||
      String(nextPhotoReturnLabel ?? "").trim()
    );
    patch.status = deriveStatus(nextConditions, nextPhotoEvidence ?? null, {
      packageHasInheritedClaimPhotos: packageInherited,
      hasItemEvidenceUrls: hasItemUrls,
    });
    if (patch.store_id != null) {
      const sid = String(patch.store_id).trim();
      if (sid && !isUuidString(sid)) {
        patch.store_id = await resolveClaimSubmissionStoreId(
          resolveOrganizationIdForWrite(ex.organization_id),
          null,
          { package_id: nextPackageId },
        );
      }
    }
    // Inherit pallet_id from package when package_id is set to a real package
    if ("package_id" in updates && updates.package_id) {
      const pkgId = uuidOrNull(String(updates.package_id));
      if (pkgId && updates.pallet_id === undefined) {
        const { data: pkg } = await supabaseServer.from("packages")
          .select("pallet_id").eq("id", pkgId).maybeSingle();
        const pl = pkg?.pallet_id ? String(pkg.pallet_id).trim() : "";
        if (pl && isUuidString(pl)) patch.pallet_id = pl;
      }
    }
    const clean = omitUndefined(patch);
    const { data, error } = await supabaseServer.from("returns")
      .update(clean)
      .eq("id", rid).select(RETURN_SELECT).single();
    if (error) throw new Error(parseDuplicateError(error.message));
    const rec = data as unknown as ReturnRecord;

    if (rec.status === "ready_for_claim") {
      const storePlat = storePlatformFromEmbed(rec.stores);
      if (shouldAutoEnqueueAmazonClaimSubmission(rec.marketplace, rec.conditions ?? [], storePlat)) {
        const ev = rec.estimated_value;
        const n = Number(ev);
        const claimAmount = Number.isFinite(n) && n > 0 ? n : 100;
        const orderForClaim =
          (rec.order_id?.trim() || (await fetchPackageOrderId(rec.package_id ?? null)) || "").trim() || null;
        const sourcePayload = await buildClaimSubmissionSourcePayloadForReturn(
          rec.conditions ?? [],
          orderForClaim,
          rec.package_id ?? null,
          {
            expirationDate: rec.expiration_date ?? null,
            batchNumber: rec.batch_number ?? null,
            operatorNotes: rec.notes ?? null,
          },
          { selectedEvidenceUrls: null },
        );
        const { error: subErr } = await upsertClaimSubmissionForReadyReturn({
          organizationId: resolveOrganizationIdForWrite(rec.organization_id),
          returnId: rec.id,
          storeId: rec.store_id,
          claimAmount,
          sourcePayload,
          returnHint: { store_id: rec.store_id ?? null, package_id: rec.package_id ?? null },
        });
        if (subErr) throw new Error(`Claim queue update failed: ${subErr.message}`);
      }
    }

    void logReturnAudit({ organizationId: DEFAULT_ORG, returnId, action: "updated", actor: actor ?? DEFAULT_ACTOR });
    return { ok: true, data: rec };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to update return." };
  }
}

export async function deleteReturn(returnId: string, actor?: string): Promise<{ ok: boolean; error?: string }> {
  try {
    void logReturnAudit({ organizationId: DEFAULT_ORG, returnId, action: "deleted", actor: actor ?? DEFAULT_ACTOR });
    const { error } = await supabaseServer.from("returns").delete().eq("id", returnId);
    if (error) throw new Error(error.message);
    return { ok: true };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to delete return." };
  }
}

/** Returns in the claim workflow (evidence gathering or ready to file). */
export async function listClaimPipelineReturns(
  organizationId?: string,
): Promise<{ ok: boolean; data: ReturnRecord[]; error?: string }> {
  try {
    const { data, error } = await supabaseServer.from("returns")
      .select(RETURN_SELECT)
      .eq("organization_id", organizationId ?? DEFAULT_ORG)
      .in("status", ["ready_for_claim", "pending_evidence"])
      .is("deleted_at", null)
      .order("created_at", { ascending: false })
      .limit(200);
    if (error) throw new Error(error.message);
    return { ok: true, data: (data ?? []) as unknown as ReturnRecord[] };
  } catch (err) {
    return { ok: false, data: [], error: err instanceof Error ? err.message : "Failed to load claim pipeline items." };
  }
}

export async function listReturns(organizationId?: string): Promise<{ ok: boolean; data: ReturnRecord[]; error?: string }> {
  try {
    const { data, error } = await supabaseServer.from("returns").select(RETURN_SELECT)
      .eq("organization_id", organizationId ?? DEFAULT_ORG)
      .is("deleted_at", null)
      .order("created_at", { ascending: false }).limit(200);
    if (error) {
      console.error("[listReturns] Supabase error:", error.message, "| code:", error.code, "| details:", error.details);
      throw new Error(error.message);
    }
    return { ok: true, data: (data ?? []) as unknown as ReturnRecord[] };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Failed to load returns.";
    console.error("[listReturns] Caught error:", msg);
    return { ok: false, data: [], error: msg };
  }
}

export async function listReturnsByPallet(palletId: string): Promise<{ ok: boolean; data: ReturnRecord[]; error?: string }> {
  try {
    const { data, error } = await supabaseServer.from("returns").select(RETURN_SELECT)
      .eq("pallet_id", palletId).is("deleted_at", null).order("created_at", { ascending: false });
    if (error) throw new Error(error.message);
    return { ok: true, data: (data ?? []) as unknown as ReturnRecord[] };
  } catch (err) {
    return { ok: false, data: [], error: err instanceof Error ? err.message : "Failed to load pallet items." };
  }
}

export async function listAuditLog(organizationId?: string, limit = 50): Promise<{ ok: boolean; data: AuditLogRecord[]; error?: string }> {
  try {
    const { data, error } = await supabaseServer.from("return_audit_log").select("*")
      .eq("organization_id", organizationId ?? DEFAULT_ORG)
      .order("created_at", { ascending: false }).limit(limit);
    if (error) throw new Error(error.message);
    return { ok: true, data: (data ?? []) as AuditLogRecord[] };
  } catch (err) {
    return { ok: false, data: [], error: err instanceof Error ? err.message : "Failed to load audit log." };
  }
}

// ─── Dashboard high-level snapshot (UTC “today”) ───────────────────────────────

export type DashboardSnapshot = {
  returnsToday: number;
  palletCount: number;
  packageCount: number;
  /** `claim_submissions` rows awaiting marketplace filing (Agent / ops). */
  claimsReadyToSend: number;
  /** Sum of `returns.estimated_value` (null treated as 0) for ROI-style dashboard. */
  returnsEstimatedValueUsd: number;
};

export async function getDashboardSnapshot(
  organizationId?: string,
): Promise<{ ok: boolean; data?: DashboardSnapshot; error?: string }> {
  const org = organizationId ?? DEFAULT_ORG;
  const startUtc = new Date();
  startUtc.setUTCHours(0, 0, 0, 0);
  const iso = startUtc.toISOString();
  try {
    const [
      { count: returnsToday, error: e1 },
      { count: palletCount, error: e2 },
      { count: packageCount, error: e3 },
      { count: claimsReadyToSend, error: e4 },
      { data: estRows, error: e5 },
    ] = await Promise.all([
      supabaseServer.from("returns").select("id", { count: "exact", head: true }).eq("organization_id", org).gte("created_at", iso).is("deleted_at", null),
      supabaseServer.from("pallets").select("id", { count: "exact", head: true }).eq("organization_id", org).is("deleted_at", null),
      supabaseServer.from("packages").select("id", { count: "exact", head: true }).eq("organization_id", org).is("deleted_at", null),
      supabaseServer
        .from("claim_submissions")
        .select("id", { count: "exact", head: true })
        .eq("organization_id", org)
        .eq("status", "ready_to_send"),
      supabaseServer.from("returns").select("estimated_value").eq("organization_id", org).is("deleted_at", null).limit(10000),
    ]);
    if (e1) throw new Error(e1.message);
    if (e2) throw new Error(e2.message);
    if (e3) throw new Error(e3.message);
    if (e4) throw new Error(e4.message);
    if (e5) throw new Error(e5.message);

    let returnsEstimatedValueUsd = 0;
    for (const row of estRows ?? []) {
      const raw = (row as { estimated_value?: unknown }).estimated_value;
      const n = Number(raw);
      if (Number.isFinite(n)) returnsEstimatedValueUsd += n;
    }

    return {
      ok: true,
      data: {
        returnsToday: returnsToday ?? 0,
        palletCount: palletCount ?? 0,
        packageCount: packageCount ?? 0,
        claimsReadyToSend: claimsReadyToSend ?? 0,
        returnsEstimatedValueUsd: Math.round(returnsEstimatedValueUsd * 100) / 100,
      },
    };
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : "Failed to load dashboard snapshot.",
    };
  }
}

// ─── Dashboard analytics (serialized for recharts) ─────────────────────────────

export type ReturnsAnalyticsPayload = {
  totalReturns: number;
  totalPallets: number;
  avgProcessingHours: number;
  conditionSlices: { name: string; value: number }[];
  carrierBars: { name: string; count: number }[];
  /** Items processed grouped by operator (created_by). */
  operatorStats: { operator: string; count: number }[];
};

export async function getReturnsAnalyticsData(
  organizationId?: string,
): Promise<{ ok: boolean; data?: ReturnsAnalyticsPayload; error?: string }> {
  try {
    const org = organizationId ?? DEFAULT_ORG;
    const [{ data: retRows, error: e1 }, { data: pkgRows, error: e2 }, { data: pltRows, error: e3 }] = await Promise.all([
      supabaseServer.from("returns").select("id,conditions,created_at,updated_at,package_id,created_by").eq("organization_id", org).limit(500),
      supabaseServer.from("packages").select("id,carrier_name").eq("organization_id", org).limit(500),
      supabaseServer.from("pallets").select("id").eq("organization_id", org).limit(500),
    ]);
    if (e1) throw new Error(e1.message);
    if (e2) throw new Error(e2.message);
    if (e3) throw new Error(e3.message);

    const returns = (retRows ?? []) as { id: string; conditions: string[]; created_at: string; updated_at: string; package_id: string | null; created_by: string | null }[];
    const packages = (pkgRows ?? []) as { id: string; carrier_name: string | null }[];
    const pkgCarrier = new Map(packages.map((p) => [p.id, p.carrier_name ?? "Unknown"]));

    const conditionCounts = new Map<string, number>();
    for (const r of returns) {
      for (const c of r.conditions ?? []) {
        conditionCounts.set(c, (conditionCounts.get(c) ?? 0) + 1);
      }
    }
    const conditionSlices = [...conditionCounts.entries()]
      .map(([name, value]) => ({ name, value }))
      .filter((x) => x.value > 0)
      .sort((a, b) => b.value - a.value)
      .slice(0, 12);

    const carrierCounts = new Map<string, number>();
    for (const r of returns) {
      if (!r.package_id) continue;
      const car = pkgCarrier.get(r.package_id) ?? "Unknown";
      carrierCounts.set(car, (carrierCounts.get(car) ?? 0) + 1);
    }
    const carrierBars = [...carrierCounts.entries()]
      .map(([name, count]) => ({ name, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 8);

    let avgProcessingHours = 0;
    if (returns.length) {
      const hrs = returns.map(
        (r) => (new Date(r.updated_at).getTime() - new Date(r.created_at).getTime()) / 3600000,
      );
      avgProcessingHours = hrs.reduce((a, b) => a + b, 0) / hrs.length;
    }

    const operatorCounts = new Map<string, number>();
    for (const r of returns) {
      const op = r.created_by?.trim() || "Unknown";
      operatorCounts.set(op, (operatorCounts.get(op) ?? 0) + 1);
    }
    const operatorStats = [...operatorCounts.entries()]
      .map(([operator, count]) => ({ operator, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 10);

    return {
      ok: true,
      data: {
        totalReturns: returns.length,
        totalPallets: (pltRows ?? []).length,
        avgProcessingHours,
        conditionSlices,
        carrierBars,
        operatorStats,
      },
    };
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : "Failed to load analytics.",
    };
  }
}
