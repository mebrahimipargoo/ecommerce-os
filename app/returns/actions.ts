"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { isUuidString } from "../../lib/uuid";
import {
  CLAIM_SUBMISSION_RETURN_ID_COLUMN,
  CLAIM_SUBMISSIONS_TABLE,
} from "../claim-engine/claim-submissions-constants";
import { defectReasonsPayload } from "./claim-condition-labels";
import { RETURN_SELECT } from "./returns-constants";

const DEFAULT_ORG   = "00000000-0000-0000-0000-000000000001";
/** Legacy default label for audit logs (TEXT only — never write to UUID columns). */
const DEFAULT_ACTOR = "operator";

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
export async function getOrgSettings(orgId = DEFAULT_ORG): Promise<OrgSettings> {
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

// ─── Claim-eligible conditions ────────────────────────────────────────────────

const CLAIM_CONDITIONS = new Set([
  "empty_box", "missing_item", "damaged_customer", "damaged_carrier", "damaged_warehouse",
  "damaged_box", "scratched",
  "wrong_item_junk", "wrong_item_different", "missing_parts", "expired",
]);

/** True when the package has outer-box + return-label photos operators can inherit for claims. */
async function fetchPackageInheritedClaimPhotosReady(packageId: string | null | undefined): Promise<boolean> {
  if (!packageId) return false;
  const { data } = await supabaseServer
    .from("packages")
    .select("photo_opened_url, photo_closed_url, photo_return_label_url")
    .eq("id", packageId)
    .maybeSingle();
  if (!data) return false;
  const outer = !!(data.photo_opened_url || data.photo_closed_url);
  const label = !!data.photo_return_label_url;
  return outer && label;
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

function buildClaimSourcePayload(
  conditions: string[],
  orderId: string | null | undefined,
): Record<string, unknown> {
  const oid = orderId?.trim();
  const payload: Record<string, unknown> = {
    ...defectReasonsPayload(conditions),
    claim_type: conditions.find((c) => CLAIM_CONDITIONS.has(c)) ?? null,
  };
  if (oid) payload.amazon_order_id = oid;
  return payload;
}

/** Queues a `claim_submissions` row for the Python agent (`ready_to_send`). */
async function upsertClaimSubmissionForReadyReturn(opts: {
  organizationId: string;
  returnId: string;
  storeId: string | null | undefined;
  claimAmount: number;
  sourcePayload: Record<string, unknown>;
}): Promise<{ error: { message: string } | null }> {
  const { error } = await supabaseServer.from(CLAIM_SUBMISSIONS_TABLE).upsert(
    {
      organization_id: opts.organizationId,
      [CLAIM_SUBMISSION_RETURN_ID_COLUMN]: opts.returnId,
      store_id: opts.storeId ?? null,
      status: "ready_to_send",
      claim_amount: opts.claimAmount,
      report_url: null,
      source_payload: opts.sourcePayload,
      updated_at: new Date().toISOString(),
    },
    { onConflict: "return_id" },
  );
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
  pallet_number: string; manifest_photo_url: string | null;
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
};

export type PackageInsertPayload = {
  package_number: string; tracking_number?: string;
  carrier_name?: string; rma_number?: string; expected_item_count?: number;
  pallet_id?: string; store_id?: string; organization_id?: string; created_by?: string;
  manifest_url?: string;
  photo_url?: string;
  photo_closed_url?: string;
  photo_opened_url?: string;
  photo_return_label_url?: string;
};

export type PackageUpdatePayload = Partial<Pick<
  PackageRecord,
  "carrier_name" | "tracking_number" | "rma_number" | "expected_item_count" | "status" | "discrepancy_note" | "pallet_id" | "manifest_url" | "photo_url" | "photo_closed_url" | "photo_opened_url" | "photo_return_label_url"
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
  pallet_id?: string; package_id?: string;
  /** UUID of the connected store — added in 20260325_saas_stores_usage.sql */
  store_id?: string;
  /** External IDs for API / lifecycle / scoring (optional). */
  order_id?: string | null;
  customer_id?: string | null;
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
  conditions: string[]; status: string;
  notes: string | null; photo_evidence: Record<string, number> | null;
  expiration_date: string | null; batch_number: string | null;
  /** Claim evidence URLs — added in 20260325_claims_evidence_expiry.sql */
  photo_item_url?: string | null;
  photo_expiry_url?: string | null;
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
  | "photo_evidence" | "photo_item_url" | "photo_expiry_url"
  | "asin" | "fnsku" | "sku" | "store_id" | "marketplace"
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
const PALLET_SELECT = "id,organization_id,pallet_number,manifest_photo_url,store_id,status,notes,item_count,created_by,created_by_id,updated_by,updated_by_id,created_at,updated_at";
const PKG_SELECT    = "id,organization_id,package_number,tracking_number,carrier_name,rma_number,expected_item_count,actual_item_count,pallet_id,store_id,status,discrepancy_note,manifest_url,photo_closed_url,photo_opened_url,photo_return_label_url,created_by,created_by_id,updated_by,updated_by_id,created_at,updated_at";

/** Supabase relation embeds: live child counts + joined store name/platform. */
const PKG_LIST_SELECT = `${PKG_SELECT},stores(name,platform),returns(count)`;
const PALLET_LIST_SELECT = `${PALLET_SELECT},stores(name,platform),packages(count),returns(count)`;

function normalizePackageRow(row: Record<string, unknown>): PackageRecord {
  const ret = row.returns as { count: number }[] | undefined;
  const { returns: _r, ...rest } = row;
  const base = rest as PackageRecord;
  const c = ret?.[0]?.count;
  if (typeof c === "number") return { ...base, actual_item_count: c };
  return base;
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
    const orgId = payload.organization_id ?? DEFAULT_ORG;
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
    if (payload.store_id)      insertRow.store_id      = payload.store_id;
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
    const { error } = await supabaseServer.from("pallets")
      .update({ status, updated_by_id: resolveActorUserId(actor) }).eq("id", palletId);
    if (error) throw new Error(error.message);
    void logPalletAudit({ organizationId: DEFAULT_ORG, palletId, action: "status_changed", field: "status", newValue: status, actor: actor ?? DEFAULT_ACTOR });
    return { ok: true };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to update pallet." };
  }
}

export async function deletePallet(palletId: string, actor?: string): Promise<{ ok: boolean; error?: string }> {
  try {
    void logPalletAudit({ organizationId: DEFAULT_ORG, palletId, action: "deleted", actor: actor ?? DEFAULT_ACTOR });
    const { error } = await supabaseServer.from("pallets").delete().eq("id", palletId);
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
    const orgId = payload.organization_id ?? DEFAULT_ORG;
    const actor = payload.created_by      ?? DEFAULT_ACTOR;
    const { data, error } = await supabaseServer.from("packages")
      .insert({
        organization_id:     orgId,
        package_number:      payload.package_number.trim(),
        tracking_number:     payload.tracking_number?.trim() || null,
        carrier_name:        payload.carrier_name?.trim() || null,
        rma_number:          payload.rma_number?.trim() || null,
        expected_item_count: payload.expected_item_count ?? 0,
        pallet_id:           payload.pallet_id ?? null,
        ...(payload.store_id         ? { store_id:         payload.store_id         } : {}),
        manifest_url:        payload.manifest_url ?? null,
        ...(payload.photo_url        ? { photo_url:        payload.photo_url        } : {}),
        ...(payload.photo_closed_url        ? { photo_closed_url:        payload.photo_closed_url        } : {}),
        ...(payload.photo_opened_url        ? { photo_opened_url:        payload.photo_opened_url        } : {}),
        ...(payload.photo_return_label_url  ? { photo_return_label_url:  payload.photo_return_label_url  } : {}),
        status:              "open",
        created_by_id:       resolveActorUserId(payload.created_by),
      })
      .select(PKG_LIST_SELECT).single();
    if (error) throw new Error(parseDuplicateError(error.message));
    void logPackageAudit({ organizationId: orgId, packageId: (data as { id: string }).id, action: "created", actor });
    return { ok: true, data: normalizePackageRow(data as Record<string, unknown>) };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to create package." };
  }
}

function omitUndefined<T extends Record<string, unknown>>(obj: T): Record<string, unknown> {
  return Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== undefined));
}

export async function updatePackage(
  packageId: string,
  updates: PackageUpdatePayload,
  actor?: string,
): Promise<{ ok: boolean; data?: PackageRecord; error?: string }> {
  try {
    const payload = omitUndefined({
      ...updates,
      updated_by_id: resolveActorUserId(actor),
    } as Record<string, unknown>);
    delete payload.updated_by;
    delete payload.created_by;
    const { data, error } = await supabaseServer.from("packages")
      .update(payload)
      .eq("id", packageId).select(PKG_LIST_SELECT).single();
    if (error) throw new Error(parseDuplicateError(error.message));
    const row = normalizePackageRow(data as Record<string, unknown>);
    // Keep denormalized returns.pallet_id in sync when package moves between pallets
    if ("pallet_id" in payload) {
      const { error: syncErr } = await supabaseServer.from("returns")
        .update({ pallet_id: row.pallet_id })
        .eq("package_id", packageId);
      if (syncErr) console.error("[updatePackage] sync returns.pallet_id:", syncErr.message);
    }
    void logPackageAudit({ organizationId: DEFAULT_ORG, packageId, action: "updated", actor: actor ?? DEFAULT_ACTOR });
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
    const { data, error } = await supabaseServer.from("returns").select(RETURN_SELECT)
      .eq("package_id", packageId).is("deleted_at", null).order("created_at", { ascending: false });
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
    const actor = opts?.actor ?? DEFAULT_ACTOR;
    const { data: pkg, error: fetchErr } = await supabaseServer.from("packages")
      .select("expected_item_count,actual_item_count,organization_id")
      .eq("id", packageId).single();
    if (fetchErr) throw new Error(fetchErr.message);
    const hasDiscrepancy = (pkg.expected_item_count > 0 && pkg.expected_item_count !== pkg.actual_item_count) || !!opts?.discrepancyNote;
    const newStatus: PackageStatus = hasDiscrepancy ? "suspicious" : "closed";
    const { error } = await supabaseServer.from("packages")
      .update({ status: newStatus, discrepancy_note: opts?.discrepancyNote ?? null, updated_by_id: resolveActorUserId(actor) })
      .eq("id", packageId);
    if (error) throw new Error(error.message);
    void logPackageAudit({ organizationId: pkg.organization_id ?? DEFAULT_ORG, packageId, action: "status_changed", field: "status", newValue: newStatus, actor });
    return { ok: true, status: newStatus };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to close package." };
  }
}

export async function deletePackage(packageId: string, actor?: string): Promise<{ ok: boolean; error?: string }> {
  try {
    void logPackageAudit({ organizationId: DEFAULT_ORG, packageId, action: "deleted", actor: actor ?? DEFAULT_ACTOR });
    const { error } = await supabaseServer.from("packages").delete().eq("id", packageId);
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
    const orgId = payload.organization_id ?? DEFAULT_ORG;
    const packageInherited = await fetchPackageInheritedClaimPhotosReady(payload.package_id ?? null);
    const hasItemUrls = !!(payload.photo_item_url?.trim() || payload.photo_expiry_url?.trim());
    const status = deriveStatus(payload.conditions, payload.photo_evidence ?? null, {
      packageHasInheritedClaimPhotos: packageInherited,
      hasItemEvidenceUrls: hasItemUrls,
    });

    // Inherit pallet_id from package when not explicitly provided
    let effectivePalletId = payload.pallet_id ?? null;
    if (payload.package_id && !effectivePalletId) {
      const { data: pkg } = await supabaseServer.from("packages")
        .select("pallet_id").eq("id", payload.package_id).single();
      if (pkg?.pallet_id) effectivePalletId = pkg.pallet_id;
    }

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
      package_id:      payload.package_id ?? null,
      status,
      created_by_id:   resolveActorUserId(payload.created_by),
    };
    // Post-migration columns — only written once their migrations are applied
    if (payload.store_id)         insertRow.store_id         = payload.store_id;
    if (payload.asin)             insertRow.asin             = payload.asin.trim();
    if (payload.fnsku)            insertRow.fnsku            = payload.fnsku.trim();
    if (payload.sku)              insertRow.sku              = payload.sku.trim();
    if (payload.order_id)        insertRow.order_id        = payload.order_id.trim();
    if (payload.customer_id)    insertRow.customer_id    = payload.customer_id.trim();
    if (payload.photo_item_url)   insertRow.photo_item_url   = payload.photo_item_url;
    if (payload.photo_expiry_url) insertRow.photo_expiry_url = payload.photo_expiry_url;

    const { data, error } = await supabaseServer.from("returns")
      .insert(insertRow).select(RETURN_SELECT).single();

    if (error) throw new Error(parseDuplicateError(error.message));
    const rec = data as unknown as ReturnRecord;

    if (status === "ready_for_claim") {
      const ev = rec.estimated_value;
      const n = Number(ev);
      const claimAmount = Number.isFinite(n) && n > 0 ? n : 100;
      const sourcePayload = buildClaimSourcePayload(payload.conditions, payload.order_id ?? null);

      const { error: subErr } = await upsertClaimSubmissionForReadyReturn({
        organizationId: rec.organization_id ?? orgId,
        returnId: rec.id,
        storeId: rec.store_id ?? payload.store_id ?? null,
        claimAmount,
        sourcePayload,
      });
      if (subErr) {
        await supabaseServer.from("returns").delete().eq("id", rec.id);
        throw new Error(
          `Return could not be queued for claims: ${subErr.message}. Check claim_submissions columns and RLS.`,
        );
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
    const { data: existing, error: loadErr } = await supabaseServer
      .from("returns")
      .select(RETURN_SELECT)
      .eq("id", returnId)
      .single();
    if (loadErr || !existing) throw new Error(loadErr?.message ?? "Return not found.");

    const patch: Record<string, unknown> = { ...updates, updated_by_id: resolveActorUserId(actor) };
    delete patch.updated_by;
    delete patch.created_by;
    // Remove post-migration columns from patch — they may not be in the DB yet
    delete patch.inherited_tracking_number;
    delete patch.inherited_carrier;
    delete patch.customer_id;
    delete (patch as Record<string, unknown>).product_identifier;

    const ex = existing as unknown as ReturnRecord;
    const nextConditions = (updates.conditions !== undefined ? updates.conditions : ex.conditions) ?? [];
    const nextPhotoEvidence = updates.photo_evidence !== undefined ? updates.photo_evidence : ex.photo_evidence;
    const nextPackageId =
      updates.package_id !== undefined ? updates.package_id : ex.package_id;
    const nextPhotoItem =
      updates.photo_item_url !== undefined ? updates.photo_item_url : ex.photo_item_url;
    const nextPhotoExpiry =
      updates.photo_expiry_url !== undefined ? updates.photo_expiry_url : ex.photo_expiry_url;
    const packageInherited = await fetchPackageInheritedClaimPhotosReady(nextPackageId);
    const hasItemUrls = !!(String(nextPhotoItem ?? "").trim() || String(nextPhotoExpiry ?? "").trim());
    patch.status = deriveStatus(nextConditions, nextPhotoEvidence ?? null, {
      packageHasInheritedClaimPhotos: packageInherited,
      hasItemEvidenceUrls: hasItemUrls,
    });
    // Inherit pallet_id from package when package_id is set to a real package
    if ("package_id" in updates && updates.package_id) {
      const { data: pkg } = await supabaseServer.from("packages")
        .select("pallet_id").eq("id", updates.package_id).single();
      if (pkg?.pallet_id && updates.pallet_id === undefined) patch.pallet_id = pkg.pallet_id;
    }
    // Clearing package → item is orphaned; clear pallet link for consistency
    if ("package_id" in updates && (updates.package_id === null || updates.package_id === "")) {
      patch.package_id = null;
      patch.pallet_id = null;
    }
    const clean = omitUndefined(patch);
    const { data, error } = await supabaseServer.from("returns")
      .update(clean)
      .eq("id", returnId).select(RETURN_SELECT).single();
    if (error) throw new Error(parseDuplicateError(error.message));
    const rec = data as unknown as ReturnRecord;

    if (rec.status === "ready_for_claim") {
      const ev = rec.estimated_value;
      const n = Number(ev);
      const claimAmount = Number.isFinite(n) && n > 0 ? n : 100;
      const sourcePayload = buildClaimSourcePayload(rec.conditions ?? [], rec.order_id ?? null);
      const { error: subErr } = await upsertClaimSubmissionForReadyReturn({
        organizationId: rec.organization_id ?? DEFAULT_ORG,
        returnId: rec.id,
        storeId: rec.store_id,
        claimAmount,
        sourcePayload,
      });
      if (subErr) throw new Error(`Claim queue update failed: ${subErr.message}`);
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
