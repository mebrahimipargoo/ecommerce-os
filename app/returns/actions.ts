"use server";

import { supabaseServer } from "../../lib/supabase-server";
import { resolveOrganizationId } from "../../lib/organization";
import {
  assertRowOrgAccess,
  resolveTenantListScope,
  resolveWriteOrganizationId,
  type TenantQueryOpts,
} from "../../lib/server-tenant";

export type { TenantQueryOpts } from "../../lib/server-tenant";
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
import {
  PACKAGE_LIST_SELECT,
  PACKAGE_MUTATION_SELECT,
  PALLET_LIST_SELECT,
  PALLET_MUTATION_SELECT,
  RETURN_LIST_SELECT,
  RETURN_SELECT,
} from "./returns-constants";
import {
  hasReturnPhotoEvidenceCounts,
  hasReturnPhotoEvidenceUrlSlots,
  type ReturnPhotoEvidenceRow,
} from "../../lib/return-photo-evidence";
import type {
  AuditLogRecord,
  DashboardSnapshot,
  ExpectedItem,
  OrgSettings,
  PackageInsertPayload,
  PackageRecord,
  PackageStatus,
  PackageUpdatePayload,
  PalletInsertPayload,
  PalletRecord,
  PalletStatus,
  PalletUpdatePayload,
  ReturnInsertPayload,
  ReturnRecord,
  ReturnsAnalyticsPayload,
  ReturnUpdatePayload,
} from "./returns-action-types";

const DEFAULT_ORG = resolveOrganizationId();

/** Ensures new nullable columns never surface as `undefined` to the client. */
function normalizeReturnRecordFromRow(raw: unknown): ReturnRecord {
  const base = raw as Record<string, unknown>;
  const r = raw as ReturnRecord;
  return {
    ...r,
    marketplace: String((base.marketplace as string | undefined) ?? ""),
    rma_number: (r.rma_number as string | null | undefined) ?? null,
  };
}

/** Collapse duplicate rows (same `id`) from PostgREST embed fan-out; keep newest-first by `created_at`. */
function dedupeReturnsById(rows: unknown[] | null | undefined): ReturnRecord[] {
  const map = new Map<string, ReturnRecord>();
  for (const raw of rows ?? []) {
    const r = normalizeReturnRecordFromRow(raw);
    if (r?.id && typeof r.id === "string" && !map.has(r.id)) {
      map.set(r.id, r);
    }
  }
  const out = [...map.values()];
  out.sort((a, b) => (b.created_at ?? "").localeCompare(a.created_at ?? ""));
  return out;
}

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
 * `auth.users.id`. Keeps `created_by` / `updated_by` valid UUIDs.
 */
const MVP_ACTOR_USER_ID = "00000000-0000-0000-0000-0000000000fe";

/** Maps UI `actor` (name or UUID) to a UUID for `created_by` / `updated_by`. */
function resolveActorUserId(actor?: string | null): string {
  const raw = (actor ?? DEFAULT_ACTOR).trim();
  if (isUuidString(raw)) return raw;
  return MVP_ACTOR_USER_ID;
}

// ─── Organisation Settings ────────────────────────────────────────────────────

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
      .maybeSingle();
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

function deriveStatus(
  conditions: string[],
  photoEvidence: ReturnPhotoEvidenceRow | undefined,
  _opts?: {
    /** @deprecated Claim readiness uses `returns.photo_evidence` only — package inheritance removed. */
    packageHasInheritedClaimPhotos?: boolean;
  },
): string {
  const needsClaim = conditions.some((c) => CLAIM_CONDITIONS.has(c));
  if (!needsClaim) return "received";
  const pe = photoEvidence ?? null;
  const fromItem =
    hasReturnPhotoEvidenceCounts(pe) ||
    hasReturnPhotoEvidenceUrlSlots(pe);
  if (fromItem) return "ready_for_claim";
  return "pending_evidence";
}

/** Builds `source_payload` for `claim_submissions` including package + pallet evidence and optional extra URLs (e.g. condition-category uploads). Exported for batch sync. */
export async function buildClaimSubmissionSourcePayloadForReturn(
  conditions: string[],
  amazonOrderId: string | null | undefined,
  _packageId: string | null | undefined,
  claimMeta?: {
    expirationDate?: string | null;
    batchNumber?: string | null;
    operatorNotes?: string | null;
  },
  extras?: { selectedEvidenceUrls?: string[] | null },
): Promise<Record<string, unknown>> {
  const base = buildClaimSourcePayload(conditions, amazonOrderId, claimMeta);
  const urls = extras?.selectedEvidenceUrls?.filter((u) => typeof u === "string" && u.trim().length > 0) ?? [];
  if (urls.length > 0) return { ...base, selected_claim_evidence_urls: urls };
  return base;
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

function sumManifestLineQty(lines: ExpectedItem[] | null | undefined): number {
  if (!lines?.length) return 0;
  return lines.reduce((a, it) => a + (Number(it.expected_qty) > 0 ? Number(it.expected_qty) : 1), 0);
}

// ─── SELECT strings ───────────────────────────────────────────────────────────

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
  const base = row as PackageRecord;
  const md = parseManifestData(row.manifest_data);
  const next: PackageRecord = {
    ...base,
    rma_number: (base.rma_number as string | null | undefined) ?? null,
    carrier_name: (base.carrier_name as string | null | undefined) ?? null,
    tracking_number: (base.tracking_number as string | null | undefined) ?? null,
  };
  return md !== undefined ? { ...next, manifest_data: md } : next;
}

function normalizePalletRow(row: Record<string, unknown>): PalletRecord {
  const base = row as PalletRecord;
  return {
    ...base,
    notes: (base.notes as string | null | undefined) ?? null,
    tracking_number: (base.tracking_number as string | null | undefined) ?? null,
  };
}

// ─── Pallet Actions ───────────────────────────────────────────────────────────

export async function createPallet(
  payload: PalletInsertPayload,
): Promise<{ ok: boolean; data?: PalletRecord; error?: string }> {
  try {
    const orgId = await resolveWriteOrganizationId(
      payload.actor_profile_id,
      payload.organization_id,
    );
    const actor = payload.created_by      ?? DEFAULT_ACTOR;
    const insertRow: Record<string, unknown> = {
      organization_id: orgId,
      pallet_number: payload.pallet_number.trim(),
      notes: payload.notes?.trim() || null,
      status: "open",
      created_by: uuidFkOrNull(payload.actor_profile_id ?? null, "created_by") ?? resolveActorUserId(payload.created_by),
    };
    if (payload.photo_url !== undefined) insertRow.photo_url = String(payload.photo_url ?? "").trim() || null;
    if (payload.bol_photo_url !== undefined) insertRow.bol_photo_url = String(payload.bol_photo_url ?? "").trim() || null;
    if (payload.manifest_photo_url !== undefined) {
      insertRow.manifest_photo_url = String(payload.manifest_photo_url ?? "").trim() || null;
    }
    const sid = uuidFkOrNull(payload.store_id ?? null, "store_id");
    if (sid) insertRow.store_id = sid;
    const { data, error } = await supabaseServer.from("pallets")
      .insert(insertRow)
      .select(PALLET_MUTATION_SELECT).single();
    if (error) throw new Error(error.message);
    const row = normalizePalletRow(data as unknown as Record<string, unknown>);
    void logPalletAudit({ organizationId: orgId, palletId: row.id, action: "created", actor });
    return { ok: true, data: row };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to create pallet." };
  }
}

export async function listPallets(
  tenant?: TenantQueryOpts,
): Promise<{ ok: boolean; data: PalletRecord[]; error?: string }> {
  try {
    const scope = await resolveTenantListScope(tenant);
    let q = supabaseServer.from("pallets").select(PALLET_LIST_SELECT)
      .is("deleted_at", null)
      .order("created_at", { ascending: false }).limit(200);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { data, error } = await q;
    if (error) {
      console.error("[listPallets] Supabase error:", error.message, "| code:", error.code);
      throw new Error(error.message);
    }
    const rows = (data ?? []) as unknown as Record<string, unknown>[];
    return { ok: true, data: rows.map(normalizePalletRow) };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Failed to load pallets.";
    console.error("[listPallets] Caught error:", msg);
    return { ok: false, data: [], error: msg };
  }
}

export async function listOpenPallets(
  tenant?: TenantQueryOpts,
): Promise<{ ok: boolean; data: PalletRecord[]; error?: string }> {
  try {
    const scope = await resolveTenantListScope(tenant);
    let q = supabaseServer.from("pallets").select(PALLET_LIST_SELECT)
      .eq("status", "open")
      .is("deleted_at", null)
      .order("created_at", { ascending: false }).limit(50);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { data, error } = await q;
    if (error) throw new Error(error.message);
    const rows = (data ?? []) as unknown as Record<string, unknown>[];
    return { ok: true, data: rows.map(normalizePalletRow) };
  } catch (err) {
    return { ok: false, data: [], error: err instanceof Error ? err.message : "Failed to load open pallets." };
  }
}

export async function updatePalletStatus(
  palletId: string,
  status: PalletStatus,
  actor?: string,
  actorProfileId?: string | null,
): Promise<{ ok: boolean; error?: string }> {
  try {
    const id = uuidOrNull(palletId);
    if (!id) throw new Error("Invalid pallet id.");
    const scope = await resolveTenantListScope({ actorProfileId });
    let q = supabaseServer.from("pallets")
      .update({ status, updated_by: uuidFkOrNull(actorProfileId ?? null, "updated_by") ?? resolveActorUserId(actor) }).eq("id", id);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { error } = await q;
    if (error) throw new Error(error.message);
    void logPalletAudit({
      organizationId: scope.mode === "single" ? scope.organizationId : DEFAULT_ORG,
      palletId: id,
      action: "status_changed",
      field: "status",
      newValue: status,
      actor: actor ?? DEFAULT_ACTOR,
    });
    return { ok: true };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to update pallet." };
  }
}

export async function updatePallet(
  palletId: string,
  updates: PalletUpdatePayload,
  actor?: string,
  organizationId?: string | null,
  actorProfileId?: string | null,
): Promise<{ ok: boolean; data?: PalletRecord; error?: string }> {
  try {
    const id = uuidOrNull(palletId);
    if (!id) throw new Error("Invalid pallet id.");
    const org = await resolveWriteOrganizationId(actorProfileId, organizationId);
    const row: Record<string, unknown> = {
      ...omitUndefined(updates as Record<string, unknown>),
      updated_by: uuidFkOrNull(actorProfileId ?? null, "updated_by") ?? resolveActorUserId(actor),
    };
    delete row.created_by;
    delete row.photo_evidence;
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
      .select(PALLET_MUTATION_SELECT)
      .single();
    if (error) throw new Error(error.message);
    const rec = normalizePalletRow(data as unknown as Record<string, unknown>);
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

export async function deletePallet(
  palletId: string,
  actor?: string,
  actorProfileId?: string | null,
): Promise<{ ok: boolean; error?: string }> {
  try {
    const id = uuidOrNull(palletId);
    if (!id) throw new Error("Invalid pallet id.");
    const scope = await resolveTenantListScope({ actorProfileId });
    void logPalletAudit({
      organizationId: scope.mode === "single" ? scope.organizationId : DEFAULT_ORG,
      palletId: id,
      action: "deleted",
      actor: actor ?? DEFAULT_ACTOR,
    });
    let q = supabaseServer.from("pallets").delete().eq("id", id);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { error } = await q;
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
    const orgId = await resolveWriteOrganizationId(
      payload.actor_profile_id,
      payload.organization_id,
    );
    const actor = payload.created_by      ?? DEFAULT_ACTOR;
    const expected_item_count = coerceNonNegativeInt(payload.expected_item_count, 0);
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
      created_by: uuidFkOrNull(payload.actor_profile_id ?? null, "created_by") ?? resolveActorUserId(payload.created_by),
    };
    if (storeIdFk) insertRow.store_id = storeIdFk;
    if (payload.photo_evidence != null) insertRow.photo_evidence = payload.photo_evidence;
    if (payload.photo_url !== undefined) insertRow.photo_url = String(payload.photo_url ?? "").trim() || null;
    if (payload.photo_return_label_url !== undefined) {
      insertRow.photo_return_label_url = String(payload.photo_return_label_url ?? "").trim() || null;
    }
    if (payload.photo_opened_url !== undefined) {
      insertRow.photo_opened_url = String(payload.photo_opened_url ?? "").trim() || null;
    }
    if (payload.photo_closed_url !== undefined) {
      insertRow.photo_closed_url = String(payload.photo_closed_url ?? "").trim() || null;
    }
    if (payload.manifest_photo_url !== undefined) {
      insertRow.manifest_photo_url = String(payload.manifest_photo_url ?? "").trim() || null;
    }
    if (payload.order_id?.trim()) insertRow.order_id = payload.order_id.trim();

    const { data, error } = await supabaseServer.from("packages")
      .insert(insertRow)
      .select(PACKAGE_MUTATION_SELECT).single();
    if (error) throw new Error(parseDuplicateError(error.message));
    void logPackageAudit({ organizationId: orgId, packageId: (data as unknown as { id: string }).id, action: "created", actor });
    return { ok: true, data: normalizePackageRow(data as unknown as Record<string, unknown>) };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to create package." };
  }
}

export async function updatePackage(
  packageId: string,
  updates: PackageUpdatePayload,
  actor?: string,
  actorProfileId?: string | null,
): Promise<{ ok: boolean; data?: PackageRecord; error?: string }> {
  try {
    const pkgId = uuidOrNull(packageId);
    if (!pkgId) throw new Error("Invalid package id.");
    const scope = await resolveTenantListScope({ actorProfileId });
    const safeUpdates = { ...(updates as Record<string, unknown>) };
    delete safeUpdates.updated_by;
    const payload = omitUndefined({
      ...safeUpdates,
      updated_by: uuidFkOrNull(actorProfileId ?? null, "updated_by") ?? resolveActorUserId(actor),
    } as Record<string, unknown>);
    delete payload.created_by;
    if ("pallet_id" in payload) payload.pallet_id = uuidOrNull(payload.pallet_id as string);
    if ("store_id" in payload) {
      const s = uuidOrNull(payload.store_id as string);
      payload.store_id = s;
    }
    delete payload.manifest_data;
    delete payload.expected_items;
    if ("expected_item_count" in payload) {
      payload.expected_item_count = coerceNonNegativeInt(payload.expected_item_count, 0);
    }
    let q = supabaseServer.from("packages")
      .update(payload)
      .eq("id", pkgId);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { data, error } = await q.select(PACKAGE_MUTATION_SELECT).single();
    if (error) throw new Error(parseDuplicateError(error.message));
    const row = normalizePackageRow(data as unknown as Record<string, unknown>);
    // Keep denormalized returns.pallet_id in sync when package moves between pallets
    if ("pallet_id" in payload) {
      let syncQ = supabaseServer.from("returns")
        .update({ pallet_id: row.pallet_id })
        .eq("package_id", pkgId);
      if (scope.mode === "single") syncQ = syncQ.eq("organization_id", scope.organizationId);
      const { error: syncErr } = await syncQ;
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

export async function listPackages(
  tenant?: TenantQueryOpts,
): Promise<{ ok: boolean; data: PackageRecord[]; error?: string }> {
  try {
    const scope = await resolveTenantListScope(tenant);
    let q = supabaseServer.from("packages").select(PACKAGE_LIST_SELECT)
      .is("deleted_at", null)
      .order("created_at", { ascending: false }).limit(200);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { data, error } = await q;
    if (error) {
      console.error("[listPackages] Supabase error:", error.message, "| code:", error.code);
      throw new Error(error.message);
    }
    const rows = (data ?? []) as unknown as Record<string, unknown>[];
    return { ok: true, data: rows.map(normalizePackageRow) };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Failed to load packages.";
    console.error("[listPackages] Caught error:", msg);
    return { ok: false, data: [], error: msg };
  }
}

export async function listOpenPackages(
  tenant?: TenantQueryOpts,
): Promise<{ ok: boolean; data: PackageRecord[]; error?: string }> {
  try {
    const scope = await resolveTenantListScope(tenant);
    let q = supabaseServer.from("packages").select(PACKAGE_LIST_SELECT)
      .eq("status", "open")
      .is("deleted_at", null)
      .order("created_at", { ascending: false }).limit(50);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { data, error } = await q;
    if (error) throw new Error(error.message);
    const rows = (data ?? []) as unknown as Record<string, unknown>[];
    return { ok: true, data: rows.map(normalizePackageRow) };
  } catch (err) {
    return { ok: false, data: [], error: err instanceof Error ? err.message : "Failed to load open packages." };
  }
}

export async function listReturnsByPackage(
  packageId: string,
  tenant?: TenantQueryOpts,
): Promise<{ ok: boolean; data: ReturnRecord[]; error?: string }> {
  try {
    const id = uuidOrNull(packageId);
    if (!id) return { ok: true, data: [] };
    const scope = await resolveTenantListScope(tenant);
    let q = supabaseServer.from("returns").select(RETURN_LIST_SELECT)
      .eq("package_id", id).is("deleted_at", null).order("created_at", { ascending: false });
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { data, error } = await q;
    if (error) throw new Error(error.message);
    return { ok: true, data: dedupeReturnsById(data ?? []) };
  } catch (err) {
    return { ok: false, data: [], error: err instanceof Error ? err.message : "Failed to load package items." };
  }
}

export async function closePackage(
  packageId: string,
  opts?: { discrepancyNote?: string; actor?: string; actorProfileId?: string | null },
): Promise<{ ok: boolean; status?: PackageStatus; error?: string }> {
  try {
    const id = uuidOrNull(packageId);
    if (!id) throw new Error("Invalid package id.");
    const actor = opts?.actor ?? DEFAULT_ACTOR;
    const scope = await resolveTenantListScope({ actorProfileId: opts?.actorProfileId });
    const { data: pkg, error: fetchErr } = await supabaseServer.from("packages")
      .select("expected_item_count,actual_item_count,organization_id")
      .eq("id", id).single();
    if (fetchErr) throw new Error(fetchErr.message);
    if (scope.mode === "single" && pkg.organization_id !== scope.organizationId) {
      throw new Error("Forbidden: package belongs to another organization.");
    }
    const hasDiscrepancy = (pkg.expected_item_count > 0 && pkg.expected_item_count !== pkg.actual_item_count) || !!opts?.discrepancyNote;
    const newStatus: PackageStatus = hasDiscrepancy ? "suspicious" : "closed";
    let q = supabaseServer.from("packages")
      .update({ status: newStatus, discrepancy_note: opts?.discrepancyNote ?? null, updated_by: resolveActorUserId(actor) })
      .eq("id", id);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { error } = await q;
    if (error) throw new Error(error.message);
    void logPackageAudit({ organizationId: pkg.organization_id ?? DEFAULT_ORG, packageId: id, action: "status_changed", field: "status", newValue: newStatus, actor });
    return { ok: true, status: newStatus };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to close package." };
  }
}

export async function deletePackage(
  packageId: string,
  actor?: string,
  actorProfileId?: string | null,
): Promise<{ ok: boolean; error?: string }> {
  try {
    const id = uuidOrNull(packageId);
    if (!id) throw new Error("Invalid package id.");
    const scope = await resolveTenantListScope({ actorProfileId });
    void logPackageAudit({
      organizationId: scope.mode === "single" ? scope.organizationId : DEFAULT_ORG,
      packageId: id,
      action: "deleted",
      actor: actor ?? DEFAULT_ACTOR,
    });
    let q = supabaseServer.from("packages").delete().eq("id", id);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { error } = await q;
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
    const orgId = await resolveWriteOrganizationId(
      payload.actor_profile_id,
      payload.organization_id,
    );
    const packageIdFk = uuidFkOrNull(payload.package_id ?? null, "package_id");
    /** Status / claims use `returns.photo_evidence` only — do not read packages.photo_evidence here. */
    const status = deriveStatus(payload.conditions, payload.photo_evidence ?? null, {});

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
        `Invalid store_id: "${rawStore}" is not a valid UUID. Pick a Store from the dropdown.`,
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
      rma_number:      payload.rma_number?.trim() || null,
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
      created_by: uuidFkOrNull(payload.actor_profile_id ?? null, "created_by") ?? resolveActorUserId(payload.created_by),
      store_id:        resolvedStoreId,
    };
    // Post-migration columns — only written once their migrations are applied
    if (payload.asin)             insertRow.asin             = payload.asin.trim();
    if (payload.fnsku)            insertRow.fnsku            = payload.fnsku.trim();
    if (payload.sku)              insertRow.sku              = payload.sku.trim();
    if (effectiveAmazonOrderId) insertRow.order_id = String(effectiveAmazonOrderId);

    const { data, error } = await supabaseServer.from("returns")
      .insert(insertRow).select(RETURN_SELECT).single();

    if (error) throw new Error(parseDuplicateError(error.message));
    const rec = normalizeReturnRecordFromRow(data);

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
          organizationId: orgId,
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
  actorProfileId?: string | null,
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
    const ex = existing as unknown as ReturnRecord;
    await assertRowOrgAccess(actorProfileId, ex.organization_id);

    const safeUpdates = { ...(updates as Record<string, unknown>) };
    delete safeUpdates.updated_by;
    const patch: Record<string, unknown> = { ...safeUpdates };
    delete patch.created_by;
    // Remove post-migration columns from patch — they may not be in the DB yet
    delete patch.inherited_tracking_number;
    delete patch.inherited_carrier;
    delete patch.customer_id;

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
    if ("rma_number" in updates) {
      const v = updates.rma_number;
      patch.rma_number = v === null || v === undefined ? null : String(v).trim() || null;
    }

    const nextPackageId =
      updates.package_id !== undefined
        ? uuidOrNull(
            updates.package_id === null || updates.package_id === ""
              ? null
              : String(updates.package_id),
          )
        : uuidOrNull(ex.package_id);
    patch.status = deriveStatus(nextConditions, nextPhotoEvidence ?? null, {});
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
    const clean = omitUndefined({
      ...patch,
      updated_by: uuidFkOrNull(actorProfileId ?? null, "updated_by") ?? resolveActorUserId(actor),
    });
    const scope = await resolveTenantListScope({ actorProfileId });
    let uq = supabaseServer.from("returns")
      .update(clean)
      .eq("id", rid);
    if (scope.mode === "single") uq = uq.eq("organization_id", scope.organizationId);
    const { data, error } = await uq.select(RETURN_SELECT).single();
    if (error) throw new Error(parseDuplicateError(error.message));
    const rec = normalizeReturnRecordFromRow(data);

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
          organizationId: rec.organization_id,
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

export async function deleteReturn(
  returnId: string,
  actor?: string,
  actorProfileId?: string | null,
): Promise<{ ok: boolean; error?: string }> {
  try {
    const scope = await resolveTenantListScope({ actorProfileId });
    void logReturnAudit({
      organizationId: scope.mode === "single" ? scope.organizationId : DEFAULT_ORG,
      returnId,
      action: "deleted",
      actor: actor ?? DEFAULT_ACTOR,
    });
    let q = supabaseServer.from("returns").delete().eq("id", returnId);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { error } = await q;
    if (error) throw new Error(error.message);
    return { ok: true };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : "Failed to delete return." };
  }
}

/** Deletes many returns in one round-trip; only returns ok after Supabase confirms success. */
export async function bulkDeleteReturns(
  returnIds: string[],
  actor?: string,
  actorProfileId?: string | null,
): Promise<{ ok: boolean; error?: string }> {
  try {
    const validIds = [...new Set(returnIds.map((id) => uuidOrNull(id)).filter((id): id is string => !!id))];
    if (validIds.length === 0) {
      return { ok: false, error: "No valid return ids to delete." };
    }
    const a = actor ?? DEFAULT_ACTOR;
    const scope = await resolveTenantListScope({ actorProfileId });
    for (const id of validIds) {
      void logReturnAudit({
        organizationId: scope.mode === "single" ? scope.organizationId : DEFAULT_ORG,
        returnId: id,
        action: "deleted",
        actor: a,
      });
    }
    let q = supabaseServer.from("returns").delete().in("id", validIds);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { error } = await q;
    if (error) {
      console.error("[bulkDeleteReturns] Supabase error:", error.message, "| code:", error.code);
      return { ok: false, error: error.message };
    }
    return { ok: true };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Failed to delete returns.";
    console.error("[bulkDeleteReturns] Caught error:", msg);
    return { ok: false, error: msg };
  }
}

/** Exact row count for returns (non-deleted) — use with `listReturns()` to detect truncation from `.limit()`. */
export async function countReturns(
  tenant?: TenantQueryOpts,
): Promise<{ ok: boolean; count: number; error?: string }> {
  try {
    const scope = await resolveTenantListScope(tenant);
    let q = supabaseServer
      .from("returns")
      .select("id", { count: "exact", head: true })
      .is("deleted_at", null);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { count, error } = await q;
    if (error) {
      console.error("[countReturns] Supabase error:", error.message, "| code:", error.code);
      return { ok: false, count: 0, error: error.message };
    }
    return { ok: true, count: count ?? 0 };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Failed to count returns.";
    console.error("[countReturns] Caught error:", msg);
    return { ok: false, count: 0, error: msg };
  }
}

/** Returns in the claim workflow (evidence gathering or ready to file). */
export async function listClaimPipelineReturns(
  tenant?: TenantQueryOpts,
): Promise<{ ok: boolean; data: ReturnRecord[]; error?: string }> {
  try {
    const scope = await resolveTenantListScope(tenant);
    let q = supabaseServer.from("returns")
      .select(RETURN_LIST_SELECT)
      .in("status", ["ready_for_claim", "pending_evidence"])
      .is("deleted_at", null)
      .order("created_at", { ascending: false })
      .limit(200);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { data, error } = await q;
    if (error) throw new Error(error.message);
    return { ok: true, data: dedupeReturnsById(data ?? []) };
  } catch (err) {
    return { ok: false, data: [], error: err instanceof Error ? err.message : "Failed to load claim pipeline items." };
  }
}

export async function listReturns(
  tenant?: TenantQueryOpts,
): Promise<{ ok: boolean; data: ReturnRecord[]; error?: string }> {
  try {
    const scope = await resolveTenantListScope(tenant);
    let q = supabaseServer.from("returns").select(RETURN_LIST_SELECT)
      .is("deleted_at", null)
      .order("created_at", { ascending: false }).limit(200);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { data, error } = await q;
    if (error) {
      console.error("[listReturns] Supabase error:", error.message, "| code:", error.code, "| details:", error.details);
      throw new Error(error.message);
    }
    return { ok: true, data: dedupeReturnsById(data ?? []) };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Failed to load returns.";
    console.error("[listReturns] Caught error:", msg);
    return { ok: false, data: [], error: msg };
  }
}

export async function listReturnsByPallet(
  palletId: string,
  tenant?: TenantQueryOpts,
): Promise<{ ok: boolean; data: ReturnRecord[]; error?: string }> {
  try {
    const scope = await resolveTenantListScope(tenant);
    let q = supabaseServer.from("returns").select(RETURN_LIST_SELECT)
      .eq("pallet_id", palletId).is("deleted_at", null).order("created_at", { ascending: false });
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { data, error } = await q;
    if (error) throw new Error(error.message);
    return { ok: true, data: dedupeReturnsById(data ?? []) };
  } catch (err) {
    return { ok: false, data: [], error: err instanceof Error ? err.message : "Failed to load pallet items." };
  }
}

export async function listAuditLog(
  tenant?: TenantQueryOpts,
  limit = 50,
): Promise<{ ok: boolean; data: AuditLogRecord[]; error?: string }> {
  try {
    const scope = await resolveTenantListScope(tenant);
    let q = supabaseServer.from("return_audit_log").select("*")
      .order("created_at", { ascending: false }).limit(limit);
    if (scope.mode === "single") q = q.eq("organization_id", scope.organizationId);
    const { data, error } = await q;
    if (error) throw new Error(error.message);
    return { ok: true, data: (data ?? []) as AuditLogRecord[] };
  } catch (err) {
    return { ok: false, data: [], error: err instanceof Error ? err.message : "Failed to load audit log." };
  }
}

// ─── Dashboard high-level snapshot (UTC “today”) ───────────────────────────────

export async function getDashboardSnapshot(
  tenant?: TenantQueryOpts,
): Promise<{ ok: boolean; data?: DashboardSnapshot; error?: string }> {
  const startUtc = new Date();
  startUtc.setUTCHours(0, 0, 0, 0);
  const iso = startUtc.toISOString();
  try {
    const scope = await resolveTenantListScope(tenant);
    let qReturnsToday = supabaseServer.from("returns").select("id", { count: "exact", head: true }).gte("created_at", iso).is("deleted_at", null);
    let qPallets = supabaseServer.from("pallets").select("id", { count: "exact", head: true }).is("deleted_at", null);
    let qPackages = supabaseServer.from("packages").select("id", { count: "exact", head: true }).is("deleted_at", null);
    let qClaims = supabaseServer
      .from("claim_submissions")
      .select("id", { count: "exact", head: true })
      .eq("status", "ready_to_send");
    let qEst = supabaseServer.from("returns").select("estimated_value").is("deleted_at", null).limit(10000);
    if (scope.mode === "single") {
      qReturnsToday = qReturnsToday.eq("organization_id", scope.organizationId);
      qPallets = qPallets.eq("organization_id", scope.organizationId);
      qPackages = qPackages.eq("organization_id", scope.organizationId);
      qClaims = qClaims.eq("organization_id", scope.organizationId);
      qEst = qEst.eq("organization_id", scope.organizationId);
    }
    const [
      { count: returnsToday, error: e1 },
      { count: palletCount, error: e2 },
      { count: packageCount, error: e3 },
      { count: claimsReadyToSend, error: e4 },
      { data: estRows, error: e5 },
    ] = await Promise.all([
      qReturnsToday,
      qPallets,
      qPackages,
      qClaims,
      qEst,
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

export async function getReturnsAnalyticsData(
  tenant?: TenantQueryOpts,
): Promise<{ ok: boolean; data?: ReturnsAnalyticsPayload; error?: string }> {
  try {
    const scope = await resolveTenantListScope(tenant);
    let qRet = supabaseServer.from("returns").select("id,conditions,created_at,updated_at,package_id,created_by").limit(500);
    let qPkg = supabaseServer.from("packages").select("id,carrier_name").limit(500);
    let qPlt = supabaseServer.from("pallets").select("id").limit(500);
    if (scope.mode === "single") {
      qRet = qRet.eq("organization_id", scope.organizationId);
      qPkg = qPkg.eq("organization_id", scope.organizationId);
      qPlt = qPlt.eq("organization_id", scope.organizationId);
    }
    const [{ data: retRows, error: e1 }, { data: pkgRows, error: e2 }, { data: pltRows, error: e3 }] = await Promise.all([
      qRet,
      qPkg,
      qPlt,
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

    // Resolve operator UUIDs to display names via profiles.
    const rawOperatorCounts = new Map<string, number>();
    for (const r of returns) {
      const op = r.created_by?.trim() || "Unknown";
      rawOperatorCounts.set(op, (rawOperatorCounts.get(op) ?? 0) + 1);
    }
    const profileIds = [...rawOperatorCounts.keys()].filter(
      (id) => id !== "Unknown" && isUuidString(id),
    );
    const nameById = new Map<string, string>();
    if (profileIds.length > 0) {
      const { data: profRows } = await supabaseServer
        .from("profiles")
        .select("id, full_name")
        .in("id", profileIds);
      for (const p of (profRows ?? []) as { id?: string; full_name?: string | null }[]) {
        if (p.id) nameById.set(p.id, (p.full_name?.trim() || "").slice(0, 40) || p.id.slice(0, 8));
      }
    }
    const operatorStats = [...rawOperatorCounts.entries()]
      .map(([id, count]) => ({
        operator: nameById.get(id) ?? (id === "Unknown" ? "Unknown" : id.slice(0, 8)),
        count,
      }))
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

/**
 * Fetch expected items for a package from synced Amazon raw-data tables.
 *
 * Priority:
 *   1. `amazon_removals` — matched by `tracking_number` (exact, case-insensitive).
 *      Groups by sku/fnsku and sums `shipped_quantity`.
 *   2. `amazon_returns`  — matched by `lpn` (FBA return LPN = tracking number).
 *      Groups by sku and counts rows (no shipped_quantity column).
 *   3. Returns empty array when neither table has a match — caller falls back to
 *      `manifest_data` (OCR packing-slip).
 */
export async function getAmazonExpectedItems(
  trackingNumber: string,
  organizationId: string,
): Promise<{ ok: true; data: ExpectedItem[] } | { ok: false; error: string }> {
  try {
    const tn = trackingNumber.trim();

    // ── 1. amazon_removals ────────────────────────────────────────────────────
    const { data: removalRows, error: removalErr } = await supabaseServer
      .from("amazon_removals")
      .select("sku, fnsku, shipped_quantity, raw_data")
      .eq("organization_id", organizationId)
      .ilike("tracking_number", tn);

    if (removalErr) throw new Error(removalErr.message);

    if (removalRows && removalRows.length > 0) {
      const bySkuMap = new Map<string, { sku: string; qty: number; name: string }>();
      for (const row of removalRows as {
        sku?: string | null;
        fnsku?: string | null;
        shipped_quantity?: number | null;
        raw_data?: Record<string, unknown> | null;
      }[]) {
        const sku = row.fnsku?.trim() || row.sku?.trim() || "UNKNOWN";
        const qty = Number(row.shipped_quantity) || 1;
        const rd = row.raw_data ?? {};
        const name =
          (rd["product-name"] as string | null)?.trim() ||
          (rd["product_name"] as string | null)?.trim() ||
          (rd["title"] as string | null)?.trim() ||
          sku;
        const existing = bySkuMap.get(sku);
        if (existing) {
          existing.qty += qty;
        } else {
          bySkuMap.set(sku, { sku, qty, name });
        }
      }
      const data: ExpectedItem[] = [...bySkuMap.values()].map((v) => ({
        sku: v.sku,
        expected_qty: v.qty,
        description: v.name,
      }));
      return { ok: true, data };
    }

    // ── 2. amazon_returns (FBA returns — lpn = tracking number) ──────────────
    const { data: returnRows, error: returnErr } = await supabaseServer
      .from("amazon_returns")
      .select("sku, asin, product_name, raw_data")
      .eq("organization_id", organizationId)
      .ilike("lpn", tn);

    if (returnErr) throw new Error(returnErr.message);

    if (returnRows && returnRows.length > 0) {
      const bySkuMap = new Map<string, { sku: string; qty: number; name: string }>();
      for (const row of returnRows as {
        sku?: string | null;
        asin?: string | null;
        product_name?: string | null;
        raw_data?: Record<string, unknown> | null;
      }[]) {
        const sku = row.sku?.trim() || row.asin?.trim() || "UNKNOWN";
        const rd = row.raw_data ?? {};
        const name =
          row.product_name?.trim() ||
          (rd["product-name"] as string | null)?.trim() ||
          (rd["product_name"] as string | null)?.trim() ||
          sku;
        const existing = bySkuMap.get(sku);
        if (existing) {
          existing.qty += 1;
        } else {
          bySkuMap.set(sku, { sku, qty: 1, name });
        }
      }
      const data: ExpectedItem[] = [...bySkuMap.values()].map((v) => ({
        sku: v.sku,
        expected_qty: v.qty,
        description: v.name,
      }));
      return { ok: true, data };
    }

    return { ok: true, data: [] };
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : "Failed to load Amazon expected items.",
    };
  }
}
