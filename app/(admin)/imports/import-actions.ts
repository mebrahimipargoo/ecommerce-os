"use server";

import { supabaseServer } from "../../../lib/supabase-server";
import { getSessionUserIdFromCookies } from "../../../lib/supabase-server-auth";
import { updateUploadAfterChunk as updateUploadProgressAfterChunk } from "../../../lib/import-upload-progress";
import {
  AMAZON_LEDGER_UPLOAD_SOURCE,
  mergeUploadMetadata,
  parseRawReportMetadata,
  type RawReportUploadMetadata,
} from "../../../lib/raw-report-upload-metadata";
import type { RawReportUploadRow } from "../../../lib/raw-report-upload-row";
import type { ImportFpsSnapshot } from "../../../lib/import-ui-action-state";
import { resolveOrganizationId } from "../../../lib/organization";
import { resolveWriteOrganizationId } from "../../../lib/server-tenant";
import type { RawReportType } from "../../../lib/raw-report-types";
import { isUuidString } from "../../../lib/uuid";
import { DB_TABLES, RAW_REPORTS_BUCKET, RAW_REPORT_UPLOADS_SELECT } from "../lib/constants";

/**
 * Inserts use **only** snake_case keys that match PostgREST / Postgres.
 * If you see "Could not find column in schema cache", reload PostgREST:
 *   SQL: `NOTIFY pgrst, 'reload schema';`
 * (or restart the Supabase API / project.)
 */

/**
 * Returns the `stores.organization_id` for a UUID, or `null` if not found.
 * Used to derive the tenant scope for an import from the user-selected target store
 * (so super_admins do not accidentally write under the platform/parent org).
 */
async function lookupStoreOrganizationId(storeId: string | null | undefined): Promise<string | null> {
  const id = (storeId ?? "").trim();
  if (!id || !isUuidString(id)) return null;
  const { data, error } = await supabaseServer
    .from(DB_TABLES.stores)
    .select("organization_id")
    .eq("id", id)
    .maybeSingle();
  if (error || !data) return null;
  const oid = String((data as { organization_id?: unknown }).organization_id ?? "").trim();
  return isUuidString(oid) ? oid : null;
}

function serializeColumnMappingJson(
  m: Record<string, string> | null | undefined,
): Record<string, string> | null {
  if (!m || Object.keys(m).length === 0) return null;
  return JSON.parse(JSON.stringify(m)) as Record<string, string>;
}

/**
 * Resolves the actor profile id for import operations.
 *
 * Priority order:
 *   1. Explicit UUID passed from the browser (localStorage-stored profile id).
 *   2. Supabase Auth session cookie — covers the common case where localStorage
 *      `current_user_profile_id` is absent but the user IS authenticated.
 *      This is the key fix for the Ghost History bug: without this step, the
 *      fallback returned the first profile in the default org, causing the list
 *      query to use a different org_id than the one used during insert.
 *   3. First profile in the env-var default org (single-tenant seed fallback).
 *   4. Any profile in the system (last resort).
 */
async function resolveActorForImportAction(explicitUserId?: string | null): Promise<string | null> {
  const raw = explicitUserId?.trim();
  if (raw && isUuidString(raw)) {
    const { data } = await supabaseServer
      .from(DB_TABLES.profiles)
      .select("id")
      .eq("id", raw)
      .maybeSingle();
    if (data?.id) return String(data.id);
  }

  // Cookie-based session fallback (step 2 — critical for Ghost History fix).
  try {
    const sessionUid = await getSessionUserIdFromCookies();
    if (sessionUid && isUuidString(sessionUid)) {
      const { data } = await supabaseServer
        .from(DB_TABLES.profiles)
        .select("id")
        .eq("id", sessionUid)
        .maybeSingle();
      if (data?.id) return String(data.id);
    }
  } catch {
    // Cookies unavailable in some edge-runtime or test contexts — continue.
  }

  const orgId = resolveOrganizationId();
  const { data: firstInOrg } = await supabaseServer
    .from(DB_TABLES.profiles)
    .select("id")
    .eq("organization_id", orgId)
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();
  if (firstInOrg?.id) return String(firstInOrg.id);

  const { data: anyProfile } = await supabaseServer
    .from(DB_TABLES.profiles)
    .select("id")
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();
  return anyProfile?.id ? String(anyProfile.id) : null;
}

export async function getImportLedgerActorUserId(): Promise<
  { ok: true; actorUserId: string } | { ok: false }
> {
  const id = await resolveActorForImportAction(null);
  if (!id) return { ok: false };
  return { ok: true, actorUserId: id };
}

export async function updateUploadAfterChunk(
  input: Parameters<typeof updateUploadProgressAfterChunk>[0],
): ReturnType<typeof updateUploadProgressAfterChunk> {
  return updateUploadProgressAfterChunk(input);
}

async function audit(
  orgId: string,
  userId: string | null,
  action: string,
  entityId: string | null,
  detail?: Record<string, unknown>,
): Promise<void> {
  await supabaseServer.from("raw_report_import_audit").insert({
    organization_id: orgId,
    user_profile_id: userId,
    action,
    entity_id: entityId,
    detail: detail ?? null,
  });
}

/**
 * Statuses that count as "still active" for duplicate-detection purposes.
 * `failed` and `superseded` are intentionally excluded so the user can re-upload
 * after fixing a bad import without being blocked, and so superseded duplicates
 * stay invisible to the duplicate guard.
 */
const PRODUCT_IDENTITY_ACTIVE_STATUSES: readonly string[] = [
  "uploading",
  "pending",
  "ready",
  "uploaded",
  "mapped",
  "needs_mapping",
  "processing",
  "staged",
  "synced",
  "complete",
];

/** Snapshot of an existing Product Identity upload returned to the UI. */
export type ExistingProductIdentityUpload = {
  upload_id: string;
  organization_id: string;
  store_id: string | null;
  status: string;
  file_name: string;
  content_sha256: string;
  created_at: string;
  updated_at: string;
};

/**
 * Looks for an existing Product Identity upload that already covers the same
 * (organization_id, store_id, content_sha256). Used by the UI as a preflight
 * check before creating a new upload session and by `createRawReportUploadSession`
 * as a server-side defense-in-depth guard.
 *
 * Only "active" statuses are considered (see PRODUCT_IDENTITY_ACTIVE_STATUSES) —
 * failed or superseded prior attempts do not block a new upload.
 */
export async function findActiveProductIdentityImport(input: {
  organizationId?: string | null;
  storeId: string;
  contentSha256: string;
  actorUserId?: string | null;
}): Promise<
  | { ok: true; existing: ExistingProductIdentityUpload | null }
  | { ok: false; error: string }
> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  const storeId = input.storeId?.trim() ?? "";
  if (!isUuidString(storeId)) return { ok: false, error: "Invalid store id." };
  const sha = input.contentSha256?.trim().toLowerCase() ?? "";
  if (!/^[a-f0-9]{64}$/.test(sha)) {
    return { ok: false, error: "Invalid content SHA-256 (must be 64 lowercase hex chars)." };
  }

  // Resolve org with the same store-aware priority used by createRawReportUploadSession,
  // so the UI preflight always queries the same scope the server will write under.
  const storeOwnerOrg = await lookupStoreOrganizationId(storeId);
  const orgId = await resolveWriteOrganizationId(
    userId,
    (input.organizationId?.trim() || null) ?? storeOwnerOrg,
  );
  if (!isUuidString(orgId)) return { ok: false, error: "Invalid tenant scope." };

  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .select("id, organization_id, status, file_name, metadata, created_at, updated_at")
      .eq("organization_id", orgId)
      .eq("report_type", "PRODUCT_IDENTITY")
      .in("status", PRODUCT_IDENTITY_ACTIVE_STATUSES)
      .contains("metadata", { content_sha256: sha, import_store_id: storeId })
      .order("created_at", { ascending: false })
      .limit(1);

    if (error) return { ok: false, error: error.message };

    const row = (data ?? [])[0] as
      | (Record<string, unknown> & { metadata?: Record<string, unknown> | null })
      | undefined;
    if (!row) return { ok: true, existing: null };

    const meta = (row.metadata && typeof row.metadata === "object" && !Array.isArray(row.metadata))
      ? (row.metadata as Record<string, unknown>)
      : {};
    const importStoreId =
      typeof meta.import_store_id === "string" && isUuidString(meta.import_store_id)
        ? meta.import_store_id
        : typeof meta.ledger_store_id === "string" && isUuidString(meta.ledger_store_id)
          ? meta.ledger_store_id
          : null;

    return {
      ok: true,
      existing: {
        upload_id: String(row.id),
        organization_id: String(row.organization_id ?? ""),
        store_id: importStoreId,
        status: String(row.status ?? ""),
        file_name: String(row.file_name ?? ""),
        content_sha256: sha,
        created_at: String(row.created_at ?? ""),
        updated_at: String(row.updated_at ?? ""),
      },
    };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Duplicate lookup failed." };
  }
}

/**
 * Mark a Product Identity upload as `superseded` and remove the rows it
 * personally wrote so a re-import of the same file can run cleanly.
 *
 * Scope of cleanup (intentionally narrow):
 *   * `product_identifier_map` — only rows tagged with `source_upload_id` of
 *     the superseded upload AND `source_report_type` in the Product Identity
 *     family. All Listings / inventory ledger bridge rows are NEVER touched.
 *   * `catalog_products`        — only rows tagged with `source_upload_id` of
 *     the superseded upload AND `source_report_type` in the Product Identity
 *     family. Existing All Listings catalog rows survive.
 *   * `catalog_identity_unresolved_backlog` — same source_upload_id scope.
 *   * `products` — left alone. The replacement import will upsert by
 *     (organization_id, store_id, sku) and the priority-aware merge
 *     guarantees no field downgrade.
 *   * `raw_report_uploads.status` flips to `superseded` and metadata records
 *     `superseded_at` / `superseded_by`.
 *
 * Note: storage objects from the superseded upload remain on disk — they are
 * tied to the row id and are also reused if the replacement upload uses the
 * same storage_prefix. Use `deleteRawReportUpload` if you want a hard delete.
 */
export async function supersedeProductIdentityImport(input: {
  uploadId: string;
  replacementUploadId?: string | null;
  actorUserId?: string | null;
}): Promise<
  | {
      ok: true;
      supersededUploadId: string;
      removedIdentifierMapRows: number;
      removedCatalogProductRows: number;
      removedBacklogRows: number;
    }
  | { ok: false; error: string }
> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  const uploadId = input.uploadId?.trim() ?? "";
  if (!isUuidString(uploadId)) return { ok: false, error: "Invalid upload id." };

  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id, report_type, status, metadata")
    .eq("id", uploadId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Upload row has invalid organization_id." };

  const reportType = String((row as { report_type?: unknown }).report_type ?? "");
  if (reportType !== "PRODUCT_IDENTITY") {
    return {
      ok: false,
      error: `Refusing to supersede a non-Product-Identity upload (report_type=${reportType}).`,
    };
  }

  const replacementUploadId = input.replacementUploadId?.trim();
  const replacementOk = replacementUploadId && isUuidString(replacementUploadId)
    ? replacementUploadId
    : null;

  const productIdentitySources = ["PRODUCT_IDENTITY_IMPORT", "PRODUCT_IDENTITY"] as const;

  // ── Scoped DELETEs (no destructive cross-tenant queries) ───────────────
  const idMapDel = await supabaseServer
    .from("product_identifier_map")
    .delete()
    .eq("organization_id", orgId)
    .eq("source_upload_id", uploadId)
    .in("source_report_type", productIdentitySources)
    .select("id");
  if (idMapDel.error) return { ok: false, error: idMapDel.error.message };

  const catalogDel = await supabaseServer
    .from("catalog_products")
    .delete()
    .eq("organization_id", orgId)
    .eq("source_upload_id", uploadId)
    .in("source_report_type", productIdentitySources)
    .select("id");
  if (catalogDel.error) return { ok: false, error: catalogDel.error.message };

  const backlogDel = await supabaseServer
    .from("catalog_identity_unresolved_backlog")
    .delete()
    .eq("organization_id", orgId)
    .eq("source_upload_id", uploadId)
    .in("source_report_type", productIdentitySources)
    .select("id");
  if (backlogDel.error) return { ok: false, error: backlogDel.error.message };

  // ── Mark the upload row superseded (status + metadata audit trail) ────
  const now = new Date().toISOString();
  const supersedeMetaPatch: Record<string, unknown> = {
    superseded_at: now,
    error_message: "",
  };
  if (replacementOk) supersedeMetaPatch.superseded_by_upload_id = replacementOk;

  const merged = mergeUploadMetadata((row as { metadata?: unknown }).metadata, supersedeMetaPatch);
  const { error: upErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({ status: "superseded", metadata: merged, updated_at: now })
    .eq("id", uploadId)
    .eq("organization_id", orgId);
  if (upErr) return { ok: false, error: upErr.message };

  await audit(orgId, userId, "import.product_identity.superseded", uploadId, {
    removedIdentifierMapRows: idMapDel.data?.length ?? 0,
    removedCatalogProductRows: catalogDel.data?.length ?? 0,
    removedBacklogRows: backlogDel.data?.length ?? 0,
    replacement_upload_id: replacementOk,
  });

  return {
    ok: true,
    supersededUploadId: uploadId,
    removedIdentifierMapRows: idMapDel.data?.length ?? 0,
    removedCatalogProductRows: catalogDel.data?.length ?? 0,
    removedBacklogRows: backlogDel.data?.length ?? 0,
  };
}

/** True if any prior upload in this org has the same content fingerprint in `metadata.md5_hash`. */
export async function rawUploadExistsWithMd5Hash(
  md5Hash: string,
  actorUserId?: string | null,
): Promise<{ ok: true; exists: boolean } | { ok: false; error: string }> {
  const userId = await resolveActorForImportAction(actorUserId);
  const orgId = await resolveWriteOrganizationId(userId, null);
  const normalized = md5Hash.trim().toLowerCase();
  if (!/^[a-f0-9]{32}$/.test(normalized)) {
    return { ok: false, error: "Invalid MD5 hash." };
  }

  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .select("id")
      .eq("organization_id", orgId)
      .contains("metadata", { md5_hash: normalized })
      .limit(1)
      .maybeSingle();

    if (error) return { ok: false, error: error.message };
    return { ok: true, exists: !!data };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Lookup failed." };
  }
}

export async function listRawReportUploads(input?: {
  /** Effective tenant scope; super_admin may pass the ledger “Target organization” id. */
  organizationId?: string | null;
  actorUserId?: string | null;
}): Promise<{ ok: true; rows: RawReportUploadRow[] } | { ok: false; error: string }> {
  const actorId = await resolveActorForImportAction(input?.actorUserId);

  // Validate the requested organizationId is actually an `organizations.id`
  // (not a `stores.id`) before forwarding to the resolver. This preserves the
  // Ghost History fix — historically the UI sometimes passed a storeId, which
  // would mask the actor's home org for super_admins. `resolveWriteOrganizationId`
  // already gates the override to roles that may pick a workspace org.
  const requestedRaw = (input?.organizationId ?? "").trim();
  if (!requestedRaw || !isUuidString(requestedRaw)) {
    return { ok: false, error: "Active organization is required. Select a workspace organization in the app header." };
  }
  let requestedOrgId: string | null = null;
  const { data: orgRow } = await supabaseServer
    .from("organizations")
    .select("id")
    .eq("id", requestedRaw)
    .maybeSingle();
  if (orgRow?.id) requestedOrgId = String(orgRow.id);
  if (!requestedOrgId) {
    return { ok: false, error: "Selected workspace organization does not exist." };
  }

  const orgId = await resolveWriteOrganizationId(actorId, requestedOrgId);

  console.info(
    "[listRawReportUploads] org:",
    orgId,
    "| actor:",
    actorId,
    "| requested:",
    requestedOrgId ?? "(none)",
    "| passed actorUserId:",
    input?.actorUserId ?? "(none)",
  );

  if (!isUuidString(orgId)) {
    return { ok: false, error: "Could not resolve organization — please sign in again." };
  }

  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .select(RAW_REPORT_UPLOADS_SELECT)
      .eq("organization_id", orgId)
      .order("created_at", { ascending: false })
      .limit(100);

    if (error) return { ok: false, error: error.message };

    const base = (data ?? []) as Record<string, unknown>[];

    const uploadIds = base
      .map((raw) => String((raw as { id?: unknown }).id ?? "").trim())
      .filter((id) => isUuidString(id));
    const fpsByUpload = new Map<string, ImportFpsSnapshot>();
    if (uploadIds.length > 0) {
      const { data: fpsRows, error: fpsErr } = await supabaseServer
        .from("file_processing_status")
        .select(
          [
            "upload_id",
            "phase1_status",
            "phase2_status",
            "phase3_status",
            "phase4_status",
            "current_phase",
            "current_phase_label",
            "current_target_table",
            "status",
            "upload_pct",
            "process_pct",
            "sync_pct",
            "phase1_upload_pct",
            "phase2_stage_pct",
            "phase3_raw_sync_pct",
            "phase4_generic_pct",
            "staged_rows_written",
            "raw_rows_written",
            "raw_rows_skipped_existing",
            "generic_rows_written",
            "total_rows",
            "processed_rows",
            "file_rows_total",
            "data_rows_total",
            "rows_eligible_for_generic",
            "duplicate_rows_skipped",
            "canonical_rows_new",
            "canonical_rows_updated",
            "canonical_rows_unchanged",
            "upload_bytes_written",
            "upload_bytes_total",
          ].join(", "),
        )
        .in("upload_id", uploadIds);
      const numOrNull = (v: unknown): number | null =>
        typeof v === "number" && Number.isFinite(v) ? v : null;
      if (!fpsErr && fpsRows) {
        for (const fr of fpsRows as unknown as Record<string, unknown>[]) {
          const uid = String(fr.upload_id ?? "").trim();
          if (!isUuidString(uid)) continue;
          fpsByUpload.set(uid, {
            phase1_status: fr.phase1_status != null ? String(fr.phase1_status) : null,
            phase2_status: fr.phase2_status != null ? String(fr.phase2_status) : null,
            phase3_status: fr.phase3_status != null ? String(fr.phase3_status) : null,
            phase4_status: fr.phase4_status != null ? String(fr.phase4_status) : null,
            current_phase: fr.current_phase != null ? String(fr.current_phase) : null,
            current_phase_label: fr.current_phase_label != null ? String(fr.current_phase_label) : null,
            current_target_table: fr.current_target_table != null ? String(fr.current_target_table) : null,
            row_status: fr.status != null ? String(fr.status) : null,
            upload_pct: numOrNull(fr.upload_pct),
            process_pct: numOrNull(fr.process_pct),
            sync_pct: numOrNull(fr.sync_pct),
            phase1_upload_pct: numOrNull(fr.phase1_upload_pct),
            phase2_stage_pct: numOrNull(fr.phase2_stage_pct),
            phase3_raw_sync_pct: numOrNull(fr.phase3_raw_sync_pct),
            phase4_generic_pct: numOrNull(fr.phase4_generic_pct),
            staged_rows_written: numOrNull(fr.staged_rows_written),
            raw_rows_written: numOrNull(fr.raw_rows_written),
            raw_rows_skipped_existing: numOrNull(fr.raw_rows_skipped_existing),
            generic_rows_written: numOrNull(fr.generic_rows_written),
            total_rows: numOrNull(fr.total_rows),
            processed_rows: numOrNull(fr.processed_rows),
            file_rows_total: numOrNull(fr.file_rows_total),
            data_rows_total: numOrNull(fr.data_rows_total),
            rows_eligible_for_generic: numOrNull(fr.rows_eligible_for_generic),
            duplicate_rows_skipped: numOrNull(fr.duplicate_rows_skipped),
            canonical_rows_new: numOrNull(fr.canonical_rows_new),
            canonical_rows_updated: numOrNull(fr.canonical_rows_updated),
            canonical_rows_unchanged: numOrNull(fr.canonical_rows_unchanged),
            upload_bytes_written: numOrNull(fr.upload_bytes_written),
            upload_bytes_total: numOrNull(fr.upload_bytes_total),
          });
        }
      }
    }

    const rows: RawReportUploadRow[] = base.map((raw) => {
      const r = raw as Record<string, unknown>;
      const meta = parseRawReportMetadata(r.metadata);
      const metaObj =
        r.metadata && typeof r.metadata === "object" && !Array.isArray(r.metadata)
          ? (r.metadata as Record<string, unknown>)
          : null;
      const id = String(r.id);
      return {
        id,
        organization_id: String(r.organization_id ?? ""),
        file_name: String(r.file_name ?? ""),
        report_type: String(r.report_type ?? ""),
        storage_prefix: meta.storagePrefix,
        status: String(r.status ?? ""),
        upload_progress: meta.uploadProgress,
        process_progress: meta.processProgress,
        uploaded_bytes: meta.uploadedBytes,
        total_bytes: meta.totalBytes,
        row_count: meta.rowCount,
        column_mapping:
          r.column_mapping && typeof r.column_mapping === "object"
            ? (r.column_mapping as Record<string, string>)
            : null,
        errorMessage: meta.errorMessage,
        metadata: metaObj,
        created_by: (r.created_by as string | null) ?? null,
        created_at: String(r.created_at ?? ""),
        updated_at: String(r.updated_at ?? ""),
        created_by_name: null,
        file_processing_status: fpsByUpload.get(id) ?? null,
      };
    });

    const uploaderIds = [...new Set(rows.map((row) => row.created_by).filter(Boolean))] as string[];
    if (uploaderIds.length > 0) {
      const { data: profRows, error: profErr } = await supabaseServer
        .from(DB_TABLES.profiles)
        .select("id, full_name")
        .in("id", uploaderIds);
      const fullNameById = new Map<string, string>();
      if (!profErr && profRows) {
        for (const p of profRows as { id?: string; full_name?: string | null }[]) {
          if (p.id) fullNameById.set(String(p.id), String(p.full_name ?? "").trim());
        }
      }

      // Prefer per-upload `getUserById` over `listUsers(1000)`: polling History calls this
      // every few seconds; bulk listUsers taxed Auth APIs and triggered timeouts/non-JSON
      // responses that surfaced as fetchServerAction "unexpected response".
      const emailById = new Map<string, string>();
      for (const uid of uploaderIds) {
        try {
          const { data: udat, error: uidErr } =
            await supabaseServer.auth.admin.getUserById(uid);
          if (!uidErr && udat?.user?.email) {
            const em = String(udat.user.email).trim();
            if (em) emailById.set(uid, em);
          }
        } catch {
          /* Auth edge cases — rely on profile full_name below */
        }
      }
      for (const row of rows) {
        if (row.created_by) {
          const fromAuth = emailById.get(row.created_by)?.trim();
          const fromProfile = fullNameById.get(row.created_by);
          row.created_by_name = fromAuth || fromProfile || null;
        }
      }
    }

    return { ok: true, rows };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Failed to load uploads." };
  }
}

export async function createRawReportUploadSession(input: {
  fileName: string;
  totalBytes: number;
  /** Header classification + user selection — persisted as `raw_report_uploads.report_type`. */
  reportType: RawReportType;
  /** Lowercase hex MD5 of full file content (32 hex) — API validation; may be SHA-256 prefix for compat */
  md5Hash: string;
  /** Lowercase hex SHA-256 of entire file (64 hex). When set, re-importing the same file removes prior REMOVAL_ORDER / REMOVAL_SHIPMENT data for that report type. */
  contentSha256?: string | null;
  fileExtension: string;
  fileSizeBytes: number;
  uploadChunksCount: number;
  /** Saved on the same row as the session — no separate mapping write required. */
  columnMapping?: Record<string, string> | null;
  /** Original CSV header row — stored in metadata so the mapping modal can offer dropdowns. */
  csvHeaders?: string[] | null;
  /**
   * Record-First strategy: create the DB row with `uploading` BEFORE sending any chunks.
   * This guarantees the file appears in History the moment the upload starts.
   * Defaults to `"uploading"`.
   */
  initialStatus?: "uploading" | "pending";
  actorUserId?: string | null;
  /**
   * Explicit tenant scope (e.g. the page-level "Organization scope" the user selected in the UI).
   * Super-admins may pass any org UUID; regular admins are scoped to their own org.
   */
  organizationId?: string | null;
  /**
   * Target store the row will be associated with (`stores.id`). Persisted into
   * `metadata.import_store_id` / `metadata.ledger_store_id` so the entire pipeline
   * (raw_report_uploads → process → sync → products / catalog_products /
   * product_identifier_map) carries the same tenant + store scope.
   *
   * SAFETY: when provided, the server treats `stores.organization_id` as the
   * preferred tenant scope (super_admins may still override via `organizationId`),
   * and **rejects** the insert when the resolved org does not own the store.
   * This is what stops Product Identity rows from being written under the
   * parent/platform organization.
   */
  storeId?: string | null;
  /**
   * Product Identity idempotency guard.
   *
   * Default `false`: if an active (non-failed, non-superseded) Product Identity
   * upload already exists for the same (organization_id, store_id, content_sha256),
   * the insert is refused with a structured error so the UI can prompt the user.
   *
   * Set to `true` AFTER calling `supersedeProductIdentityImport(...)` on the
   * existing duplicate — this records that the caller already replaced the prior
   * row and is allowed to create a fresh one.
   */
  supersedeExistingDuplicate?: boolean | null;
}): Promise<
  | { ok: true; id: string; storagePrefix: string }
  | {
      ok: false;
      error: string;
      /** Set when refusal was specifically the Product Identity duplicate guard. */
      duplicate?: ExistingProductIdentityUpload | null;
    }
> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  const requestedOrg = input.organizationId?.trim() ?? null;
  if (!requestedOrg || !isUuidString(requestedOrg)) {
    return {
      ok: false,
      error: "Active organization is required. Choose a workspace organization in the app header before importing.",
    };
  }
  const storeOwnerOrg = await lookupStoreOrganizationId(input.storeId);

  // Tenant resolution priority for imports:
  //   1. UI-supplied `organizationId` (super_admin scope picker)
  //   2. The owner org of the selected target store (`stores.organization_id`)
  //   3. The actor's profile org (legacy single-tenant fallback)
  // We never silently default to the parent/platform org for super_admins —
  // either the picker or the store's own organization wins.
  const orgId = await resolveWriteOrganizationId(userId, requestedOrg);

  // Hard guard: when a store is specified, it MUST belong to the resolved org.
  // Without this, a super_admin whose profile lives in the platform/parent org
  // could create import rows under the parent while selecting a tenant store —
  // the exact bug that shipped Product Identity rows under the wrong org.
  const storeIdRaw = input.storeId?.trim() ?? "";
  if (storeIdRaw) {
    if (!isUuidString(storeIdRaw)) {
      return { ok: false, error: "Invalid store id." };
    }
    if (!storeOwnerOrg) {
      return { ok: false, error: "Selected target store does not exist." };
    }
    if (storeOwnerOrg !== orgId) {
      return {
        ok: false,
        error:
          "Selected target store belongs to a different organization than the import scope. " +
          "Pick a store that belongs to the selected organization (or change the organization scope).",
      };
    }
  }

  const storagePrefix = `${orgId}/${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

  /** Plain JSON object for `column_mapping` JSONB (PostgREST). */
  const column_mapping =
    input.columnMapping && Object.keys(input.columnMapping).length > 0
      ? (JSON.parse(JSON.stringify(input.columnMapping)) as Record<string, string>)
      : null;

  const md5 = input.md5Hash.trim().toLowerCase();
  if (!/^[a-f0-9]{32}$/.test(md5)) {
    return { ok: false, error: "Invalid MD5 hash." };
  }

  console.info("[createRawReportUploadSession] inserting org:", orgId, "| actor:", userId, "| file:", input.fileName);

  /** Technical tracking lives only in `metadata` JSONB. */
  const sha =
    typeof input.contentSha256 === "string" && /^[a-f0-9]{64}$/.test(input.contentSha256.trim().toLowerCase())
      ? input.contentSha256.trim().toLowerCase()
      : undefined;

  // ── Product Identity idempotency guard (defense-in-depth) ───────────────
  // The UI runs the same check up-front and prompts the user to Replace; this
  // server-side check exists so a stale/buggy client cannot still create a
  // second active session for the same (org, store, content_sha256). Other
  // report types (FBA Returns, Removals, listings, etc.) keep their existing
  // behavior because their idempotency is enforced at sync time via
  // `(organization_id, source_file_sha256, source_physical_row_number)`.
  if (
    input.reportType === "PRODUCT_IDENTITY" &&
    sha &&
    storeIdRaw &&
    !input.supersedeExistingDuplicate
  ) {
    const dup = await findActiveProductIdentityImport({
      organizationId: orgId,
      storeId: storeIdRaw,
      contentSha256: sha,
      actorUserId: userId,
    });
    if (dup.ok && dup.existing) {
      return {
        ok: false,
        error:
          "This file was already imported for this store and is still active. " +
          "Replace the previous import to re-upload, or pick a different file.",
        duplicate: dup.existing,
      };
    }
  }

  const metadata = mergeUploadMetadata(null, {
    total_bytes: input.totalBytes,
    storage_prefix: storagePrefix,
    upload_progress: 0,
    uploaded_bytes: 0,
    process_progress: 0,
    md5_hash: md5,
    ...(sha ? { content_sha256: sha } : {}),
    file_name: input.fileName,
    file_extension: input.fileExtension,
    file_size_bytes: input.fileSizeBytes,
    upload_chunks_count: input.uploadChunksCount,
    ...(input.csvHeaders && input.csvHeaders.length > 0
      ? { csv_headers: input.csvHeaders }
      : {}),
    // Lock the tenant + store pair into metadata at create-time so every
    // downstream phase (process / sync / generic) reads the same scope even
    // if the user navigates away before classification finishes.
    ...(storeIdRaw && isUuidString(storeIdRaw)
      ? { ledger_store_id: storeIdRaw, import_store_id: storeIdRaw }
      : {}),
  });

  const insertRow = {
    organization_id: orgId,
    file_name: input.fileName,
    report_type: input.reportType,
    status: (input.initialStatus ?? "uploading") as string,
    column_mapping,
    metadata,
    ...(userId ? { created_by: userId } : {}),
  };

  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .insert(insertRow)
      .select("id")
      .single();

    if (error || !data?.id) {
      return {
        ok: false,
        error:
          error?.message ??
          "Insert failed. Check Supabase migration for raw_report_uploads (organization_id) and organization_settings.",
      };
    }

    await audit(orgId, userId, "import.session_created", data.id as string, {
      fileName: input.fileName,
      totalBytes: input.totalBytes,
      hasMapping: !!column_mapping,
    });

    // Fresh progress row for this upload_id only — never inherit counters from another import.
    await supabaseServer.from("file_processing_status").upsert(
      {
        upload_id: data.id as string,
        organization_id: orgId,
        status: "uploading",
        current_phase: "upload",
        upload_pct: 0,
        process_pct: 0,
        sync_pct: 0,
        processed_rows: 0,
        total_rows: null,
        file_size_bytes: input.fileSizeBytes ?? input.totalBytes,
        uploaded_bytes: 0,
        error_message: null,
        import_metrics: {},
      },
      { onConflict: "upload_id" },
    );

    return { ok: true, id: data.id as string, storagePrefix };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Create failed." };
  }
}

/**
 * Record-First patch: after the DB row is created with `status='uploading'`, call this once
 * the CSV headers have been read and the classify-headers API has returned a result.
 * Updates `report_type`, `column_mapping`, and persists `csv_headers` in metadata so the
 * mapping modal can offer dropdowns even if the upload fails midway.
 */
export async function updateUploadSessionClassification(input: {
  uploadId: string;
  reportType: RawReportType;
  columnMapping?: Record<string, string> | null;
  csvHeaders?: string[] | null;
  actorUserId?: string | null;
  /** Storage path for the single uploaded file (e.g. `{prefix}/original.csv`). */
  rawFilePath?: string | null;
  /** The stores.id selected by the user — stored in metadata for audit/UI (not used as FK). */
  storeId?: string | null;
  /** Total data rows counted from the CSV during Phase 1. */
  totalRows?: number | null;
  /** Whether the user chose to import the full file (ignore date range). */
  importFullFile?: boolean | null;
  /** ISO date strings for the date filter (applied during Phase 2). */
  startDate?: string | null;
  endDate?: string | null;
  /**
   * Zero-based index of the actual header row detected by findHeaderRowIndex.
   * Saved so Phase 2 (stage route) can pass skipLines to csv-parser and skip
   * metadata preamble rows that Amazon's Reports Repository files prepend.
   *
   * Set to `-1` for the headerless Amazon Inventory Ledger export so Phase 2
   * knows to use `synthesizedHeaders` instead of reading row 0 as a header.
   */
  headerRowIndex?: number | null;
  /**
   * When the file has no header (e.g. headerless Amazon Inventory Ledger),
   * pass the synthesised header order here. Phase 2 will configure
   * `csv-parser` to use these directly and treat row 0 as data.
   */
  synthesizedHeaders?: string[] | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  // CRITICAL: Read the upload row's organization_id directly (do NOT resolve
  // from the actor's profile org). The previous version called
  // `resolveWriteOrganizationId(userId, null)` which only ever returns the
  // actor's home org. When a super_admin imports for a tenant store whose
  // owner org differs from their own profile org, the WHERE clause below
  // never matched the row and the UPDATE silently returned "Upload not
  // found." — leaving `report_type='UNKNOWN'` even after classification
  // succeeded. This was the root cause of the Product Identity CSV staying
  // at UNKNOWN/needs_mapping in History.
  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id, metadata")
    .eq("id", input.uploadId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };

  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) {
    return { ok: false, error: "Upload row has invalid organization_id." };
  }

  const metaPatch: Record<string, unknown> = { upload_progress: 100 };
  if (input.csvHeaders && input.csvHeaders.length > 0) {
    metaPatch.csv_headers = input.csvHeaders;
    // Stable sorted fingerprint — enables Mapping Memory lookup on future uploads
    metaPatch.headers_fingerprint = input.csvHeaders
      .map((h) => h.trim().toLowerCase())
      .sort()
      .join("|");
  }
  if (input.rawFilePath) {
    metaPatch.raw_file_path = input.rawFilePath;
    metaPatch.total_parts = 1;
    metaPatch.upload_chunks_count = 1;
  }
  if (input.storeId && isUuidString(input.storeId)) {
    // Defense-in-depth: if classification ever tries to associate the upload
    // with a store that lives in a different organization than the upload row,
    // refuse the patch. Stops cross-tenant store_id from leaking via the
    // classification step (the only other code path that writes import_store_id).
    const storeOwnerOrg = await lookupStoreOrganizationId(input.storeId);
    if (!storeOwnerOrg) {
      return { ok: false, error: "Selected target store does not exist." };
    }
    if (storeOwnerOrg !== orgId) {
      return {
        ok: false,
        error:
          "Selected target store belongs to a different organization than this upload. " +
          "Re-select a store from the upload's organization.",
      };
    }
    metaPatch.ledger_store_id = input.storeId;
    metaPatch.import_store_id = input.storeId;
  }
  if (typeof input.totalRows === "number" && Number.isFinite(input.totalRows)) {
    metaPatch.total_rows = input.totalRows;
    // NOTE: `metadata.row_count` lives in the JSONB blob, not the legacy
    // `raw_report_uploads.row_count` column (which is no longer relied on
    // by the UI or the Product Identity pipeline — see file_processing_status).
    metaPatch.row_count = 0;
  }
  if (input.importFullFile != null) {
    metaPatch.import_full_file = input.importFullFile;
  }
  if (input.startDate) metaPatch.start_date = input.startDate;
  if (input.endDate) metaPatch.end_date = input.endDate;
  if (typeof input.headerRowIndex === "number" && input.headerRowIndex >= 0) {
    metaPatch.header_row_index = input.headerRowIndex;
  }
  if (Array.isArray(input.synthesizedHeaders) && input.synthesizedHeaders.length > 0) {
    metaPatch.synthesized_headers = input.synthesizedHeaders;
    // header_row_index = -1 sentinel: row 0 is a data line, not a header.
    metaPatch.header_row_index = -1;
  }

  const mergedForWrite = mergeUploadMetadata((row as { metadata?: unknown }).metadata, metaPatch);
  const updateRow: Record<string, unknown> = {
    report_type: input.reportType,
    column_mapping: serializeColumnMappingJson(input.columnMapping ?? null),
    metadata: mergedForWrite,
    updated_at: new Date().toISOString(),
  };

  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update(updateRow)
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  if (input.reportType === "REMOVAL_SHIPMENT") {
    const cs =
      typeof mergedForWrite.content_sha256 === "string"
        ? mergedForWrite.content_sha256.trim().toLowerCase()
        : "";
    console.info(
      JSON.stringify({
        phase: "import_classification",
        report_type: "REMOVAL_SHIPMENT",
        content_sha256_present: /^[a-f0-9]{64}$/.test(cs),
      }),
    );
  }

  await audit(orgId, userId, "import.classification_updated", input.uploadId, {
    reportType: input.reportType,
    mappingKeys: input.columnMapping ? Object.keys(input.columnMapping) : [],
  });
  return { ok: true };
}

/**
 * Creates a `raw_report_uploads` row for the ledger uploader so progress/history use `organization_id`
 * (via `resolveWriteOrganizationId`) and metadata upload/process percentages.
 */
export async function createAmazonLedgerUploadSession(input: {
  fileName: string;
  fileSizeBytes: number;
  storagePath: string;
  actorUserId?: string | null;
  /** Tenant scope for `raw_report_uploads.organization_id`. */
  organization_id?: string | null;
  requestedOrganizationId?: string | null;
  /** Target store (`stores.id`); stored in metadata for audit/UI. */
  store_id?: string | null;
}): Promise<{ ok: true; id: string } | { ok: false; error: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  const sid = input.store_id?.trim() ?? "";
  const storeOwnerOrg = await lookupStoreOrganizationId(sid);
  const requested =
    (input.organization_id ?? input.requestedOrganizationId)?.trim() ?? null;
  // Same priority as createRawReportUploadSession: explicit picker → store owner → actor.
  const orgId = await resolveWriteOrganizationId(userId, requested ?? storeOwnerOrg);
  if (!isUuidString(orgId)) return { ok: false, error: "Invalid tenant scope." };

  if (sid) {
    if (!isUuidString(sid)) return { ok: false, error: "Invalid store id." };
    if (!storeOwnerOrg) return { ok: false, error: "Selected target store does not exist." };
    if (storeOwnerOrg !== orgId) {
      return {
        ok: false,
        error:
          "Selected target store belongs to a different organization than the import scope.",
      };
    }
  }
  const metadata = mergeUploadMetadata(null, {
    total_bytes: input.fileSizeBytes,
    upload_progress: 0,
    process_progress: 0,
    uploaded_bytes: 0,
    ledger_storage_path: input.storagePath,
    ...(sid && isUuidString(sid) ? { ledger_store_id: sid } : {}),
    source: AMAZON_LEDGER_UPLOAD_SOURCE,
    file_extension: "csv",
    file_size_bytes: input.fileSizeBytes,
    upload_chunks_count: 1,
    total_parts: 1,
  });

  const insertRow = {
    organization_id: orgId,
    file_name: input.fileName,
    report_type: "inventory_ledger" as const,
    status: "uploading" as const,
    column_mapping: null as Record<string, string> | null,
    metadata,
    ...(userId ? { created_by: userId } : {}),
  };

  try {
    const { data, error } = await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .insert(insertRow)
      .select("id")
      .single();

    if (error || !data?.id) {
      return {
        ok: false,
        error: error?.message ?? "Insert failed (ledger session).",
      };
    }

    await audit(orgId, userId, "import.ledger_session_created", data.id as string, {
      fileName: input.fileName,
      storagePath: input.storagePath,
    });
    return { ok: true, id: data.id as string };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Create failed." };
  }
}

export async function patchAmazonLedgerUploadSession(input: {
  uploadId: string;
  actorUserId?: string | null;
  organization_id?: string | null;
  requestedOrganizationId?: string | null;
  status?: "uploading" | "pending" | "processing" | "complete" | "synced" | "failed";
  metadataPatch: Record<string, unknown>;
}): Promise<
  { ok: true; upload_progress: number; process_progress: number } | { ok: false; error: string }
> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  const requested =
    (input.organization_id ?? input.requestedOrganizationId)?.trim() ?? null;
  const orgId = await resolveWriteOrganizationId(userId, requested);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, metadata")
    .eq("id", input.uploadId)
    .eq("organization_id", orgId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };

  const prev = (row as { metadata?: unknown }).metadata;
  const merged = mergeUploadMetadata(prev, input.metadataPatch as Partial<RawReportUploadMetadata>);

  const updateRow: Record<string, unknown> = {
    metadata: merged,
    updated_at: new Date().toISOString(),
  };
  if (input.status) updateRow.status = input.status;

  const { error: upErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update(updateRow)
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (upErr) return { ok: false, error: upErr.message };

  const parsed = parseRawReportMetadata(merged);
  return { ok: true, upload_progress: parsed.uploadProgress, process_progress: parsed.processProgress };
}

export async function getAmazonLedgerUploadProgress(input: {
  uploadId: string;
  actorUserId?: string | null;
  organization_id?: string | null;
  requestedOrganizationId?: string | null;
}): Promise<
  | { ok: true; upload_progress: number; process_progress: number; status: string }
  | { ok: false; error: string }
> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  const requested =
    (input.organization_id ?? input.requestedOrganizationId)?.trim() ?? null;
  const orgId = await resolveWriteOrganizationId(userId, requested);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const { data: row, error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("metadata, status")
    .eq("id", input.uploadId)
    .eq("organization_id", orgId)
    .maybeSingle();

  if (error || !row) return { ok: false, error: error?.message ?? "Not found." };
  const parsed = parseRawReportMetadata((row as { metadata?: unknown }).metadata);
  return {
    ok: true,
    upload_progress: parsed.uploadProgress,
    process_progress: parsed.processProgress,
    status: String((row as { status?: unknown }).status ?? ""),
  };
}

export async function finalizeRawReportUpload(input: {
  uploadId: string;
  /** Accurate data row count (excluding header; trailing blanks ignored). */
  rowCount?: number | null;
  columnMapping?: Record<string, string> | null;
  actorUserId?: string | null;
  /**
   * Terminal status after upload completes.
   * `mapped`        = AI mapping complete; Phase 2 (Process) button shows in History.
   * `needs_mapping` = AI could not resolve required fields; user must use Map Columns.
   * `ready`/`pending` = legacy aliases accepted for backward compatibility.
   */
  targetStatus?: "mapped" | "ready" | "pending" | "needs_mapping";
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  // Read org from the stored row so we never rely on the env-var default.
  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id, metadata")
    .eq("id", input.uploadId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Upload row has invalid organization_id." };

  const metadata = mergeUploadMetadata((row as { metadata?: unknown }).metadata, {
    upload_progress: 100,
    process_progress: 0,
    ...(input.rowCount != null ? { row_count: input.rowCount } : {}),
  });

  const finalStatus = input.targetStatus ?? "ready";

  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({
      status: finalStatus,
      // Only overwrite column_mapping when explicitly provided — prevents wiping AI-generated
      // mapping that was saved by updateUploadSessionClassification moments earlier.
      ...(input.columnMapping !== undefined
        ? { column_mapping: serializeColumnMappingJson(input.columnMapping) }
        : {}),
      metadata,
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  const parsedMeta = parseRawReportMetadata(metadata);
  const byteTotal =
    parsedMeta.totalBytes > 0
      ? parsedMeta.totalBytes
      : Math.max(parsedMeta.uploadedBytes ?? 0, 0);

  await supabaseServer.from("file_processing_status").upsert(
    {
      upload_id: input.uploadId,
      organization_id: orgId,
      status: "pending",
      current_phase: null,
      upload_pct: 100,
      process_pct: 0,
      sync_pct: 0,
      processed_rows: 0,
      file_size_bytes: byteTotal > 0 ? byteTotal : null,
      uploaded_bytes: byteTotal > 0 ? byteTotal : null,
      error_message: null,
    },
    { onConflict: "upload_id" },
  );

  await audit(orgId, userId, "import.upload_finalized", input.uploadId, {
    rowCount: input.rowCount ?? null,
    targetStatus: finalStatus,
  });
  return { ok: true };
}

/**
 * Saves the user-verified column mapping and transitions the upload from `needs_mapping`
 * back to `pending` so the Sync pipeline can run.
 * Optionally updates `report_type` when the user also corrected the report kind.
 */
export async function approveColumnMapping(input: {
  uploadId: string;
  mapping: Record<string, string>;
  reportType?: RawReportType | null;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  // Read org from the row directly so super_admin imports targeting a tenant
  // store are not silently rejected when their profile org differs.
  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id")
    .eq("id", input.uploadId)
    .maybeSingle();
  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Upload row has invalid organization_id." };

  const updateRow: Record<string, unknown> = {
    column_mapping: serializeColumnMappingJson(input.mapping),
    status: "ready",
    updated_at: new Date().toISOString(),
  };
  if (input.reportType) updateRow.report_type = input.reportType;

  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update(updateRow)
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(orgId, userId, "import.mapping_approved", input.uploadId, {
    keys: Object.keys(input.mapping),
    reportType: input.reportType ?? null,
  });
  return { ok: true };
}

export async function failRawReportUpload(input: {
  uploadId: string;
  message: string;
  actorUserId?: string | null;
}): Promise<void> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return;

  const { data: row } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id, metadata")
    .eq("id", input.uploadId)
    .maybeSingle();

  const orgId = String((row as { organization_id?: unknown } | null)?.organization_id ?? "").trim();
  const metadata = mergeUploadMetadata((row as { metadata?: unknown } | null)?.metadata, {
    error_message: input.message,
  });

  if (isUuidString(orgId)) {
    await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .update({
        status: "failed",
        metadata,
        updated_at: new Date().toISOString(),
      })
      .eq("id", input.uploadId)
      .eq("organization_id", orgId);

    await audit(orgId, userId, "import.upload_failed", input.uploadId, { message: input.message });
  }
}

/**
 * Resets a row that is stuck in "processing" status back to a retryable state.
 * Safe to call even if the row is not stuck — it only transitions FROM "processing".
 * "staged" → resets to "mapped" (re-run Process then Sync).
 * Otherwise resets to "mapped".
 */
export async function resetStuckUpload(input: {
  uploadId: string;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id, metadata, status")
    .eq("id", input.uploadId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Invalid organization." };

  const currentStatus = String((row as { status?: unknown }).status ?? "");
  if (currentStatus !== "processing") {
    return { ok: false, error: `Row is not stuck — current status is "${currentStatus}".` };
  }

  const metadata = mergeUploadMetadata((row as { metadata?: unknown }).metadata, {
    error_message: "Reset by user after stuck in processing.",
    process_progress: 0,
  });

  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({ status: "mapped", metadata, updated_at: new Date().toISOString() })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId)
    .eq("status", "processing");

  if (error) return { ok: false, error: error.message };

  await audit(orgId, userId, "import.stuck_reset", input.uploadId, { previousStatus: "processing" });
  return { ok: true };
}

export async function updateRawReportType(input: {
  uploadId: string;
  reportType: RawReportType;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  // Read row's actual org so super_admin updates against tenant-org rows are not silently rejected.
  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id")
    .eq("id", input.uploadId)
    .maybeSingle();
  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Upload row has invalid organization_id." };

  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({
      report_type: input.reportType,
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(orgId, userId, "import.report_type_updated", input.uploadId, {
    reportType: input.reportType,
  });
  return { ok: true };
}

export async function recordColumnMappingDecision(input: {
  uploadId: string;
  mapping: Record<string, string>;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id")
    .eq("id", input.uploadId)
    .maybeSingle();
  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Upload row has invalid organization_id." };

  const { error } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({
      column_mapping: serializeColumnMappingJson(input.mapping),
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (error) return { ok: false, error: error.message };

  await audit(orgId, userId, "import.mapping_applied", input.uploadId, { keys: Object.keys(input.mapping) });
  return { ok: true };
}

async function removeObjectsUnderStoragePrefix(prefix: string): Promise<void> {
  const trimmed = prefix.replace(/\/+$/, "");
  if (!trimmed) return;
  const { data: files, error } = await supabaseServer.storage
    .from(RAW_REPORTS_BUCKET)
    .list(trimmed);
  if (error || !files?.length) return;
  const paths = files.map((f) => `${trimmed}/${f.name}`);
  await supabaseServer.storage.from(RAW_REPORTS_BUCKET).remove(paths);
}

/**
 * Full-cleanup delete: removes the upload row, Storage objects (chunk prefix and/or
 * single-file ledger path), and domain rows keyed by `upload_id`.
 */
export async function deleteRawReportUpload(
  uploadId: string,
  actorUserId?: string | null,
): Promise<{ ok: true } | { ok: false; error: string }> {
  if (!isUuidString(uploadId)) return { ok: false, error: "Invalid upload id." };
  void actorUserId;

  // Fetch by ID only — read org from the row to avoid env-var default org drift.
  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id, metadata")
    .eq("id", uploadId)
    .maybeSingle();
  if (fetchErr || !row) {
    return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  }
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Upload row has invalid organization_id." };

  const metaRaw = (row as { metadata?: unknown }).metadata;
  const metaObj =
    metaRaw && typeof metaRaw === "object" && !Array.isArray(metaRaw)
      ? (metaRaw as Record<string, unknown>)
      : {};

  // ── 1. Storage cleanup ────────────────────────────────────────────────────
  const prefix = parseRawReportMetadata(metaRaw).storagePrefix;
  if (prefix) {
    await removeObjectsUnderStoragePrefix(prefix);
  }
  const ledgerPath =
    typeof metaObj.ledger_storage_path === "string" ? metaObj.ledger_storage_path.trim() : "";
  if (ledgerPath) {
    await supabaseServer.storage.from(RAW_REPORTS_BUCKET).remove([ledgerPath]);
  }

  // ── 2. Domain table cleanup (errors are swallowed — missing column is non-fatal) ──
  // expected_returns (legacy FBA Returns pipeline)
  await supabaseServer
    .from("expected_returns")
    .delete()
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);

  // expected_packages (legacy Removal Order pipeline — kept for old data)
  await supabaseServer
    .from("expected_packages")
    .delete()
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);

  // expected_removals (legacy Removal Order pipeline)
  await supabaseServer
    .from("expected_removals")
    .delete()
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);

  // ── amazon_ domain tables (new amazon_ prefix standard) ──────────────────
  const AMAZON_DOMAIN_TABLES = [
    "amazon_returns",
    "amazon_removals",
    "amazon_removal_shipments",
    "amazon_inventory_ledger",
    "amazon_reimbursements",
    "amazon_settlements",
    "amazon_safet_claims",
    "amazon_transactions",
    "amazon_reports_repository",
  ] as const;

  for (const tbl of AMAZON_DOMAIN_TABLES) {
    await supabaseServer
      .from(tbl)
      .delete()
      .eq("organization_id", orgId)
      .eq("upload_id", uploadId);
  }

  // amazon_staging cleanup (renamed from amazon_ledger_staging)
  await supabaseServer
    .from(DB_TABLES.amazonLedgerStaging)
    .delete()
    .eq("organization_id", orgId)
    .eq("upload_id", uploadId);

  // ── 3. Delete the upload row itself ──────────────────────────────────────
  const { error: delErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .delete()
    .eq("id", uploadId)
    .eq("organization_id", orgId);
  if (delErr) return { ok: false, error: delErr.message };

  return { ok: true };
}

/**
 * REMOVAL_ORDER / REMOVAL_SHIPMENT Sync (Phase 3): if the user re-uploads the same file bytes,
 * delete older `raw_report_uploads` rows (and their domain/storage data) that share
 * `metadata.content_sha256` for the same `report_type`, so removals/shipment archive do not
 * accumulate duplicates. Requires `content_sha256` on the current upload.
 */
export async function removeOlderRemovalImportsWithSameFileContent(
  organizationId: string,
  currentUploadId: string,
  metadata: unknown,
  reportType: "REMOVAL_ORDER" | "REMOVAL_SHIPMENT" = "REMOVAL_ORDER",
): Promise<{ ok: true; removedUploadIds: string[] } | { ok: false; error: string }> {
  if (!isUuidString(organizationId) || !isUuidString(currentUploadId)) {
    return { ok: false, error: "Invalid ids." };
  }
  const m = metadata && typeof metadata === "object" && !Array.isArray(metadata)
    ? (metadata as Record<string, unknown>)
    : {};
  const contentSha256 =
    typeof m.content_sha256 === "string" ? m.content_sha256.trim().toLowerCase() : "";
  if (!/^[a-f0-9]{64}$/.test(contentSha256)) {
    return { ok: true, removedUploadIds: [] };
  }

  try {
    const { data: rows, error } = await supabaseServer
      .from(DB_TABLES.rawReportUploads)
      .select("id")
      .eq("organization_id", organizationId)
      .eq("report_type", reportType)
      .neq("id", currentUploadId)
      .contains("metadata", { content_sha256: contentSha256 });

    if (error) return { ok: false, error: error.message };
    const ids = (rows ?? []).map((r) => String((r as { id: unknown }).id)).filter(isUuidString);
    const removedUploadIds: string[] = [];
    for (const id of ids) {
      const del = await deleteRawReportUpload(id, null);
      if (del.ok) removedUploadIds.push(id);
    }
    return { ok: true, removedUploadIds };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Cleanup failed." };
  }
}

/**
 * Reset a Product Identity upload that is stuck in `processing` (Phase 2 or
 * Phase 3) without any progress. Safe to call when the Vercel/serverless
 * worker timed out mid-flight.
 *
 * What it does:
 *   * Deletes only the `product_identity_staging_rows` rows for this upload.
 *     This is safe: staging rows are ephemeral and are recreated cleanly on
 *     re-run.
 *   * Does NOT touch `products`, `catalog_products`, or `product_identifier_map` —
 *     those are owned by the supersede flow.
 *   * Resets `raw_report_uploads.status` to `mapped` so the user can click
 *     Process again.
 *   * Resets `file_processing_status` phase2/phase3 fields to zero.
 */
export async function resetStuckProductIdentityUpload(input: {
  uploadId: string;
  actorUserId?: string | null;
}): Promise<{ ok: boolean; error?: string; stagingRowsDeleted?: number }> {
  const userId = await resolveActorForImportAction(input.actorUserId);
  if (!isUuidString(input.uploadId)) return { ok: false, error: "Invalid upload id." };

  const { data: row, error: fetchErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .select("id, organization_id, metadata, status, report_type")
    .eq("id", input.uploadId)
    .maybeSingle();

  if (fetchErr || !row) return { ok: false, error: fetchErr?.message ?? "Upload not found." };
  const orgId = String((row as { organization_id?: unknown }).organization_id ?? "").trim();
  if (!isUuidString(orgId)) return { ok: false, error: "Invalid organization." };

  const reportType = String((row as { report_type?: unknown }).report_type ?? "");
  if (reportType !== "PRODUCT_IDENTITY") {
    return { ok: false, error: `This action only applies to PRODUCT_IDENTITY uploads (got "${reportType}").` };
  }

  const currentStatus = String((row as { status?: unknown }).status ?? "");
  const allowedStatuses = ["processing", "staged", "failed"];
  if (!allowedStatuses.includes(currentStatus)) {
    return {
      ok: false,
      error: `Upload is not in a resettable state (current: "${currentStatus}"). Expected one of: ${allowedStatuses.join(", ")}.`,
    };
  }

  // 1. Delete staging rows for this upload only.
  const { data: stagingDel, error: stagingDelErr } = await supabaseServer
    .from("product_identity_staging_rows")
    .delete()
    .eq("upload_id", input.uploadId)
    .eq("organization_id", orgId)
    .select("id");

  if (stagingDelErr) {
    return { ok: false, error: `Staging cleanup failed: ${stagingDelErr.message}` };
  }

  const stagingRowsDeleted = stagingDel?.length ?? 0;

  // 2. Reset raw_report_uploads back to `mapped`.
  const metadata = mergeUploadMetadata((row as { metadata?: unknown }).metadata, {
    error_message: "Reset by user — staging rows cleared. Re-run Process.",
    process_progress: 0,
    sync_progress: 0,
    import_metrics: { current_phase: "upload" },
  });

  const { error: upErr } = await supabaseServer
    .from(DB_TABLES.rawReportUploads)
    .update({
      status: "mapped",
      import_pipeline_failed_at: null,
      metadata,
      updated_at: new Date().toISOString(),
    })
    .eq("id", input.uploadId)
    .eq("organization_id", orgId);

  if (upErr) return { ok: false, error: upErr.message };

  // 3. Reset file_processing_status.
  await supabaseServer.from("file_processing_status").upsert({
    upload_id: input.uploadId,
    organization_id: orgId,
    status: "pending",
    current_phase: "upload",
    current_phase_label: "Ready for Process",
    upload_pct: 100,
    phase1_upload_pct: 100,
    process_pct: 0,
    phase2_stage_pct: 0,
    phase2_status: null,
    phase2_completed_at: null,
    phase3_raw_sync_pct: 0,
    phase3_status: null,
    phase3_completed_at: null,
    sync_pct: 0,
    processed_rows: 0,
    staged_rows_written: 0,
    raw_rows_written: 0,
    error_message: null,
    next_action_key: "process",
    next_action_label: "Process",
    import_metrics: { current_phase: "upload" },
  }, { onConflict: "upload_id" });

  await audit(orgId, userId, "import.product_identity_reset_stuck", input.uploadId, {
    stagingRowsDeleted,
    previousStatus: currentStatus,
  });

  return { ok: true, stagingRowsDeleted };
}
