import { AMAZON_REMOVALS_BUSINESS_CONFLICT_COLUMNS } from "./amazon-removals-business-key";

/**
 * Central routing for Amazon raw-report imports.
 *
 * Pipeline (all non-ledger file uploads):
 *   1) Upload raw file → raw_report_uploads + Storage
 *   2) Stage rows → amazon_staging (or listing raw table for catalog exports)
 *   3) Parse/map from staging (combined with step 2 for CSV reports)
 *   4) Duplicate detection (staging hash; sync upsert against domain unique indexes)
 *   5) Sync → amazon_* domain table(s)
 *   6) Optional enrichment / generate (removals worklist, removal shipment tree RPCs, etc.)
 *
 * Listing exports: Phase 2 lands in `amazon_staging`; Phase 3 upserts into
 * `amazon_listing_report_rows_raw` on (organization_id, source_file_sha256, source_physical_row_number).
 * Phase 4 Generic merges into `catalog_products` (same route as removal shipment tree, different body).
 */

/** Canonical sync routing key (maps from raw_report_uploads.report_type). */
export type AmazonSyncKind =
  | "FBA_RETURNS"
  | "REMOVAL_ORDER"
  | "REMOVAL_SHIPMENT"
  | "INVENTORY_LEDGER"
  | "REIMBURSEMENTS"
  | "SETTLEMENT"
  | "SAFET_CLAIMS"
  | "TRANSACTIONS"
  | "REPORTS_REPOSITORY"
  | "ALL_ORDERS"
  | "REPLACEMENTS"
  | "FBA_GRADE_AND_RESELL"
  | "MANAGE_FBA_INVENTORY"
  | "FBA_INVENTORY"
  | "RESERVED_INVENTORY"
  | "FEE_PREVIEW"
  | "MONTHLY_STORAGE_FEES"
  | "CATEGORY_LISTINGS"
  | "ALL_LISTINGS"
  | "ACTIVE_LISTINGS"
  | "UNKNOWN";

export type StagingTarget = "amazon_staging";

export type DedupeMode =
  | "source_line_hash"
  | "staging_line"
  | "lpn"
  | "safet_claim_id"
  | "settlement_line"
  | "removal_shipment_staging";

export type AmazonReportRegistryEntry = {
  staging: StagingTarget;
  domainTable: string | null;
  dedupeMode: DedupeMode;
  /** PostgREST upsert arbiter — must match a UNIQUE index (see migrations). */
  conflictColumns: string | null;
  /** Down-only RPC enrichment after sync (if any). */
  postSyncEnrichment: "removal_shipment_tree" | "none";
  /** Whether removals worklist generate is applicable after sync. */
  generateWorklistAfterSync: boolean;
};

/**
 * Routing matrix (report_type → behavior).
 *
 * | report_type | staging | target table(s) | dedupe | post-step | generate |
 * |-------------|---------|-----------------|--------|-----------|----------|
 * | FBA_RETURNS | amazon_staging | amazon_returns | physical file row | none | no |
 * | REMOVAL_ORDER | amazon_staging | amazon_removals | business_line | none | yes |
 * | REMOVAL_SHIPMENT | amazon_staging | amazon_removal_shipments + RPC | removal_shipment_staging | removal_shipment_tree | no |
 * | INVENTORY_LEDGER | amazon_staging | amazon_inventory_ledger | source_line_hash | none | no |
 * | REIMBURSEMENTS | amazon_staging | amazon_reimbursements | source_line_hash | none | no |
 * | SETTLEMENT | amazon_staging | amazon_settlements | settlement_line | none | no |
 * | SAFET_CLAIMS | amazon_staging | amazon_safet_claims | safet_claim_id | none | no |
 * | TRANSACTIONS | amazon_staging | amazon_transactions | source_line_hash | none | no |
 * | REPORTS_REPOSITORY | amazon_staging | amazon_reports_repository | source_line_hash | none | no |
 * | ALL_ORDERS … MONTHLY_STORAGE_FEES | amazon_staging | amazon_* archive | source_line_hash | none | no |
 * | CATEGORY/ALL/ACTIVE_LISTINGS | amazon_staging | amazon_listing_report_rows_raw | physical file row | catalog (generic) | no |
 */
export const AMAZON_REPORT_REGISTRY: Record<AmazonSyncKind, AmazonReportRegistryEntry> = {
  FBA_RETURNS: {
    staging: "amazon_staging",
    domainTable: "amazon_returns",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  REMOVAL_ORDER: {
    staging: "amazon_staging",
    domainTable: "amazon_removals",
    dedupeMode: "staging_line",
    conflictColumns: AMAZON_REMOVALS_BUSINESS_CONFLICT_COLUMNS,
    postSyncEnrichment: "none",
    generateWorklistAfterSync: true,
  },
  REMOVAL_SHIPMENT: {
    staging: "amazon_staging",
    domainTable: "amazon_removal_shipments",
    dedupeMode: "removal_shipment_staging",
    conflictColumns: "organization_id,upload_id,amazon_staging_id",
    postSyncEnrichment: "removal_shipment_tree",
    generateWorklistAfterSync: false,
  },
  INVENTORY_LEDGER: {
    staging: "amazon_staging",
    domainTable: "amazon_inventory_ledger",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  REIMBURSEMENTS: {
    staging: "amazon_staging",
    domainTable: "amazon_reimbursements",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  SETTLEMENT: {
    staging: "amazon_staging",
    domainTable: "amazon_settlements",
    dedupeMode: "settlement_line",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  SAFET_CLAIMS: {
    staging: "amazon_staging",
    domainTable: "amazon_safet_claims",
    dedupeMode: "safet_claim_id",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  TRANSACTIONS: {
    staging: "amazon_staging",
    domainTable: "amazon_transactions",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  REPORTS_REPOSITORY: {
    staging: "amazon_staging",
    domainTable: "amazon_reports_repository",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  ALL_ORDERS: {
    staging: "amazon_staging",
    domainTable: "amazon_all_orders",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  REPLACEMENTS: {
    staging: "amazon_staging",
    domainTable: "amazon_replacements",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  FBA_GRADE_AND_RESELL: {
    staging: "amazon_staging",
    domainTable: "amazon_fba_grade_and_resell",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  MANAGE_FBA_INVENTORY: {
    staging: "amazon_staging",
    domainTable: "amazon_manage_fba_inventory",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  FBA_INVENTORY: {
    staging: "amazon_staging",
    domainTable: "amazon_fba_inventory",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  RESERVED_INVENTORY: {
    staging: "amazon_staging",
    domainTable: "amazon_reserved_inventory",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  FEE_PREVIEW: {
    staging: "amazon_staging",
    domainTable: "amazon_fee_preview",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  MONTHLY_STORAGE_FEES: {
    staging: "amazon_staging",
    domainTable: "amazon_monthly_storage_fees",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  CATEGORY_LISTINGS: {
    staging: "amazon_staging",
    domainTable: "amazon_listing_report_rows_raw",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  ALL_LISTINGS: {
    staging: "amazon_staging",
    domainTable: "amazon_listing_report_rows_raw",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  ACTIVE_LISTINGS: {
    staging: "amazon_staging",
    domainTable: "amazon_listing_report_rows_raw",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
  UNKNOWN: {
    staging: "amazon_staging",
    domainTable: null,
    dedupeMode: "source_line_hash",
    conflictColumns: null,
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
  },
};

/** Domain table per kind — shared with `app/api/settings/imports/sync/route.ts`. */
export const DOMAIN_TABLE: Record<AmazonSyncKind, string | null> = Object.fromEntries(
  (Object.keys(AMAZON_REPORT_REGISTRY) as AmazonSyncKind[]).map((k) => [k, AMAZON_REPORT_REGISTRY[k].domainTable]),
) as Record<AmazonSyncKind, string | null>;

/** Supabase `onConflict` column list — must match registry conflictColumns. */
export const CONFLICT_KEY: Record<AmazonSyncKind, string | null> = Object.fromEntries(
  (Object.keys(AMAZON_REPORT_REGISTRY) as AmazonSyncKind[]).map((k) => [
    k,
    AMAZON_REPORT_REGISTRY[k].conflictColumns,
  ]),
) as Record<AmazonSyncKind, string | null>;

export function isListingAmazonSyncKind(kind: AmazonSyncKind): boolean {
  return kind === "CATEGORY_LISTINGS" || kind === "ALL_LISTINGS" || kind === "ACTIVE_LISTINGS";
}

/** Listing catalog merge and removal shipment tree run in POST /api/settings/imports/generic after raw_synced. */
export function requiresPhase4Generic(kind: AmazonSyncKind): boolean {
  return kind === "REMOVAL_SHIPMENT" || isListingAmazonSyncKind(kind);
}

/** Maps `raw_report_uploads.report_type` (canonical + legacy slugs) → sync kind. */
export function resolveAmazonImportSyncKind(reportType: string | null | undefined): AmazonSyncKind {
  const rt = String(reportType ?? "").trim();
  if (rt === "FBA_RETURNS" || rt === "fba_customer_returns") return "FBA_RETURNS";
  if (rt === "REMOVAL_ORDER") return "REMOVAL_ORDER";
  if (rt === "REMOVAL_SHIPMENT") return "REMOVAL_SHIPMENT";
  if (rt === "INVENTORY_LEDGER" || rt === "inventory_ledger") return "INVENTORY_LEDGER";
  if (rt === "REIMBURSEMENTS" || rt === "reimbursements") return "REIMBURSEMENTS";
  if (rt === "SETTLEMENT" || rt === "settlement_repository") return "SETTLEMENT";
  if (rt === "SAFET_CLAIMS" || rt === "safe_t_claims") return "SAFET_CLAIMS";
  if (rt === "TRANSACTIONS" || rt === "transaction_view") return "TRANSACTIONS";
  if (rt === "REPORTS_REPOSITORY") return "REPORTS_REPOSITORY";
  if (rt === "ALL_ORDERS") return "ALL_ORDERS";
  if (rt === "REPLACEMENTS") return "REPLACEMENTS";
  if (rt === "FBA_GRADE_AND_RESELL") return "FBA_GRADE_AND_RESELL";
  if (rt === "MANAGE_FBA_INVENTORY") return "MANAGE_FBA_INVENTORY";
  if (rt === "FBA_INVENTORY") return "FBA_INVENTORY";
  if (rt === "RESERVED_INVENTORY") return "RESERVED_INVENTORY";
  if (rt === "FEE_PREVIEW") return "FEE_PREVIEW";
  if (rt === "MONTHLY_STORAGE_FEES") return "MONTHLY_STORAGE_FEES";
  if (rt === "CATEGORY_LISTINGS") return "CATEGORY_LISTINGS";
  if (rt === "ALL_LISTINGS") return "ALL_LISTINGS";
  if (rt === "ACTIVE_LISTINGS") return "ACTIVE_LISTINGS";
  return "UNKNOWN";
}
