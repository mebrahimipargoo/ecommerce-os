import { AMAZON_REMOVALS_BUSINESS_CONFLICT_COLUMNS } from "./amazon-removals-business-key";

/**
 * Single source of truth for Amazon raw-report import engine behavior.
 * Phases: upload → process → sync → generic (optional) → complete | failed
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
  | "PRODUCT_IDENTITY"
  | "ALL_ORDERS"
  | "REPLACEMENTS"
  | "FBA_GRADE_AND_RESELL"
  | "MANAGE_FBA_INVENTORY"
  | "FBA_INVENTORY"
  | "INBOUND_PERFORMANCE"
  | "AMAZON_FULFILLED_INVENTORY"
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

export type AmazonReportFamily =
  | "returns"
  | "removal"
  | "ledger"
  | "financial"
  | "listing"
  | "safet"
  | "repository"
  | "identity"
  | "archive"
  | "unknown";

export type AmazonImportPhaseModel = "unified_v1";

export type AmazonReportRegistryEntry = {
  staging: StagingTarget;
  /** @deprecated use sync_target_table — kept for callers using DOMAIN_TABLE */
  domainTable: string | null;
  dedupeMode: DedupeMode;
  conflictColumns: string | null;
  postSyncEnrichment: "removal_shipment_tree" | "none";
  generateWorklistAfterSync: boolean;
  report_family: AmazonReportFamily;
  phase_model: AmazonImportPhaseModel;
  stage_target_table: StagingTarget;
  sync_target_table: string | null;
  /** Phase-4 destination label (table or logical subsystem). */
  generic_target_table: string | null;
  supports_generic: boolean;
  supports_worklist: boolean;
  progress_strategy: "batch_csv_stream" | "batch_listing_physical_lines";
  physical_identity_strategy: string;
  business_identity_strategy: string;
};

export type AmazonImportEngineConfig = AmazonReportRegistryEntry & { kind: AmazonSyncKind };

export const AMAZON_REPORT_REGISTRY: Record<AmazonSyncKind, AmazonReportRegistryEntry> = {
  FBA_RETURNS: {
    staging: "amazon_staging",
    domainTable: "amazon_returns",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "returns",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_returns",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "file_sha256_plus_physical_row_number",
    business_identity_strategy: "physical_line",
  },
  REMOVAL_ORDER: {
    staging: "amazon_staging",
    domainTable: "amazon_removals",
    dedupeMode: "staging_line",
    conflictColumns: AMAZON_REMOVALS_BUSINESS_CONFLICT_COLUMNS,
    postSyncEnrichment: "none",
    generateWorklistAfterSync: true,
    report_family: "removal",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_removals",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: true,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "staging_row_lineage",
    business_identity_strategy: "removal_order_composite",
  },
  REMOVAL_SHIPMENT: {
    staging: "amazon_staging",
    domainTable: "amazon_removal_shipments",
    dedupeMode: "removal_shipment_staging",
    conflictColumns: "organization_id,upload_id,amazon_staging_id",
    postSyncEnrichment: "removal_shipment_tree",
    generateWorklistAfterSync: false,
    report_family: "removal",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_removal_shipments",
    generic_target_table: "expected_packages",
    supports_generic: true,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "upload_plus_staging_id",
    business_identity_strategy: "removal_shipment_staging_line",
  },
  INVENTORY_LEDGER: {
    staging: "amazon_staging",
    domainTable: "amazon_inventory_ledger",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "ledger",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_inventory_ledger",
    generic_target_table: "product_identifier_map",
    supports_generic: true,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  REIMBURSEMENTS: {
    staging: "amazon_staging",
    domainTable: "amazon_reimbursements",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "financial",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_reimbursements",
    generic_target_table: "financial_reference_resolver",
    supports_generic: true,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  SETTLEMENT: {
    staging: "amazon_staging",
    domainTable: "amazon_settlements",
    dedupeMode: "settlement_line",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "financial",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_settlements",
    generic_target_table: "financial_reference_resolver",
    supports_generic: true,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "file_sha256_plus_physical_row_number",
    business_identity_strategy: "settlement_line",
  },
  SAFET_CLAIMS: {
    staging: "amazon_staging",
    domainTable: "amazon_safet_claims",
    dedupeMode: "safet_claim_id",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "safet",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_safet_claims",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "safet_claim_anchor",
  },
  TRANSACTIONS: {
    staging: "amazon_staging",
    domainTable: "amazon_transactions",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "financial",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_transactions",
    generic_target_table: "financial_reference_resolver",
    supports_generic: true,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  REPORTS_REPOSITORY: {
    staging: "amazon_staging",
    domainTable: "amazon_reports_repository",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "repository",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_reports_repository",
    /** Phase 4: non-blocking bookkeeping (physical lines + raw_data already landed in Phase 3). */
    generic_target_table: "amazon_reports_repository",
    supports_generic: true,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  PRODUCT_IDENTITY: {
    staging: "amazon_staging",
    domainTable: "product_identifier_map",
    dedupeMode: "source_line_hash",
    conflictColumns: null,
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "identity",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "product_identifier_map",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "file_sha256_plus_physical_row_number",
    business_identity_strategy: "product_identity_sku_identifiers",
  },
  ALL_ORDERS: {
    staging: "amazon_staging",
    domainTable: "amazon_all_orders",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "archive",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_all_orders",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  REPLACEMENTS: {
    staging: "amazon_staging",
    domainTable: "amazon_replacements",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "archive",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_replacements",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  FBA_GRADE_AND_RESELL: {
    staging: "amazon_staging",
    domainTable: "amazon_fba_grade_and_resell",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "archive",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_fba_grade_and_resell",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  MANAGE_FBA_INVENTORY: {
    staging: "amazon_staging",
    domainTable: "amazon_manage_fba_inventory",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "archive",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_manage_fba_inventory",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  FBA_INVENTORY: {
    staging: "amazon_staging",
    domainTable: "amazon_fba_inventory",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "archive",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_fba_inventory",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  INBOUND_PERFORMANCE: {
    staging: "amazon_staging",
    domainTable: "amazon_inbound_performance",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "archive",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_inbound_performance",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  AMAZON_FULFILLED_INVENTORY: {
    staging: "amazon_staging",
    domainTable: "amazon_amazon_fulfilled_inventory",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "archive",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_amazon_fulfilled_inventory",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  RESERVED_INVENTORY: {
    staging: "amazon_staging",
    domainTable: "amazon_reserved_inventory",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "archive",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_reserved_inventory",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  FEE_PREVIEW: {
    staging: "amazon_staging",
    domainTable: "amazon_fee_preview",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "archive",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_fee_preview",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  MONTHLY_STORAGE_FEES: {
    staging: "amazon_staging",
    domainTable: "amazon_monthly_storage_fees",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "archive",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_monthly_storage_fees",
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "source_line_hash",
    business_identity_strategy: "line_hash_unique",
  },
  CATEGORY_LISTINGS: {
    staging: "amazon_staging",
    domainTable: "amazon_listing_report_rows_raw",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "listing",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_listing_report_rows_raw",
    generic_target_table: "catalog_products",
    supports_generic: true,
    supports_worklist: false,
    progress_strategy: "batch_listing_physical_lines",
    physical_identity_strategy: "file_sha256_plus_physical_row_number",
    business_identity_strategy: "listing_raw_line",
  },
  ALL_LISTINGS: {
    staging: "amazon_staging",
    domainTable: "amazon_listing_report_rows_raw",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "listing",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_listing_report_rows_raw",
    generic_target_table: "catalog_products",
    supports_generic: true,
    supports_worklist: false,
    progress_strategy: "batch_listing_physical_lines",
    physical_identity_strategy: "file_sha256_plus_physical_row_number",
    business_identity_strategy: "listing_raw_line",
  },
  ACTIVE_LISTINGS: {
    staging: "amazon_staging",
    domainTable: "amazon_listing_report_rows_raw",
    dedupeMode: "source_line_hash",
    conflictColumns: "organization_id,source_file_sha256,source_physical_row_number",
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "listing",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: "amazon_listing_report_rows_raw",
    generic_target_table: "catalog_products",
    supports_generic: true,
    supports_worklist: false,
    progress_strategy: "batch_listing_physical_lines",
    physical_identity_strategy: "file_sha256_plus_physical_row_number",
    business_identity_strategy: "listing_raw_line",
  },
  UNKNOWN: {
    staging: "amazon_staging",
    domainTable: null,
    dedupeMode: "source_line_hash",
    conflictColumns: null,
    postSyncEnrichment: "none",
    generateWorklistAfterSync: false,
    report_family: "unknown",
    phase_model: "unified_v1",
    stage_target_table: "amazon_staging",
    sync_target_table: null,
    generic_target_table: null,
    supports_generic: false,
    supports_worklist: false,
    progress_strategy: "batch_csv_stream",
    physical_identity_strategy: "none",
    business_identity_strategy: "none",
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

export function resolveAmazonImportEngineConfig(kind: AmazonSyncKind): AmazonImportEngineConfig {
  return { kind, ...AMAZON_REPORT_REGISTRY[kind] };
}

export function isListingAmazonSyncKind(kind: AmazonSyncKind): boolean {
  return kind === "CATEGORY_LISTINGS" || kind === "ALL_LISTINGS" || kind === "ACTIVE_LISTINGS";
}

/** Phase 4 required when registry declares generic support. */
export function requiresPhase4Generic(kind: AmazonSyncKind): boolean {
  return AMAZON_REPORT_REGISTRY[kind].supports_generic;
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
  if (rt === "PRODUCT_IDENTITY" || rt === "PRODUCT_IDENTITY_CSV") return "PRODUCT_IDENTITY";
  if (rt === "ALL_ORDERS") return "ALL_ORDERS";
  if (rt === "REPLACEMENTS") return "REPLACEMENTS";
  if (rt === "FBA_GRADE_AND_RESELL") return "FBA_GRADE_AND_RESELL";
  if (rt === "MANAGE_FBA_INVENTORY") return "MANAGE_FBA_INVENTORY";
  if (rt === "FBA_INVENTORY") return "FBA_INVENTORY";
  if (rt === "INBOUND_PERFORMANCE") return "INBOUND_PERFORMANCE";
  if (rt === "AMAZON_FULFILLED_INVENTORY") return "AMAZON_FULFILLED_INVENTORY";
  if (rt === "RESERVED_INVENTORY") return "RESERVED_INVENTORY";
  if (rt === "FEE_PREVIEW") return "FEE_PREVIEW";
  if (rt === "MONTHLY_STORAGE_FEES") return "MONTHLY_STORAGE_FEES";
  if (rt === "CATEGORY_LISTINGS") return "CATEGORY_LISTINGS";
  if (rt === "ALL_LISTINGS") return "ALL_LISTINGS";
  if (rt === "ACTIVE_LISTINGS") return "ACTIVE_LISTINGS";
  return "UNKNOWN";
}
