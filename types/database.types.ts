/**
 * Supabase `public` schema typings — infrastructure layer.
 *
 * Reflects the LIVE database schema AFTER migration 20260413_returns_module_full_sync.sql
 * is applied. Columns are ordered as they appear in `information_schema.columns`
 * (creation order) to make future diffing easy.
 *
 * For a fully-regenerated file run:
 *   `npx supabase gen types typescript --project-id <ref> > types/database.types.ts`
 */

export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[];

// ---------------------------------------------------------------------------
// returns
// ---------------------------------------------------------------------------

/**
 * `public.returns` row shape — includes all columns added through
 * migration 20260413 (item_name, conditions, notes, photo_evidence, etc.).
 * Legacy columns (raw_return_data, product_id, unit_sale_price,
 * amazon_fees_lost, return_shipping_fee, currency, condition_note) are
 * preserved as nullable to protect historical data.
 */
export type ReturnsRow = {
  /** Primary key. */
  id: string;
  /** FK to `stores` — resolves marketplace channel. */
  order_id: string | null;
  /** Carrier / seller return label scan code. */
  lpn: string | null;
  /** Workflow status: received | pending_evidence | ready_for_claim | claim_filed | closed. */
  status: string;
  /** Tenant FK. */
  organization_id: string | null;
  /** FK to `stores`. */
  store_id: string | null;
  /** FK to `pallets` (denormalised from package). */
  pallet_id: string | null;
  /** FK to `packages`. */
  package_id: string | null;
  /** Legacy JSONB blob — preserved for historical records. */
  raw_return_data: Json | null;
  created_at: string;
  /** Legacy FK to products catalog — preserved. */
  product_id: string | null;
  /** Legacy pricing fields — preserved. */
  unit_sale_price: number | null;
  amazon_fees_lost: number | null;
  return_shipping_fee: number | null;
  currency: string | null;
  /** Legacy free-text condition note — superseded by `conditions` array + `notes`. */
  condition_note: string | null;
  /** Seller RMA / authorisation number. Added in 20260402120000. */
  rma_number: string | null;
  /** Marketplace channel label (e.g. "Amazon"). */
  marketplace: string | null;
  // --- Added by migration 20260413 ---
  /** Human-readable product name. */
  item_name: string | null;
  /** Defect/condition labels array (e.g. ["damaged","expired"]). */
  conditions: string[] | null;
  /** General operator notes. */
  notes: string | null;
  /** Structured photo gallery JSONB. */
  photo_evidence: Json | null;
  expiration_date: string | null;
  batch_number: string | null;
  asin: string | null;
  fnsku: string | null;
  sku: string | null;
  product_identifier: string | null;
  created_by: string | null;
  updated_by: string | null;
  updated_at: string | null;
  /** Estimated reimbursement value in USD. */
  estimated_value: number | null;
  /** Soft-delete timestamp — NULL means active. */
  deleted_at: string | null;
  // --- PostgREST embed (list selects only) ---
  stores?: { name: string; platform: string } | null;
};

// ---------------------------------------------------------------------------
// packages
// ---------------------------------------------------------------------------

/**
 * `public.packages` row shape — includes all columns added through
 * migration 20260413.
 */
export type PackagesRow = {
  id: string;
  pallet_id: string | null;
  tracking_number: string | null;
  status: string | null;
  organization_id: string | null;
  store_id: string | null;
  package_number: string | null;
  expected_item_count: number | null;
  actual_item_count: number | null;
  created_at: string | null;
  carrier_name: string | null;
  rma_number: string | null;
  // --- Added by migration 20260413 ---
  manifest_url: string | null;
  discrepancy_note: string | null;
  order_id: string | null;
  created_by: string | null;
  updated_by: string | null;
  updated_at: string | null;
  photo_url: string | null;
  photo_return_label_url: string | null;
  photo_opened_url: string | null;
  photo_closed_url: string | null;
  manifest_photo_url: string | null;
  /** Soft-delete timestamp — NULL means active. */
  deleted_at: string | null;
  /** Structured photo gallery JSONB. */
  photo_evidence: Json | null;
  /** Parsed packing-slip lines [{sku, expected_qty, description}]. */
  manifest_data: Json | null;
  // --- PostgREST embed ---
  stores?: { name: string; platform: string } | null;
};

// ---------------------------------------------------------------------------
// pallets
// ---------------------------------------------------------------------------

/**
 * `public.pallets` row shape — includes all columns added through
 * migration 20260413.
 */
export type PalletsRow = {
  id: string;
  pallet_number: string;
  status: string | null;
  organization_id: string | null;
  store_id: string | null;
  item_count: number | null;
  created_at: string | null;
  tracking_number: string | null;
  notes: string | null;
  // --- Added by migration 20260413 ---
  created_by: string | null;
  updated_by: string | null;
  updated_at: string | null;
  photo_url: string | null;
  /** Bill of lading photo URL. */
  bol_photo_url: string | null;
  manifest_photo_url: string | null;
  /** Soft-delete timestamp — NULL means active. */
  deleted_at: string | null;
  // --- PostgREST embed ---
  stores?: { name: string; platform: string } | null;
};

// ---------------------------------------------------------------------------
// profiles
// ---------------------------------------------------------------------------

/**
 * `public.profiles` row shape — workspace user directory.
 * `organization_id` is the tenant FK referencing `organization_settings.organization_id`
 * (no standalone `organizations` or `companies` table exists — Rule 5).
 */
export type ProfileRow = {
  /** PK — mirrors `auth.users.id`. */
  id: string;
  /** Tenant FK → `organization_settings.organization_id`. NOT NULL in practice. */
  organization_id: string | null;
  full_name: string | null;
  /** Roles: super_admin | admin | operator */
  role: string | null;
  /** Preferred FK → `public.roles.id` (migration 20260621100000). */
  role_id: string | null;
  /** Public URL of profile photo in the `profiles` storage bucket. */
  photo_url: string | null;
  created_at: string;
  updated_at: string | null;
};

// ---------------------------------------------------------------------------
// roles (access foundation)
// ---------------------------------------------------------------------------

export type RoleScope = "system" | "tenant";

/**
 * `public.roles` — scoped role catalog (system vs tenant).
 */
export type RolesRow = {
  id: string;
  key: string;
  name: string;
  description: string | null;
  scope: RoleScope;
  is_system: boolean;
  is_assignable: boolean;
  created_at: string;
};

// ---------------------------------------------------------------------------
// claim_submissions
// Protected: created_by column and status values (including 'failed').
// ---------------------------------------------------------------------------

/**
 * `public.claim_submissions` row shape.
 * PROTECTED: Do not remove `created_by` or alter the `status` value set.
 * Status values: pending | ready_to_send | submitted | failed | investigating | closed.
 */
export type ClaimSubmissionsRow = {
  id: string;
  /** Tenant FK — uses `organization_id` (rename migration 20260409 NOT applied to live DB). */
  organization_id: string | null;
  store_id: string | null;
  return_id: string | null;
  status: string | null;
  submission_id: string | null;
  claim_amount: number | null;
  currency: string | null;
  reimbursement_amount: number | null;
  source_payload: Json | null;
  /** PROTECTED — set by the Python AI agent on submission. */
  created_by: string | null;
  created_at: string | null;
  // --- Added by migration 20260413 ---
  updated_at: string | null;
  /** URL to the filed claim report / confirmation PDF (set by Python agent). */
  report_url: string | null;
};

// ---------------------------------------------------------------------------
// platform_settings (singleton: id = true)
// ---------------------------------------------------------------------------

export type PlatformSettingsRow = {
  id: boolean;
  app_name: string;
  logo_url: string | null;
  created_at: string;
  updated_at: string;
};

// ---------------------------------------------------------------------------
// organization_settings
// ---------------------------------------------------------------------------

/**
 * `public.organization_settings` row shape.
 */
export type OrganizationSettingsRow = {
  id: string;
  /** Tenant FK — `organization_id` (rename to company_id NOT applied to live DB). */
  organization_id: string | null;
  is_ai_label_ocr_enabled: boolean | null;
  default_claim_evidence: Json | null;
  logo_url: string | null;
  credentials: Json | null;
  updated_at: string | null;
  // --- Added by migration 20260413 ---
  is_ai_packing_slip_ocr_enabled: boolean | null;
  /** Human-readable tenant label shown in admin workspace picker. */
  company_display_name: string | null;
  /** Pre-selected store FK for new returns/packages in this org. */
  default_store_id: string | null;
  /** Enables verbose debug logging/UI for this tenant. */
  is_debug_mode_enabled: boolean | null;
};

// ---------------------------------------------------------------------------
// Database type map
// ---------------------------------------------------------------------------

/** Minimal `Database` shape for Returns Processing — extend as needed. */
export type Database = {
  public: {
    Tables: {
      profiles: {
        Row: ProfileRow;
        Insert: Record<string, unknown>;
        Update: Record<string, unknown>;
        /**
         * FK: profiles.organization_id → public.organizations(id)
         * (migration 20260622100000_profiles_organization_id_fk_organizations.sql;
         * supersedes 20260414 target of organization_settings for PostgREST embeds.)
         */
        Relationships: [
          {
            foreignKeyName: "profiles_organization_id_fkey";
            columns: ["organization_id"];
            referencedRelation: "organizations";
            referencedColumns: ["id"];
          },
        ];
      };
      returns: {
        Row: ReturnsRow;
        Insert: Record<string, unknown>;
        Update: Record<string, unknown>;
        Relationships: [];
      };
      packages: {
        Row: PackagesRow;
        Insert: Record<string, unknown>;
        Update: Record<string, unknown>;
        Relationships: [];
      };
      pallets: {
        Row: PalletsRow;
        Insert: Record<string, unknown>;
        Update: Record<string, unknown>;
        Relationships: [];
      };
      claim_submissions: {
        Row: ClaimSubmissionsRow;
        Insert: Record<string, unknown>;
        Update: Record<string, unknown>;
        Relationships: [];
      };
      organization_settings: {
        Row: OrganizationSettingsRow;
        Insert: Record<string, unknown>;
        Update: Record<string, unknown>;
        Relationships: [];
      };
      platform_settings: {
        Row: PlatformSettingsRow;
        Insert: Record<string, unknown>;
        Update: Record<string, unknown>;
        Relationships: [];
      };
    };
    Views: Record<string, never>;
    Functions: Record<string, never>;
    Enums: Record<string, never>;
    CompositeTypes: Record<string, never>;
  };
};

export type Tables<T extends keyof Database["public"]["Tables"]> =
  Database["public"]["Tables"][T]["Row"];
