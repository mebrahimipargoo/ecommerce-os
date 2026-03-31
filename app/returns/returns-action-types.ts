/**
 * Return / package / pallet row types — kept out of `actions.ts` because `"use server"`
 * modules may only export async functions (Next.js).
 */
import type { ReturnPhotoEvidenceRow } from "../../lib/return-photo-evidence";

export interface OrgSettings {
  is_ai_label_ocr_enabled: boolean;
  is_ai_packing_slip_ocr_enabled: boolean;
}

export type PalletStatus = "open" | "closed" | "submitted";

export type PalletRecord = {
  id: string; organization_id: string;
  pallet_number: string;
  tracking_number?: string | null;
  /** Canonical image storage — `{ urls: string[] }`. */
  photo_evidence?: unknown | null;
  status: PalletStatus; notes: string | null; item_count: number;
  created_by?: string | null;
  updated_by?: string | null;
  created_at: string; updated_at: string;
  store_id?: string | null;
  stores?: { name: string; platform: string } | null;
  child_packages_count?: number;
  child_returns_count?: number;
};

export type PalletInsertPayload = {
  pallet_number: string;
  photo_evidence?: Record<string, unknown> | null;
  store_id?: string;
  notes?: string; organization_id?: string; created_by?: string;
};

export type PalletUpdatePayload = Partial<Pick<
  PalletRecord,
  | "status" | "notes" | "tracking_number" | "photo_evidence"
>>;

export type PackageStatus = "open" | "closed" | "suspicious" | "submitted";

export type ExpectedItem = { sku: string; expected_qty: number; description?: string };

export type PackageRecord = {
  id: string; organization_id: string;
  package_number: string; tracking_number: string | null;
  carrier_name: string | null;
  rma_number?: string | null;
  expected_item_count: number; actual_item_count: number;
  pallet_id: string | null; status: PackageStatus;
  discrepancy_note: string | null;
  expected_items?: ExpectedItem[] | null;
  manifest_url?: string | null;
  manifest_data?: ExpectedItem[] | null;
  store_id?: string | null;
  stores?: { name: string; platform: string } | null;
  created_by?: string | null;
  updated_by?: string | null;
  created_at: string; updated_at: string;
  order_id?: string | null;
  /** Canonical image storage — `{ urls: string[] }` (opened → label → optional closed ordering for claims). */
  photo_evidence?: unknown | null;
};

export type PackageInsertPayload = {
  package_number: string; tracking_number?: string;
  carrier_name?: string; rma_number?: string; expected_item_count?: number;
  pallet_id?: string; store_id?: string; organization_id?: string; created_by?: string;
  manifest_url?: string;
  manifest_data?: ExpectedItem[] | null;
  order_id?: string | null;
  photo_evidence?: Record<string, unknown> | null;
};

export type PackageUpdatePayload = Partial<Pick<
  PackageRecord,
  | "carrier_name" | "tracking_number" | "rma_number" | "expected_item_count" | "status" | "discrepancy_note" | "pallet_id" | "manifest_url" | "manifest_data"
  | "order_id"
  | "expected_items"
  | "photo_evidence"
>>;

export type ReturnInsertPayload = {
  lpn?: string;
  marketplace: string; item_name: string;
  asin?: string;
  fnsku?: string;
  sku?: string;
  conditions: string[];
  notes?: string;
  photo_evidence?: Record<string, string | number | string[] | null> | null;
  expiration_date?: string; batch_number?: string;
  pallet_id?: string; package_id?: string;
  store_id?: string;
  amazon_order_id?: string | null;
  order_id?: string | null;
  customer_id?: string | null;
  claim_evidence_selected_urls?: string[] | null;
  organization_id?: string; created_by?: string;
};

export type ReturnRecord = {
  id: string; organization_id: string;
  lpn: string | null;
  inherited_tracking_number?: string | null;
  inherited_carrier?: string | null;
  marketplace: string; item_name: string;
  asin?: string | null;
  fnsku?: string | null;
  sku?: string | null;
  product_identifier?: string | null;
  conditions: string[]; status: string;
  notes: string | null;
  photo_evidence: ReturnPhotoEvidenceRow;
  expiration_date: string | null; batch_number: string | null;
  store_id?: string | null;
  stores?: { name: string; platform: string } | null;
  pallet_id: string | null; package_id: string | null;
  order_id?: string | null;
  customer_id?: string | null;
  created_by?: string | null;
  updated_by?: string | null;
  created_at: string; updated_at: string;
  estimated_value?: number | null;
};

export type ReturnUpdatePayload = Partial<Pick<
  ReturnRecord,
  | "lpn" | "item_name" | "notes" | "status"
  | "conditions" | "expiration_date" | "batch_number"
  | "package_id" | "pallet_id"
  | "photo_evidence"
  | "asin" | "fnsku" | "sku" | "product_identifier" | "store_id" | "marketplace"
  | "order_id"
>>;

export type AuditLogRecord = {
  id: string; organization_id: string;
  return_id: string | null; pallet_id: string | null;
  action: string; field: string | null;
  old_value: string | null; new_value: string | null;
  actor: string; created_at: string;
};

export type DashboardSnapshot = {
  returnsToday: number;
  palletCount: number;
  packageCount: number;
  claimsReadyToSend: number;
  returnsEstimatedValueUsd: number;
};

export type ReturnsAnalyticsPayload = {
  totalReturns: number;
  totalPallets: number;
  avgProcessingHours: number;
  conditionSlices: { name: string; value: number }[];
  carrierBars: { name: string; count: number }[];
  operatorStats: { operator: string; count: number }[];
};
