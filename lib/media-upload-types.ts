/** Shared types for media uploads — no `"use server"` (Next.js allows only async functions in server action modules). */

export const STORAGE_BUCKETS = ["media", "incident-photos"] as const;
export type StorageBucketName = (typeof STORAGE_BUCKETS)[number];

/** Storage prefixes under the `media` or `incident-photos` bucket (service-role upload bypasses Storage RLS). */
export type MediaUploadFolder =
  | "packages"
  | "packages/claim_closed"
  | "packages/claim_opened"
  | "packages/claim_return_label"
  | "packages/manifest"
  | "pallets"
  | "pallets/manifest"
  | "pallets/bol"
  | "evidence/wizard"
  | "incident";
