/** Shared types for media uploads — no `"use server"` (Next.js allows only async functions in server action modules). */

/** Buckets allowed by {@link uploadMediaFileAction} — evidence uses `media`, packing-slip/manifest scans use `manifests`. */
export const STORAGE_BUCKETS = ["media", "manifests"] as const;
export type StorageBucketName = (typeof STORAGE_BUCKETS)[number];

/** Storage path prefixes under the `media` / `manifests` buckets (service-role upload bypasses Storage RLS). */
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
