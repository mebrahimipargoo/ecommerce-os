"use server";

/**
 * Next.js requires every export from a `"use server"` module to be an async function.
 * Re-exporting with `export { x } from "…"` is not valid here — wrap each action explicitly.
 */
import * as impl from "../../(admin)/imports/import-actions";

export async function updateUploadAfterChunk(
  ...args: Parameters<typeof impl.updateUploadAfterChunk>
): ReturnType<typeof impl.updateUploadAfterChunk> {
  return impl.updateUploadAfterChunk(...args);
}

export async function rawUploadExistsWithMd5Hash(
  ...args: Parameters<typeof impl.rawUploadExistsWithMd5Hash>
): ReturnType<typeof impl.rawUploadExistsWithMd5Hash> {
  return impl.rawUploadExistsWithMd5Hash(...args);
}

export async function listRawReportUploads(
  ...args: Parameters<typeof impl.listRawReportUploads>
): ReturnType<typeof impl.listRawReportUploads> {
  return impl.listRawReportUploads(...args);
}

export async function createRawReportUploadSession(
  ...args: Parameters<typeof impl.createRawReportUploadSession>
): ReturnType<typeof impl.createRawReportUploadSession> {
  return impl.createRawReportUploadSession(...args);
}

export async function createAmazonLedgerUploadSession(
  ...args: Parameters<typeof impl.createAmazonLedgerUploadSession>
): ReturnType<typeof impl.createAmazonLedgerUploadSession> {
  return impl.createAmazonLedgerUploadSession(...args);
}

export async function patchAmazonLedgerUploadSession(
  ...args: Parameters<typeof impl.patchAmazonLedgerUploadSession>
): ReturnType<typeof impl.patchAmazonLedgerUploadSession> {
  return impl.patchAmazonLedgerUploadSession(...args);
}

export async function getAmazonLedgerUploadProgress(
  ...args: Parameters<typeof impl.getAmazonLedgerUploadProgress>
): ReturnType<typeof impl.getAmazonLedgerUploadProgress> {
  return impl.getAmazonLedgerUploadProgress(...args);
}

export async function finalizeRawReportUpload(
  ...args: Parameters<typeof impl.finalizeRawReportUpload>
): ReturnType<typeof impl.finalizeRawReportUpload> {
  return impl.finalizeRawReportUpload(...args);
}

export async function failRawReportUpload(
  ...args: Parameters<typeof impl.failRawReportUpload>
): ReturnType<typeof impl.failRawReportUpload> {
  return impl.failRawReportUpload(...args);
}

export async function updateRawReportType(
  ...args: Parameters<typeof impl.updateRawReportType>
): ReturnType<typeof impl.updateRawReportType> {
  return impl.updateRawReportType(...args);
}

export async function recordColumnMappingDecision(
  ...args: Parameters<typeof impl.recordColumnMappingDecision>
): ReturnType<typeof impl.recordColumnMappingDecision> {
  return impl.recordColumnMappingDecision(...args);
}

export async function deleteRawReportUpload(
  ...args: Parameters<typeof impl.deleteRawReportUpload>
): ReturnType<typeof impl.deleteRawReportUpload> {
  return impl.deleteRawReportUpload(...args);
}
